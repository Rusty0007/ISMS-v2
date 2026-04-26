"""
System Administration Routes
All endpoints require the `system_admin` role.
"""

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import func, text
from sqlalchemy.orm import Session

from app.database import get_db
from app.middleware.auth import require_role
from app.models.models import (
    Court, Match, MatchHistory, Profile, SecurityAuditLog, Tournament,
    TournamentRegistration, UserRole, UserRoleModel,
)
from app.services.broadcast import broadcast_match as _broadcast
from app.services.rating_rebuilder import rebuild_all_ratings_from_history

logger = logging.getLogger(__name__)

router = APIRouter()

# Convenience dependency — all admin routes use this
_admin = Depends(require_role("system_admin"))


# ── Schemas ───────────────────────────────────────────────────────────────────

class RoleUpdateRequest(BaseModel):
    add: list[str] = []
    remove: list[str] = []


# ── Stats ─────────────────────────────────────────────────────────────────────

@router.get("/stats")
def get_stats(
    _: dict = _admin,
    db: Session = Depends(get_db),
):
    total_users       = db.query(func.count(Profile.id)).scalar() or 0
    total_tournaments = db.query(func.count(Tournament.id)).scalar() or 0
    total_matches     = db.query(func.count(Match.id)).scalar() or 0
    active_tournaments = (
        db.query(func.count(Tournament.id))
        .filter(Tournament.status.in_(["upcoming", "ongoing"]))
        .scalar() or 0
    )
    completed_matches = (
        db.query(func.count(Match.id))
        .filter(Match.status == "completed")
        .scalar() or 0
    )

    # Role breakdown
    role_counts_raw = (
        db.query(UserRoleModel.role, func.count(UserRoleModel.id))
        .group_by(UserRoleModel.role)
        .all()
    )
    role_breakdown = {
        (r.value if hasattr(r, "value") else str(r)): cnt
        for r, cnt in role_counts_raw
    }

    return {
        "total_users":        total_users,
        "total_tournaments":  total_tournaments,
        "total_matches":      total_matches,
        "active_tournaments": active_tournaments,
        "completed_matches":  completed_matches,
        "role_breakdown":     role_breakdown,
    }


# ── Users ─────────────────────────────────────────────────────────────────────

@router.get("/users")
def list_users(
    q: str = Query("", description="Search by username or email"),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    _: dict = _admin,
    db: Session = Depends(get_db),
):
    query = db.query(Profile)
    if q:
        like = f"%{q}%"
        query = query.filter(
            Profile.first_name.ilike(like) | Profile.last_name.ilike(like) | Profile.email.ilike(like)
        )

    total = query.count()
    users = (
        query
        .order_by(Profile.created_at.desc())
        .offset((page - 1) * limit)
        .limit(limit)
        .all()
    )

    result = []
    for u in users:
        roles = [
            r.role.value if hasattr(r.role, "value") else str(r.role)
            for r in u.roles
        ]
        result.append({
            "id":    str(u.id),
            "email": u.email,
            "first_name": u.first_name,
            "last_name":  u.last_name,
            "avatar_url": u.avatar_url,
            "roles":      roles,
            "profile_setup_complete": bool(u.profile_setup_complete),
            "created_at": u.created_at.isoformat() if u.created_at else None,
        })

    return {"users": result, "total": total, "page": page, "limit": limit}


@router.patch("/users/{user_id}/roles")
def update_user_roles(
    user_id: str,
    body: RoleUpdateRequest,
    current_admin: dict = _admin,
    db: Session = Depends(get_db),
):
    profile = db.query(Profile).filter(Profile.id == user_id).first()
    if not profile:
        raise HTTPException(status_code=404, detail="User not found.")

    # Prevent removing own system_admin role
    if "system_admin" in body.remove and str(profile.id) == current_admin["id"]:
        raise HTTPException(
            status_code=400,
            detail="You cannot remove your own system_admin role."
        )

    valid_roles = {r.value for r in UserRole}

    for role_str in body.add:
        if role_str not in valid_roles:
            raise HTTPException(status_code=400, detail=f"Invalid role: {role_str}")
        exists = (
            db.query(UserRoleModel)
            .filter(UserRoleModel.user_id == user_id, UserRoleModel.role == role_str)
            .first()
        )
        if not exists:
            db.add(UserRoleModel(user_id=user_id, role=role_str))

    for role_str in body.remove:
        if role_str not in valid_roles:
            raise HTTPException(status_code=400, detail=f"Invalid role: {role_str}")
        db.query(UserRoleModel).filter(
            UserRoleModel.user_id == user_id,
            UserRoleModel.role == role_str,
        ).delete()

    db.commit()

    updated_roles = [
        r.role.value if hasattr(r.role, "value") else str(r.role)
        for r in db.query(UserRoleModel).filter(UserRoleModel.user_id == user_id).all()
    ]
    return {"user_id": user_id, "roles": updated_roles}


# ── Tournaments ───────────────────────────────────────────────────────────────

@router.get("/tournaments")
def list_all_tournaments(
    q: str = Query(""),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    _: dict = _admin,
    db: Session = Depends(get_db),
):
    query = db.query(Tournament)
    if q:
        query = query.filter(Tournament.name.ilike(f"%{q}%"))

    total = query.count()
    tournaments = (
        query
        .order_by(Tournament.created_at.desc())
        .offset((page - 1) * limit)
        .limit(limit)
        .all()
    )

    result = []
    for t in tournaments:
        organizer = db.query(Profile).filter(Profile.id == t.organizer_id).first()
        participant_count = (
            db.query(func.count(TournamentRegistration.id))
            .filter(
                TournamentRegistration.tournament_id == t.id,
                TournamentRegistration.status == "confirmed",
            )
            .scalar() or 0
        )
        result.append({
            "id":          str(t.id),
            "name":        t.name,
            "sport":       t.sport.value if hasattr(t.sport, "value") else str(t.sport),
            "status":      t.status,
            "format":      t.format,
            "organizer":   f"{organizer.first_name or ''} {organizer.last_name or ''}".strip() if organizer else None,
            "organizer_id": str(t.organizer_id),
            "participants": participant_count,
            "max_players": t.max_participants,
            "created_at":  t.created_at.isoformat() if t.created_at else None,
        })

    return {"tournaments": result, "total": total, "page": page, "limit": limit}


@router.delete("/tournaments/{tournament_id}")
def admin_delete_tournament(
    tournament_id: str,
    _: dict = _admin,
    db: Session = Depends(get_db),
):
    tournament = db.query(Tournament).filter(Tournament.id == tournament_id).first()
    if not tournament:
        raise HTTPException(status_code=404, detail="Tournament not found.")

    db.query(TournamentRegistration).filter(
        TournamentRegistration.tournament_id == tournament_id
    ).delete()
    db.delete(tournament)
    db.commit()
    return {"message": "Tournament deleted."}


# ── Audit Logs ────────────────────────────────────────────────────────────────

@router.get("/audit-logs")
def list_audit_logs(
    user_id: str | None = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=200),
    _: dict = _admin,
    db: Session = Depends(get_db),
):
    query = db.query(SecurityAuditLog)
    if user_id:
        query = query.filter(SecurityAuditLog.user_id == user_id)

    total = query.count()
    logs = (
        query
        .order_by(SecurityAuditLog.created_at.desc())
        .offset((page - 1) * limit)
        .limit(limit)
        .all()
    )

    user_ids = list({str(l.user_id) for l in logs if l.user_id})
    profiles = (
        db.query(Profile.id, Profile.first_name, Profile.last_name)
        .filter(Profile.id.in_(user_ids))
        .all()
    ) if user_ids else []
    name_map = {str(p.id): f"{p.first_name or ''} {p.last_name or ''}".strip() for p in profiles}

    return {
        "logs": [
            {
                "id":         str(l.id),
                "user_id":    str(l.user_id),
                "name":       name_map.get(str(l.user_id), "unknown"),
                "event_type": l.event_type,
                "ip_address": l.ip_address,
                "details":    l.details,
                "created_at": l.created_at.isoformat() if l.created_at else None,
            }
            for l in logs
        ],
        "total": total,
        "page":  page,
        "limit": limit,
    }


# ── Stuck Match Recovery ──────────────────────────────────────────────────────

class ForceCompleteRequest(BaseModel):
    winner_id: str
    reason: str = "admin_force_complete"


@router.post("/matches/{match_id}/force-complete")
def admin_force_complete_match(
    match_id: str,
    body: ForceCompleteRequest,
    current_admin: dict = _admin,
    db: Session = Depends(get_db),
):
    """
    Force a stuck 'ongoing' match to 'completed' without running Glicko-2
    or the stored procedure. Use when fn_complete_match() has failed and the
    match is permanently stuck. Ratings are NOT updated — this is a recovery tool.
    """
    match = db.query(Match).filter(Match.id == match_id).first()
    if not match:
        raise HTTPException(status_code=404, detail="Match not found.")

    status_val = match.status.value if hasattr(match.status, "value") else str(match.status)
    if status_val == "completed":
        raise HTTPException(status_code=400, detail="Match is already completed.")

    setattr(match, "status", "completed")
    setattr(match, "winner_id", body.winner_id)
    setattr(match, "completed_at", datetime.now(timezone.utc))

    # Release the court if one was held
    if match.court_id is not None:
        court = db.query(Court).filter(Court.id == match.court_id).first()
        if court:
            setattr(court, "status", "available")

    db.add(MatchHistory(
        match_id=match_id,
        event_type="admin_force_complete",
        recorded_by=current_admin["id"],
        description=f"Admin force-completed match. Reason: {body.reason}. Ratings NOT updated.",
        meta={"winner_id": body.winner_id, "admin_id": current_admin["id"], "reason": body.reason},
    ))
    db.commit()

    _broadcast(match_id, {"type": "match_completed", "winner_id": body.winner_id, "forced": True})
    logger.warning(
        f"[admin] Match {match_id} force-completed by admin {current_admin['id']}. "
        f"Winner: {body.winner_id}. Reason: {body.reason}"
    )
    return {"message": "Match force-completed. Ratings were NOT updated.", "match_id": match_id}


@router.post("/ratings/rebuild")
def admin_rebuild_ratings(
    current_admin: dict = _admin,
    db: Session = Depends(get_db),
):
    """
    Rebuild all player ratings and win/loss records from completed match history.
    Use this after rating formula fixes or data repair.
    """
    try:
        summary = rebuild_all_ratings_from_history(db)
        db.commit()
    except Exception as exc:
        db.rollback()
        logger.exception("[admin] Rating rebuild failed")
        raise HTTPException(status_code=500, detail=f"Rating rebuild failed: {exc}")

    logger.warning(
        f"[admin] Ratings rebuilt from history by admin {current_admin['id']}: {summary.to_dict()}"
    )
    return {"message": "Ratings rebuilt from completed match history.", "summary": summary.to_dict()}
