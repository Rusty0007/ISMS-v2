"""
System Administration Routes
All endpoints require the `system_admin` role.
"""

import csv
import io
import logging
import os
import subprocess
from datetime import datetime, timezone, timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy import func, text
from sqlalchemy.orm import Session

from app.config import settings
from app.database import get_db
from app.middleware.auth import require_role
from app.models.models import (
    Court, Match, MatchHistory, MatchmakingQueue, PlayerRating, Profile,
    SecurityAuditLog, Tournament, TournamentRegistration, UserRole, UserRoleModel,
)
from app.services.broadcast import broadcast_match as _broadcast
from app.services.matchmaking import get_model_info
from app.services.rating_rebuilder import rebuild_all_ratings_from_history

logger = logging.getLogger(__name__)

router = APIRouter()

# Convenience dependency — all admin routes use this
_admin = Depends(require_role("system_admin"))


# ── Schemas ───────────────────────────────────────────────────────────────────

class RoleUpdateRequest(BaseModel):
    add: list[str] = []
    remove: list[str] = []


def _scalar_count(db: Session, sql: str, params: dict) -> int:
    return int(db.execute(text(sql), params).scalar() or 0)


def _float_or(value: Any, default: float) -> float:
    return float(value) if value is not None else default


def _execute_user_cleanup(db: Session, target_user_id: str, fallback_admin_id: str) -> dict[str, int]:
    """
    Remove or detach rows that point at a profile before the profile is deleted.

    Historical match/feed records are preserved where possible by nulling profile
    references. Ownership fields that are non-nullable are reassigned to the
    deleting system admin so the records remain manageable.
    """
    params = {"user_id": target_user_id, "admin_id": fallback_admin_id}
    counts: dict[str, int] = {}

    statements: list[tuple[str, str]] = [
        # Reassign non-null ownership/creator fields.
        ("clubs_reassigned", "UPDATE clubs SET admin_id = :admin_id WHERE admin_id = :user_id"),
        ("open_play_sessions_reassigned", "UPDATE open_play_sessions SET created_by = :admin_id WHERE created_by = :user_id"),
        ("open_play_queue_creators_reassigned", "UPDATE open_play_queue_entries SET created_by = :admin_id WHERE created_by = :user_id"),
        ("court_bookings_reassigned", "UPDATE court_bookings SET requested_by = :admin_id WHERE requested_by = :user_id"),
        ("tournaments_reassigned", "UPDATE tournaments SET organizer_id = :admin_id WHERE organizer_id = :user_id"),
        ("tournament_referee_registered_by_reassigned", "UPDATE tournament_referee_registrations SET registered_by = :admin_id WHERE registered_by = :user_id"),
        ("referee_open_requests_reassigned", "UPDATE referee_open_requests SET posted_by = :admin_id WHERE posted_by = :user_id"),
        ("court_rotations_reassigned", "UPDATE court_rotations SET created_by = :admin_id WHERE created_by = :user_id"),

        # Remove active participation/invitation rows tied to the user.
        ("club_invites_deleted", "DELETE FROM club_invites WHERE invited_by = :user_id OR invited_user = :user_id"),
        ("open_play_participants_deleted", "DELETE FROM open_play_participants WHERE user_id = :user_id"),
        ("open_play_queue_entries_deleted", "DELETE FROM open_play_queue_entries WHERE player1_id = :user_id OR player2_id = :user_id"),
        ("open_play_assignment_players_deleted", "DELETE FROM open_play_assignment_players WHERE user_id = :user_id"),
        ("open_play_court_managers_deleted", "DELETE FROM open_play_court_managers WHERE user_id = :user_id OR assigned_by = :user_id"),
        ("leader_party_members_deleted", "DELETE FROM party_members WHERE party_id IN (SELECT id FROM parties WHERE leader_id = :user_id)"),
        ("leader_party_invitations_deleted", "DELETE FROM party_invitations WHERE party_id IN (SELECT id FROM parties WHERE leader_id = :user_id)"),
        ("parties_deleted", "DELETE FROM parties WHERE leader_id = :user_id"),
        ("party_invitations_deleted", "DELETE FROM party_invitations WHERE inviter_id = :user_id OR invitee_id = :user_id"),
        ("club_checkins_deleted", "DELETE FROM club_checkins WHERE user_id = :user_id"),
        ("tournament_group_members_deleted", "DELETE FROM tournament_group_members WHERE entry_id IN (SELECT id FROM tournament_registrations WHERE player_id = :user_id)"),
        ("tournament_group_standings_deleted", "DELETE FROM tournament_group_standings WHERE entry_id IN (SELECT id FROM tournament_registrations WHERE player_id = :user_id)"),
        ("tournament_registrations_deleted", "DELETE FROM tournament_registrations WHERE player_id = :user_id"),
        ("tournament_referee_registrations_deleted", "DELETE FROM tournament_referee_registrations WHERE user_id = :user_id"),
        ("referee_invites_deleted", "DELETE FROM referee_invites WHERE invited_by = :user_id OR invited_user = :user_id"),
        ("friendships_deleted", "DELETE FROM friendships WHERE requester_id = :user_id OR addressee_id = :user_id"),
        ("post_reactions_on_user_posts_deleted", "DELETE FROM post_reactions WHERE post_id IN (SELECT id FROM feed_posts WHERE author_id = :user_id)"),
        ("post_comments_on_user_posts_deleted", "DELETE FROM post_comments WHERE post_id IN (SELECT id FROM feed_posts WHERE author_id = :user_id)"),
        ("feed_notifications_on_user_posts_deleted", "DELETE FROM feed_notifications WHERE post_id IN (SELECT id FROM feed_posts WHERE author_id = :user_id)"),
        ("feed_posts_deleted", "DELETE FROM feed_posts WHERE author_id = :user_id"),
        ("post_comments_deleted", "DELETE FROM post_comments WHERE author_id = :user_id"),
        ("post_reactions_deleted", "DELETE FROM post_reactions WHERE user_id = :user_id"),
        ("feed_notifications_deleted", "DELETE FROM feed_notifications WHERE user_id = :user_id"),
        ("notifications_deleted", "DELETE FROM notifications WHERE user_id = :user_id"),
        ("matchmaking_queue_deleted", "DELETE FROM matchmaking_queue WHERE user_id = :user_id"),

        # Null optional references on preserved records.
        ("open_play_queue_partner_cleared", "UPDATE open_play_queue_entries SET player2_id = NULL WHERE player2_id = :user_id"),
        ("courts_creator_cleared", "UPDATE courts SET created_by = NULL WHERE created_by = :user_id"),
        ("tournament_partners_cleared", "UPDATE tournament_registrations SET partner_id = NULL WHERE partner_id = :user_id"),
        ("referee_open_filled_cleared", "UPDATE referee_open_requests SET filled_by = NULL WHERE filled_by = :user_id"),
        ("rally_events_cleared", "UPDATE rally_events SET scored_by = CASE WHEN scored_by = :user_id THEN NULL ELSE scored_by END, tagged_player = CASE WHEN tagged_player = :user_id THEN NULL ELSE tagged_player END WHERE scored_by = :user_id OR tagged_player = :user_id"),
        ("leaderboard_cache_deleted", "DELETE FROM leaderboard_cache WHERE user_id = :user_id"),
        ("match_history_cleared", "UPDATE match_history SET player_id = CASE WHEN player_id = :user_id THEN NULL ELSE player_id END, recorded_by = CASE WHEN recorded_by = :user_id THEN NULL ELSE recorded_by END WHERE player_id = :user_id OR recorded_by = :user_id"),
        ("rotation_members_cleared", "UPDATE court_rotation_members SET user_id = NULL WHERE user_id = :user_id"),

        # Invalidate unfinished matches involving the user, then detach the profile.
        ("matches_invalidated", """
            UPDATE matches
            SET status = 'invalidated'
            WHERE status NOT IN ('completed', 'cancelled', 'invalidated')
              AND (
                player1_id = :user_id OR player2_id = :user_id OR player3_id = :user_id OR player4_id = :user_id
                OR team1_player1 = :user_id OR team1_player2 = :user_id OR team2_player1 = :user_id OR team2_player2 = :user_id
                OR referee_id = :user_id
              )
        """),
        ("matches_cleared", """
            UPDATE matches SET
                player1_id = CASE WHEN player1_id = :user_id THEN NULL ELSE player1_id END,
                player2_id = CASE WHEN player2_id = :user_id THEN NULL ELSE player2_id END,
                player3_id = CASE WHEN player3_id = :user_id THEN NULL ELSE player3_id END,
                player4_id = CASE WHEN player4_id = :user_id THEN NULL ELSE player4_id END,
                team1_player1 = CASE WHEN team1_player1 = :user_id THEN NULL ELSE team1_player1 END,
                team1_player2 = CASE WHEN team1_player2 = :user_id THEN NULL ELSE team1_player2 END,
                team2_player1 = CASE WHEN team2_player1 = :user_id THEN NULL ELSE team2_player1 END,
                team2_player2 = CASE WHEN team2_player2 = :user_id THEN NULL ELSE team2_player2 END,
                referee_id = CASE WHEN referee_id = :user_id THEN NULL ELSE referee_id END,
                winner_id = CASE WHEN winner_id = :user_id THEN NULL ELSE winner_id END,
                result_submitted_by = CASE WHEN result_submitted_by = :user_id THEN NULL ELSE result_submitted_by END,
                result_confirmed_by = CASE WHEN result_confirmed_by = :user_id THEN NULL ELSE result_confirmed_by END
            WHERE player1_id = :user_id OR player2_id = :user_id OR player3_id = :user_id OR player4_id = :user_id
               OR team1_player1 = :user_id OR team1_player2 = :user_id OR team2_player1 = :user_id OR team2_player2 = :user_id
               OR referee_id = :user_id OR winner_id = :user_id
               OR result_submitted_by = :user_id OR result_confirmed_by = :user_id
        """),
    ]

    for key, sql in statements:
        result = db.execute(text(sql), params)
        counts[key] = int(result.rowcount or 0)  # type: ignore[attr-defined]

    return counts


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
    region_code: str | None = Query(None),
    province_code: str | None = Query(None),
    city_mun_code: str | None = Query(None),
    barangay_code: str | None = Query(None),
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
    if region_code:
        query = query.filter(Profile.region_code == region_code)
    if province_code:
        query = query.filter(Profile.province_code == province_code)
    if city_mun_code:
        query = query.filter(Profile.city_mun_code == city_mun_code)
    if barangay_code:
        query = query.filter(Profile.barangay_code == barangay_code)

    total = query.count()
    users = (
        query
        .order_by(Profile.created_at.desc())
        .offset((page - 1) * limit)
        .limit(limit)
        .all()
    )

    # Aggregate player rating stats across all sport/format combos per user
    user_ids = [str(u.id) for u in users]
    rating_aggs: dict = {}
    if user_ids:
        agg_rows = (
            db.query(
                PlayerRating.user_id,
                func.sum(PlayerRating.matches_played).label("total_matches"),
                func.max(PlayerRating.rating).label("best_rating"),
                func.max(PlayerRating.distinct_opponents_count).label("max_opponents"),
                func.max(PlayerRating.activeness_score).label("best_activeness"),
                func.max(PlayerRating.rating_status).label("rating_status"),
            )
            .filter(PlayerRating.user_id.in_(user_ids))
            .group_by(PlayerRating.user_id)
            .all()
        )
        for row in agg_rows:
            rating_aggs[str(row.user_id)] = {
                "total_matches":    int(row.total_matches or 0),
                "best_rating":      round(float(row.best_rating or 1500), 1),
                "distinct_opponents": int(row.max_opponents or 0),
                "activeness_score": round(float(row.best_activeness or 0), 3),
                "rating_status":    str(row.rating_status or "CALIBRATING"),
            }

    result = []
    for u in users:
        roles = [
            r.role.value if hasattr(r.role, "value") else str(r.role)
            for r in u.roles
        ]
        stats = rating_aggs.get(str(u.id), {
            "total_matches": 0, "best_rating": None,
            "distinct_opponents": 0, "activeness_score": 0.0,
            "rating_status": "CALIBRATING",
        })
        result.append({
            "id":    str(u.id),
            "email": u.email,
            "first_name": u.first_name,
            "last_name":  u.last_name,
            "avatar_url": u.avatar_url,
            "roles":      roles,
            "region_code":   u.region_code,
            "province_code": u.province_code,
            "city_mun_code": u.city_mun_code,
            "barangay_code": u.barangay_code,
            "profile_setup_complete": bool(u.profile_setup_complete),
            "created_at": u.created_at.isoformat() if u.created_at is not None else None,
            "total_matches":      stats["total_matches"],
            "best_rating":        stats["best_rating"],
            "distinct_opponents": stats["distinct_opponents"],
            "activeness_score":   stats["activeness_score"],
            "rating_status":      stats["rating_status"],
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


@router.delete("/users/{user_id}")
def delete_user(
    user_id: str,
    current_admin: dict = _admin,
    db: Session = Depends(get_db),
):
    if str(user_id) == str(current_admin["id"]):
        raise HTTPException(status_code=400, detail="You cannot delete your own admin account.")

    profile = db.query(Profile).filter(Profile.id == user_id).first()
    if not profile:
        raise HTTPException(status_code=404, detail="User not found.")

    target_roles = {
        r.role.value if hasattr(r.role, "value") else str(r.role)
        for r in db.query(UserRoleModel).filter(UserRoleModel.user_id == user_id).all()
    }
    if "system_admin" in target_roles:
        admin_count = _scalar_count(
            db,
            """
            SELECT COUNT(DISTINCT user_id)
            FROM user_roles
            WHERE role = 'system_admin'
            """,
            {},
        )
        if admin_count <= 1:
            raise HTTPException(status_code=400, detail="You cannot delete the last system admin.")

    deleted_email = profile.email
    deleted_name = f"{profile.first_name or ''} {profile.last_name or ''}".strip()
    cleanup_counts = _execute_user_cleanup(db, user_id, str(current_admin["id"]))

    db.delete(profile)
    try:
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("[admin] Failed to delete user %s", user_id)
        raise HTTPException(status_code=500, detail="Failed to delete user because related records could not be cleaned up.")

    logger.warning(
        "[admin] User %s (%s) deleted by admin %s. Cleanup: %s",
        user_id,
        deleted_email,
        current_admin["id"],
        cleanup_counts,
    )
    return {
        "message": "User deleted.",
        "user_id": user_id,
        "email": deleted_email,
        "name": deleted_name or None,
        "cleanup": cleanup_counts,
    }


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
            "created_at":  t.created_at.isoformat() if t.created_at is not None else None,
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

    user_ids = list({str(l.user_id) for l in logs if l.user_id is not None})
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
                "created_at": l.created_at.isoformat() if l.created_at is not None else None,
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


# ── Queue Monitor ─────────────────────────────────────────────────────────────

@router.get("/queue/monitor")
def queue_monitor(
    _: dict = _admin,
    db: Session = Depends(get_db),
):
    """Live view of every player currently in the matchmaking queue.

    The queue lives in the matches table as status='pending' (singles waiting
    for opponent) or 'assembling' (doubles gathering players).
    MatchmakingQueue is a legacy model that is never populated.
    """
    queued_matches = (
        db.query(Match)
        .filter(
            Match.status.in_(["pending", "assembling"]),
            Match.tournament_id.is_(None),
        )
        .order_by(Match.created_at.asc())
        .all()
    )

    # Batch-fetch ALL player profiles involved (host + partners for doubles)
    player_ids: set[str] = set()
    for m in queued_matches:
        for pid in [m.player1_id, m.player2_id, m.player3_id, m.player4_id]:
            if pid is not None:
                player_ids.add(str(pid))

    profiles_map: dict[str, Profile] = {}
    if player_ids:
        for p in db.query(Profile).filter(Profile.id.in_(player_ids)).all():
            profiles_map[str(p.id)] = p

    # Batch-fetch best rating per player (max across formats)
    ratings_map: dict[str, PlayerRating] = {}
    if player_ids:
        for r in db.query(PlayerRating).filter(PlayerRating.user_id.in_(player_ids)).all():
            uid = str(r.user_id)
            rating_value = _float_or(r.rating, 0.0)
            current_best = ratings_map.get(uid)
            current_best_value = _float_or(current_best.rating, 0.0) if current_best is not None else 0.0
            if current_best is None or rating_value > current_best_value:
                ratings_map[uid] = r

    def _player_entry(uid_raw) -> dict | None:
        if uid_raw is None:
            return None
        uid = str(uid_raw)
        profile = profiles_map.get(uid)
        rating  = ratings_map.get(uid)
        name = (
            f"{profile.first_name or ''} {profile.last_name or ''}".strip() or profile.email
            if profile else "Unknown"
        )
        return {
            "user_id":     uid,
            "name":        name,
            "avatar_url":  profile.avatar_url if profile else None,
            "skill_rating": _float_or(rating.rating, 1500.0) if rating else 1500.0,
        }

    now = datetime.now(timezone.utc)
    groups: dict = {}

    for m in queued_matches:
        sport = m.sport.value        if hasattr(m.sport,        "value") else str(m.sport)
        fmt   = m.match_format.value if hasattr(m.match_format, "value") else str(m.match_format)
        mode  = m.match_type.value   if hasattr(m.match_type,   "value") else str(m.match_type)
        key   = f"{sport}:{fmt}"

        created = m.created_at
        if created is not None and created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        wait_seconds = int((now - created).total_seconds()) if created is not None else 0

        if key not in groups:
            groups[key] = {"sport": sport, "format": fmt, "count": 0, "entries": []}

        is_doubles = fmt in ("doubles", "mixed_doubles")
        status_val = m.status.value if hasattr(m.status, "value") else str(m.status)

        # Count how many players have joined assembling doubles
        players_joined = sum(
            1 for pid in [m.player1_id, m.player2_id, m.player3_id, m.player4_id]
            if pid is not None
        ) if is_doubles else 1

        entry = {
            "queue_id":      str(m.id),
            "match_mode":    mode,
            "status":        status_val,
            "wait_seconds":  wait_seconds,
            "joined_at":     m.created_at.isoformat() if m.created_at is not None else None,
            "is_doubles":    is_doubles,
            "players_joined": players_joined,
            "host":          _player_entry(m.player1_id),
            "partner":       _player_entry(m.player2_id) if is_doubles else None,
        }
        groups[key]["entries"].append(entry)
        groups[key]["count"] += 1

    return {
        "total_queued": len(queued_matches),
        "groups":       list(groups.values()),
        "as_of":        now.isoformat(),
    }



# ── Live Matches ──────────────────────────────────────────────────────────────

@router.get("/matches/live")
def live_matches(
    _: dict = _admin,
    db: Session = Depends(get_db),
):
    """All currently ongoing matches with player names and durations."""
    matches = (
        db.query(Match)
        .filter(Match.status == "ongoing")
        .order_by(Match.started_at.asc())
        .all()
    )

    # Batch-fetch all player profiles in one query
    player_ids = set()
    for m in matches:
        for pid in [m.player1_id, m.player2_id, m.player3_id, m.player4_id, m.referee_id]:
            if pid is not None:
                player_ids.add(str(pid))

    name_map: dict = {}
    if player_ids:
        profiles = (
            db.query(Profile.id, Profile.first_name, Profile.last_name)
            .filter(Profile.id.in_(player_ids))
            .all()
        )
        for p in profiles:
            name_map[str(p.id)] = f"{p.first_name or ''} {p.last_name or ''}".strip()

    now = datetime.now(timezone.utc)
    result = []
    for m in matches:
        started = m.started_at
        if started is not None and started.tzinfo is None:
            started = started.replace(tzinfo=timezone.utc)
        duration_seconds = int((now - started).total_seconds()) if started is not None else 0

        sport = m.sport.value        if hasattr(m.sport,        "value") else str(m.sport)
        fmt   = m.match_format.value if hasattr(m.match_format, "value") else str(m.match_format)

        result.append({
            "match_id":         str(m.id),
            "sport":            sport,
            "format":           fmt,
            "duration_seconds": duration_seconds,
            "started_at":       m.started_at.isoformat() if m.started_at is not None else None,
            "tournament_id":    str(m.tournament_id) if m.tournament_id is not None else None,
            "court_id":         str(m.court_id)      if m.court_id      is not None else None,
            "player1":          name_map.get(str(m.player1_id)) if m.player1_id is not None else None,
            "player2":          name_map.get(str(m.player2_id)) if m.player2_id is not None else None,
            "player3":          name_map.get(str(m.player3_id)) if m.player3_id is not None else None,
            "player4":          name_map.get(str(m.player4_id)) if m.player4_id is not None else None,
            "referee":          name_map.get(str(m.referee_id)) if m.referee_id is not None else None,
        })

    return {"matches": result, "total": len(result), "as_of": now.isoformat()}


# ── System Health ─────────────────────────────────────────────────────────────

@router.get("/system/health")
def system_health(
    _: dict = _admin,
    db: Session = Depends(get_db),
):
    """Real-time status of DB, Redis, ML model, and operational counters."""
    # Database
    db_ok = False
    try:
        db.execute(text("SELECT 1"))
        db_ok = True
    except Exception:
        pass

    # Redis — ping + memory info
    redis_ok         = False
    redis_mem_used   = None
    redis_mem_max    = None
    redis_connected  = 0
    try:
        import redis as _redis_lib
        r    = _redis_lib.from_url(settings.redis_url, decode_responses=True, socket_timeout=2)
        r.ping()
        info             = r.info("memory")
        redis_ok         = True
        redis_mem_used   = info.get("used_memory_human")  # type: ignore[union-attr]
        redis_mem_max    = info.get("maxmemory_human", "unlimited")  # type: ignore[union-attr]
        redis_connected  = r.info("clients").get("connected_clients", 0)  # type: ignore[union-attr]
    except Exception:
        pass

    # Operational counters
    try:
        queue_depth    = db.query(func.count(MatchmakingQueue.id)).filter(MatchmakingQueue.status == "waiting").scalar() or 0
        active_matches = db.query(func.count(Match.id)).filter(Match.status == "ongoing").scalar() or 0
    except Exception:
        queue_depth    = 0
        active_matches = 0

    return {
        "database": {
            "ok":     db_ok,
            "status": "healthy" if db_ok else "error",
        },
        "redis": {
            "ok":               redis_ok,
            "status":           "healthy" if redis_ok else "error",
            "memory_used":      redis_mem_used,
            "memory_max":       redis_mem_max,
            "connected_clients": redis_connected,
        },
        "ml_model":      get_model_info(),
        "queue_depth":   queue_depth,
        "active_matches": active_matches,
        "as_of":         datetime.now(timezone.utc).isoformat(),
    }


# ── Activity Reports ──────────────────────────────────────────────────────────

@router.get("/reports/activity")
def activity_report(
    days: int = Query(30, ge=1, le=90),
    _: dict = _admin,
    db: Session = Depends(get_db),
):
    """System-wide activity over the last N days: registrations, matches, logins."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    registrations_by_day = db.execute(text("""
        SELECT DATE(created_at AT TIME ZONE 'UTC') AS day, COUNT(*) AS count
        FROM profiles
        WHERE created_at >= :cutoff
        GROUP BY day ORDER BY day
    """), {"cutoff": cutoff}).mappings().all()

    matches_by_day = db.execute(text("""
        SELECT DATE(COALESCE(completed_at, created_at) AT TIME ZONE 'UTC') AS day, COUNT(*) AS count
        FROM matches
        WHERE status = 'completed'
          AND COALESCE(completed_at, created_at) >= :cutoff
        GROUP BY day ORDER BY day
    """), {"cutoff": cutoff}).mappings().all()

    logins_by_day = db.execute(text("""
        SELECT DATE(created_at AT TIME ZONE 'UTC') AS day, COUNT(*) AS count
        FROM security_audit_logs
        WHERE event_type = 'login'
          AND created_at >= :cutoff
        GROUP BY day ORDER BY day
    """), {"cutoff": cutoff}).mappings().all()

    tournaments_by_day = db.execute(text("""
        SELECT DATE(created_at AT TIME ZONE 'UTC') AS day, COUNT(*) AS count
        FROM tournaments
        WHERE created_at >= :cutoff
        GROUP BY day ORDER BY day
    """), {"cutoff": cutoff}).mappings().all()

    # Summary totals
    total_new_users   = sum(r["count"] for r in registrations_by_day)
    total_matches     = sum(r["count"] for r in matches_by_day)
    total_logins      = sum(r["count"] for r in logins_by_day)
    total_tournaments = sum(r["count"] for r in tournaments_by_day)

    def _to_list(rows) -> list[dict]:  # type: ignore[no-untyped-def]
        return [{"day": str(r["day"]), "count": int(r["count"])} for r in rows]

    return {
        "days":                days,
        "summary": {
            "new_users":   total_new_users,
            "matches":     total_matches,
            "logins":      total_logins,
            "tournaments": total_tournaments,
        },
        "registrations_by_day":  _to_list(registrations_by_day),
        "matches_by_day":         _to_list(matches_by_day),
        "logins_by_day":          _to_list(logins_by_day),
        "tournaments_by_day":     _to_list(tournaments_by_day),
        "as_of": datetime.now(timezone.utc).isoformat(),
    }


# ── Backup ────────────────────────────────────────────────────────────────────

_BACKUP_DIR = os.environ.get("BACKUP_DIR", "/app/backups")


@router.get("/backup/list")
def list_backups(_: dict = _admin):
    """List available database backup files."""
    try:
        if not os.path.isdir(_BACKUP_DIR):
            return {"backups": [], "backup_dir": _BACKUP_DIR}
        files = sorted(
            [f for f in os.listdir(_BACKUP_DIR) if f.startswith("isms_backup_") and f.endswith(".sql.gz")],
            reverse=True,
        )
        details = []
        for f in files:
            path = os.path.join(_BACKUP_DIR, f)
            stat = os.stat(path)
            details.append({
                "filename":    f,
                "size_bytes":  stat.st_size,
                "size_mb":     round(stat.st_size / 1_048_576, 2),
                "created_at":  datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
            })
        return {"backups": details, "backup_dir": _BACKUP_DIR}
    except Exception as exc:
        raise HTTPException(500, f"Failed to list backups: {exc}")


@router.post("/backup/trigger")
def trigger_backup(
    current_admin: dict = _admin,
    db: Session = Depends(get_db),
):
    """Manually trigger a database backup. Runs pg_dump synchronously."""
    db_url = settings.database_url
    # Parse postgres://user:pass@host:port/db
    import re
    m = re.match(r"postgresql(?:\+\w+)?://([^:]+):([^@]+)@([^:/]+)(?::(\d+))?/(.+)", db_url)
    if not m:
        raise HTTPException(500, "Cannot parse DATABASE_URL for backup.")

    user, password, host, port, dbname = m.group(1), m.group(2), m.group(3), m.group(4) or "5432", m.group(5)

    os.makedirs(_BACKUP_DIR, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename  = f"isms_backup_{timestamp}.sql.gz"
    filepath  = os.path.join(_BACKUP_DIR, filename)

    env = {**os.environ, "PGPASSWORD": password}
    try:
        result = subprocess.run(
            ["pg_dump", f"--host={host}", f"--port={port}", f"--username={user}",
             "--no-password", "--format=plain", "--no-owner", "--no-acl", dbname],
            capture_output=True, env=env, timeout=120,
        )
        if result.returncode != 0:
            raise HTTPException(500, f"pg_dump failed: {result.stderr.decode()[:500]}")

        import gzip
        with gzip.open(filepath, "wb") as f:
            f.write(result.stdout)

        size_mb = round(os.path.getsize(filepath) / 1_048_576, 2)
        logger.warning(f"[admin] Manual backup triggered by {current_admin['id']}: {filename} ({size_mb} MB)")
        return {"message": "Backup completed.", "filename": filename, "size_mb": size_mb}

    except subprocess.TimeoutExpired:
        raise HTTPException(504, "Backup timed out after 120 seconds.")
    except FileNotFoundError:
        raise HTTPException(503, "pg_dump not found. Run backup from the db-backup container.")


# ── Bulk User Role Import ─────────────────────────────────────────────────────

@router.post("/users/import-roles")
async def import_user_roles(
    file: UploadFile = File(...),
    current_admin: dict = _admin,
    db: Session = Depends(get_db),
):
    """
    Import role assignments from a CSV file.
    CSV format (header required): email,role
    Valid roles: player, club_admin, tournament_organizer, referee, system_admin

    Returns per-row results so the admin knows what succeeded and what failed.
    """
    if not file.filename or not file.filename.endswith(".csv"):
        raise HTTPException(400, "File must be a .csv")

    content = await file.read()
    try:
        text_content = content.decode("utf-8-sig")  # handle BOM
    except UnicodeDecodeError:
        raise HTTPException(400, "CSV must be UTF-8 encoded.")

    reader = csv.DictReader(io.StringIO(text_content))
    if not reader.fieldnames or "email" not in reader.fieldnames or "role" not in reader.fieldnames:
        raise HTTPException(400, "CSV must have 'email' and 'role' columns.")

    valid_roles = {r.value for r in UserRole}
    results: list[dict] = []
    processed = 0
    errors    = 0

    for row in reader:
        email = (row.get("email") or "").strip().lower()
        role  = (row.get("role")  or "").strip().lower()

        if not email or not role:
            results.append({"email": email, "role": role, "status": "skipped", "reason": "Empty email or role"})
            errors += 1
            continue

        if role not in valid_roles:
            results.append({"email": email, "role": role, "status": "error", "reason": f"Invalid role '{role}'"})
            errors += 1
            continue

        profile = db.query(Profile).filter(Profile.email == email).first()
        if not profile:
            results.append({"email": email, "role": role, "status": "error", "reason": "User not found"})
            errors += 1
            continue

        # Prevent granting system_admin via import (must use the roles UI)
        if role == "system_admin" and str(profile.id) != current_admin["id"]:
            results.append({"email": email, "role": role, "status": "error", "reason": "system_admin cannot be assigned via import"})
            errors += 1
            continue

        existing = db.query(UserRoleModel).filter(
            UserRoleModel.user_id == profile.id,
            UserRoleModel.role    == role,
        ).first()

        if existing:
            results.append({"email": email, "role": role, "status": "skipped", "reason": "Role already assigned"})
        else:
            db.add(UserRoleModel(user_id=str(profile.id), role=role))
            results.append({"email": email, "role": role, "status": "assigned"})
            processed += 1

    db.commit()
    logger.warning(f"[admin] Bulk role import by {current_admin['id']}: {processed} assigned, {errors} errors.")
    return {"assigned": processed, "errors": errors, "results": results}
