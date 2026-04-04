from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import Optional
import uuid
import datetime as dt
from datetime import timezone

from app.database import get_db
from app.models.models import OpenPlaySession, OpenPlayParticipant, Club, ClubMember, Court, Profile
from app.middleware.auth import get_current_user
from app.services.notifications import send_notification, send_bulk_notifications

router = APIRouter()

SPORT_EMOJIS = {
    "badminton":    "🏸",
    "pickleball":   "🏓",
    "lawn_tennis":  "🎾",
    "table_tennis": "🏓",
}


# ── Request models ────────────────────────────────────────────────────────────

class CreateOpenPlaySessionRequest(BaseModel):
    title:          str
    description:    Optional[str] = None
    sport:          str
    session_date:   dt.datetime
    duration_hours: Optional[float] = 1.0
    max_players:    int
    price_per_head: Optional[float] = 0.0
    court_id:       Optional[str] = None
    skill_min:      Optional[float] = None
    skill_max:      Optional[float] = None
    notes:          Optional[str] = None

class UpdateOpenPlaySessionRequest(BaseModel):
    title:          Optional[str] = None
    description:    Optional[str] = None
    session_date:   Optional[dt.datetime] = None
    duration_hours: Optional[float] = None
    max_players:    Optional[int] = None
    price_per_head: Optional[float] = None
    skill_min:      Optional[float] = None
    skill_max:      Optional[float] = None
    notes:          Optional[str] = None
    status:         Optional[str] = None


# ── Helpers ───────────────────────────────────────────────────────────────────

def _serialize_session(session: OpenPlaySession, current_user_id: str, db: Session) -> dict:
    confirmed   = [p for p in session.participants if p.status == "confirmed"]
    waitlisted  = [p for p in session.participants if p.status == "waitlisted"]
    is_joined   = any(str(p.user_id) == current_user_id and p.status == "confirmed" for p in session.participants)

    club = db.query(Club).filter(Club.id == session.club_id).first()
    court_name = None
    if session.court_id is not None:
        court = db.query(Court).filter(Court.id == session.court_id).first()
        if court is not None:
            court_name = court.name

    return {
        "id":               str(session.id),
        "club_id":          str(session.club_id),
        "club_name":        club.name if club is not None else "",
        "title":            session.title,
        "sport":            str(session.sport),
        "sport_emoji":      SPORT_EMOJIS.get(str(session.sport), "🏅"),
        "session_date":     session.session_date.isoformat() if session.session_date is not None else None,
        "duration_hours":   float(session.duration_hours) if session.duration_hours is not None else 1.0,  # type: ignore[arg-type]
        "max_players":      session.max_players,
        "confirmed_count":  len(confirmed),
        "waitlisted_count": len(waitlisted),
        "price_per_head":   float(session.price_per_head) if session.price_per_head is not None else 0.0,  # type: ignore[arg-type]
        "status":           str(session.status),
        "is_joined":        is_joined,
        "court_name":       court_name,
        "skill_min":        float(session.skill_min) if session.skill_min is not None else None,  # type: ignore[arg-type]
        "skill_max":        float(session.skill_max) if session.skill_max is not None else None,  # type: ignore[arg-type]
        "description":      session.description,
        "notes":            session.notes,
        "created_at":       session.created_at.isoformat() if session.created_at is not None else None,
    }

def _is_club_admin(club_id: str, user_id: str, db: Session) -> bool:
    club = db.query(Club).filter(Club.id == club_id).first()
    if club is None:
        return False
    if str(club.admin_id) == user_id:
        return True
    member = db.query(ClubMember).filter(
        ClubMember.club_id == club_id,
        ClubMember.user_id == user_id,
        ClubMember.role.in_(["admin", "assistant"]),
    ).first()
    return member is not None


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/clubs/{club_id}/open-play")
def list_club_sessions(
    club_id: str,
    status: Optional[str] = Query(None),
    sport: Optional[str] = Query(None),
    date: Optional[str] = Query(None),   # YYYY-MM-DD filter
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    club = db.query(Club).filter(Club.id == club_id).first()
    if club is None:
        raise HTTPException(status_code=404, detail="Club not found.")

    q = db.query(OpenPlaySession).filter(OpenPlaySession.club_id == club_id)

    if date is not None:
        try:
            target = dt.date.fromisoformat(date)
        except ValueError:
            raise HTTPException(400, "Invalid date format. Use YYYY-MM-DD.")
        day_start = dt.datetime(target.year, target.month, target.day, 0, 0, 0, tzinfo=timezone.utc)
        day_end   = day_start + dt.timedelta(days=1)
        q = q.filter(OpenPlaySession.session_date >= day_start, OpenPlaySession.session_date < day_end)
    elif status is not None:
        # Only apply status filter when no specific date is given
        q = q.filter(OpenPlaySession.status == status)

    if sport is not None:
        q = q.filter(OpenPlaySession.sport == sport)
    q = q.order_by(OpenPlaySession.session_date.asc())

    sessions = q.all()
    return {"sessions": [_serialize_session(s, str(current_user["id"]), db) for s in sessions]}


@router.get("/open-play/sessions")
def list_all_sessions(
    sport: Optional[str] = Query(None),
    status: Optional[str] = Query("upcoming"),
    limit: int = Query(20, le=50),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    q = db.query(OpenPlaySession)
    if status is not None:
        q = q.filter(OpenPlaySession.status == status)
    if sport is not None:
        q = q.filter(OpenPlaySession.sport == sport)
    q = q.order_by(OpenPlaySession.session_date.asc()).limit(limit)

    sessions = q.all()
    return {"sessions": [_serialize_session(s, str(current_user["id"]), db) for s in sessions]}


@router.get("/open-play/{session_id}")
def get_session(
    session_id: str,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    session = db.query(OpenPlaySession).filter(OpenPlaySession.id == session_id).first()
    if session is None:
        raise HTTPException(status_code=404, detail="Session not found.")

    data = _serialize_session(session, str(current_user["id"]), db)

    # Add full participant list
    participants = []
    for p in session.participants:
        if p.status == "cancelled":
            continue
        profile = db.query(Profile).filter(Profile.id == p.user_id).first()
        participants.append({
            "user_id":   str(p.user_id),
            "username":  profile.username if profile is not None else "unknown",
            "status":    p.status,
            "joined_at": p.joined_at.isoformat() if p.joined_at is not None else None,
        })
    data["participants"] = participants
    return data


@router.post("/clubs/{club_id}/open-play")
def create_session(
    club_id: str,
    body: CreateOpenPlaySessionRequest,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    if not _is_club_admin(club_id, str(current_user["id"]), db):
        raise HTTPException(status_code=403, detail="Club admin access required.")

    session = OpenPlaySession(
        club_id        = uuid.UUID(club_id),
        created_by     = current_user["id"],
        title          = body.title,
        description    = body.description,
        sport          = body.sport,
        session_date   = body.session_date,
        duration_hours = body.duration_hours,
        max_players    = body.max_players,
        price_per_head = body.price_per_head,
        court_id       = uuid.UUID(body.court_id) if body.court_id is not None else None,
        skill_min      = body.skill_min,
        skill_max      = body.skill_max,
        notes          = body.notes,
        status         = "upcoming",
    )
    db.add(session)
    db.flush()

    # Notify all club members
    club = db.query(Club).filter(Club.id == club_id).first()
    members = db.query(ClubMember).filter(ClubMember.club_id == club_id).all()
    member_ids = [str(m.user_id) for m in members if str(m.user_id) != str(current_user["id"])]
    if member_ids:
        creator = db.query(Profile).filter(Profile.id == current_user["id"]).first()
        username = creator.username if creator is not None else "Someone"
        send_bulk_notifications(
            user_ids    = member_ids,
            notif_type  = "open_play_session",
            title       = "New Open Play Session",
            body        = f"{username} scheduled '{body.title}' at {club.name if club else ''}",
            reference_id= str(session.id),
        )

    db.commit()
    return {"session_id": str(session.id), "message": "Session created."}


@router.put("/open-play/{session_id}")
def update_session(
    session_id: str,
    body: UpdateOpenPlaySessionRequest,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    session = db.query(OpenPlaySession).filter(OpenPlaySession.id == session_id).first()
    if session is None:
        raise HTTPException(status_code=404, detail="Session not found.")

    if str(session.created_by) != str(current_user["id"]) and not _is_club_admin(str(session.club_id), str(current_user["id"]), db):
        raise HTTPException(status_code=403, detail="Not authorized.")

    if body.title is not None:          setattr(session, "title",          body.title)
    if body.description is not None:    setattr(session, "description",    body.description)
    if body.session_date is not None:   setattr(session, "session_date",   body.session_date)
    if body.duration_hours is not None: setattr(session, "duration_hours", body.duration_hours)
    if body.max_players is not None:    setattr(session, "max_players",    body.max_players)
    if body.price_per_head is not None: setattr(session, "price_per_head", body.price_per_head)
    if body.skill_min is not None:      setattr(session, "skill_min",      body.skill_min)
    if body.skill_max is not None:      setattr(session, "skill_max",      body.skill_max)
    if body.notes is not None:          setattr(session, "notes",          body.notes)
    if body.status is not None:         setattr(session, "status",         body.status)

    db.commit()
    return {"message": "Session updated."}


@router.post("/open-play/{session_id}/join")
def join_session(
    session_id: str,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    session = db.query(OpenPlaySession).filter(OpenPlaySession.id == session_id).first()
    if session is None:
        raise HTTPException(status_code=404, detail="Session not found.")
    if session.status not in ("upcoming", "ongoing"):
        raise HTTPException(status_code=400, detail="Session is not open for joining.")

    # Check if already a participant
    existing = db.query(OpenPlayParticipant).filter(
        OpenPlayParticipant.session_id == session_id,
        OpenPlayParticipant.user_id    == current_user["id"],
        OpenPlayParticipant.status     != "cancelled",
    ).first()
    if existing is not None:
        raise HTTPException(status_code=400, detail="Already joined this session.")

    confirmed_count = sum(1 for p in session.participants if str(p.status) == "confirmed")
    participant_status = "confirmed" if confirmed_count < session.max_players else "waitlisted"  # type: ignore[operator]

    participant = OpenPlayParticipant(
        session_id = uuid.UUID(session_id),
        user_id    = current_user["id"],
        status     = participant_status,
    )
    db.add(participant)
    db.flush()

    # Notify session creator
    joiner = db.query(Profile).filter(Profile.id == current_user["id"]).first()
    username = joiner.username if joiner is not None else "Someone"
    send_notification(
        user_id      = str(session.created_by),
        notif_type   = "open_play_join",
        title        = "New Player Joined" if participant_status == "confirmed" else "Player on Waitlist",
        body         = f"@{username} joined '{session.title}'" if participant_status == "confirmed" else f"@{username} is waitlisted for '{session.title}'",
        reference_id = str(session.club_id),
    )

    db.commit()
    return {"status": participant_status, "message": f"You are {participant_status}."}


@router.post("/open-play/{session_id}/leave")
def leave_session(
    session_id: str,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    participant = db.query(OpenPlayParticipant).filter(
        OpenPlayParticipant.session_id == session_id,
        OpenPlayParticipant.user_id    == current_user["id"],
        OpenPlayParticipant.status     != "cancelled",
    ).first()
    if participant is None:
        raise HTTPException(status_code=404, detail="You are not in this session.")

    was_confirmed = str(participant.status) == "confirmed"
    setattr(participant, "status", "cancelled")
    db.flush()

    # If a confirmed spot opened up, promote the first waitlisted player
    if was_confirmed:
        session = db.query(OpenPlaySession).filter(OpenPlaySession.id == session_id).first()
        if session is not None:
            waitlisted = sorted(
                [p for p in session.participants if str(p.status) == "waitlisted"],
                key=lambda p: p.joined_at or dt.datetime.min,
            )
            if waitlisted:
                promoted = waitlisted[0]
                setattr(promoted, "status", "confirmed")
                send_notification(
                    user_id      = str(promoted.user_id),
                    notif_type   = "open_play_promoted",
                    title        = "You're In!",
                    body         = f"A spot opened up — you've been confirmed for '{session.title}'",
                    reference_id = str(session.club_id),
                )

    db.commit()
    return {"message": "You have left the session."}


@router.delete("/open-play/{session_id}")
def cancel_session(
    session_id: str,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    session = db.query(OpenPlaySession).filter(OpenPlaySession.id == session_id).first()
    if session is None:
        raise HTTPException(status_code=404, detail="Session not found.")

    if not _is_club_admin(str(session.club_id), str(current_user["id"]), db):
        raise HTTPException(status_code=403, detail="Club admin access required.")

    setattr(session, "status", "cancelled")

    # Notify all confirmed participants
    confirmed_ids = [str(p.user_id) for p in session.participants if str(p.status) == "confirmed" and str(p.user_id) != str(current_user["id"])]
    if confirmed_ids:
        send_bulk_notifications(
            user_ids    = confirmed_ids,
            notif_type  = "open_play_cancelled",
            title       = "Session Cancelled",
            body        = f"'{session.title}' has been cancelled.",
            reference_id= str(session.club_id),
        )

    db.commit()
    return {"message": "Session cancelled."}
