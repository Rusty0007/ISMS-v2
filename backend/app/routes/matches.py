from fastapi import APIRouter, HTTPException, Depends, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, timezone, timedelta
import asyncio
import json
from sqlalchemy.orm import Session
from sqlalchemy import or_, text
from app.database import get_db, SessionLocal
from app.middleware.auth import get_current_user
from app.models.models import (
    Match, MatchSet, MatchAcceptance, RallyEvent,
    PlayerRating, Profile, MatchHistory,
    Club, ClubMember, Court,
)
from app.services.sport_rulesets import get_ruleset
from app.utils.glicko2 import update as glicko_update
from app.config import settings
from app.services.training_data_collector import save_training_row
from app.services.matchmaking import find_best_opponent, get_model_info, score_candidate
from app.services.broadcast import broadcast_match as _broadcast
from app.services.notifications import send_notification


router = APIRouter()

VALID_SPORTS  = ["pickleball", "badminton", "lawn_tennis", "table_tennis"]


def _notify_duty_holders(db: Session, club_id, match_id, club_name: str):
    """Notify club owner, admins, and today's assistant about a pending approval match."""
    from app.models.models import ClubMember, Club
    from datetime import date
    today = date.today()
    members = db.query(ClubMember).filter(ClubMember.club_id == club_id).all()
    notified = set()
    for m in members:
        is_duty = (
            str(m.role) in ("admin", "owner") or
            (str(m.role) == "assistant" and m.duty_date == today)
        )
        if is_duty and m.user_id is not None and str(m.user_id) not in notified:  # type: ignore[truthy-bool]
            send_notification(
                user_id      = str(m.user_id),
                title        = "Match Pending Approval",
                body         = f"A match has been auto-assigned at {club_name} and awaits your confirmation.",
                notif_type   = "match_pending_approval",
                reference_id = str(match_id),
            )
            notified.add(str(m.user_id))

    # Fallback: notify Club.admin_id directly if no duty holder was found via ClubMember roles
    if not notified:
        club = db.query(Club).filter(Club.id == club_id).first()
        if club and club.admin_id is not None:
            send_notification(
                user_id      = str(club.admin_id),
                title        = "Match Pending Approval",
                body         = f"A match has been auto-assigned at {club_name} and awaits your confirmation.",
                notif_type   = "match_pending_approval",
                reference_id = str(match_id),
            )
VALID_FORMATS = ["singles", "doubles", "mixed_doubles"]
VALID_EVENTS  = ["shot", "violation", "rally_outcome", "momentum"]


def _safe_win_rate(rating: PlayerRating | None, default: float = 0.5) -> float:
    if rating is None or int(rating.matches_played or 0) <= 0:  # type: ignore[arg-type]
        return default
    return float(int(rating.wins or 0)) / float(int(rating.matches_played or 1))  # type: ignore[arg-type]

# ── Request models ───────────────────────────────────────────────────────────

class CreateFriendlyRequest(BaseModel):
    sport: str
    match_format: str = "singles"
    opponent_id: str

class JoinQueueRequest(BaseModel):
    sport:              str
    match_format:       str           = "singles"
    preferred_club_id:  Optional[str] = None   # only match within this club if set
    preferred_indoor:   Optional[bool] = None  # True=indoor, False=outdoor, None=any
    play_city_code:     Optional[str] = None   # override profile location for this queue entry
    play_province_code: Optional[str] = None
    play_region_code:   Optional[str] = None

class BookMatchRequest(BaseModel):
    sport: str
    match_format: str = "singles"
    opponent_id: str
    scheduled_at: datetime

class RecordEventRequest(BaseModel):
    set_number:    int
    rally_number:  int
    event_type:    str
    event_code:    str
    tagged_player: Optional[str] = None
    notes:         Optional[str] = None
    is_offline:    bool = False

class UpdateScoreRequest(BaseModel):
    player1_score: Optional[int] = None
    player2_score: Optional[int] = None
    team1_score:   Optional[int] = None
    team2_score:   Optional[int] = None

class CompleteMatchRequest(BaseModel):
    winner_id: str

class ScoreTestRequest(BaseModel):
    rating_a:     float
    rating_b:     float
    win_rate_a:   float = 0.5
    win_rate_b:   float = 0.5
    sport:        str   = "badminton"
    match_format: str   = "singles"
    wait_seconds: int   = 60


# ── Debug ────────────────────────────────────────────────────────────────────

@router.get("/debug/model")
def model_status(current_user: dict = Depends(get_current_user)):
    return get_model_info()

@router.post("/debug/score")
def test_score(data: ScoreTestRequest, current_user: dict = Depends(get_current_user)):
    score = score_candidate(
        rating_a=data.rating_a, rd_a=200, win_rate_a=data.win_rate_a,
        activeness_a=0.5, streak_a=0, city_a=None, province_a=None, region_a=None,
        rating_b=data.rating_b, rd_b=200, win_rate_b=data.win_rate_b,
        activeness_b=0.5, streak_b=0, city_b=None, province_b=None, region_b=None,
        sport=data.sport, match_format=data.match_format, wait_seconds=data.wait_seconds,
    )
    return {
        "rating_a": data.rating_a, "rating_b": data.rating_b,
        "rating_diff": abs(data.rating_a - data.rating_b),
        "ml_score": score,
        "verdict": (
            "Excellent match" if score >= 0.85 else
            "Good match"      if score >= 0.70 else
            "Average match"   if score >= 0.55 else
            "Poor match"
        )
    }


# ── Friendly match ───────────────────────────────────────────────────────────

@router.post("/friendly", status_code=201)
def create_friendly_match(
    data: CreateFriendlyRequest,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if data.sport not in VALID_SPORTS:
        raise HTTPException(400, "Invalid sport.")
    if data.match_format not in VALID_FORMATS:
        raise HTTPException(400, "Invalid match format.")

    player1_id = current_user["id"]
    if player1_id == data.opponent_id:
        raise HTTPException(400, "Cannot create a match against yourself.")

    match = Match(
        sport=data.sport, match_type="friendly",
        match_format=data.match_format, status="pending",
        player1_id=player1_id, player2_id=data.opponent_id,
    )
    db.add(match)
    db.flush()
    db.add(MatchSet(match_id=match.id, set_number=1, player1_score=0, player2_score=0))
    db.commit()

    return {"message": "Friendly match created.", "match_id": str(match.id)}


# ── Queue match ──────────────────────────────────────────────────────────────

@router.post("/queue/join", status_code=201)
def join_queue(
    data: JoinQueueRequest,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if data.sport not in VALID_SPORTS:
        raise HTTPException(400, "Invalid sport.")
    if data.match_format not in VALID_FORMATS:
        raise HTTPException(400, "Invalid match format.")

    user_id = current_user["id"]

    # Already in queue?
    if data.match_format == "singles":
        existing = db.query(Match).filter(
            Match.match_type == "queue",
            Match.sport == data.sport,
            Match.match_format == data.match_format,
            Match.player1_id == user_id,
            Match.status == "pending",
            Match.player2_id.is_(None),
        ).first()
    else:
        existing = db.query(Match).filter(
            Match.match_type == "queue",
            Match.sport == data.sport,
            Match.match_format == data.match_format,
            Match.status.in_(["assembling", "pending"]),
            or_(
                Match.player1_id == user_id,
                Match.player2_id == user_id,
                Match.player3_id == user_id,
                Match.player4_id == user_id,
            ),
        ).first()

    if existing:
        raise HTTPException(400, "Already in queue for this sport and format.")

    # ── Validate club preference ───────────────────────────────────────────────
    preferred_club_id = None
    if data.preferred_club_id:
        club_uuid = data.preferred_club_id
        membership = db.query(ClubMember).filter(
            ClubMember.club_id == club_uuid,
            ClubMember.user_id == user_id,
        ).first()
        if not membership:
            raise HTTPException(403, "You are not a member of that club.")
        preferred_club_id = club_uuid

    # ── Singles ───────────────────────────────────────────────────────────────
    if data.match_format == "singles":
        my_rating  = db.query(PlayerRating).filter(
            PlayerRating.user_id == user_id,
            PlayerRating.sport == data.sport,
            PlayerRating.match_format == data.match_format,
        ).first()
        my_profile = db.query(Profile).filter(Profile.id == user_id).first()

        # Resolve play location: explicit override → fall back to profile
        resolved_city     = data.play_city_code     or (my_profile.city_mun_code  if my_profile else None)
        resolved_province = data.play_province_code or (my_profile.province_code  if my_profile else None)
        resolved_region   = data.play_region_code   or (my_profile.region_code    if my_profile else None)

        my_player = {
            "rating":           float(my_rating.rating)             if my_rating else 1500.0,  # type: ignore[arg-type]
            "rating_deviation": float(my_rating.rating_deviation)   if my_rating else 200.0,  # type: ignore[arg-type]
            "win_rate":         _safe_win_rate(my_rating),
            "activeness_score": float(my_rating.activeness_score)   if my_rating else 0.5,  # type: ignore[arg-type]
            "current_streak":   float(my_rating.current_win_streak) if my_rating else 0.0,  # type: ignore[arg-type]
            "city_code":        resolved_city,
            "province_code":    resolved_province,
            "region_code":      resolved_region,
        }

        queue_q = db.query(Match).filter(
            Match.match_type == "queue",
            Match.sport == data.sport,
            Match.match_format == data.match_format,
            Match.status == "pending",
            Match.player2_id.is_(None),
            Match.player1_id != user_id,
        )
        # If club preference set, only consider queue entries from the same club
        if preferred_club_id:
            queue_q = queue_q.filter(Match.club_id == preferred_club_id)
        queue_matches = queue_q.all()

        now_utc = datetime.now(timezone.utc)

        candidates = []
        for queued in queue_matches:
            opp_r = db.query(PlayerRating).filter(
                PlayerRating.user_id == queued.player1_id,
                PlayerRating.sport == data.sport,
                PlayerRating.match_format == data.match_format,
            ).first()
            opp_p = db.query(Profile).filter(Profile.id == queued.player1_id).first()
            # Actual wait time for this queued player
            q_wait = int((now_utc - queued.created_at.replace(tzinfo=timezone.utc)).total_seconds())
            # Boost if this candidate has an active referee boost
            if opp_p and opp_p.referee_boost_until is not None and opp_p.referee_boost_until.replace(tzinfo=timezone.utc) > now_utc:
                q_wait = max(q_wait, 900)
            # Use stored queue location snapshot; fall back to live profile if missing
            cand_city     = queued.queue_city_code     or (opp_p.city_mun_code  if opp_p else None)
            cand_province = queued.queue_province_code or (opp_p.province_code  if opp_p else None)
            cand_region   = queued.queue_region_code   or (opp_p.region_code    if opp_p else None)
            candidates.append({
                "player_id":          str(queued.player1_id),
                "match_id":           str(queued.id),
                "rating":             float(opp_r.rating)             if opp_r else 1200.0,  # type: ignore[arg-type]
                "rating_deviation":   float(opp_r.rating_deviation)   if opp_r else 200.0,  # type: ignore[arg-type]
                "win_rate":           _safe_win_rate(opp_r),
                "activeness_score":   float(opp_r.activeness_score)   if opp_r else 0.5,  # type: ignore[arg-type]
                "current_streak":     float(opp_r.current_win_streak) if opp_r else 0.0,  # type: ignore[arg-type]
                "city_code":          cand_city,
                "province_code":      cand_province,
                "region_code":        cand_region,
                "queue_wait_seconds": q_wait,
            })

        # Check if joining player has an active referee priority boost
        my_boost = my_profile is not None and my_profile.referee_boost_until is not None and \
                   my_profile.referee_boost_until.replace(tzinfo=timezone.utc) > now_utc
        effective_wait = 900 if my_boost else 0

        best = find_best_opponent(
            player=my_player, candidates=candidates,
            sport=data.sport, match_format=data.match_format, wait_seconds=effective_wait,
        )

        if best:
            found = db.query(Match).filter(Match.id == best["match_id"]).first()
            if found:
                setattr(found, "player2_id", user_id)
                setattr(found, "status", "ongoing")
                setattr(found, "started_at", datetime.now(timezone.utc))
                db.add(MatchSet(match_id=found.id, set_number=1, player1_score=0, player2_score=0))
                # Auto-assign available court from preferred club
                club_for_match = preferred_club_id or (str(found.club_id) if found.club_id is not None else None)
                needs_approval = False
                if club_for_match and found.court_id is None:
                    court_q = db.query(Court).filter(
                        Court.club_id == club_for_match,
                        Court.status == "available",
                    )
                    if data.preferred_indoor is not None:
                        court_q = court_q.filter(Court.is_indoor == data.preferred_indoor)
                    avail = court_q.first()
                    if avail:
                        setattr(found, "court_id", avail.id)
                        setattr(found, "club_id",  avail.club_id)
                        setattr(avail, "status",   "occupied")
                        # Check if club requires manual approval
                        club_obj = db.query(Club).filter(Club.id == avail.club_id).first()
                        if club_obj and str(club_obj.approval_mode) == "manual":
                            setattr(found, "status", "pending_approval")
                            needs_approval = True
                            _notify_duty_holders(db, avail.club_id, found.id, str(club_obj.name))
                if my_profile is not None and my_boost:
                    setattr(my_profile, "referee_boost_until", None)
                db.commit()
                return {
                    "status": "matched",
                    "message": "Opponent found! Awaiting club confirmation." if needs_approval else "Opponent found!",
                    "match_id": str(found.id),
                    "court_assigned": bool(found.court_id),
                    "pending_approval": needs_approval,
                }

        new_match = Match(
            sport=data.sport, match_type="queue",
            match_format=data.match_format, status="pending",
            player1_id=user_id,
            club_id=preferred_club_id,
            queue_city_code=resolved_city,
            queue_province_code=resolved_province,
            queue_region_code=resolved_region,
        )
        db.add(new_match)
        if my_profile is not None and my_boost:
            setattr(my_profile, "referee_boost_until", None)
        db.commit()
        return {"status": "queued", "message": "Added to matchmaking queue.", "match_id": str(new_match.id)}

    # ── Doubles ───────────────────────────────────────────────────────────────
    assembling = db.query(Match).filter(
        Match.match_type == "queue",
        Match.sport == data.sport,
        Match.match_format == data.match_format,
        Match.status == "assembling",
    ).all()

    def slot_count(m):
        return sum(1 for pid in [m.player1_id, m.player2_id, m.player3_id, m.player4_id] if pid)

    candidates = [
        m for m in assembling
        if user_id not in [str(m.player1_id), str(m.player2_id), str(m.player3_id), str(m.player4_id)]
    ]
    candidates.sort(key=slot_count, reverse=True)

    for candidate in candidates:
        count = slot_count(candidate)
        if count == 3:
            setattr(candidate, "player4_id", user_id)
            setattr(candidate, "status", "ongoing")
            setattr(candidate, "started_at", datetime.now(timezone.utc))
            db.add(MatchSet(match_id=candidate.id, set_number=1, player1_score=0, player2_score=0))
            # Auto-assign court if club set
            needs_approval = False
            if candidate.club_id is not None and candidate.court_id is None:
                court_q = db.query(Court).filter(
                    Court.club_id == str(candidate.club_id),
                    Court.status == "available",
                )
                if data.preferred_indoor is not None:
                    court_q = court_q.filter(Court.is_indoor == data.preferred_indoor)
                avail = court_q.first()
                if avail:
                    setattr(candidate, "court_id", avail.id)
                    setattr(avail, "status", "occupied")
                    club_obj = db.query(Club).filter(Club.id == avail.club_id).first()
                    if club_obj and str(club_obj.approval_mode) == "manual":
                        setattr(candidate, "status", "pending_approval")
                        needs_approval = True
                        _notify_duty_holders(db, avail.club_id, candidate.id, str(club_obj.name))
            db.commit()
            return {
                "status": "matched",
                "message": "All 4 players ready! Awaiting club confirmation." if needs_approval else "All 4 players ready!",
                "match_id": str(candidate.id),
                "players_joined": 4,
                "pending_approval": needs_approval,
            }
        elif count == 2:
            setattr(candidate, "player3_id", user_id)
            db.commit()
            return {"status": "assembling", "message": "Joined! Waiting for one more player.", "match_id": str(candidate.id), "players_joined": 3}
        elif count == 1:
            setattr(candidate, "player2_id", user_id)
            db.commit()
            return {"status": "assembling", "message": "Joined! Waiting for 2 more players.", "match_id": str(candidate.id), "players_joined": 2}

    new_match = Match(
        sport=data.sport, match_type="queue",
        match_format=data.match_format, status="assembling",
        player1_id=user_id,
        club_id=preferred_club_id,
    )
    db.add(new_match)
    db.commit()
    return {"status": "assembling", "message": "Added to queue.", "match_id": str(new_match.id), "players_joined": 1}


@router.get("/queue/status")
def get_queue_status(
    sport: str,
    match_format: str = "singles",
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]

    if match_format == "singles":
        # Check if the match already flipped to "ongoing" (player 2 just joined)
        match = db.query(Match).filter(
            Match.match_type == "queue", Match.sport == sport,
            Match.match_format == match_format,
            Match.player1_id == user_id,
            Match.status.in_(["pending", "pending_approval", "ongoing"]),
            Match.winner_id.is_(None),
        ).order_by(Match.created_at.desc()).first()
        if not match:
            return {"status": "not_in_queue"}
        if match.status.value in ("ongoing", "pending_approval") or match.player2_id is not None:
            return {"status": "matched", "match_id": str(match.id),
                    "pending_approval": match.status.value == "pending_approval"}
        return {"status": "waiting", "match_id": str(match.id)}

    # Doubles — include "ongoing" / "pending_approval" so players get notified
    match = db.query(Match).filter(
        Match.match_type == "queue", Match.sport == sport,
        Match.match_format == match_format,
        Match.status.in_(["assembling", "pending", "pending_approval", "ongoing"]),
        or_(
            Match.player1_id == user_id, Match.player2_id == user_id,
            Match.player3_id == user_id, Match.player4_id == user_id,
        ),
        Match.winner_id.is_(None),
    ).order_by(Match.created_at.desc()).first()

    if not match:
        return {"status": "not_in_queue"}

    if match.status.value in ("ongoing", "pending_approval"):
        return {"status": "matched", "match_id": str(match.id),
                "pending_approval": match.status.value == "pending_approval"}

    players_joined = sum(1 for pid in [match.player1_id, match.player2_id, match.player3_id, match.player4_id] if pid is not None)
    return {"status": "assembling", "match_id": str(match.id), "players_joined": players_joined}


@router.delete("/queue/leave")
def leave_queue(
    sport: str,
    match_format: str = "singles",
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]

    if match_format == "singles":
        match = db.query(Match).filter(
            Match.match_type == "queue", Match.player1_id == user_id,
            Match.sport == sport, Match.match_format == match_format,
            Match.status == "pending", Match.player2_id.is_(None),
        ).first()
        if match:
            setattr(match, "status", "cancelled")
            db.commit()
        return {"message": "Left queue."}

    match = db.query(Match).filter(
        Match.match_type == "queue", Match.sport == sport,
        Match.match_format == match_format, Match.status == "assembling",
        or_(
            Match.player1_id == user_id, Match.player2_id == user_id,
            Match.player3_id == user_id, Match.player4_id == user_id,
        ),
    ).first()

    if not match:
        return {"message": "Not in queue."}

    slots = ["player1_id", "player2_id", "player3_id", "player4_id"]
    remaining = [getattr(match, s) for s in slots if getattr(match, s) and str(getattr(match, s)) != user_id]

    if not remaining:
        setattr(match, "status", "cancelled")
    else:
        for i, slot in enumerate(slots):
            setattr(match, slot, remaining[i] if i < len(remaining) else None)

    db.commit()
    return {"message": "Left queue."}


# ── Book match ───────────────────────────────────────────────────────────────

@router.post("/book", status_code=201)
def book_match(
    data: BookMatchRequest,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if data.sport not in VALID_SPORTS:
        raise HTTPException(400, "Invalid sport.")
    if data.match_format not in VALID_FORMATS:
        raise HTTPException(400, "Invalid match format.")

    player1_id = current_user["id"]
    if player1_id == data.opponent_id:
        raise HTTPException(400, "Cannot book a match against yourself.")

    match = Match(
        sport=data.sport, match_type="book",
        match_format=data.match_format, status="pending",
        player1_id=player1_id, player2_id=data.opponent_id,
        scheduled_at=data.scheduled_at,
    )
    db.add(match)
    db.commit()
    return {"message": "Match booked.", "match_id": str(match.id)}


# ── Match lifecycle ──────────────────────────────────────────────────────────

@router.get("/my")
def get_my_matches(
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]
    matches = db.query(Match).filter(
        or_(
            Match.player1_id == user_id, Match.player2_id == user_id,
            Match.player3_id == user_id, Match.player4_id == user_id,
        )
    ).order_by(Match.created_at.desc()).all()

    def fmt(m):
        return {
            "id": str(m.id), "sport": m.sport.value,
            "match_type": m.match_type.value, "match_format": m.match_format.value,
            "status": m.status.value,
            "player1_id": str(m.player1_id) if m.player1_id is not None else None,
            "player2_id": str(m.player2_id) if m.player2_id is not None else None,
            "player3_id": str(m.player3_id) if m.player3_id is not None else None,
            "player4_id": str(m.player4_id) if m.player4_id is not None else None,
            "winner_id":  str(m.winner_id)  if m.winner_id  is not None else None,
            "scheduled_at": str(m.scheduled_at) if m.scheduled_at is not None else None,
            "started_at":   str(m.started_at)   if m.started_at   is not None else None,
            "completed_at": str(m.completed_at) if m.completed_at is not None else None,
            "created_at":   str(m.created_at),
        }

    return {"matches": [fmt(m) for m in matches]}


@router.websocket("/ws/{match_id}")
async def match_ws(websocket: WebSocket, match_id: str):
    await websocket.accept()

    try:
        db = SessionLocal()
        try:
            match = db.query(Match).filter(Match.id == match_id).first()
            sets  = db.query(MatchSet).filter(MatchSet.match_id == match_id).order_by(MatchSet.set_number).all()
            acc   = db.query(MatchAcceptance).filter(MatchAcceptance.match_id == match_id).all()
            await websocket.send_json({
                "type":  "init",
                "match": {
                    "id": str(match.id), "sport": match.sport.value,
                    "status": match.status.value,
                    "player1_id": str(match.player1_id) if match.player1_id is not None else None,
                    "player2_id": str(match.player2_id) if match.player2_id is not None else None,
                    "referee_id": str(match.referee_id) if match.referee_id is not None else None,
                } if match else None,
                "sets": [{"set_number": s.set_number, "player1_score": s.player1_score, "player2_score": s.player2_score} for s in sets],
                "acceptances": [{"user_id": str(a.user_id), "decision": a.decision} for a in acc],
            })
        finally:
            db.close()
    except Exception:
        pass

    try:
        import redis.asyncio as aioredis
    except ImportError:
        await websocket.close(code=1011)
        return

    aredis = aioredis.from_url(settings.redis_url, decode_responses=True)
    pubsub = aredis.pubsub()
    try:
        await pubsub.subscribe(f"match:{match_id}")
    except Exception:
        await websocket.close(code=1011)
        await aredis.aclose()
        return

    async def redis_to_ws():
        try:
            async for msg in pubsub.listen():
                if msg.get("type") == "message":
                    try:
                        await websocket.send_json(json.loads(msg["data"]))
                    except Exception:
                        return
        except Exception:
            pass

    async def ws_keepalive():
        try:
            while True:
                await websocket.receive_text()
        except (WebSocketDisconnect, Exception):
            pass

    try:
        await asyncio.gather(redis_to_ws(), ws_keepalive())
    finally:
        try:
            await pubsub.unsubscribe(f"match:{match_id}")
            await aredis.aclose()
        except Exception:
            pass


@router.get("/{match_id}")
def get_match(
    match_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    match = db.query(Match).filter(Match.id == match_id).first()
    if not match:
        raise HTTPException(404, "Match not found.")

    sets = db.query(MatchSet).filter(MatchSet.match_id == match_id).order_by(MatchSet.set_number).all()
    acc  = db.query(MatchAcceptance).filter(MatchAcceptance.match_id == match_id).all()

    return {
        "match": {
            "id": str(match.id), "sport": match.sport.value,
            "match_type": match.match_type.value, "match_format": match.match_format.value,
            "status": match.status.value,
            "player1_id": str(match.player1_id) if match.player1_id is not None else None,
            "player2_id": str(match.player2_id) if match.player2_id is not None else None,
            "player3_id": str(match.player3_id) if match.player3_id is not None else None,
            "player4_id": str(match.player4_id) if match.player4_id is not None else None,
            "referee_id": str(match.referee_id) if match.referee_id is not None else None,
            "winner_id":  str(match.winner_id)  if match.winner_id  is not None else None,
            "court_id":   str(match.court_id)   if match.court_id   is not None else None,
            "scheduled_at": str(match.scheduled_at) if match.scheduled_at is not None else None,
            "started_at":   str(match.started_at)   if match.started_at   is not None else None,
            "completed_at": str(match.completed_at) if match.completed_at is not None else None,
        },
        "sets": [
            {"set_number": s.set_number, "player1_score": s.player1_score, "player2_score": s.player2_score}
            for s in sets
        ],
        "acceptances": [{"user_id": str(a.user_id), "decision": a.decision} for a in acc],
    }


@router.post("/{match_id}/accept")
def accept_match(
    match_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    match = db.query(Match).filter(Match.id == match_id).first()
    if not match:
        raise HTTPException(404, "Match not found.")

    user_id = current_user["id"]
    all_players = [str(pid) for pid in [match.player1_id, match.player2_id, match.player3_id, match.player4_id] if pid is not None]

    if user_id not in all_players:
        raise HTTPException(403, "Not a participant of this match.")
    if match.status.value != "pending":
        raise HTTPException(400, "Match is not awaiting acceptance.")

    existing = db.query(MatchAcceptance).filter(
        MatchAcceptance.match_id == match_id,
        MatchAcceptance.user_id == user_id,
    ).first()
    if existing:
        setattr(existing, "decision", "accepted")
        setattr(existing, "decided_at", datetime.now(timezone.utc))
    else:
        db.add(MatchAcceptance(match_id=match_id, user_id=user_id, decision="accepted"))
    db.commit()

    all_acc = db.query(MatchAcceptance).filter(MatchAcceptance.match_id == match_id).all()
    accepted_ids = {str(a.user_id) for a in all_acc if str(a.decision) == "accepted"}
    all_accepted = all(p in accepted_ids for p in all_players)

    _broadcast(match_id, {
        "type": "acceptance_update",
        "acceptances": [{"user_id": str(a.user_id), "decision": a.decision} for a in all_acc],
        "all_accepted": all_accepted,
    })
    return {"message": "Accepted.", "all_accepted": all_accepted}


@router.post("/{match_id}/reject")
def reject_match(
    match_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    match = db.query(Match).filter(Match.id == match_id).first()
    if not match:
        raise HTTPException(404, "Match not found.")

    user_id = current_user["id"]
    all_players = [str(pid) for pid in [match.player1_id, match.player2_id, match.player3_id, match.player4_id] if pid is not None]

    if user_id not in all_players:
        raise HTTPException(403, "Not a participant of this match.")
    if match.status.value != "pending":
        raise HTTPException(400, "Match is not awaiting acceptance.")

    existing = db.query(MatchAcceptance).filter(
        MatchAcceptance.match_id == match_id,
        MatchAcceptance.user_id == user_id,
    ).first()
    if existing:
        setattr(existing, "decision", "rejected")
        setattr(existing, "decided_at", datetime.now(timezone.utc))
    else:
        db.add(MatchAcceptance(match_id=match_id, user_id=user_id, decision="rejected"))

    setattr(match, "status", "cancelled")
    db.commit()

    all_acc = db.query(MatchAcceptance).filter(MatchAcceptance.match_id == match_id).all()
    _broadcast(match_id, {
        "type": "match_cancelled",
        "acceptances": [{"user_id": str(a.user_id), "decision": a.decision} for a in all_acc],
    })
    return {"message": "Match declined and cancelled."}


@router.post("/{match_id}/start")
def start_match(
    match_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    match = db.query(Match).filter(Match.id == match_id).first()
    if not match:
        raise HTTPException(404, "Match not found.")
    if str(match.player1_id) != current_user["id"]:
        raise HTTPException(403, "Only the match creator can start the match.")
    if match.status.value != "pending":
        raise HTTPException(400, f"Match is already {match.status.value}.")

    all_players = [str(pid) for pid in [match.player1_id, match.player2_id, match.player3_id, match.player4_id] if pid is not None]
    all_acc = db.query(MatchAcceptance).filter(MatchAcceptance.match_id == match_id).all()
    accepted_ids = {str(a.user_id) for a in all_acc if str(a.decision) == "accepted"}

    if not all(p in accepted_ids for p in all_players):
        raise HTTPException(400, "Not all players have accepted the match.")

    setattr(match, "status", "ongoing")
    setattr(match, "started_at", datetime.now(timezone.utc))
    db.commit()

    _broadcast(match_id, {"type": "match_started"})
    return {"message": "Match started."}


# ── Live scoring ─────────────────────────────────────────────────────────────

@router.post("/{match_id}/events", status_code=201)
def record_event(
    match_id: str,
    data: RecordEventRequest,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if data.event_type not in VALID_EVENTS:
        raise HTTPException(400, "Invalid event type.")

    match = db.query(Match).filter(Match.id == match_id).first()
    if not match:
        raise HTTPException(404, "Match not found.")
    if match.status.value != "ongoing":
        raise HTTPException(400, "Match is not ongoing.")

    event = RallyEvent(
        match_id=match_id, set_number=data.set_number,
        rally_number=data.rally_number, scored_by=current_user["id"],
        event_type=data.event_type, event_code=data.event_code,
        tagged_player=data.tagged_player, notes=data.notes, is_offline=data.is_offline,
    )
    db.add(event)
    db.commit()
    return {"message": "Event recorded.", "event": {"id": str(event.id)}}


@router.get("/{match_id}/events")
def get_events(
    match_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    events = db.query(RallyEvent).filter(
        RallyEvent.match_id == match_id,
    ).order_by(RallyEvent.set_number, RallyEvent.rally_number).all()

    return {"events": [
        {
            "id": str(e.id), "set_number": e.set_number,
            "rally_number": e.rally_number, "event_type": e.event_type.value,
            "event_code": e.event_code,
            "scored_by": str(e.scored_by) if e.scored_by is not None else None,
        }
        for e in events
    ]}


@router.put("/{match_id}/sets/{set_number}/score")
def update_score(
    match_id: str,
    set_number: int,
    data: UpdateScoreRequest,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    match = db.query(Match).filter(Match.id == match_id).first()
    if not match or match.status.value != "ongoing":
        raise HTTPException(400, "Match is not ongoing.")

    update_data = {k: v for k, v in data.model_dump().items() if v is not None}

    match_set = db.query(MatchSet).filter(
        MatchSet.match_id == match_id, MatchSet.set_number == set_number,
    ).first()

    if match_set:
        for k, v in update_data.items():
            setattr(match_set, k, v)
    else:
        db.add(MatchSet(match_id=match_id, set_number=set_number, **update_data))
    db.commit()

    all_sets = db.query(MatchSet).filter(MatchSet.match_id == match_id).order_by(MatchSet.set_number).all()
    _broadcast(match_id, {
        "type": "sets_update",
        "sets": [{"set_number": s.set_number, "player1_score": s.player1_score, "player2_score": s.player2_score} for s in all_sets],
    })
    return {"message": "Score updated."}


@router.post("/{match_id}/complete")
def complete_match(
    match_id: str,
    data: CompleteMatchRequest,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    match = db.query(Match).filter(Match.id == match_id).first()
    if not match:
        raise HTTPException(404, "Match not found.")
    if match.status.value != "ongoing":
        raise HTTPException(400, "Match is not ongoing.")

    sport        = match.sport.value
    match_format = match.match_format.value
    player1_id   = str(match.player1_id) if match.player1_id is not None else None
    player2_id   = str(match.player2_id) if match.player2_id is not None else None

    # Fallback path: no player IDs or missing rating rows → simple completion without ratings
    if not player1_id or not player2_id:
        setattr(match, "status", "completed")
        setattr(match, "winner_id", data.winner_id)
        setattr(match, "completed_at", datetime.now(timezone.utc))
        if match.court_id is not None:
            court = db.query(Court).filter(Court.id == match.court_id).first()
            if court:
                setattr(court, "status", "available")
        db.commit()
        _broadcast(match_id, {"type": "match_completed", "winner_id": data.winner_id})
        return {"message": "Match completed.", "winner_id": data.winner_id}

    p1 = db.query(PlayerRating).filter(
        PlayerRating.user_id == player1_id,
        PlayerRating.sport == sport,
        PlayerRating.match_format == match_format,
    ).first()
    p2 = db.query(PlayerRating).filter(
        PlayerRating.user_id == player2_id,
        PlayerRating.sport == sport,
        PlayerRating.match_format == match_format,
    ).first()

    if not p1 or not p2:
        setattr(match, "status", "completed")
        setattr(match, "winner_id", data.winner_id)
        setattr(match, "completed_at", datetime.now(timezone.utc))
        if match.court_id is not None:
            court = db.query(Court).filter(Court.id == match.court_id).first()
            if court:
                setattr(court, "status", "available")
        db.commit()
        _broadcast(match_id, {"type": "match_completed", "winner_id": data.winner_id})
        return {"message": "Match completed (ratings not updated — missing rating rows).", "winner_id": data.winner_id}

    # Compute Glicko-2 in Python
    p1_wins = (player1_id == data.winner_id)
    new_p1_r, new_p1_rd, new_p1_vol = glicko_update(
        rating=float(p1.rating), rd=float(p1.rating_deviation), volatility=float(p1.volatility),  # type: ignore[arg-type]
        opp_rating=float(p2.rating), opp_rd=float(p2.rating_deviation),  # type: ignore[arg-type]
        score=1.0 if p1_wins else 0.0,
    )
    new_p2_r, new_p2_rd, new_p2_vol = glicko_update(
        rating=float(p2.rating), rd=float(p2.rating_deviation), volatility=float(p2.volatility),  # type: ignore[arg-type]
        opp_rating=float(p1.rating), opp_rd=float(p1.rating_deviation),  # type: ignore[arg-type]
        score=0.0 if p1_wins else 1.0,
    )

    # Atomic DB operations via stored procedure:
    # marks match completed, releases court, updates both ratings,
    # grants referee boost, advances tournament bracket — all in one transaction
    db.execute(text("""
        SELECT fn_complete_match(
            :mid::uuid, :winner::uuid,
            :r1, :rd1, :vol1,
            :r2, :rd2, :vol2
        )
    """), {
        "mid":    match_id,
        "winner": data.winner_id,
        "r1":  new_p1_r,  "rd1": new_p1_rd,  "vol1": new_p1_vol,
        "r2":  new_p2_r,  "rd2": new_p2_rd,  "vol2": new_p2_vol,
    })
    db.commit()

    try:
        save_training_row(match_id)
    except Exception:
        pass

    _broadcast(match_id, {"type": "match_completed", "winner_id": data.winner_id})
    return {"message": "Match completed.", "winner_id": data.winner_id}


# ── Sport ruleset ─────────────────────────────────────────────────────────────

@router.get("/{match_id}/ruleset")
def get_match_ruleset(
    match_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    match = db.query(Match).filter(Match.id == match_id).first()
    if not match:
        raise HTTPException(404, "Match not found.")
    ruleset = get_ruleset(match.sport.value)
    if not ruleset:
        raise HTTPException(404, "No ruleset found for this sport.")
    return {"sport": match.sport.value, "ruleset": ruleset}


# ── Referee: record point ─────────────────────────────────────────────────────

class RecordPointRequest(BaseModel):
    team: str                              # "team1" | "team2"
    set_number: int
    attribution_type: Optional[str] = None # "winning_shot" | "opponent_error" | "other"
    player_id: Optional[str] = None        # scorer (winning_shot) or None
    cause: Optional[str] = None            # scoring cause label (winning_shot)
    actor_player_id: Optional[str] = None  # opponent who committed error (opponent_error)
    reason_code: Optional[str] = None      # error code e.g. SERVICE_FAULT (opponent_error)
    notes: Optional[str] = None
    client_action_id: Optional[str] = None # idempotency key from offline queue


def _can_score(match: Match, user_id: str) -> bool:
    all_players = [str(pid) for pid in [match.player1_id, match.player2_id, match.player3_id, match.player4_id] if pid is not None]
    is_referee   = match.referee_id is not None and str(match.referee_id) == user_id
    is_part      = user_id in all_players
    has_referee  = match.referee_id is not None
    return is_referee or (not has_referee and is_part)


@router.post("/{match_id}/point", status_code=201)
def record_point(
    match_id: str,
    data: RecordPointRequest,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    match = db.query(Match).filter(Match.id == match_id).first()
    if not match:
        raise HTTPException(404, "Match not found.")
    if match.status.value != "ongoing":
        raise HTTPException(400, "Match is not ongoing.")

    user_id = current_user["id"]
    if not _can_score(match, user_id):
        raise HTTPException(403, "Not authorized to record points.")

    # ── Idempotency — skip duplicate offline replay ────────────────────────────
    if data.client_action_id:
        existing = db.query(MatchHistory).filter(
            MatchHistory.match_id == match_id,
            MatchHistory.meta["client_action_id"].astext == data.client_action_id,
        ).first()
        if existing:
            return {"message": "Already recorded.", "event_id": str(existing.id)}

    # ── Attribution validation ────────────────────────────────────────────────
    if data.attribution_type == "winning_shot":
        if not data.player_id:
            raise HTTPException(400, "Winning shot requires player_id (the scorer).")
        if not data.cause:
            raise HTTPException(400, "Winning shot requires cause (the shot type).")
    elif data.attribution_type == "opponent_error":
        if not data.actor_player_id:
            raise HTTPException(400, "Opponent error requires actor_player_id (who committed the error).")
        if not data.reason_code:
            raise HTTPException(400, "Opponent error requires reason_code (the error type).")
    elif data.attribution_type == "other":
        if not data.cause and not data.notes:
            raise HTTPException(400, "Attribution type 'other' requires a cause or notes explaining the point.")
    # ─────────────────────────────────────────────────────────────────────────

    if data.team not in ("team1", "team2"):
        raise HTTPException(400, "team must be 'team1' or 'team2'.")

    match_set = db.query(MatchSet).filter(
        MatchSet.match_id == match_id, MatchSet.set_number == data.set_number,
    ).first()
    if not match_set:
        match_set = MatchSet(
            match_id=match_id, set_number=data.set_number,
            player1_score=0, player2_score=0, team1_score=0, team2_score=0,
        )
        db.add(match_set)
        db.flush()

    t1 = int(match_set.team1_score or match_set.player1_score or 0)  # type: ignore[arg-type]
    t2 = int(match_set.team2_score or match_set.player2_score or 0)  # type: ignore[arg-type]

    if data.team == "team1":
        t1 += 1
        setattr(match_set, "team1_score", t1); setattr(match_set, "player1_score", t1)
    else:
        t2 += 1
        setattr(match_set, "team2_score", t2); setattr(match_set, "player2_score", t2)
    db.flush()

    team_label = "Team 1" if data.team == "team1" else "Team 2"

    if data.attribution_type == "opponent_error" and data.reason_code:
        # Get actor player username for description
        actor_label = ""
        if data.actor_player_id:
            actor = db.query(Profile).filter(Profile.id == data.actor_player_id).first()
            actor_label = f" by @{actor.username}" if actor and actor.username is not None else ""
        description = f"Point → {team_label} · {data.reason_code.replace('_', ' ').title()}{actor_label} (opponent error)"
    elif data.attribution_type == "winning_shot" and data.cause:
        scorer_label = ""
        if data.player_id:
            scorer = db.query(Profile).filter(Profile.id == data.player_id).first()
            scorer_label = f" by @{scorer.username}" if scorer and scorer.username is not None else ""
        description = f"Point → {team_label} · {data.cause}{scorer_label}"
    else:
        description = f"Point → {team_label}" + (f" ({data.cause})" if data.cause else "")

    meta: dict = {"attribution_type": data.attribution_type or "other"}
    if data.cause:             meta["cause"]             = data.cause
    if data.reason_code:       meta["reason_code"]       = data.reason_code
    if data.actor_player_id:   meta["actor_player_id"]   = data.actor_player_id
    if data.notes:             meta["notes"]             = data.notes
    if data.client_action_id:  meta["client_action_id"]  = data.client_action_id

    db.add(MatchHistory(
        match_id=match_id, event_type="point", team=data.team,
        player_id=data.player_id, recorded_by=user_id,
        description=description, set_number=data.set_number,
        team1_score=t1, team2_score=t2,
        meta=meta,
    ))
    db.commit()

    # ── Sport-rule enforcement ────────────────────────────────────────────────
    ruleset    = get_ruleset(match.sport.value)
    set_winner = None
    next_set   = None
    match_winner_team = None

    if ruleset:
        pts_to_win = ruleset.get("points_per_set") or ruleset.get("games_per_set") or 21
        win_by     = ruleset.get("win_by", 2)
        max_pts    = ruleset.get("max_points")  # None means no cap

        # Determine if current set is won
        if t1 >= pts_to_win and t1 - t2 >= win_by:
            set_winner = "team1"
        elif t2 >= pts_to_win and t2 - t1 >= win_by:
            set_winner = "team2"
        elif max_pts and (t1 >= max_pts or t2 >= max_pts):
            set_winner = "team1" if t1 > t2 else "team2"

        if set_winner:
            # Count sets won from all completed sets
            all_sets_now = db.query(MatchSet).filter(MatchSet.match_id == match_id).order_by(MatchSet.set_number).all()
            sets_to_win  = ruleset.get("sets_to_win", 2)

            def _set_winner_team(s: MatchSet) -> str | None:
                s1 = int(s.team1_score or s.player1_score or 0)  # type: ignore[arg-type]
                s2 = int(s.team2_score or s.player2_score or 0)  # type: ignore[arg-type]
                if s1 >= pts_to_win and s1 - s2 >= win_by: return "team1"
                if s2 >= pts_to_win and s2 - s1 >= win_by: return "team2"
                if max_pts and (s1 >= max_pts or s2 >= max_pts):
                    return "team1" if s1 > s2 else "team2"
                return None

            t1_sets = sum(1 for s in all_sets_now if _set_winner_team(s) == "team1")
            t2_sets = sum(1 for s in all_sets_now if _set_winner_team(s) == "team2")

            if t1_sets >= sets_to_win:
                match_winner_team = "team1"
            elif t2_sets >= sets_to_win:
                match_winner_team = "team2"
            else:
                # Auto-create next set
                next_set = data.set_number + 1
                db.add(MatchSet(
                    match_id=match_id, set_number=next_set,
                    player1_score=0, player2_score=0,
                    team1_score=0, team2_score=0,
                ))
                db.commit()

    all_sets = db.query(MatchSet).filter(MatchSet.match_id == match_id).order_by(MatchSet.set_number).all()
    broadcast_payload: dict = {
        "type": "sets_update",
        "sets": [
            {"set_number": s.set_number,
             "player1_score": s.player1_score, "player2_score": s.player2_score,
             "team1_score": s.team1_score,     "team2_score": s.team2_score}
            for s in all_sets
        ],
        "last_event": {"type": "point", "team": data.team, "description": description},
    }
    if set_winner:
        broadcast_payload["set_winner"] = set_winner
        broadcast_payload["set_number_won"] = data.set_number
        if next_set:
            broadcast_payload["next_set"] = next_set
    if match_winner_team:
        broadcast_payload["match_winner_team"] = match_winner_team
        broadcast_payload["winner_id"] = str(match.player1_id) if match_winner_team == "team1" else str(match.player2_id)

    _broadcast(match_id, broadcast_payload)
    return {
        "message": "Point recorded.",
        "team1_score": t1, "team2_score": t2, "set_number": data.set_number,
        "set_winner": set_winner, "next_set": next_set,
        "match_winner_team": match_winner_team,
    }


# ── Referee: record violation ─────────────────────────────────────────────────

class RecordViolationRequest(BaseModel):
    player_id: str
    violation_code: str
    set_number: int
    award_point_to: Optional[str] = None  # "team1" | "team2" | null
    notes: Optional[str] = None
    client_action_id: Optional[str] = None  # idempotency key from offline queue


@router.post("/{match_id}/violation", status_code=201)
def record_violation(
    match_id: str,
    data: RecordViolationRequest,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    match = db.query(Match).filter(Match.id == match_id).first()
    if not match:
        raise HTTPException(404, "Match not found.")
    if match.status.value != "ongoing":
        raise HTTPException(400, "Match is not ongoing.")

    user_id = current_user["id"]
    if not _can_score(match, user_id):
        raise HTTPException(403, "Not authorized to record violations.")

    # ── Idempotency — skip duplicate offline replay ────────────────────────────
    if data.client_action_id:
        existing = db.query(MatchHistory).filter(
            MatchHistory.match_id == match_id,
            MatchHistory.meta["client_action_id"].astext == data.client_action_id,
        ).first()
        if existing:
            return {"message": "Already recorded.", "event_id": str(existing.id)}

    violator     = db.query(Profile).filter(Profile.id == data.player_id).first()
    violator_name = f"@{violator.username}" if violator else "Unknown"

    t1 = t2 = None
    if data.award_point_to in ("team1", "team2"):
        match_set = db.query(MatchSet).filter(
            MatchSet.match_id == match_id, MatchSet.set_number == data.set_number,
        ).first()
        if match_set:
            t1 = int(match_set.team1_score or match_set.player1_score or 0)  # type: ignore[arg-type]
            t2 = int(match_set.team2_score or match_set.player2_score or 0)  # type: ignore[arg-type]
            if data.award_point_to == "team1":
                t1 += 1
                setattr(match_set, "team1_score", t1); setattr(match_set, "player1_score", t1)
            else:
                t2 += 1
                setattr(match_set, "team2_score", t2); setattr(match_set, "player2_score", t2)
            db.flush()

    pt_label    = f" → point to {'Team 1' if data.award_point_to == 'team1' else 'Team 2'}" if data.award_point_to else ""
    description = f"Violation: {data.violation_code} by {violator_name}{pt_label}"

    db.add(MatchHistory(
        match_id=match_id, event_type="violation", team=data.award_point_to,
        player_id=data.player_id, recorded_by=user_id,
        description=description, set_number=data.set_number,
        team1_score=t1, team2_score=t2,
        meta={"violation_code": data.violation_code, "notes": data.notes, **({"client_action_id": data.client_action_id} if data.client_action_id else {})},
    ))
    db.commit()

    _broadcast(match_id, {"type": "violation", "description": description, "award_point_to": data.award_point_to})
    return {"message": "Violation recorded.", "description": description}


# ── Referee: undo last point ──────────────────────────────────────────────────

@router.post("/{match_id}/undo")
def undo_last_point(
    match_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    match = db.query(Match).filter(Match.id == match_id).first()
    if not match:
        raise HTTPException(404, "Match not found.")
    if match.status.value != "ongoing":
        raise HTTPException(400, "Match is not ongoing.")

    user_id = current_user["id"]
    if not _can_score(match, user_id):
        raise HTTPException(403, "Not authorized to undo.")

    last = db.query(MatchHistory).filter(
        MatchHistory.match_id == match_id,
        MatchHistory.event_type.in_(["point", "violation"]),
        MatchHistory.team.isnot(None),
    ).order_by(MatchHistory.created_at.desc()).first()

    if not last:
        raise HTTPException(400, "No points to undo.")

    match_set = db.query(MatchSet).filter(
        MatchSet.match_id == match_id, MatchSet.set_number == last.set_number,
    ).first()
    if match_set and last.team is not None:
        if str(last.team) == "team1":
            new_s = max(0, int(match_set.team1_score or match_set.player1_score or 1) - 1)  # type: ignore[arg-type]
            setattr(match_set, "team1_score", new_s); setattr(match_set, "player1_score", new_s)
        else:
            new_s = max(0, int(match_set.team2_score or match_set.player2_score or 1) - 1)  # type: ignore[arg-type]
            setattr(match_set, "team2_score", new_s); setattr(match_set, "player2_score", new_s)

    db.add(MatchHistory(
        match_id=match_id, event_type="undo", recorded_by=user_id,
        description=f"Undid: {last.description}", set_number=last.set_number,
    ))
    db.delete(last)
    db.commit()

    all_sets = db.query(MatchSet).filter(MatchSet.match_id == match_id).order_by(MatchSet.set_number).all()
    _broadcast(match_id, {
        "type": "sets_update",
        "sets": [{"set_number": s.set_number, "player1_score": s.player1_score, "player2_score": s.player2_score} for s in all_sets],
        "last_event": {"type": "undo"},
    })
    return {"message": "Last point undone."}


# ── Match history timeline ────────────────────────────────────────────────────

@router.get("/{match_id}/history")
def get_match_history(
    match_id: str,
    limit: int = 30,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    entries = db.query(MatchHistory).filter(
        MatchHistory.match_id == match_id,
    ).order_by(MatchHistory.created_at.desc()).limit(limit).all()

    return {"history": [
        {
            "id":          str(e.id),
            "event_type":  e.event_type,
            "team":        e.team,
            "player_id":   str(e.player_id)   if e.player_id   is not None else None,
            "recorded_by": str(e.recorded_by) if e.recorded_by is not None else None,
            "description": e.description,
            "set_number":  e.set_number,
            "team1_score": e.team1_score,
            "team2_score": e.team2_score,
            "meta":        e.meta,
            "created_at":  str(e.created_at),
        }
        for e in reversed(entries)
    ]}
