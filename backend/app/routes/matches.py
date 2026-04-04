from fastapi import APIRouter, HTTPException, Depends, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, timezone, timedelta
import asyncio
import json
from sqlalchemy.orm import Session
from sqlalchemy import or_, and_, text
from sqlalchemy.exc import IntegrityError
from app.database import get_db, SessionLocal
from app.middleware.auth import get_current_user
from app.models.models import (
    Match, MatchSet, MatchAcceptance, RallyEvent,
    PlayerRating, Profile, MatchHistory,
    Club, ClubMember, Court,
    Tournament, TournamentRegistration, TournamentGroupStanding,
    Party,
)
from app.services.sport_rulesets import get_ruleset
from app.utils.glicko2 import update as glicko_update
from app.config import settings
from app.services.training_data_collector import save_training_row
from app.services.matchmaking import find_best_opponent, get_model_info, score_candidate, run_matchmaking
from app.services.broadcast import broadcast_match as _broadcast
from app.services.notifications import send_notification


# ── Internal helpers ──────────────────────────────────────────────────────────

def _fetch_h2h_counts(db: Session, user_id: str, opponent_ids: list) -> dict:
    """Return {opponent_id: count} of completed 1v1 matches the user has played vs each opponent."""
    if not opponent_ids:
        return {}
    rows = db.query(Match).filter(
        Match.status == "completed",
        Match.match_format == "singles",
        or_(
            and_(Match.player1_id == user_id, Match.player2_id.in_(opponent_ids)),
            and_(Match.player2_id == user_id, Match.player1_id.in_(opponent_ids)),
        ),
    ).all()
    counts: dict = {}
    for m in rows:
        opp = str(m.player2_id) if str(m.player1_id) == user_id else str(m.player1_id)
        counts[opp] = counts.get(opp, 0) + 1
    return counts


def _update_activeness(db: Session, user_id: str, sport: str, match_format: str) -> None:
    """Recalculate activeness_score from matches played in the last 30 days."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=30)
    recent = db.query(Match).filter(
        Match.status == "completed",
        Match.completed_at >= cutoff,
        or_(
            Match.player1_id == user_id,
            Match.player2_id == user_id,
            Match.player3_id == user_id,
            Match.player4_id == user_id,
        ),
    ).count()
    # 15 matches/month → full activeness (1.0)
    new_score = round(min(1.0, recent / 15.0), 4)
    rating = db.query(PlayerRating).filter(
        PlayerRating.user_id == user_id,
        PlayerRating.sport == sport,
        PlayerRating.match_format == match_format,
    ).first()
    if rating:
        setattr(rating, "activeness_score", new_score)


def _ensure_rating_row(db: Session, user_id: str, sport: str, match_format: str) -> PlayerRating:
    """Get or create a PlayerRating row so match completion can always update progression."""
    row = db.query(PlayerRating).filter(
        PlayerRating.user_id == user_id,
        PlayerRating.sport == sport,
        PlayerRating.match_format == match_format,
    ).first()
    if row:
        return row

    row = PlayerRating(
        user_id=user_id,
        sport=sport,
        match_format=match_format,
        rating=1500,
        rating_deviation=350,
        volatility=0.06,
        matches_played=0,
        wins=0,
        losses=0,
        current_win_streak=0,
        current_loss_streak=0,
        rating_status="CALIBRATING",
        calibration_matches_played=0,
        is_leaderboard_eligible=False,
    )
    try:
        with db.begin_nested():
            db.add(row)
            db.flush()
    except IntegrityError:
        # Another concurrent request created the row first.
        row = db.query(PlayerRating).filter(
            PlayerRating.user_id == user_id,
            PlayerRating.sport == sport,
            PlayerRating.match_format == match_format,
        ).first()
        if row:
            return row
        raise
    return row


def _apply_rating_result(
    row: PlayerRating,
    *,
    won: bool,
    new_rating: float,
    new_rd: float,
    new_vol: float,
) -> None:
    """Apply rating + progression updates to a single PlayerRating row."""
    setattr(row, "rating", new_rating)
    setattr(row, "rating_deviation", new_rd)
    setattr(row, "volatility", new_vol)

    matches_played = int(getattr(row, "matches_played", 0) or 0)
    wins = int(getattr(row, "wins", 0) or 0)
    losses = int(getattr(row, "losses", 0) or 0)
    current_win_streak = int(getattr(row, "current_win_streak", 0) or 0)
    current_loss_streak = int(getattr(row, "current_loss_streak", 0) or 0)
    calibration_matches = int(getattr(row, "calibration_matches_played", 0) or 0)

    setattr(row, "matches_played", matches_played + 1)
    setattr(row, "wins", wins + (1 if won else 0))
    setattr(row, "losses", losses + (0 if won else 1))
    setattr(row, "current_win_streak", (current_win_streak + 1) if won else 0)
    setattr(row, "current_loss_streak", 0 if won else (current_loss_streak + 1))

    calibration_matches += 1
    setattr(row, "calibration_matches_played", calibration_matches)
    if str(getattr(row, "rating_status", "")) == "CALIBRATING" and calibration_matches >= 10:
        setattr(row, "rating_status", "RATED")
        setattr(row, "is_leaderboard_eligible", True)
        if getattr(row, "calibration_completed_at", None) is None:
            setattr(row, "calibration_completed_at", datetime.now(timezone.utc))

    setattr(row, "updated_at", datetime.now(timezone.utc))


def _canonicalize_doubles_slots(match: Match) -> None:
    """Normalize doubles player slots:
    player1/team1 captain, player2/team2 captain, player3/team1 partner, player4/team2 partner.
    """
    if match.team1_player1 is None or match.team2_player1 is None:
        return

    setattr(match, "player1_id", match.team1_player1)
    setattr(match, "player2_id", match.team2_player1)
    setattr(match, "player3_id", match.team1_player2)
    setattr(match, "player4_id", match.team2_player2)


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
    match_mode:         str           = "quick"  # quick | ranked | club
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

    # ── ML pairing quality check ───────────────────────────────────────────────
    p1_rating = db.query(PlayerRating).filter(
        PlayerRating.user_id == player1_id,
        PlayerRating.sport   == data.sport,
        PlayerRating.match_format == data.match_format,
    ).first()
    p2_rating = db.query(PlayerRating).filter(
        PlayerRating.user_id == data.opponent_id,
        PlayerRating.sport   == data.sport,
        PlayerRating.match_format == data.match_format,
    ).first()
    p1_profile = db.query(Profile).filter(Profile.id == player1_id).first()
    p2_profile = db.query(Profile).filter(Profile.id == data.opponent_id).first()

    ml_score = score_candidate(
        rating_a     = float(p1_rating.rating)          if p1_rating else 1500.0,  # type: ignore[arg-type]
        rd_a         = float(p1_rating.rating_deviation) if p1_rating else 200.0,  # type: ignore[arg-type]
        win_rate_a   = _safe_win_rate(p1_rating),
        activeness_a = float(p1_rating.activeness_score) if p1_rating else 0.5,  # type: ignore[arg-type]
        streak_a     = int(p1_rating.current_win_streak) if p1_rating else 0,  # type: ignore[arg-type]
        city_a       = str(p1_profile.city_mun_code)  if p1_profile and p1_profile.city_mun_code  is not None else None,  # type: ignore[arg-type]
        province_a   = str(p1_profile.province_code)  if p1_profile and p1_profile.province_code  is not None else None,  # type: ignore[arg-type]
        region_a     = str(p1_profile.region_code)    if p1_profile and p1_profile.region_code    is not None else None,  # type: ignore[arg-type]
        rating_b     = float(p2_rating.rating)          if p2_rating else 1500.0,  # type: ignore[arg-type]
        rd_b         = float(p2_rating.rating_deviation) if p2_rating else 200.0,  # type: ignore[arg-type]
        win_rate_b   = _safe_win_rate(p2_rating),
        activeness_b = float(p2_rating.activeness_score) if p2_rating else 0.5,  # type: ignore[arg-type]
        streak_b     = int(p2_rating.current_win_streak) if p2_rating else 0,  # type: ignore[arg-type]
        city_b       = str(p2_profile.city_mun_code)  if p2_profile and p2_profile.city_mun_code  is not None else None,  # type: ignore[arg-type]
        province_b   = str(p2_profile.province_code)  if p2_profile and p2_profile.province_code  is not None else None,  # type: ignore[arg-type]
        region_b     = str(p2_profile.region_code)    if p2_profile and p2_profile.region_code    is not None else None,  # type: ignore[arg-type]
        sport        = data.sport,
        match_format = data.match_format,
        wait_seconds = 0,
        h2h_count    = _fetch_h2h_counts(db, player1_id, [data.opponent_id]).get(data.opponent_id, 0),
    )

    balance_label = (
        "Excellent"  if ml_score >= 0.85 else
        "Good"       if ml_score >= 0.70 else
        "Average"    if ml_score >= 0.50 else
        "Uneven"
    )
    # ─────────────────────────────────────────────────────────────────────────

    match = Match(
        sport=data.sport, match_type="friendly",
        match_format=data.match_format, status="pending",
        player1_id=player1_id, player2_id=data.opponent_id,
        ml_match_score=ml_score,
    )
    db.add(match)
    db.flush()
    db.add(MatchSet(match_id=match.id, set_number=1, player1_score=0, player2_score=0))
    db.commit()

    return {
        "message":       "Friendly match created.",
        "match_id":      str(match.id),
        "ml_score":      ml_score,
        "balance":       balance_label,
        "balance_warn":  balance_label == "Uneven",
    }


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

    # ── Auto-invalidate stale ongoing matches the user abandoned ─────────────
    # Ongoing queue matches with no scores and started > 30 min ago are dead.
    stale_cutoff = datetime.now(timezone.utc) - timedelta(minutes=30)
    stale_matches = db.query(Match).filter(
        Match.match_type.in_(["queue", "ranked"]),
        Match.sport == data.sport,
        Match.status == "ongoing",
        Match.winner_id.is_(None),
        or_(
            Match.player1_id == user_id, Match.player2_id == user_id,
            Match.player3_id == user_id, Match.player4_id == user_id,
        ),
        Match.started_at < stale_cutoff,
    ).all()
    invalidated_any = False
    for stale in stale_matches:
        has_scores = db.query(MatchSet).filter(
            MatchSet.match_id == stale.id,
            or_(
                MatchSet.player1_score > 0, MatchSet.player2_score > 0,
                MatchSet.team1_score > 0,   MatchSet.team2_score > 0,
            ),
        ).first()
        if not has_scores:
            setattr(stale, "status", "invalidated")
            db.query(MatchHistory).filter(MatchHistory.match_id == stale.id).delete(synchronize_session=False)
            linked_parties = db.query(Party).filter(Party.match_id == stale.id).all()
            for linked_party in linked_parties:
                setattr(linked_party, "status", "disbanded")
                setattr(linked_party, "match_id", None)
            _broadcast(str(stale.id), {"type": "match_invalidated", "match_id": str(stale.id)})
            invalidated_any = True
    if invalidated_any:
        db.commit()
    # ─────────────────────────────────────────────────────────────────────────

    # Hard guard: if a live queue/ranked match still exists for this sport/format, return it.
    # This avoids URL thrash from creating a new queue while a previous live match remains active.
    active_live = db.query(Match).filter(
        Match.match_type.in_(["queue", "ranked"]),
        Match.sport == data.sport,
        Match.match_format == data.match_format,
        Match.status.in_(["ongoing", "pending_approval"]),
        Match.winner_id.is_(None),
        or_(
            Match.player1_id == user_id, Match.player2_id == user_id,
            Match.player3_id == user_id, Match.player4_id == user_id,
        ),
    ).order_by(Match.created_at.desc()).first()
    if active_live:
        status_val = active_live.status.value if hasattr(active_live.status, "value") else str(active_live.status)
        return {
            "status": "matched",
            "message": "You already have an active match for this queue.",
            "match_id": str(active_live.id),
            "pending_approval": status_val == "pending_approval",
        }

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

    # ── Validate match_mode ────────────────────────────────────────────────────
    match_mode = data.match_mode if data.match_mode in ("quick", "ranked", "club") else "quick"
    # club mode requires a preferred_club_id
    if match_mode == "club" and not preferred_club_id:
        raise HTTPException(400, "match_mode='club' requires preferred_club_id.")

    # ── Singles ───────────────────────────────────────────────────────────────
    if data.match_format == "singles":
        my_rating  = db.query(PlayerRating).filter(
            PlayerRating.user_id == user_id,
            PlayerRating.sport == data.sport,
            PlayerRating.match_format == data.match_format,
        ).first()
        my_profile = db.query(Profile).filter(Profile.id == user_id).first()

        # Ranked mode: only RATED players may enter the ranked queue
        if match_mode == "ranked":
            if not my_rating or str(my_rating.rating_status) != "RATED":
                raise HTTPException(400, "You must complete calibration (10 matches) before joining the ranked queue.")

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

        # Ranked queue uses match_type="ranked"; quick/club use "queue"
        db_match_type = "ranked" if match_mode == "ranked" else "queue"

        queue_q = db.query(Match).filter(
            Match.match_type == db_match_type,
            Match.sport == data.sport,
            Match.match_format == data.match_format,
            Match.status == "pending",
            Match.player2_id.is_(None),
            Match.player1_id != user_id,
        )
        # Club mode: only match within the selected club
        if preferred_club_id:
            queue_q = queue_q.filter(Match.club_id == preferred_club_id)
        queue_matches = queue_q.all()

        now_utc = datetime.now(timezone.utc)

        # ── Fetch H2H counts for all candidates in one query ──────────────────
        candidate_player_ids = [str(q.player1_id) for q in queue_matches if q.player1_id is not None]
        h2h_map = _fetch_h2h_counts(db, user_id, candidate_player_ids)
        # ─────────────────────────────────────────────────────────────────────

        candidates = []
        for queued in queue_matches:
            opp_r = db.query(PlayerRating).filter(
                PlayerRating.user_id == queued.player1_id,
                PlayerRating.sport == data.sport,
                PlayerRating.match_format == data.match_format,
            ).first()
            opp_p = db.query(Profile).filter(Profile.id == queued.player1_id).first()

            # Ranked mode: skip unrated candidates
            if match_mode == "ranked" and (not opp_r or str(opp_r.rating_status) != "RATED"):
                continue

            # Actual wait time for this queued player
            q_wait = int((now_utc - queued.created_at.replace(tzinfo=timezone.utc)).total_seconds())
            # Boost if this candidate has an active referee boost
            if opp_p and opp_p.referee_boost_until is not None and opp_p.referee_boost_until.replace(tzinfo=timezone.utc) > now_utc:
                q_wait = max(q_wait, 900)
            # Use stored queue location snapshot; fall back to live profile if missing
            cand_city     = queued.queue_city_code     or (opp_p.city_mun_code  if opp_p else None)
            cand_province = queued.queue_province_code or (opp_p.province_code  if opp_p else None)
            cand_region   = queued.queue_region_code   or (opp_p.region_code    if opp_p else None)
            opp_id        = str(queued.player1_id)
            candidates.append({
                "player_id":          opp_id,
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
                "h2h_count":          h2h_map.get(opp_id, 0),  # real H2H count
            })

        # Check if joining player has an active referee priority boost
        my_boost = my_profile is not None and my_profile.referee_boost_until is not None and \
                   my_profile.referee_boost_until.replace(tzinfo=timezone.utc) > now_utc
        effective_wait = 900 if my_boost else 0

        best = find_best_opponent(
            player=my_player, candidates=candidates,
            sport=data.sport, match_format=data.match_format,
            wait_seconds=effective_wait, mode=match_mode,
        )

        if best:
            found = db.query(Match).filter(Match.id == best["match_id"]).first()
            if found:
                setattr(found, "player2_id", user_id)
                setattr(found, "status", "ongoing")
                setattr(found, "started_at", datetime.now(timezone.utc))
                setattr(found, "ml_match_score", best.get("_ml_score"))  # save ML score
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
                    "ml_score": best.get("_ml_score"),
                    "court_assigned": bool(found.court_id),
                    "pending_approval": needs_approval,
                }

        new_match = Match(
            sport=data.sport, match_type=db_match_type,
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

    def get_match_players(m):
        return [pid for pid in [m.player1_id, m.player2_id, m.player3_id, m.player4_id] if pid]

    candidates = [
        m for m in assembling
        if user_id not in [str(pid) for pid in get_match_players(m)]
    ]
    # Prioritize matches that are almost full
    candidates.sort(key=lambda m: len(get_match_players(m)), reverse=True)

    for candidate in candidates:
        players = get_match_players(candidate)
        count = len(players)
        
        if count == 3:
            # 4th player joins — time to balance teams!
            all_player_ids = [str(pid) for pid in players] + [str(user_id)]
            
            # Fetch ratings for all 4 players to run balancing
            player_stats = []
            for pid in all_player_ids:
                r = db.query(PlayerRating).filter(
                    PlayerRating.user_id == pid,
                    PlayerRating.sport == data.sport,
                    PlayerRating.match_format == data.match_format,
                ).first()
                p = db.query(Profile).filter(Profile.id == pid).first()
                player_stats.append({
                    "player_id": pid,
                    "rating": float(r.rating) if r else 1200.0,  # type: ignore[arg-type]
                    "rating_deviation": float(r.rating_deviation) if r else 200.0,  # type: ignore[arg-type]
                    "win_rate": _safe_win_rate(r),
                    "activeness_score": float(r.activeness_score) if r else 0.5,  # type: ignore[arg-type]
                    "current_streak": int(r.current_win_streak) if r else 0,  # type: ignore[arg-type]
                    "city_code": p.city_mun_code if p else None,
                    "province_code": p.province_code if p else None,
                    "region_code": p.region_code if p else None,
                })

            # Run balancing service
            best_split = run_matchmaking(player_stats, data.sport, data.match_format)
            
            if best_split:
                # Update match with optimal team assignments
                team_a = best_split["team_a"]
                team_b = best_split["team_b"]

                # Canonical doubles mapping:
                # team1 = player1 + player3, team2 = player2 + player4.
                setattr(candidate, "player1_id", team_a[0]["player_id"])  # team1 captain
                setattr(candidate, "player2_id", team_b[0]["player_id"])  # team2 captain
                setattr(candidate, "player3_id", team_a[1]["player_id"])  # team1 partner
                setattr(candidate, "player4_id", team_b[1]["player_id"])  # team2 partner
                setattr(candidate, "team1_player1", team_a[0]["player_id"])
                setattr(candidate, "team1_player2", team_a[1]["player_id"])
                setattr(candidate, "team2_player1", team_b[0]["player_id"])
                setattr(candidate, "team2_player2", team_b[1]["player_id"])
                
                setattr(candidate, "status", "ongoing")
                setattr(candidate, "started_at", datetime.now(timezone.utc))
                setattr(candidate, "ml_match_score", best_split["score"])  # save ML score
                db.add(MatchSet(match_id=candidate.id, set_number=1, player1_score=0, player2_score=0))
                
                # Auto-assign court if club set
                needs_approval = False
                club_id = candidate.club_id if candidate.club_id is not None else preferred_club_id
                if club_id is not None and candidate.court_id is None:
                    court_q = db.query(Court).filter(
                        Court.club_id == str(club_id),
                        Court.status == "available",
                    )
                    if data.preferred_indoor is not None:
                        court_q = court_q.filter(Court.is_indoor == data.preferred_indoor)
                    avail = court_q.first()
                    if avail:
                        setattr(candidate, "court_id", avail.id)
                        setattr(candidate, "club_id",  avail.club_id)
                        setattr(avail, "status", "occupied")
                        club_obj = db.query(Club).filter(Club.id == avail.club_id).first()
                        if club_obj and str(club_obj.approval_mode) == "manual":
                            setattr(candidate, "status", "pending_approval")
                            needs_approval = True
                            _notify_duty_holders(db, avail.club_id, candidate.id, str(club_obj.name))
                
                db.commit()
                return {
                    "status": "matched",
                    "message": "Balanced teams found! Awaiting club confirmation." if needs_approval else "Balanced teams found!",
                    "match_id": str(candidate.id),
                    "players_joined": 4,
                    "pending_approval": needs_approval,
                    "split_score": best_split["score"],
                }

        elif count < 3:
            # Just fill the next available slot
            slots = ["player1_id", "player2_id", "player3_id", "player4_id"]
            for slot in slots:
                if getattr(candidate, slot) is None:
                    setattr(candidate, slot, user_id)
                    db.commit()
                    return {
                        "status": "assembling", 
                        "message": f"Joined! Waiting for {3-count} more player{'s' if 3-count > 1 else ''}.", 
                        "match_id": str(candidate.id), 
                        "players_joined": count + 1
                    }

    # No suitable assembling match found — create a new one
    new_match = Match(
        sport=data.sport, match_type="queue",
        match_format=data.match_format, status="assembling",
        player1_id=user_id,
        club_id=preferred_club_id,
    )
    db.add(new_match)
    db.commit()
    return {"status": "assembling", "message": "Started a new queue.", "match_id": str(new_match.id), "players_joined": 1}


@router.get("/queue/me")
def get_my_queue(
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Returns the current user's active queue entry (any sport/format) without needing params."""
    user_id = current_user["id"]

    match = db.query(Match).filter(
        Match.match_type == "queue",
        Match.status.in_(["pending", "assembling"]),
        Match.winner_id.is_(None),
        or_(
            Match.player1_id == user_id, Match.player2_id == user_id,
            Match.player3_id == user_id, Match.player4_id == user_id,
        ),
    ).order_by(Match.created_at.desc()).first()

    if not match:
        return {"in_queue": False}

    players_joined = sum(
        1 for pid in [match.player1_id, match.player2_id, match.player3_id, match.player4_id]
        if pid is not None
    )

    return {
        "in_queue":      True,
        "sport":         match.sport.value,
        "match_format":  match.match_format.value,
        "status":        match.status.value,   # "pending" | "assembling"
        "players_joined": players_joined,
        "queued_at":     match.created_at.isoformat() if match.created_at is not None else None,  # type: ignore[union-attr]
    }


@router.get("/queue/status")
def get_queue_status(
    sport: str,
    match_format: str = "singles",
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]

    if match_format == "singles":
        # Include both player slots for live states so player2 can't accidentally requeue.
        match = db.query(Match).filter(
            Match.match_type == "queue", Match.sport == sport,
            Match.match_format == match_format,
            Match.status.in_(["pending", "pending_approval", "ongoing"]),
            Match.winner_id.is_(None),
            or_(
                Match.player1_id == user_id,
                and_(Match.player2_id == user_id, Match.status.in_(["pending_approval", "ongoing"])),
            ),
        ).order_by(Match.created_at.desc()).first()
        if not match:
            return {"status": "not_in_queue"}
        status_val = match.status.value if hasattr(match.status, "value") else str(match.status)
        if status_val == "invalidated":
            return {"status": "not_in_queue"}
        if status_val in ("ongoing", "pending_approval") or match.player2_id is not None:
            return {"status": "matched", "match_id": str(match.id),
                    "pending_approval": status_val == "pending_approval"}
        return {"status": "waiting", "match_id": str(match.id)}

    # Doubles — include "ongoing" / "pending_approval" so players get notified
    match = db.query(Match).filter(
        Match.match_type == "queue", Match.sport == sport,
        Match.match_format == match_format,
        Match.status.in_(["assembling", "pending", "pending_approval", "ongoing"]),
        Match.status != "invalidated",
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

@router.get("/book/suggest")
def suggest_book_opponents(
    sport:        str,
    match_format: str = "singles",
    limit:        int = 10,
    current_user: dict = Depends(get_current_user),
    db:           Session = Depends(get_db),
):
    """
    Suggest the best opponents for a booked match.
    Returns up to `limit` ranked candidates with their ML quality scores.
    """
    if sport not in VALID_SPORTS:
        raise HTTPException(400, "Invalid sport.")
    if match_format not in VALID_FORMATS:
        raise HTTPException(400, "Invalid match format.")
    limit = max(1, min(limit, 50))

    user_id   = current_user["id"]
    my_rating = db.query(PlayerRating).filter(
        PlayerRating.user_id == user_id,
        PlayerRating.sport   == sport,
        PlayerRating.match_format == match_format,
    ).first()
    my_profile = db.query(Profile).filter(Profile.id == user_id).first()

    # Fetch all registered players for this sport (excluding self)
    rated_players = db.query(PlayerRating).filter(
        PlayerRating.sport        == sport,
        PlayerRating.match_format == match_format,
        PlayerRating.user_id      != user_id,
    ).all()

    # Fetch H2H counts for all candidates at once
    candidate_ids = [str(r.user_id) for r in rated_players]
    h2h_map       = _fetch_h2h_counts(db, user_id, candidate_ids)

    # Build candidate dicts and score each one
    scored = []
    for opp_r in rated_players:
        opp_p = db.query(Profile).filter(Profile.id == opp_r.user_id).first()
        opp_id = str(opp_r.user_id)

        ml_score = score_candidate(
            rating_a     = float(my_rating.rating)          if my_rating else 1500.0,  # type: ignore[arg-type]
            rd_a         = float(my_rating.rating_deviation) if my_rating else 200.0,  # type: ignore[arg-type]
            win_rate_a   = _safe_win_rate(my_rating),
            activeness_a = float(my_rating.activeness_score) if my_rating else 0.5,  # type: ignore[arg-type]
            streak_a     = int(my_rating.current_win_streak) if my_rating else 0,  # type: ignore[arg-type]
            city_a       = str(my_profile.city_mun_code) if my_profile and my_profile.city_mun_code is not None else None,  # type: ignore[arg-type]
            province_a   = str(my_profile.province_code) if my_profile and my_profile.province_code is not None else None,  # type: ignore[arg-type]
            region_a     = str(my_profile.region_code)   if my_profile and my_profile.region_code   is not None else None,  # type: ignore[arg-type]
            rating_b     = float(opp_r.rating),  # type: ignore[arg-type]
            rd_b         = float(opp_r.rating_deviation),  # type: ignore[arg-type]
            win_rate_b   = _safe_win_rate(opp_r),
            activeness_b = float(opp_r.activeness_score),  # type: ignore[arg-type]
            streak_b     = int(opp_r.current_win_streak),  # type: ignore[arg-type]
            city_b       = str(opp_p.city_mun_code) if opp_p and opp_p.city_mun_code is not None else None,  # type: ignore[arg-type]
            province_b   = str(opp_p.province_code) if opp_p and opp_p.province_code is not None else None,  # type: ignore[arg-type]
            region_b     = str(opp_p.region_code)   if opp_p and opp_p.region_code   is not None else None,  # type: ignore[arg-type]
            sport        = sport,
            match_format = match_format,
            wait_seconds = 0,
            h2h_count    = h2h_map.get(opp_id, 0),
        )
        scored.append({
            "player_id":    opp_id,
            "username":     opp_p.username   if opp_p else None,
            "rating":       float(opp_r.rating),  # type: ignore[arg-type]
            "rating_status": str(opp_r.rating_status),
            "ml_score":     ml_score,
            "balance":      (
                "Excellent" if ml_score >= 0.85 else
                "Good"      if ml_score >= 0.70 else
                "Average"   if ml_score >= 0.50 else
                "Uneven"
            ),
            "h2h_count":    h2h_map.get(opp_id, 0),
        })

    # Sort by ML score descending, return top N
    scored.sort(key=lambda x: x["ml_score"], reverse=True)
    return {
        "sport":        sport,
        "match_format": match_format,
        "suggestions":  scored[:limit],
    }


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

@router.get("")
def list_matches(
    limit: int = 50,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Return recent matches for the current user (used by dashboard for pending counts)."""
    user_id = current_user["id"]
    matches = db.query(Match).filter(
        Match.status != "invalidated",
        or_(
            Match.player1_id == user_id, Match.player2_id == user_id,
            Match.player3_id == user_id, Match.player4_id == user_id,
        )
    ).order_by(Match.created_at.desc()).limit(min(limit, 200)).all()

    return {
        "matches": [
            {
                "id":           str(m.id),
                "sport":        m.sport.value if hasattr(m.sport, "value") else str(m.sport),
                "match_type":   m.match_type.value if hasattr(m.match_type, "value") else str(m.match_type),
                "match_format": m.match_format.value if hasattr(m.match_format, "value") else str(m.match_format),
                "status":       m.status.value if hasattr(m.status, "value") else str(m.status),
                "player1_id":   str(m.player1_id)  if m.player1_id  is not None else None,
                "player2_id":   str(m.player2_id)  if m.player2_id  is not None else None,
                "winner_id":    str(m.winner_id)   if m.winner_id   is not None else None,
                "scheduled_at": str(m.scheduled_at) if m.scheduled_at is not None else None,
                "created_at":   str(m.created_at),
            }
            for m in matches
        ]
    }


@router.get("/my")
def get_my_matches(
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]
    matches = db.query(Match).filter(
        Match.status != "invalidated",
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

    if match.referee_id is None:
        _broadcast(match_id, {
            "type":    "match_announcement",
            "message": "⏳ Waiting for a referee to start the game.",
        })

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
    is_doubles   = match_format in ("doubles", "mixed_doubles")

    # Normalize doubles slot mapping so team anchors/opponents are consistent.
    if is_doubles:
        has_explicit_team_slots = any([
            match.team1_player1 is not None,
            match.team1_player2 is not None,
            match.team2_player1 is not None,
            match.team2_player2 is not None,
        ])
        has_all_raw_slots = all([
            match.player1_id is not None,
            match.player2_id is not None,
            match.player3_id is not None,
            match.player4_id is not None,
        ])
        match_type_val = match.match_type.value if hasattr(match.match_type, "value") else str(match.match_type)

        if (not has_explicit_team_slots) and has_all_raw_slots and match_type_val in ("queue", "ranked") and getattr(match, "party_id", None) is None:
            # Legacy non-party queue balancing used adjacent slots:
            # [player1, player2] vs [player3, player4].
            setattr(match, "team1_player1", match.player1_id)
            setattr(match, "team1_player2", match.player2_id)
            setattr(match, "team2_player1", match.player3_id)
            setattr(match, "team2_player2", match.player4_id)
        else:
            if match.team1_player1 is None and match.player1_id is not None:
                setattr(match, "team1_player1", match.player1_id)
            if match.team2_player1 is None and match.player2_id is not None:
                setattr(match, "team2_player1", match.player2_id)
            if match.team1_player2 is None and match.player3_id is not None:
                setattr(match, "team1_player2", match.player3_id)
            if match.team2_player2 is None and match.player4_id is not None:
                setattr(match, "team2_player2", match.player4_id)

        _canonicalize_doubles_slots(match)
        db.flush()

    player1_id = str(match.player1_id) if match.player1_id is not None else None  # team1 captain
    player2_id = str(match.player2_id) if match.player2_id is not None else None  # team2 captain
    team1_partner_id = str(match.player3_id) if (is_doubles and match.player3_id is not None) else None
    team2_partner_id = str(match.player4_id) if (is_doubles and match.player4_id is not None) else None

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

    participants = {pid for pid in [player1_id, player2_id, team1_partner_id, team2_partner_id] if pid}
    if data.winner_id not in participants:
        raise HTTPException(400, "Winner must be a participant in this match.")

    winner_anchor_id = data.winner_id
    if is_doubles:
        if data.winner_id in {player1_id, team1_partner_id}:
            winner_anchor_id = player1_id
        elif data.winner_id in {player2_id, team2_partner_id}:
            winner_anchor_id = player2_id

    # Ensure rating rows exist so progression updates are not skipped.
    p1 = _ensure_rating_row(db, player1_id, sport, match_format)
    p2 = _ensure_rating_row(db, player2_id, sport, match_format)
    p3 = _ensure_rating_row(db, team1_partner_id, sport, match_format) if (is_doubles and team1_partner_id) else None
    p4 = _ensure_rating_row(db, team2_partner_id, sport, match_format) if (is_doubles and team2_partner_id) else None

    # Compute Glicko-2 in Python
    p1_wins = winner_anchor_id == player1_id
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
            CAST(:mid AS uuid), CAST(:winner AS uuid),
            :r1, :rd1, :vol1,
            :r2, :rd2, :vol2
        )
    """), {
        "mid":    match_id,
        "winner": winner_anchor_id,
        "r1":  new_p1_r,  "rd1": new_p1_rd,  "vol1": new_p1_vol,
        "r2":  new_p2_r,  "rd2": new_p2_rd,  "vol2": new_p2_vol,
    })
    db.commit()

    # Stored procedure currently updates only player1/player2.
    # Apply equivalent progression for doubles partners when present.
    if is_doubles and p3 and p4:
        new_p3_r, new_p3_rd, new_p3_vol = glicko_update(
            rating=float(p3.rating), rd=float(p3.rating_deviation), volatility=float(p3.volatility),  # type: ignore[arg-type]
            opp_rating=float(p2.rating), opp_rd=float(p2.rating_deviation),  # type: ignore[arg-type]
            score=1.0 if p1_wins else 0.0,
        )
        new_p4_r, new_p4_rd, new_p4_vol = glicko_update(
            rating=float(p4.rating), rd=float(p4.rating_deviation), volatility=float(p4.volatility),  # type: ignore[arg-type]
            opp_rating=float(p1.rating), opp_rd=float(p1.rating_deviation),  # type: ignore[arg-type]
            score=0.0 if p1_wins else 1.0,
        )

        _apply_rating_result(
            p3,
            won=p1_wins,
            new_rating=new_p3_r,
            new_rd=new_p3_rd,
            new_vol=new_p3_vol,
        )
        _apply_rating_result(
            p4,
            won=not p1_wins,
            new_rating=new_p4_r,
            new_rd=new_p4_rd,
            new_vol=new_p4_vol,
        )
        db.commit()

    # Ensure party state can't keep redirecting users back into an already-finished match.
    linked_parties = db.query(Party).filter(Party.match_id == match.id).all()
    party_changed = False
    for linked_party in linked_parties:
        party_status = linked_party.status.value if hasattr(linked_party.status, "value") else str(linked_party.status)
        if party_status in ("forming", "ready", "in_queue", "match_found"):
            setattr(linked_party, "status", "disbanded")
            setattr(linked_party, "match_id", None)
            party_changed = True
    if party_changed:
        db.commit()

    try:
        save_training_row(match_id)
    except Exception:
        pass

    # Update activeness scores for all participants in this format.
    try:
        _update_activeness(db, player1_id, sport, match_format)
        _update_activeness(db, player2_id, sport, match_format)
        if is_doubles and team1_partner_id:
            _update_activeness(db, team1_partner_id, sport, match_format)
        if is_doubles and team2_partner_id:
            _update_activeness(db, team2_partner_id, sport, match_format)
        db.commit()
    except Exception:
        pass

    try:
        _update_pool_standing(match, winner_anchor_id, db)
    except Exception:
        pass

    _broadcast(match_id, {"type": "match_completed", "winner_id": winner_anchor_id})
    return {"message": "Match completed.", "winner_id": winner_anchor_id}


def _update_pool_standing(match: Match, winner_id: str, db) -> None:
    """Update pool-play group standings after a match completes."""
    if match.tournament_id is None:
        return
    t = db.query(Tournament).filter(Tournament.id == match.tournament_id).first()
    if not t or str(t.format) not in ("pool_play", "TournamentFormat.pool_play"):
        return
    bracket_side = getattr(match, "bracket_side", None) or ""
    if not bracket_side.startswith("G"):
        return

    p1_id = str(match.player1_id) if match.player1_id is not None else None
    p2_id = str(match.player2_id) if match.player2_id is not None else None
    if not p1_id or not p2_id:
        return

    reg1 = db.query(TournamentRegistration).filter(
        TournamentRegistration.tournament_id == match.tournament_id,
        TournamentRegistration.player_id == p1_id,
        TournamentRegistration.status == "confirmed",
    ).first()
    reg2 = db.query(TournamentRegistration).filter(
        TournamentRegistration.tournament_id == match.tournament_id,
        TournamentRegistration.player_id == p2_id,
        TournamentRegistration.status == "confirmed",
    ).first()
    if not reg1 or not reg2:
        return

    s1 = db.query(TournamentGroupStanding).filter(TournamentGroupStanding.entry_id == reg1.id).first()
    s2 = db.query(TournamentGroupStanding).filter(TournamentGroupStanding.entry_id == reg2.id).first()
    if not s1 or not s2:
        return

    sets   = db.query(MatchSet).filter(MatchSet.match_id == match.id).all()
    p1_pts = sum((s.player1_score or 0) + (s.team1_score or 0) for s in sets)
    p2_pts = sum((s.player2_score or 0) + (s.team2_score or 0) for s in sets)

    is_p1_win = winner_id == p1_id
    s1.played += 1
    s1.wins   += 1 if is_p1_win else 0
    s1.losses += 0 if is_p1_win else 1
    s1.points_for     += p1_pts
    s1.points_against += p2_pts
    s1.point_diff      = s1.points_for - s1.points_against

    s2.played += 1
    s2.wins   += 0 if is_p1_win else 1
    s2.losses += 1 if is_p1_win else 0
    s2.points_for     += p2_pts
    s2.points_against += p1_pts
    s2.point_diff      = s2.points_for - s2.points_against

    db.commit()


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

    # Per-match best_of override (highest priority — set by promote-to-knockout)
    match_best_of = getattr(match, "best_of", None)
    if match_best_of in (1, 3, 5):
        ruleset = dict(ruleset)
        if match_best_of == 1:
            ruleset["sets_to_win"] = 1
            ruleset["max_sets"]    = 1
        elif match_best_of == 3:
            ruleset["sets_to_win"] = 2
            ruleset["max_sets"]    = 3
        elif match_best_of == 5:
            ruleset["sets_to_win"] = 3
            ruleset["max_sets"]    = 5
    elif match.tournament_id is not None:
        # Fall back to tournament knockout_best_of override when match is in a knockout stage
        t = db.query(Tournament).filter(Tournament.id == match.tournament_id).first()
        if t and _is_knockout_match(match, t):
            best_of = getattr(t, "knockout_best_of", 3) or 3
            if best_of == 1:
                ruleset = dict(ruleset)  # shallow copy so we don't mutate the cached dict
                ruleset["sets_to_win"] = 1
                ruleset["max_sets"]    = 1

    # Per-match score_limit override (points_per_set only — doesn't affect lawn_tennis games_per_set)
    _score_limit: int | None = getattr(match, "score_limit", None)
    if _score_limit and "points_per_set" in ruleset:
        ruleset = dict(ruleset)
        ruleset["points_per_set"] = _score_limit
        ruleset["score_limit"]    = _score_limit

    return {"sport": match.sport.value, "ruleset": ruleset}


# ── Score limit override ───────────────────────────────────────────────────────

class SetScoreLimitRequest(BaseModel):
    score_limit: int  # must be 11, 15, or 21

@router.patch("/{match_id}/score-limit", status_code=200)
def set_match_score_limit(
    match_id: str,
    data: SetScoreLimitRequest,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if data.score_limit not in (11, 15, 21):
        raise HTTPException(400, "Score limit must be 11, 15, or 21.")
    match = db.query(Match).filter(Match.id == match_id).first()
    if not match:
        raise HTTPException(404, "Match not found.")
    if not _can_score(match, current_user["id"]):
        raise HTTPException(403, "Only the assigned referee may change the score limit.")
    setattr(match, "score_limit", data.score_limit)
    db.commit()
    return {"score_limit": data.score_limit}


# ── Invalidate abandoned match ────────────────────────────────────────────────

@router.patch("/{match_id}/invalidate", status_code=200)
def invalidate_match(
    match_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Mark an ongoing queue match as invalidated (abandoned with no scores recorded).
    Any participant may call this. Only valid for ongoing matches with no point history."""
    user_id = current_user["id"]
    match = db.query(Match).filter(Match.id == match_id).first()
    if not match:
        raise HTTPException(404, "Match not found.")

    participant_ids = [str(p) for p in [match.player1_id, match.player2_id,
                                         match.player3_id, match.player4_id,
                                         match.referee_id] if p is not None]
    if user_id not in participant_ids:
        raise HTTPException(403, "Not a participant of this match.")

    status_val = match.status.value if hasattr(match.status, "value") else str(match.status)
    if status_val not in ("ongoing", "pending", "assembling", "pending_approval"):
        raise HTTPException(400, f"Cannot invalidate a match with status '{status_val}'.")

    # Guard: refuse if any actual points have been recorded (initial 0-0 set is always seeded)
    has_scores = db.query(MatchSet).filter(
        MatchSet.match_id == match_id,
        or_(
            MatchSet.player1_score > 0, MatchSet.player2_score > 0,
            MatchSet.team1_score > 0,   MatchSet.team2_score > 0,
        ),
    ).first()
    if has_scores:
        raise HTTPException(400, "Cannot invalidate a match that already has scores recorded.")

    setattr(match, "status", "invalidated")

    # Invalidated matches should not retain user-facing timelines.
    db.query(MatchHistory).filter(MatchHistory.match_id == match_id).delete(synchronize_session=False)

    # Disband any party whose match_id points to this match
    linked_parties = db.query(Party).filter(Party.match_id == match.id).all()
    for linked_party in linked_parties:
        setattr(linked_party, "status", "disbanded")
        setattr(linked_party, "match_id", None)

    db.commit()
    _broadcast(match_id, {"type": "match_invalidated", "match_id": match_id})
    return {"message": "Match invalidated."}


def _is_knockout_match(match: Match, tournament: Tournament) -> bool:
    """Return True if this match is part of the knockout stage."""
    fmt = str(tournament.format)
    if any(f in fmt for f in ("single_elimination", "double_elimination")):
        return True  # every match is knockout
    if "group_stage_knockout" in fmt:
        side = getattr(match, "bracket_side", "") or ""
        return side == "K"
    return False   # pool_play, round_robin, swiss — no knockout


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


class ServeChangeRequest(BaseModel):
    set_number: int
    event_type: str          # "loss_of_serve" | "side_out"
    fault_team: str          # "team1" | "team2"
    fault_player_id: Optional[str] = None
    new_serving_team: str    # "team1" | "team2"
    new_server_slot: int     # 0 = server 1, 1 = server 2
    client_action_id: Optional[str] = None


def _can_score(match: Match, user_id: str) -> bool:
    """Only the assigned referee may record points. A referee is required."""
    return match.referee_id is not None and str(match.referee_id) == user_id


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
        _sl: int | None = getattr(match, "score_limit", None)
        pts_to_win = int(_sl) if _sl else (ruleset.get("points_per_set") or ruleset.get("games_per_set") or 21)
        win_by     = ruleset.get("win_by", 2)
        max_pts    = ruleset.get("max_points")  # None means no cap
        # Only apply max_pts cap when it's strictly above pts_to_win (avoids premature set end if score_limit was raised)
        effective_max = max_pts if (max_pts and max_pts > pts_to_win) else None

        # Determine if current set is won
        if t1 >= pts_to_win and t1 - t2 >= win_by:
            set_winner = "team1"
        elif t2 >= pts_to_win and t2 - t1 >= win_by:
            set_winner = "team2"
        elif effective_max and (t1 >= effective_max or t2 >= effective_max):
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
                if effective_max and (s1 >= effective_max or s2 >= effective_max):
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


@router.post("/{match_id}/serve-change", status_code=201)
def record_serve_change(
    match_id: str,
    data: ServeChangeRequest,
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
        raise HTTPException(403, "Not authorized to record events.")

    if data.client_action_id:
        existing = db.query(MatchHistory).filter(
            MatchHistory.match_id == match_id,
            MatchHistory.meta["client_action_id"].astext == data.client_action_id,
        ).first()
        if existing:
            return {"message": "Already recorded.", "event_id": str(existing.id)}

    if data.fault_team not in ("team1", "team2"):
        raise HTTPException(400, "fault_team must be 'team1' or 'team2'.")
    if data.new_serving_team not in ("team1", "team2"):
        raise HTTPException(400, "new_serving_team must be 'team1' or 'team2'.")

    fault_label = "Team 1" if data.fault_team == "team1" else "Team 2"
    new_server_label = "Team 1" if data.new_serving_team == "team1" else "Team 2"

    if data.fault_player_id:
        actor = db.query(Profile).filter(Profile.id == data.fault_player_id).first()
        if actor and actor.username is not None:
            fault_label = f"@{actor.username}"

    if data.event_type == "loss_of_serve":
        description = f"Loss of serve — {fault_label} · Server 2 serves next"
    else:
        description = f"Side out — {fault_label} · {new_server_label} now serving"

    match_set = db.query(MatchSet).filter(
        MatchSet.match_id == match_id,
        MatchSet.set_number == data.set_number,
    ).first()
    t1 = int(match_set.team1_score or match_set.player1_score or 0) if match_set else 0  # type: ignore[arg-type]
    t2 = int(match_set.team2_score or match_set.player2_score or 0) if match_set else 0  # type: ignore[arg-type]

    meta: dict = {
        "event_type": data.event_type,
        "fault_team": data.fault_team,
        "new_serving_team": data.new_serving_team,
        "new_server_slot": data.new_server_slot,
    }
    if data.fault_player_id:  meta["fault_player_id"]  = data.fault_player_id
    if data.client_action_id: meta["client_action_id"] = data.client_action_id

    db.add(MatchHistory(
        match_id=match_id,
        event_type="serve_change",
        team=data.fault_team,
        player_id=data.fault_player_id or None,
        recorded_by=user_id,
        description=description,
        set_number=data.set_number,
        team1_score=t1,
        team2_score=t2,
        meta=meta,
    ))
    db.commit()

    _broadcast(match_id, {
        "type": "serve_change",
        "event_type": data.event_type,
        "fault_team": data.fault_team,
        "new_serving_team": data.new_serving_team,
        "new_server_slot": data.new_server_slot,
        "description": description,
    })

    return {"message": "Serve change recorded.", "description": description}


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
    match = db.query(Match).filter(Match.id == match_id).first()
    if not match:
        raise HTTPException(404, "Match not found.")
    if (match.status.value if hasattr(match.status, "value") else str(match.status)) == "invalidated":
        return {"history": []}

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
