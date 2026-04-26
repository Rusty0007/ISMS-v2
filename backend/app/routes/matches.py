import logging
from fastapi import APIRouter, HTTPException, Depends, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from typing import Optional, cast
from datetime import datetime, timezone, timedelta
import asyncio
import json

logger = logging.getLogger(__name__)
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
from app.services.matchmaking import (
    find_best_opponent,
    get_model_info,
    score_candidate,
    run_matchmaking,
    can_join_doubles_lobby,
    is_mixed_doubles_pool_viable,
    normalize_gender,
)
from app.services.match_lobby import ensure_match_lobby_rows
from app.services.broadcast import broadcast_match as _broadcast
from app.services.notifications import send_notification
from app.services.performance_rating import (
    refresh_performance_metrics,
    redistribute_match_ratings_by_performance,
)
from app.services.rate_limit import check_rate_limit, scoring_rate_limit_key
from app.services.rating_policy import (
    ML_MATCHMAKING_MIN_MATCHES,
    leaderboard_eligible,
    matchmaking_eligible,
    opponent_ids_for_user,
)

_MATCH_START_TIMEOUT_WINDOW = timedelta(minutes=5)
_MATCH_START_TIMEOUT_STATUSES = ("pending", "awaiting_players", "pending_approval", "ongoing")
_SCORING_RATE_LIMIT_MAX_CALLS = 300
_SCORING_RATE_LIMIT_WINDOW_SECONDS = 60


# ── Internal helpers ──────────────────────────────────────────────────────────
def _status_value(value) -> str:
    return value.value if hasattr(value, "value") else str(value)


def _humanize_history_label(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.replace("_", " ").strip()
    return cleaned.title() if cleaned else None


def _profile_display_name(db: Session, player_id: str | None) -> str | None:
    if not player_id:
        return None
    profile = db.query(Profile).filter(Profile.id == player_id).first()
    if not profile:
        return str(player_id)[:8]
    full_name = f"{profile.first_name or ''} {profile.last_name or ''}".strip()
    return full_name or str(player_id)[:8]


def _as_aware_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _required_player_count(match: Match) -> int:
    return 4 if _status_value(match.match_format) in ("doubles", "mixed_doubles") else 2


def _assigned_player_ids(match: Match) -> list[str]:
    seen: set[str] = set()
    ordered_ids: list[str] = []
    for pid in [match.player1_id, match.player2_id, match.player3_id, match.player4_id]:
        if pid is None:
            continue
        pid_str = str(pid)
        if pid_str in seen:
            continue
        seen.add(pid_str)
        ordered_ids.append(pid_str)
    return ordered_ids


def _match_participant_ids(match: Match) -> list[str]:
    participant_ids = _assigned_player_ids(match)
    if match.referee_id is not None:
        referee_id = str(match.referee_id)
        if referee_id not in participant_ids:
            participant_ids.append(referee_id)
    return participant_ids


def _match_has_required_players(match: Match) -> bool:
    return len(_assigned_player_ids(match)) >= _required_player_count(match)


def _match_start_timeout_anchor(match: Match) -> datetime | None:
    status_val = _status_value(match.status)
    if status_val not in _MATCH_START_TIMEOUT_STATUSES:
        return None
    if match.tournament_id is not None or _status_value(match.match_type) == "tournament":
        return None
    if not _match_has_required_players(match):
        return None

    created_at = _as_aware_utc(cast(datetime | None, getattr(match, "created_at", None)))
    scheduled_at = _as_aware_utc(cast(datetime | None, getattr(match, "scheduled_at", None)))
    called_at = _as_aware_utc(cast(datetime | None, getattr(match, "called_at", None)))
    started_at = _as_aware_utc(cast(datetime | None, getattr(match, "started_at", None)))

    if status_val == "pending":
        return scheduled_at or created_at
    if status_val == "awaiting_players":
        return called_at or scheduled_at
    return started_at or called_at or scheduled_at or created_at


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


def _count_distinct_opponents(db: Session, user_id: str, sport: str, match_format: str) -> int:
    completed_matches = db.query(Match).filter(
        Match.status == "completed",
        Match.sport == sport,
        Match.match_format == match_format,
        or_(
            Match.player1_id == user_id,
            Match.player2_id == user_id,
            Match.player3_id == user_id,
            Match.player4_id == user_id,
            Match.team1_player1 == user_id,
            Match.team1_player2 == user_id,
            Match.team2_player1 == user_id,
            Match.team2_player2 == user_id,
        ),
    ).all()

    opponents: set[str] = set()
    for match in completed_matches:
        opponents.update(opponent_ids_for_user(match, user_id))
    return len(opponents)


def _refresh_rating_eligibility(db: Session, user_ids: list[str], sport: str, match_format: str) -> None:
    now = datetime.now(timezone.utc)
    for user_id in {str(uid) for uid in user_ids if uid}:
        rating = db.query(PlayerRating).filter(
            PlayerRating.user_id == user_id,
            PlayerRating.sport == sport,
            PlayerRating.match_format == match_format,
        ).first()
        if rating is None:
            continue

        matches_played = int(rating.matches_played or 0)
        distinct_opponents = _count_distinct_opponents(db, user_id, sport, match_format)
        rd = float(rating.rating_deviation or 999.0)
        matchmaking_ready = matchmaking_eligible(matches_played)
        leaderboard_ready = leaderboard_eligible(matches_played, distinct_opponents, rd)

        setattr(rating, "distinct_opponents_count", distinct_opponents)
        setattr(rating, "is_matchmaking_eligible", matchmaking_ready)
        setattr(rating, "is_leaderboard_eligible", leaderboard_ready)
        setattr(rating, "rating_status", "RATED" if leaderboard_ready else "CALIBRATING")
        if leaderboard_ready and getattr(rating, "calibration_completed_at", None) is None:
            setattr(rating, "calibration_completed_at", now)
        if not leaderboard_ready:
            setattr(rating, "calibration_completed_at", None)


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
        distinct_opponents_count=0,
        is_matchmaking_eligible=False,
        is_leaderboard_eligible=False,
        performance_rating=50,
        performance_confidence=0,
        performance_coverage_pct=0,
        performance_reliable=False,
        performance_matches_with_events=0,
        performance_total_points=0,
        performance_attributed_points=0,
        performance_winning_shots=0,
        performance_forced_errors_drawn=0,
        performance_errors_committed=0,
        performance_serve_faults=0,
        performance_violations=0,
        performance_clutch_points_won=0,
        performance_clutch_errors=0,
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
    setattr(row, "is_matchmaking_eligible", matchmaking_eligible(matches_played + 1))

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
_MATCH_PAGE_PRESENCE_PREFIX = "isms:match:presence:"
_MATCH_PAGE_PRESENCE_TTL_SECONDS = 75
_MATCH_PAGE_DISCONNECT_THRESHOLD = 3

try:
    import redis as _redis_sync
    _match_presence_redis = _redis_sync.from_url(settings.redis_url, decode_responses=True)
    _match_presence_redis.ping()
except Exception:
    _match_presence_redis = None


def _match_presence_key(match_id: str, user_id: str) -> str:
    return f"{_MATCH_PAGE_PRESENCE_PREFIX}{match_id}:{user_id}"


def _touch_match_presence(match_id: str, user_id: str) -> None:
    if _match_presence_redis is None:
        return
    try:
        _match_presence_redis.setex(
            _match_presence_key(match_id, user_id),
            _MATCH_PAGE_PRESENCE_TTL_SECONDS,
            datetime.now(timezone.utc).isoformat(),
        )
    except Exception:
        pass


def _is_match_user_connected(match_id: str, user_id: str) -> bool:
    if _match_presence_redis is None:
        return False
    try:
        return bool(_match_presence_redis.exists(_match_presence_key(match_id, user_id)))
    except Exception:
        return False


def _serialize_match_presence(match: Match) -> dict:
    match_id = str(match.id)
    player_ids = [
        str(pid)
        for pid in [match.player1_id, match.player2_id, match.player3_id, match.player4_id]
        if pid is not None
    ]
    connected_player_ids = [pid for pid in player_ids if _is_match_user_connected(match_id, pid)]
    disconnected_player_ids = [pid for pid in player_ids if pid not in connected_player_ids]
    referee_id = str(match.referee_id) if match.referee_id is not None else None
    referee_connected = bool(referee_id and _is_match_user_connected(match_id, referee_id))

    match_type_val = match.match_type.value if hasattr(match.match_type, "value") else str(match.match_type)
    match_format_val = match.match_format.value if hasattr(match.match_format, "value") else str(match.match_format)

    return {
        "connected_player_ids": connected_player_ids,
        "disconnected_player_ids": disconnected_player_ids,
        "connected_player_count": len(connected_player_ids),
        "disconnected_player_count": len(disconnected_player_ids),
        "total_players": len(player_ids),
        "referee_id": referee_id,
        "referee_connected": referee_connected,
        "disconnect_threshold": _MATCH_PAGE_DISCONNECT_THRESHOLD,
        "ttl_seconds": _MATCH_PAGE_PRESENCE_TTL_SECONDS,
        "auto_invalidate_enabled": (
            match_type_val in ("queue", "ranked")
            and match_format_val in ("doubles", "mixed_doubles")
        ),
    }


def _match_has_recorded_scores(db: Session, match_id: str) -> bool:
    return db.query(MatchSet).filter(
        MatchSet.match_id == match_id,
        or_(
            MatchSet.player1_score > 0, MatchSet.player2_score > 0,
            MatchSet.team1_score > 0,   MatchSet.team2_score > 0,
        ),
    ).first() is not None


def _invalidate_match_without_scores(db: Session, match: Match) -> None:
    setattr(match, "status", "invalidated")
    if match.court_id is not None:
        court = db.query(Court).filter(Court.id == match.court_id).first()
        if court is not None:
            setattr(court, "status", "available")
    db.query(MatchHistory).filter(MatchHistory.match_id == match.id).delete(synchronize_session=False)
    linked_parties = db.query(Party).filter(Party.match_id == match.id).all()
    for linked_party in linked_parties:
        setattr(linked_party, "status", "disbanded")
        setattr(linked_party, "match_id", None)


def _maybe_auto_invalidate_disconnected_match(db: Session, match: Match, presence: dict) -> str | None:
    if _match_presence_redis is None:
        return None

    status_val = match.status.value if hasattr(match.status, "value") else str(match.status)
    match_type_val = match.match_type.value if hasattr(match.match_type, "value") else str(match.match_type)
    match_format_val = match.match_format.value if hasattr(match.match_format, "value") else str(match.match_format)

    if status_val != "ongoing":
        return None
    if match_type_val not in ("queue", "ranked"):
        return None
    if match_format_val not in ("doubles", "mixed_doubles"):
        return None
    if bool(presence.get("referee_connected")):
        return None
    if int(presence.get("disconnected_player_count", 0) or 0) < _MATCH_PAGE_DISCONNECT_THRESHOLD:
        return None
    if _match_has_recorded_scores(db, str(match.id)):
        return None

    started_at = cast(datetime | None, getattr(match, "started_at", None))
    created_at = cast(datetime | None, getattr(match, "created_at", None))
    grace_started_at = started_at if started_at is not None else created_at
    if grace_started_at is None:
        return None
    if datetime.now(timezone.utc) - grace_started_at < timedelta(seconds=_MATCH_PAGE_PRESENCE_TTL_SECONDS):
        return None

    _invalidate_match_without_scores(db, match)
    db.commit()

    reason = "players_disconnected"
    match_id = str(match.id)
    _broadcast(match_id, {"type": "match_invalidated", "match_id": match_id, "reason": reason})

    participant_ids = [
        str(pid)
        for pid in [match.player1_id, match.player2_id, match.player3_id, match.player4_id, match.referee_id]
        if pid is not None
    ]
    for participant_id in participant_ids:
        send_notification(
            user_id=participant_id,
            title="Match Auto-Invalidated",
            body="This match was invalidated because three players disconnected and no referee was present in the match room.",
            notif_type="match_invalidated",
            reference_id=match_id,
        )

    return reason


def dispatch_due_match_start_timeouts(db: Session) -> int:
    now_utc = datetime.now(timezone.utc)
    candidates = db.query(Match).filter(
        Match.status.in_(list(_MATCH_START_TIMEOUT_STATUSES)),
        Match.winner_id.is_(None),
        Match.completed_at.is_(None),
        Match.tournament_id.is_(None),
    ).all()

    timed_out_matches: list[tuple[str, list[str]]] = []
    for match in candidates:
        timeout_anchor = _match_start_timeout_anchor(match)
        if timeout_anchor is None:
            continue
        if now_utc - timeout_anchor < _MATCH_START_TIMEOUT_WINDOW:
            continue
        if _match_has_recorded_scores(db, str(match.id)):
            continue

        match_id = str(match.id)
        participant_ids = _match_participant_ids(match)
        _invalidate_match_without_scores(db, match)
        timed_out_matches.append((match_id, participant_ids))

    if not timed_out_matches:
        return 0

    db.commit()

    for match_id, participant_ids in timed_out_matches:
        _broadcast(match_id, {"type": "match_invalidated", "match_id": match_id, "reason": "start_timeout"})
        for participant_id in participant_ids:
            send_notification(
                user_id=participant_id,
                title="Match Auto-Invalidated",
                body="This match was invalidated because it did not start within 5 minutes.",
                notif_type="match_invalidated",
                reference_id=match_id,
            )

    return len(timed_out_matches)


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
                extra_data   = {"club_id": str(club_id)},
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
                extra_data   = {"club_id": str(club_id)},
            )


def _requires_club_match_approval(club: Club | None) -> bool:
    if club is None:
        return False
    if str(getattr(club, "approval_mode", "auto") or "auto") != "manual":
        return False
    return not settings.bypass_club_match_approval


VALID_FORMATS = ["singles", "doubles", "mixed_doubles"]
VALID_EVENTS  = ["shot", "violation", "rally_outcome", "momentum"]


def _safe_win_rate(rating: PlayerRating | None, default: float = 0.5) -> float:
    if rating is None or int(rating.matches_played or 0) <= 0:  # type: ignore[arg-type]
        return default
    return float(int(rating.wins or 0)) / float(int(rating.matches_played or 1))  # type: ignore[arg-type]


def _profile_gender_key(profile: Profile | None) -> str | None:
    return normalize_gender(getattr(profile, "gender", None) if profile else None)


def _require_supported_mixed_doubles_gender(profile: Profile | None) -> None:
    if _profile_gender_key(profile) is None:
        raise HTTPException(
            400,
            "Mixed doubles requires your profile gender to be set to male or female.",
        )


def _resolve_match_mode(match: Match) -> str:
    match_type_val = match.match_type.value if hasattr(match.match_type, "value") else str(match.match_type)
    if match_type_val == "ranked":
        return "ranked"
    return "normal"


def _normalize_queue_mode(raw_mode: str | None) -> str:
    mode = (raw_mode or "ranked").strip().lower()
    if mode in ("normal", "quick", "friendly"):
        return "quick"
    if mode in ("ranked", "club"):
        return "ranked"
    return "ranked"


def _queue_match_type_for_mode(match_mode: str) -> str:
    return "ranked" if match_mode == "ranked" else "queue"


def _get_joined_club_ids_for_sport(db: Session, user_id: str, sport: str) -> list[str]:
    rows = (
        db.query(ClubMember.club_id)
        .join(Club, Club.id == ClubMember.club_id)
        .filter(
            ClubMember.user_id == user_id,
            Club.is_active.is_(True),
            or_(Club.sport == sport, Club.sport.is_(None)),
        )
        .order_by(ClubMember.joined_at.asc())
        .all()
    )

    club_ids: list[str] = []
    seen: set[str] = set()
    for row in rows:
        club_id = getattr(row, "club_id", None)
        if club_id is None and row:
            club_id = row[0]
        if club_id is None:
            continue
        club_key = str(club_id)
        if club_key in seen:
            continue
        seen.add(club_key)
        club_ids.append(club_key)
    return club_ids


def _ordered_club_overlap(*club_lists: list[str]) -> list[str]:
    if not club_lists:
        return []

    common = set(club_lists[0])
    for club_list in club_lists[1:]:
        common &= set(club_list)
        if not common:
            return []

    return [club_id for club_id in club_lists[0] if club_id in common]


def _match_player_ids(match: Match) -> list[str]:
    return [
        str(pid)
        for pid in [match.player1_id, match.player2_id, match.player3_id, match.player4_id]
        if pid is not None
    ]


def _get_match_candidate_club_ids(db: Session, match: Match, sport: str) -> list[str]:
    if match.club_id is not None:
        return [str(match.club_id)]

    player_ids = _match_player_ids(match)
    if not player_ids:
        return []

    player_club_lists = [
        _get_joined_club_ids_for_sport(db, player_id, sport)
        for player_id in player_ids
    ]
    return _ordered_club_overlap(*player_club_lists)

# ── Request models ───────────────────────────────────────────────────────────

class CreateFriendlyRequest(BaseModel):
    sport: str
    match_format: str = "singles"
    opponent_id: str

class JoinQueueRequest(BaseModel):
    sport:              str
    match_format:       str           = "singles"
    match_mode:         str           = "ranked"  # ranked | normal
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
    raw_mode = (data.match_mode or "ranked").strip().lower()
    requested_mode = "ranked" if data.preferred_club_id else _normalize_queue_mode(raw_mode)
    requested_db_match_type = _queue_match_type_for_mode(requested_mode)
    club_scoped_search = raw_mode == "club" or bool(data.preferred_club_id)
    format_label = data.match_format.replace("_", " ")

    ranked_rating_row = None
    if requested_mode == "ranked":
        ranked_rating_row = db.query(PlayerRating).filter(
            PlayerRating.user_id == user_id,
            PlayerRating.sport == data.sport,
            PlayerRating.match_format == data.match_format,
        ).first()
        if not ranked_rating_row or not bool(getattr(ranked_rating_row, "is_matchmaking_eligible", False)):
            raise HTTPException(
                400,
                f"You must complete ML matchmaking calibration ({ML_MATCHMAKING_MIN_MATCHES} {format_label} matches) before joining the ranked queue.",
            )

    # Already in queue?
    if data.match_format == "singles":
        existing = db.query(Match).filter(
            Match.match_type == requested_db_match_type,
            Match.sport == data.sport,
            Match.match_format == data.match_format,
            Match.player1_id == user_id,
            Match.status == "pending",
            Match.player2_id.is_(None),
        ).first()
    else:
        existing = db.query(Match).filter(
            Match.match_type == requested_db_match_type,
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
        Match.status.in_(["awaiting_players", "ongoing", "pending_approval"]),
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
            "match_status": status_val,
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
    match_mode = requested_mode
    search_club_ids: list[str] = []
    automatic_club_search = False
    if club_scoped_search and not preferred_club_id:
        search_club_ids = _get_joined_club_ids_for_sport(db, user_id, data.sport)
        if len(search_club_ids) < 3:
            raise HTTPException(400, "Random match requires at least 3 joined clubs for this sport.")
        automatic_club_search = True

    club_cache: dict[str, list[str]] = {}
    if search_club_ids:
        club_cache[user_id] = search_club_ids

    def get_player_club_ids(player_id: str) -> list[str]:
        if player_id not in club_cache:
            club_cache[player_id] = _get_joined_club_ids_for_sport(db, player_id, data.sport)
        return club_cache[player_id]

    # ── Singles ───────────────────────────────────────────────────────────────
    if data.match_format == "singles":
        my_rating  = ranked_rating_row or db.query(PlayerRating).filter(
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

        db_match_type = requested_db_match_type

        queue_q = db.query(Match).filter(
            Match.match_type == db_match_type,
            Match.sport == data.sport,
            Match.match_format == data.match_format,
            Match.status == "pending",
            Match.player2_id.is_(None),
            Match.player1_id != user_id,
        )
        # Club mode with a chosen club stays inside that one venue.
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
            shared_club_ids: list[str] = []
            if preferred_club_id:
                if queued.club_id is not None:
                    queued_club_id = str(queued.club_id)
                    if queued_club_id != preferred_club_id:
                        continue
                else:
                    candidate_club_ids = get_player_club_ids(str(queued.player1_id))
                    if preferred_club_id not in candidate_club_ids:
                        continue
                shared_club_ids = [preferred_club_id]
            elif automatic_club_search:
                if queued.club_id is not None:
                    queued_club_id = str(queued.club_id)
                    if queued_club_id not in search_club_ids:
                        continue
                    shared_club_ids = [queued_club_id]
                else:
                    candidate_club_ids = get_player_club_ids(str(queued.player1_id))
                    shared_club_ids = _ordered_club_overlap(search_club_ids, candidate_club_ids)
                    if not shared_club_ids:
                        continue

            opp_r = db.query(PlayerRating).filter(
                PlayerRating.user_id == queued.player1_id,
                PlayerRating.sport == data.sport,
                PlayerRating.match_format == data.match_format,
            ).first()
            opp_p = db.query(Profile).filter(Profile.id == queued.player1_id).first()

            # Ranked/ML mode: both players must have enough history for the model to judge them.
            if match_mode == "ranked" and (not opp_r or not bool(getattr(opp_r, "is_matchmaking_eligible", False))):
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
                "shared_club_id":     shared_club_ids[0] if shared_club_ids else None,
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
                club_for_match = preferred_club_id or best.get("shared_club_id") or (str(found.club_id) if found.club_id is not None else None)
                if club_for_match:
                    setattr(found, "club_id", club_for_match)
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
                        club_obj = db.query(Club).filter(Club.id == avail.club_id).first()
                        if _requires_club_match_approval(club_obj):
                            setattr(found, "status", "pending_approval")
                            needs_approval = True
                            _notify_duty_holders(db, avail.club_id, found.id, str(club_obj.name))
                if my_profile is not None and my_boost:
                    setattr(my_profile, "referee_boost_until", None)
                db.commit()
                found_status = found.status.value if hasattr(found.status, "value") else str(found.status)
                return {
                    "status": "matched",
                    "message": "Opponent found! Awaiting club confirmation." if needs_approval else "Opponent found!",
                    "match_id": str(found.id),
                    "match_status": found_status,
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
        Match.match_type == requested_db_match_type,
        Match.sport == data.sport,
        Match.match_format == data.match_format,
        Match.status == "assembling",
    ).all()

    def get_match_players(m):
        return [pid for pid in [m.player1_id, m.player2_id, m.player3_id, m.player4_id] if pid]

    # Build incoming player stats dict once — reused by entry gates at every slot
    _doubles_r = db.query(PlayerRating).filter(
        PlayerRating.user_id == user_id,
        PlayerRating.sport == data.sport,
        PlayerRating.match_format == data.match_format,
    ).first()
    _doubles_p = db.query(Profile).filter(Profile.id == user_id).first()
    if data.match_format == "mixed_doubles":
        _require_supported_mixed_doubles_gender(_doubles_p)
    my_player_doubles = {
        "player_id":         user_id,
        "rating":           float(_doubles_r.rating)           if _doubles_r else 1500.0,  # type: ignore[arg-type]
        "rating_deviation": float(_doubles_r.rating_deviation) if _doubles_r else 200.0,   # type: ignore[arg-type]
        "win_rate":         _safe_win_rate(_doubles_r),
        "activeness_score": float(_doubles_r.activeness_score) if _doubles_r else 0.5,     # type: ignore[arg-type]
        "current_streak":   int(_doubles_r.current_win_streak) if _doubles_r else 0,       # type: ignore[arg-type]
        "performance_rating": float(_doubles_r.performance_rating) if _doubles_r and _doubles_r.performance_rating is not None else 50.0,  # type: ignore[arg-type]
        "performance_confidence": float(_doubles_r.performance_confidence) if _doubles_r and _doubles_r.performance_confidence is not None else 0.0,  # type: ignore[arg-type]
        "performance_reliable": bool(_doubles_r.performance_reliable) if _doubles_r else False,
        "city_code":        _doubles_p.city_mun_code if _doubles_p else None,
        "province_code":    _doubles_p.province_code if _doubles_p else None,
        "region_code":      _doubles_p.region_code   if _doubles_p else None,
        "gender":           _profile_gender_key(_doubles_p),
    }

    candidates = [
        m for m in assembling
        if user_id not in [str(pid) for pid in get_match_players(m)]
    ]
    # Prioritize matches that are almost full
    candidates.sort(key=lambda m: len(get_match_players(m)), reverse=True)

    for candidate in candidates:
        players = get_match_players(candidate)
        count = len(players)
        if match_mode == "ranked":
            eligible_existing = db.query(PlayerRating).filter(
                PlayerRating.user_id.in_([str(pid) for pid in players]),
                PlayerRating.sport == data.sport,
                PlayerRating.match_format == data.match_format,
                PlayerRating.is_matchmaking_eligible == True,  # noqa: E712
            ).count()
            if eligible_existing < count:
                continue

        candidate_club_ids = _get_match_candidate_club_ids(db, candidate, data.sport)

        if preferred_club_id:
            if preferred_club_id not in candidate_club_ids:
                continue
            resolved_club_id = preferred_club_id
        elif automatic_club_search:
            shared_candidate_clubs = _ordered_club_overlap(search_club_ids, candidate_club_ids)
            if not shared_candidate_clubs:
                continue
            resolved_club_id = shared_candidate_clubs[0]
        else:
            resolved_club_id = str(candidate.club_id) if candidate.club_id is not None else None

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
                    "performance_rating": float(r.performance_rating) if r and r.performance_rating is not None else 50.0,  # type: ignore[arg-type]
                    "performance_confidence": float(r.performance_confidence) if r and r.performance_confidence is not None else 0.0,  # type: ignore[arg-type]
                    "performance_reliable": bool(r.performance_reliable) if r else False,
                    "city_code": p.city_mun_code if p else None,
                    "province_code": p.province_code if p else None,
                    "region_code": p.region_code if p else None,
                    "gender": _profile_gender_key(p),
                })

            if data.match_format == "mixed_doubles" and not is_mixed_doubles_pool_viable(player_stats):
                logger.info(
                    f"[2v2/{match_mode}] Mixed doubles pool is not gender-viable for lobby {candidate.id}"
                )
                continue

            # ── Skill gate for 4th player ─────────────────────────────────────
            # player_stats[:3] = existing lobby players, player_stats[3] = incoming
            lobby_age_4 = int(
                (datetime.now(timezone.utc) - candidate.created_at.replace(tzinfo=timezone.utc)).total_seconds()
            )
            if not can_join_doubles_lobby(
                incoming       = player_stats[3],
                lobby_players  = player_stats[:3],
                sport          = data.sport,
                match_format   = data.match_format,
                lobby_wait_seconds = lobby_age_4,
                mode           = match_mode,
            ):
                logger.info(
                    f"[2v2/{match_mode}] 4th-player gate: skill gap too large "
                    f"(incoming={player_stats[3]['rating']:.0f}, "
                    f"lobby_avg={(sum(p['rating'] for p in player_stats[:3])/3):.0f}) — skipping lobby"
                )
                continue
            # ─────────────────────────────────────────────────────────────────

            # Run balancing service
            best_split = run_matchmaking(player_stats, data.sport, data.match_format, mode=match_mode)
            
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
                
                setattr(candidate, "ml_match_score", best_split["score"])  # save ML score
                if resolved_club_id is not None:
                    setattr(candidate, "club_id", resolved_club_id)
                
                # Auto-assign court if club set
                needs_approval = False
                club_id = candidate.club_id if candidate.club_id is not None else resolved_club_id
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
                        if _requires_club_match_approval(club_obj):
                            setattr(candidate, "status", "pending_approval")
                            setattr(candidate, "started_at", datetime.now(timezone.utc))
                            db.add(MatchSet(match_id=candidate.id, set_number=1, player1_score=0, player2_score=0))
                            needs_approval = True
                            _notify_duty_holders(db, avail.club_id, candidate.id, str(club_obj.name))

                if not needs_approval:
                    setattr(candidate, "status", "awaiting_players")
                    setattr(candidate, "called_at", datetime.now(timezone.utc))
                    setattr(candidate, "started_at", None)
                    ensure_match_lobby_rows(db, candidate)
                
                db.commit()
                candidate_status = candidate.status.value if hasattr(candidate.status, "value") else str(candidate.status)
                return {
                    "status": "matched",
                    "message": "Balanced teams found! Awaiting club confirmation." if needs_approval else "Balanced teams found!",
                    "match_id": str(candidate.id),
                    "match_status": candidate_status,
                    "players_joined": 4,
                    "pending_approval": needs_approval,
                    "split_score": best_split["score"],
                }

        elif count < 3:
            # ── Skill gate for slots 2 & 3 ────────────────────────────────────
            existing_stats = []
            for pid in players:
                er = db.query(PlayerRating).filter(
                    PlayerRating.user_id == pid,
                    PlayerRating.sport == data.sport,
                    PlayerRating.match_format == data.match_format,
                ).first()
                ep = db.query(Profile).filter(Profile.id == pid).first()
                existing_stats.append({
                    "rating":           float(er.rating)           if er else 1200.0,  # type: ignore[arg-type]
                    "rating_deviation": float(er.rating_deviation) if er else 200.0,   # type: ignore[arg-type]
                    "win_rate":         _safe_win_rate(er),
                    "activeness_score": float(er.activeness_score) if er else 0.5,     # type: ignore[arg-type]
                    "current_streak":   int(er.current_win_streak) if er else 0,       # type: ignore[arg-type]
                    "performance_rating": float(er.performance_rating) if er and er.performance_rating is not None else 50.0,  # type: ignore[arg-type]
                    "performance_confidence": float(er.performance_confidence) if er and er.performance_confidence is not None else 0.0,  # type: ignore[arg-type]
                    "performance_reliable": bool(er.performance_reliable) if er else False,
                    "city_code":        ep.city_mun_code if ep else None,
                    "province_code":    ep.province_code if ep else None,
                    "region_code":      ep.region_code   if ep else None,
                    "gender":           _profile_gender_key(ep),
                })
            if data.match_format == "mixed_doubles" and not is_mixed_doubles_pool_viable(existing_stats + [my_player_doubles]):
                logger.info(
                    f"[2v2/{match_mode}] Mixed doubles slot {count+1} is not gender-viable for lobby {candidate.id}"
                )
                continue
            lobby_age = int(
                (datetime.now(timezone.utc) - candidate.created_at.replace(tzinfo=timezone.utc)).total_seconds()
            )
            if not can_join_doubles_lobby(
                incoming       = my_player_doubles,
                lobby_players  = existing_stats,
                sport          = data.sport,
                match_format   = data.match_format,
                lobby_wait_seconds = lobby_age,
                mode           = match_mode,
            ):
                logger.info(
                    f"[2v2/{match_mode}] Slot {count+1} gate: skill gap too large "
                    f"(incoming={my_player_doubles['rating']:.0f}) — skipping lobby {candidate.id}"
                )
                continue
            # ─────────────────────────────────────────────────────────────────

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
        sport=data.sport, match_type=requested_db_match_type,
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
        Match.match_type.in_(["queue", "ranked"]),
        Match.status.in_(["pending", "assembling"]),
        Match.winner_id.is_(None),
        or_(
            Match.player1_id == user_id, Match.player2_id == user_id,
            Match.player3_id == user_id, Match.player4_id == user_id,
        ),
    ).order_by(Match.created_at.desc()).first()

    if not match:
        active_match = db.query(Match).filter(
            Match.match_type.in_(["queue", "ranked"]),
            Match.status.in_(["awaiting_players", "ongoing", "pending_approval"]),
            Match.winner_id.is_(None),
            or_(
                Match.player1_id == user_id, Match.player2_id == user_id,
                Match.player3_id == user_id, Match.player4_id == user_id,
            ),
        ).order_by(Match.created_at.desc()).first()

        if not active_match:
            return {"in_queue": False}

        linked_party = db.query(Party.id).filter(Party.match_id == active_match.id).first()
        return {
            "in_queue": False,
            "active_match": True,
            "match_id": str(active_match.id),
            "match_status": active_match.status.value,
            "sport": active_match.sport.value,
            "match_format": active_match.match_format.value,
            "match_mode": _resolve_match_mode(active_match),
            "return_route": "/matches/party" if linked_party else "/matches/queue",
        }

    players_joined = sum(
        1 for pid in [match.player1_id, match.player2_id, match.player3_id, match.player4_id]
        if pid is not None
    )

    return {
        "in_queue":      True,
        "sport":         match.sport.value,
        "match_format":  match.match_format.value,
        "match_mode":    _resolve_match_mode(match),
        "status":        match.status.value,   # "pending" | "assembling"
        "players_joined": players_joined,
        "queued_at":     match.created_at.isoformat() if match.created_at is not None else None,  # type: ignore[union-attr]
    }


@router.get("/queue/status")
def get_queue_status(
    sport: str,
    match_format: str = "singles",
    match_mode: str = "ranked",
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]

    if match_format == "singles":
        singles_match_type = "ranked" if match_mode in ("ranked", "club") else "queue"
        # Include both player slots for live states so player2 can't accidentally requeue.
        match = db.query(Match).filter(
            Match.match_type == singles_match_type, Match.sport == sport,
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
            return {
                "status": "matched",
                "match_id": str(match.id),
                "match_status": status_val,
                "pending_approval": status_val == "pending_approval",
            }
        return {"status": "waiting", "match_id": str(match.id)}

    doubles_match_type = "ranked" if match_mode in ("ranked", "club") else "queue"
    # Doubles — include "ongoing" / "pending_approval" so players get notified
    match = db.query(Match).filter(
        Match.match_type == doubles_match_type, Match.sport == sport,
        Match.match_format == match_format,
        Match.status.in_(["assembling", "pending", "awaiting_players", "pending_approval", "ongoing"]),
        Match.status != "invalidated",
        or_(
            Match.player1_id == user_id, Match.player2_id == user_id,
            Match.player3_id == user_id, Match.player4_id == user_id,
        ),
        Match.winner_id.is_(None),
    ).order_by(Match.created_at.desc()).first()

    if not match:
        return {"status": "not_in_queue"}

    if match.status.value in ("awaiting_players", "ongoing", "pending_approval"):
        return {
            "status": "matched",
            "match_id": str(match.id),
            "match_status": match.status.value,
            "pending_approval": match.status.value == "pending_approval",
        }

    players_joined = sum(1 for pid in [match.player1_id, match.player2_id, match.player3_id, match.player4_id] if pid is not None)
    return {"status": "assembling", "match_id": str(match.id), "players_joined": players_joined}


@router.delete("/queue/leave")
def leave_queue(
    sport: str,
    match_format: str = "singles",
    match_mode: str = "ranked",
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]

    if match_format == "singles":
        singles_match_type = "ranked" if match_mode in ("ranked", "club") else "queue"
        match = db.query(Match).filter(
            Match.match_type == singles_match_type, Match.player1_id == user_id,
            Match.sport == sport, Match.match_format == match_format,
            Match.status == "pending", Match.player2_id.is_(None),
        ).first()
        if match:
            setattr(match, "status", "cancelled")
            db.commit()
        return {"message": "Left queue."}

    doubles_match_type = "ranked" if match_mode in ("ranked", "club") else "queue"
    match = db.query(Match).filter(
        Match.match_type == doubles_match_type, Match.sport == sport,
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


@router.post("/{match_id}/presence/ping")
def ping_match_presence(
    match_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    match = db.query(Match).filter(Match.id == match_id).first()
    if not match:
        raise HTTPException(404, "Match not found.")

    user_id = current_user["id"]
    participant_ids = [
        str(pid)
        for pid in [match.player1_id, match.player2_id, match.player3_id, match.player4_id, match.referee_id]
        if pid is not None
    ]
    if user_id not in participant_ids:
        raise HTTPException(403, "Not a participant of this match.")

    status_val = match.status.value if hasattr(match.status, "value") else str(match.status)
    if _match_presence_redis is not None and status_val not in ("completed", "cancelled", "invalidated"):
        _touch_match_presence(match_id, user_id)

    presence = _serialize_match_presence(match)
    invalidated_reason = _maybe_auto_invalidate_disconnected_match(db, match, presence)
    if invalidated_reason:
        db.refresh(match)
        status_val = match.status.value if hasattr(match.status, "value") else str(match.status)
        presence = _serialize_match_presence(match)

    return {
        "match_status": status_val,
        "presence_supported": _match_presence_redis is not None,
        "invalidated_reason": invalidated_reason,
        "presence": presence,
    }


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
            "tournament_id": str(match.tournament_id) if match.tournament_id is not None else None,
            "tournament_phase": str(getattr(match, "tournament_phase", "")) if getattr(match, "tournament_phase", None) is not None else None,
            "called_at": str(match.called_at) if getattr(match, "called_at", None) is not None else None,
            "checkin_deadline_at": str(match.checkin_deadline_at) if getattr(match, "checkin_deadline_at", None) is not None else None,
            "team1_ready_at": str(match.team1_ready_at) if getattr(match, "team1_ready_at", None) is not None else None,
            "team2_ready_at": str(match.team2_ready_at) if getattr(match, "team2_ready_at", None) is not None else None,
            "referee_ready_at": str(match.referee_ready_at) if getattr(match, "referee_ready_at", None) is not None else None,
            "result_submitted_at": str(match.result_submitted_at) if getattr(match, "result_submitted_at", None) is not None else None,
            "result_confirmed_at": str(match.result_confirmed_at) if getattr(match, "result_confirmed_at", None) is not None else None,
            "dispute_reason": getattr(match, "dispute_reason", None),
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
        if match.tournament_id is not None:
            setattr(match, "result_submitted_at", datetime.now(timezone.utc))
            setattr(match, "result_submitted_by", current_user["id"])
            setattr(match, "tournament_phase", "result_pending")
            db.add(MatchHistory(
                match_id=match_id,
                event_type="result_submitted",
                recorded_by=current_user["id"],
                description="Result submitted to the tournament bracket.",
                meta={"winner_id": data.winner_id},
            ))
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
    match_history_rows = []
    if is_doubles:
        match_history_rows = (
            db.query(MatchHistory)
            .filter(
                MatchHistory.match_id == match.id,
                MatchHistory.event_type.in_(("point", "violation", "serve_change")),
            )
            .order_by(MatchHistory.created_at.asc(), MatchHistory.id.asc())
            .all()
        )

    # Compute Glicko-2 in Python.
    # For doubles, each player's opponent is the *average* rating of the opposing team
    # rather than just the anchor's individual rating — this prevents asymmetric drift
    # when partners have very different ratings.
    p1_wins = winner_anchor_id == player1_id

    # partner_updates holds (p3, p4, new_p3, new_p4) when doubles partners exist
    partner_updates: tuple | None = None

    if is_doubles and p3 and p4:
        import math as _math
        team1_avg_r  = (float(p1.rating) + float(p3.rating)) / 2  # type: ignore[arg-type]
        team1_avg_rd = _math.sqrt((float(p1.rating_deviation) ** 2 + float(p3.rating_deviation) ** 2) / 2)  # type: ignore[arg-type]
        team2_avg_r  = (float(p2.rating) + float(p4.rating)) / 2  # type: ignore[arg-type]
        team2_avg_rd = _math.sqrt((float(p2.rating_deviation) ** 2 + float(p4.rating_deviation) ** 2) / 2)  # type: ignore[arg-type]

        new_p1_r, new_p1_rd, new_p1_vol = glicko_update(
            rating=float(p1.rating), rd=float(p1.rating_deviation), volatility=float(p1.volatility),  # type: ignore[arg-type]
            opp_rating=team2_avg_r, opp_rd=team2_avg_rd,
            score=1.0 if p1_wins else 0.0,
        )
        new_p2_r, new_p2_rd, new_p2_vol = glicko_update(
            rating=float(p2.rating), rd=float(p2.rating_deviation), volatility=float(p2.volatility),  # type: ignore[arg-type]
            opp_rating=team1_avg_r, opp_rd=team1_avg_rd,
            score=0.0 if p1_wins else 1.0,
        )
        partner_updates = (
            p3,
            glicko_update(
                rating=float(p3.rating), rd=float(p3.rating_deviation), volatility=float(p3.volatility),  # type: ignore[arg-type]
                opp_rating=team2_avg_r, opp_rd=team2_avg_rd,
                score=1.0 if p1_wins else 0.0,
            ),
            p4,
            glicko_update(
                rating=float(p4.rating), rd=float(p4.rating_deviation), volatility=float(p4.volatility),  # type: ignore[arg-type]
                opp_rating=team1_avg_r, opp_rd=team1_avg_rd,
                score=0.0 if p1_wins else 1.0,
            ),
        )
        adjusted_ratings = redistribute_match_ratings_by_performance(
            match,
            match_history_rows,
            {
                player1_id: float(p1.rating),  # type: ignore[arg-type]
                player2_id: float(p2.rating),  # type: ignore[arg-type]
                team1_partner_id: float(p3.rating),  # type: ignore[arg-type]
                team2_partner_id: float(p4.rating),  # type: ignore[arg-type]
            },
            {
                player1_id: new_p1_r,
                player2_id: new_p2_r,
                team1_partner_id: partner_updates[1][0],
                team2_partner_id: partner_updates[3][0],
            },
            winner_id=data.winner_id,
        )
        new_p1_r = adjusted_ratings.get(player1_id, new_p1_r)
        new_p2_r = adjusted_ratings.get(player2_id, new_p2_r)
        partner_updates = (
            p3,
            (
                adjusted_ratings.get(team1_partner_id, partner_updates[1][0]),
                partner_updates[1][1],
                partner_updates[1][2],
            ),
            p4,
            (
                adjusted_ratings.get(team2_partner_id, partner_updates[3][0]),
                partner_updates[3][1],
                partner_updates[3][2],
            ),
        )
    else:
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
    try:
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
        db.expire(p1)
        db.expire(p2)
        _refresh_rating_eligibility(db, [player1_id, player2_id], sport, match_format)
    except Exception as exc:
        db.rollback()
        logger.error(
            f"[complete_match] fn_complete_match failed for match {match_id}: {exc}"
        )
        raise HTTPException(
            status_code=500,
            detail=(
                f"Match completion failed in the database. "
                f"The match is still marked as ongoing. "
                f"An admin can force-complete it via POST /admin/matches/{match_id}/force-complete. "
                f"Error: {exc}"
            ),
        )
    if match.tournament_id is not None:
        setattr(match, "result_submitted_at", datetime.now(timezone.utc))
        setattr(match, "result_submitted_by", current_user["id"])
        setattr(match, "tournament_phase", "result_pending")
        db.add(MatchHistory(
            match_id=match_id,
            event_type="result_submitted",
            recorded_by=current_user["id"],
            description="Result submitted to the tournament bracket.",
            meta={"winner_id": winner_anchor_id},
        ))
    db.commit()

    # Stored procedure currently updates only player1/player2.
    # Apply equivalent progression for doubles partners when present.
    if partner_updates is not None:
        _p3, (p3_r, p3_rd, p3_vol), _p4, (p4_r, p4_rd, p4_vol) = partner_updates
        _apply_rating_result(_p3, won=p1_wins,      new_rating=p3_r, new_rd=p3_rd, new_vol=p3_vol)
        _apply_rating_result(_p4, won=not p1_wins,  new_rating=p4_r, new_rd=p4_rd, new_vol=p4_vol)
        _refresh_rating_eligibility(db, [team1_partner_id, team2_partner_id], sport, match_format)
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
        refresh_performance_metrics(
            db,
            [player1_id, player2_id, team1_partner_id, team2_partner_id],
            sport=sport,
            match_format=match_format,
        )
        db.commit()
    except Exception as exc:
        db.rollback()
        logger.warning(f"[performance] Failed to refresh performance metrics for match {match_id}: {exc}")

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

def _is_knockout_match(match: Match, tournament: Tournament) -> bool:
    """Return True if this match is part of the knockout stage."""
    fmt = str(tournament.format)
    if any(f in fmt for f in ("single_elimination", "double_elimination")):
        return True  # every match is knockout
    if "group_stage_knockout" in fmt:
        side = getattr(match, "bracket_side", "") or ""
        return side == "K"
    return False   # pool_play, round_robin, swiss — no knockout


def _resolved_ruleset(match: Match, db) -> dict | None:
    """Return the ruleset for a match with best_of and score_limit overrides applied."""
    ruleset = get_ruleset(match.sport.value if hasattr(match.sport, "value") else str(match.sport))
    if not ruleset:
        return None
    ruleset = dict(ruleset)  # always copy so we never mutate the module-level cache

    match_best_of = getattr(match, "best_of", None)
    if match_best_of in (1, 3, 5):
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
        t = db.query(Tournament).filter(Tournament.id == match.tournament_id).first()
        if t:
            if _is_knockout_match(match, t):
                best_of = getattr(t, "knockout_best_of", 3) or 3
                if best_of == 1:
                    ruleset["sets_to_win"] = 1
                    ruleset["max_sets"]    = 1
            else:
                # group / pool / round-robin / swiss stage
                best_of = getattr(t, "group_stage_best_of", 1) or 1
                if best_of == 1:
                    ruleset["sets_to_win"] = 1
                    ruleset["max_sets"]    = 1
                elif best_of == 3:
                    ruleset["sets_to_win"] = 2
                    ruleset["max_sets"]    = 3

    _score_limit: int | None = getattr(match, "score_limit", None)
    if _score_limit and "points_per_set" in ruleset:
        ruleset["points_per_set"] = _score_limit
        ruleset["score_limit"]    = _score_limit

    return ruleset


@router.get("/{match_id}/ruleset")
def get_match_ruleset(
    match_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    match = db.query(Match).filter(Match.id == match_id).first()
    if not match:
        raise HTTPException(404, "Match not found.")
    ruleset = _resolved_ruleset(match, db)
    if not ruleset:
        raise HTTPException(404, "No ruleset found for this sport.")
    return {"sport": match.sport.value if hasattr(match.sport, "value") else str(match.sport), "ruleset": ruleset}


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

    # Idempotent replays should not consume rate-limit budget.
    if data.client_action_id:
        existing = db.query(MatchHistory).filter(
            MatchHistory.match_id == match_id,
            MatchHistory.meta["client_action_id"].astext == data.client_action_id,
        ).first()
        if existing:
            return {"message": "Already recorded.", "event_id": str(existing.id)}

    # Generous burst cap per referee per match. This prevents runaway API spam
    # without blocking fast referee testing/calibration scoring.
    if not check_rate_limit(
        scoring_rate_limit_key(match_id, user_id),
        max_calls=_SCORING_RATE_LIMIT_MAX_CALLS,
        window_seconds=_SCORING_RATE_LIMIT_WINDOW_SECONDS,
    ):
        raise HTTPException(429, "Too many scoring actions. Slow down.")

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
        actor_name = _profile_display_name(db, data.actor_player_id)
        description = f"Point -> {team_label} - {_humanize_history_label(data.reason_code) or 'Opponent Error'}"
        if actor_name:
            description += f" - error by {actor_name}"
        description += " (opponent error)"
    elif data.attribution_type == "winning_shot" and data.cause:
        scorer_name = _profile_display_name(db, data.player_id)
        description = f"Point -> {team_label} - {_humanize_history_label(data.cause) or data.cause}"
        if scorer_name:
            description += f" by {scorer_name}"
    else:
        description = f"Point -> {team_label}"
        if data.cause:
            description += f" - {_humanize_history_label(data.cause) or data.cause}"

    if False and data.attribution_type == "opponent_error" and data.reason_code:
        # Get actor player username for description
        actor_label = ""
        if data.actor_player_id:
            actor = db.query(Profile).filter(Profile.id == data.actor_player_id).first()
        description = f"Point → {team_label} · {data.reason_code.replace('_', ' ').title()}{actor_label} (opponent error)"
    elif False and data.attribution_type == "winning_shot" and data.cause:
        scorer_label = ""
        if data.player_id:
            scorer = db.query(Profile).filter(Profile.id == data.player_id).first()
        description = f"Point → {team_label} · {data.cause}{scorer_label}"
    elif False:
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
    ruleset    = _resolved_ruleset(match, db)
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
            setattr(match_set, "is_completed", True)
            setattr(match_set, "completed_at", datetime.now(timezone.utc))

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
                existing_next_set = db.query(MatchSet).filter(
                    MatchSet.match_id == match_id,
                    MatchSet.set_number == next_set,
                ).first()
                if existing_next_set:
                    next_set_has_scoring = db.query(MatchHistory.id).filter(
                        MatchHistory.match_id == match_id,
                        MatchHistory.set_number == next_set,
                        MatchHistory.event_type.in_(["point", "violation"]),
                    ).first() is not None
                    if not next_set_has_scoring:
                        setattr(existing_next_set, "player1_score", 0)
                        setattr(existing_next_set, "player2_score", 0)
                        setattr(existing_next_set, "team1_score", 0)
                        setattr(existing_next_set, "team2_score", 0)
                else:
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

    violator = db.query(Profile).filter(Profile.id == data.player_id).first()
    v_name = f"{violator.first_name} {violator.last_name}".strip() if violator else str(data.player_id)[:8]

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
    description = f"Violation: {data.violation_code} by {v_name}{pt_label}"

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
    fault_player_name = _profile_display_name(db, data.fault_player_id)

    if data.event_type == "loss_of_serve":
        description = f"Loss of serve - {fault_label}"
        if fault_player_name:
            description += f" - {fault_player_name}"
        description += " - Server 2 serves next"
    else:
        description = f"Side out - {fault_label}"
        if fault_player_name:
            description += f" - {fault_player_name}"
        description += f" - {new_server_label} now serving"

    if data.fault_player_id:
        actor = db.query(Profile).filter(Profile.id == data.fault_player_id).first()

    if False and data.event_type == "loss_of_serve":
        description = f"Loss of serve — {fault_label} · Server 2 serves next"
    elif False:
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
        setattr(match_set, "is_completed", False)
        setattr(match_set, "completed_at", None)

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

# ── Offline sync batch endpoint ───────────────────────────────────────────────
# Accepts an ordered list of offline-queued actions and replays them against
# the live match state.  Each action carries a client_action_id so duplicates
# are silently skipped (idempotent replay).  Returns a per-action result list
# so the client knows exactly which actions succeeded, were skipped, or failed.

class OfflineSyncAction(BaseModel):
    qid: str                          # Client-side idempotency key
    type: str                         # "point" | "serve_change" | "violation" | "undo" | "complete"
    payload: dict                     # Same payload that would be sent to the individual endpoint

class OfflineSyncRequest(BaseModel):
    actions: list[OfflineSyncAction]


@router.post("/{match_id}/offline-sync", status_code=200)
def offline_sync(
    match_id: str,
    data: OfflineSyncRequest,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Replay an offline event queue in one request.
    Processes actions sequentially.  Each action is idempotent via its qid
    (stored as client_action_id in MatchHistory.meta).
    """
    match = db.query(Match).filter(Match.id == match_id).first()
    if not match:
        raise HTTPException(404, "Match not found.")

    user_id = current_user["id"]
    if not _can_score(match, user_id):
        raise HTTPException(403, "Not authorized to sync offline actions for this match.")

    results = []
    for action in data.actions:
        action_type = action.type
        payload = action.payload
        qid = action.qid

        try:
            # Inject idempotency key so duplicates are skipped
            payload["client_action_id"] = qid

            if action_type == "point":
                req = RecordPointRequest(**payload)
                # Minimal duplicate check — reuse the same check as record_point
                existing = db.query(MatchHistory).filter(
                    MatchHistory.match_id == match_id,
                    MatchHistory.meta["client_action_id"].astext == qid,
                ).first()
                if existing:
                    results.append({"qid": qid, "status": "skipped", "reason": "already_recorded"})
                    continue

                status_val = match.status.value if hasattr(match.status, "value") else str(match.status)
                if status_val != "ongoing":
                    results.append({"qid": qid, "status": "skipped", "reason": f"match_status_{status_val}"})
                    continue

                if req.team not in ("team1", "team2"):
                    results.append({"qid": qid, "status": "error", "reason": "invalid_team"})
                    continue

                match_set = db.query(MatchSet).filter(
                    MatchSet.match_id == match_id, MatchSet.set_number == req.set_number,
                ).first()
                if not match_set:
                    match_set = MatchSet(
                        match_id=match_id, set_number=req.set_number,
                        player1_score=0, player2_score=0, team1_score=0, team2_score=0,
                    )
                    db.add(match_set)
                    db.flush()

                t1 = int(match_set.team1_score or match_set.player1_score or 0)  # type: ignore[arg-type]
                t2 = int(match_set.team2_score or match_set.player2_score or 0)  # type: ignore[arg-type]
                if req.team == "team1":
                    t1 += 1
                    setattr(match_set, "team1_score", t1); setattr(match_set, "player1_score", t1)
                else:
                    t2 += 1
                    setattr(match_set, "team2_score", t2); setattr(match_set, "player2_score", t2)

                meta: dict = {"client_action_id": qid, "via_offline_sync": True}
                if req.attribution_type: meta["attribution_type"] = req.attribution_type
                if req.player_id:        meta["player_id"]        = req.player_id
                if req.notes:            meta["notes"]            = req.notes

                db.add(MatchHistory(
                    match_id=match_id, event_type="point", team=req.team,
                    recorded_by=user_id,
                    description=f"{'Team 1' if req.team == 'team1' else 'Team 2'} scored — offline sync",
                    set_number=req.set_number, team1_score=t1, team2_score=t2,
                    meta=meta,
                ))
                db.flush()
                results.append({"qid": qid, "status": "ok"})

            elif action_type == "complete":
                status_val = match.status.value if hasattr(match.status, "value") else str(match.status)
                if status_val == "completed":
                    results.append({"qid": qid, "status": "skipped", "reason": "already_completed"})
                    continue
                # Delegate to complete_match — handled by calling its DB logic inline
                winner_id = payload.get("winner_id")
                if not winner_id:
                    results.append({"qid": qid, "status": "error", "reason": "missing_winner_id"})
                    continue
                db.commit()  # commit any preceding points first
                # Re-fetch to get updated status
                db.refresh(match)
                results.append({
                    "qid": qid,
                    "status": "pending",
                    "reason": "Call POST /matches/{id}/complete with winner_id to finalize",
                    "winner_id": winner_id,
                })

            else:
                # serve_change, violation, undo — fall back to individual endpoints
                results.append({
                    "qid": qid,
                    "status": "skipped",
                    "reason": f"type '{action_type}' must be replayed via its individual endpoint",
                })

        except Exception as exc:
            db.rollback()
            results.append({"qid": qid, "status": "error", "reason": str(exc)})

    try:
        db.commit()
    except Exception as exc:
        db.rollback()
        raise HTTPException(500, f"Batch commit failed: {exc}")

    _broadcast(match_id, {"type": "offline_sync_complete", "match_id": match_id})
    return {"results": results, "total": len(results)}


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
