import asyncio
from collections import defaultdict
from datetime import timedelta, timezone
from itertools import combinations
import json
import logging
from typing import Any, Mapping, Optional, cast
import datetime as dt
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from jose import JWTError, jwt
from pydantic import BaseModel, Field
from sqlalchemy import or_
from sqlalchemy.orm import Session

from app.config import settings
from app.database import get_db
from app.middleware.auth import get_current_user
from app.utils.skill_tiers import get_skill_tier_name
from app.models.models import (
    Club,
    ClubMember,
    Court,
    OpenPlayAssignment,
    OpenPlayAssignmentPlayer,
    OpenPlayParticipant,
    OpenPlayQueueEntry,
    OpenPlaySession,
    OpenPlaySessionCourt,
    PlayerRating,
    Profile,
)
from app.services.matchmaking import run_matchmaking, score_candidate
from app.services.notifications import send_bulk_notifications, send_notification

router = APIRouter()
logger = logging.getLogger(__name__)

SPORT_EMOJIS = {
    "badminton": "🏸",
    "pickleball": "🏓",
    "lawn_tennis": "🎾",
    "table_tennis": "🏓",
}

VALID_MATCH_FORMATS = {"singles", "doubles", "mixed_doubles"}
VALID_QUEUE_MODES = {"fifo", "balanced"}
VALID_ROTATION_MODES = {"four_on_four_off", "winners_stay_two_off"}
ACTIVE_ASSIGNMENT_STATUSES = {"called", "in_game"}
VALID_COURT_ROLES = {"standard", "challenge"}

try:
    import redis as _redis_sync
    _open_play_redis_pub = _redis_sync.from_url(settings.redis_url, decode_responses=True)
    _open_play_redis_pub.ping()
except Exception as exc:
    _open_play_redis_pub = None
    logger.warning(f"[open_play] Redis publish unavailable. Reason: {exc}")

try:
    import redis.asyncio as _aioredis  # type: ignore[import]
    _open_play_aredis = _aioredis.from_url(settings.redis_url, decode_responses=True)
except Exception as exc:
    _open_play_aredis = None
    logger.warning(f"[open_play] Async Redis unavailable for SSE. Reason: {exc}")


class CreateOpenPlaySessionRequest(BaseModel):
    title: str
    description: Optional[str] = None
    sport: str
    match_format: str = "doubles"
    session_date: dt.datetime
    duration_hours: Optional[float] = 1.0
    max_players: int
    price_per_head: Optional[float] = 0.0
    court_id: Optional[str] = None
    skill_min: Optional[float] = None
    skill_max: Optional[float] = None
    notes: Optional[str] = None
    queue_mode: str = "fifo"
    rotation_mode: str = "four_on_four_off"
    ack_timeout_seconds: int = Field(default=60, ge=15, le=300)
    target_score: int = Field(default=11, ge=1, le=99)
    win_by_two: bool = False
    auto_assign_enabled: bool = True


class UpdateOpenPlaySessionRequest(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    session_date: Optional[dt.datetime] = None
    duration_hours: Optional[float] = None
    max_players: Optional[int] = None
    price_per_head: Optional[float] = None
    skill_min: Optional[float] = None
    skill_max: Optional[float] = None
    notes: Optional[str] = None
    status: Optional[str] = None
    match_format: Optional[str] = None
    queue_mode: Optional[str] = None
    rotation_mode: Optional[str] = None
    ack_timeout_seconds: Optional[int] = Field(default=None, ge=15, le=300)
    target_score: Optional[int] = Field(default=None, ge=1, le=99)
    win_by_two: Optional[bool] = None
    auto_assign_enabled: Optional[bool] = None


class JoinQueueRequest(BaseModel):
    partner_user_id: Optional[str] = None
    ready: bool = True


class QueueEntryActionRequest(BaseModel):
    action: str  # ready|pause|leave


class CompleteAssignmentRequest(BaseModel):
    winner_side: Optional[int] = Field(default=None, ge=1, le=2)
    side1_score: Optional[int] = Field(default=None, ge=0, le=999)
    side2_score: Optional[int] = Field(default=None, ge=0, le=999)


class UpdateSessionCourtRequest(BaseModel):
    court_role: Optional[str] = None
    max_consecutive_wins: Optional[int] = Field(default=None, ge=1, le=20)
    is_active: Optional[bool] = None


def _utcnow() -> dt.datetime:
    return dt.datetime.now(timezone.utc)


def _as_datetime_value(value: Any) -> dt.datetime | None:
    return cast(dt.datetime | None, value)


def _as_utc(value: Any) -> dt.datetime | None:
    normalized = _as_datetime_value(value)
    if normalized is None:
        return None
    if normalized.tzinfo is None:
        return normalized.replace(tzinfo=timezone.utc)
    return normalized.astimezone(timezone.utc)


def _ts(value: Any) -> float:
    normalized = _as_utc(value)
    return normalized.timestamp() if normalized is not None else 0.0


def _iso_utc(value: Any) -> str | None:
    normalized = _as_utc(value)
    return normalized.isoformat() if normalized is not None else None


def _as_int_value(value: Any, default: int = 0) -> int:
    return default if value is None else int(value)


def _as_float_value(value: Any, default: float = 0.0) -> float:
    return default if value is None else float(value)


def _as_bool_value(value: Any) -> bool:
    return bool(cast(bool | None, value))


def _as_status_value(value: Any) -> str:
    return str(value)


def _as_text_value(value: Any, default: str = "") -> str:
    return default if value is None else str(value)


def _session_court_size(session: OpenPlaySession) -> int:
    return 2 if str(session.match_format) == "singles" else 4


def _entry_size(entry: OpenPlayQueueEntry) -> int:
    return 2 if entry.player2_id is not None else 1


def _entry_user_ids(entry: OpenPlayQueueEntry) -> list[str]:
    ids = [str(entry.player1_id)]
    if entry.player2_id is not None:
        ids.append(str(entry.player2_id))
    return ids


def _entry_contains_user(entry: OpenPlayQueueEntry, user_id: str) -> bool:
    return user_id in _entry_user_ids(entry)


def _queue_sort_key(session: OpenPlaySession, entry: OpenPlayQueueEntry) -> tuple[float, float, float]:
    if str(session.queue_mode) == "balanced":
        return (
            -_as_float_value(entry.skip_count),
            _ts(entry.last_played_at),
            _ts(entry.queued_at or entry.created_at),
        )
    return (
        _ts(entry.queued_at or entry.created_at),
        _ts(entry.created_at),
        _as_float_value(entry.skip_count),
    )


def _skill_label_for_rating(rating_value: float | None, rating_status: str) -> str | None:
    if rating_value is None:
        return None
    if rating_status != "RATED":
        return "Calibrating"
    return get_skill_tier_name(rating_value)


def _profile_brief(profile: Profile | None, rating: PlayerRating | None = None) -> dict | None:
    if profile is None:
        return None
    rating_value = _as_float_value(rating.rating) if rating is not None and rating.rating is not None else None
    rating_status = _as_text_value(rating.rating_status, "CALIBRATING") if rating is not None else None
    return {
        "id": str(profile.id),
        "first_name": profile.first_name,
        "last_name": profile.last_name,
        "avatar_url": profile.avatar_url,
        "rating": rating_value,
        "rating_status": rating_status,
        "skill_level": _skill_label_for_rating(rating_value, rating_status or "CALIBRATING"),
        "performance_rating": _as_float_value(rating.performance_rating, 50.0) if rating is not None else 50.0,
        "performance_confidence": _as_float_value(rating.performance_confidence, 0.0) if rating is not None else 0.0,
        "performance_reliable": bool(rating.performance_reliable) if rating is not None else False,
        "matches_played": _as_int_value(rating.matches_played) if rating is not None else None,
        "is_matchmaking_eligible": bool(rating.is_matchmaking_eligible) if rating is not None else False,
        "is_leaderboard_eligible": bool(rating.is_leaderboard_eligible) if rating is not None else False,
    }


def _display_name(profile: Profile | None) -> str:
    if profile is None:
        return "A player"
    return f"{profile.first_name or ''} {profile.last_name or ''}".strip() or "Player"


def _participant_status_rank(status: str) -> int:
    return {
        "confirmed": 0,
        "waitlisted": 1,
    }.get(status, 9)


def _serialize_participant_rows(
    participant_rows: list[OpenPlayParticipant],
    current_user_id: str,
    profiles_map: Mapping[str, Profile],
    ratings_map: Mapping[str, PlayerRating],
    participant_queue_map: Mapping[str, OpenPlayQueueEntry] | None = None,
) -> list[dict]:
    serialized_participants = []
    for participant in sorted(
        participant_rows,
        key=lambda item: (
            _participant_status_rank(_as_status_value(item.status)),
            _ts(item.joined_at),
            str(item.user_id),
        ),
    ):
        user_id = str(participant.user_id)
        queue_entry = participant_queue_map.get(user_id) if participant_queue_map is not None else None
        serialized_participants.append({
            "user_id": user_id,
            "profile": _profile_brief(profiles_map.get(user_id), ratings_map.get(user_id)),
            "status": participant.status,
            "joined_at": _iso_utc(participant.joined_at),
            "queue_status": queue_entry.status if queue_entry is not None else None,
            "is_ready": _as_bool_value(queue_entry.is_ready) if queue_entry is not None else False,
            "is_available_for_pairing": (
                participant_queue_map is not None
                and queue_entry is None
                and _as_status_value(participant.status) == "confirmed"
                and user_id != current_user_id
            ),
        })
    return serialized_participants


def _safe_win_rate(rating: PlayerRating | None) -> float:
    if rating is None:
        return 0.5
    wins = _as_int_value(rating.wins)
    losses = _as_int_value(rating.losses)
    total = wins + losses
    return wins / total if total > 0 else 0.5


def _player_snapshot(
    user_id: str,
    profiles_map: dict[str, Profile],
    ratings_map: dict[str, PlayerRating],
) -> dict:
    profile = profiles_map.get(user_id)
    rating = ratings_map.get(user_id)
    current_win_streak = _as_int_value(rating.current_win_streak) if rating is not None else 0
    current_loss_streak = _as_int_value(rating.current_loss_streak) if rating is not None else 0
    current_streak = current_win_streak if current_win_streak > 0 else -current_loss_streak
    return {
        "player_id": user_id,
        "rating": _as_float_value(rating.rating, 1500.0) if rating is not None else 1500.0,
        "rating_deviation": _as_float_value(rating.rating_deviation, 350.0) if rating is not None else 350.0,
        "win_rate": _safe_win_rate(rating),
        "activeness_score": _as_float_value(rating.activeness_score, 0.0) if rating is not None else 0.0,
        "current_streak": current_streak,
        "performance_rating": _as_float_value(rating.performance_rating, 50.0) if rating is not None else 50.0,
        "performance_confidence": _as_float_value(rating.performance_confidence, 0.0) if rating is not None else 0.0,
        "performance_reliable": _as_bool_value(rating.performance_reliable) if rating is not None else False,
        "city_code": _as_text_value(profile.city_mun_code) if profile is not None else None,
        "province_code": _as_text_value(profile.province_code) if profile is not None else None,
        "region_code": _as_text_value(profile.region_code) if profile is not None else None,
    }


def _team_average(players: list[dict], field: str, default: float) -> float:
    values = [float(player.get(field, default)) for player in players]
    return sum(values) / len(values) if values else default


def _score_fixed_team_matchup(
    team_a: list[dict],
    team_b: list[dict],
    session: OpenPlaySession,
) -> float:
    if not team_a or not team_b:
        return 0.0
    return score_candidate(
        rating_a=_team_average(team_a, "rating", 1500.0),
        rd_a=_team_average(team_a, "rating_deviation", 350.0),
        win_rate_a=_team_average(team_a, "win_rate", 0.5),
        activeness_a=_team_average(team_a, "activeness_score", 0.0),
        streak_a=int(round(_team_average(team_a, "current_streak", 0.0))),
        city_a=team_a[0].get("city_code"),
        province_a=team_a[0].get("province_code"),
        region_a=team_a[0].get("region_code"),
        rating_b=_team_average(team_b, "rating", 1500.0),
        rd_b=_team_average(team_b, "rating_deviation", 350.0),
        win_rate_b=_team_average(team_b, "win_rate", 0.5),
        activeness_b=_team_average(team_b, "activeness_score", 0.0),
        streak_b=int(round(_team_average(team_b, "current_streak", 0.0))),
        city_b=team_b[0].get("city_code"),
        province_b=team_b[0].get("province_code"),
        region_b=team_b[0].get("region_code"),
        sport=str(session.sport),
        match_format=str(session.match_format),
        wait_seconds=0,
        h2h_count=0,
    )


def _entry_player_snapshots(entry: OpenPlayQueueEntry, player_snapshots: dict[str, dict]) -> list[dict]:
    return [player_snapshots[user_id] for user_id in _entry_user_ids(entry) if user_id in player_snapshots]


def _load_ratings_map(
    user_ids: set[str],
    session: OpenPlaySession,
    db: Session,
) -> dict[str, PlayerRating]:
    if not user_ids:
        return {}
    rows = db.query(PlayerRating).filter(
        PlayerRating.user_id.in_(list(user_ids)),
        PlayerRating.sport == str(session.sport),
        PlayerRating.match_format == str(session.match_format),
    ).all()
    return {str(row.user_id): row for row in rows}


def _validate_skill_window(skill_min: float | None, skill_max: float | None) -> None:
    if skill_min is not None and skill_max is not None and skill_min > skill_max:
        raise HTTPException(400, "skill_min cannot be greater than skill_max.")


def _require_skill_eligible_player(
    session: OpenPlaySession,
    user_id: str,
    ratings_map: dict[str, PlayerRating],
) -> PlayerRating | None:
    skill_min = _as_float_value(session.skill_min) if session.skill_min is not None else None
    skill_max = _as_float_value(session.skill_max) if session.skill_max is not None else None
    rating = ratings_map.get(user_id)
    if skill_min is None and skill_max is None:
        return rating
    if rating is None or rating.rating is None:
        raise HTTPException(
            400,
            "This open play session requires a player rating for its configured skill range.",
        )
    rating_value = _as_float_value(rating.rating, 1500.0)
    if skill_min is not None and rating_value < skill_min:
        raise HTTPException(
            400,
            f"Your current rating ({rating_value:.0f}) is below this session's minimum skill rating.",
        )
    if skill_max is not None and rating_value > skill_max:
        raise HTTPException(
            400,
            f"Your current rating ({rating_value:.0f}) is above this session's maximum skill rating.",
        )
    return rating


def _default_assignment_minutes(session: OpenPlaySession) -> float:
    target_score = _as_int_value(session.target_score, 11)
    base_minutes = 10.0 if str(session.match_format) == "singles" else 12.0
    scaled_minutes = base_minutes * max(1.0, target_score / 11.0)
    if _as_bool_value(session.win_by_two):
        scaled_minutes += 2.0
    return round(max(6.0, scaled_minutes), 1)


def _estimate_assignment_minutes(session: OpenPlaySession, assignments: list[OpenPlayAssignment]) -> float:
    completed_durations: list[float] = []
    for assignment in assignments:
        if _as_status_value(assignment.status) != "completed":
            continue
        started_at = _as_utc(assignment.started_at)
        completed_at = _as_utc(assignment.completed_at)
        if started_at is None or completed_at is None or completed_at <= started_at:
            continue
        duration_minutes = (completed_at - started_at).total_seconds() / 60.0
        if 1.0 <= duration_minutes <= 180.0:
            completed_durations.append(duration_minutes)
    if completed_durations:
        return round(sum(completed_durations) / len(completed_durations), 1)
    return _default_assignment_minutes(session)


def _estimate_wait_minutes_by_entry(
    session: OpenPlaySession,
    session_courts: list[OpenPlaySessionCourt],
    waiting_ready_entries: list[OpenPlayQueueEntry],
    cycle_minutes: float,
) -> dict[str, int]:
    active_court_count = sum(
        1
        for court_state in session_courts
        if _as_bool_value(court_state.is_active) and _as_status_value(court_state.status) != "closed"
    )
    service_capacity = max(1, max(1, active_court_count) * _session_court_size(session))
    players_ahead = 0
    wait_minutes_by_entry: dict[str, int] = {}
    for entry in waiting_ready_entries:
        rounds_before_call = players_ahead // service_capacity
        wait_minutes_by_entry[str(entry.id)] = max(0, int(round(rounds_before_call * cycle_minutes)))
        players_ahead += _entry_size(entry)
    return wait_minutes_by_entry


def _resolve_assignment_completion(
    session: OpenPlaySession,
    body: CompleteAssignmentRequest,
) -> tuple[int, int | None, int | None]:
    if body.side1_score is None and body.side2_score is None:
        if body.winner_side is None:
            raise HTTPException(400, "Provide either winner_side or both side scores.")
        return body.winner_side, None, None

    if body.side1_score is None or body.side2_score is None:
        raise HTTPException(400, "Both side scores are required when submitting a scored result.")
    if body.side1_score == body.side2_score:
        raise HTTPException(400, "Open play games cannot end in a tie.")

    winner_side = 1 if body.side1_score > body.side2_score else 2
    if body.winner_side is not None and body.winner_side != winner_side:
        raise HTTPException(400, "winner_side does not match the submitted scores.")

    winner_score = max(body.side1_score, body.side2_score)
    loser_score = min(body.side1_score, body.side2_score)
    target_score = _as_int_value(session.target_score, 11)

    if not _as_bool_value(session.win_by_two):
        if winner_score != target_score:
            raise HTTPException(400, f"Winning score must be exactly {target_score} for this session.")
        if loser_score >= target_score:
            raise HTTPException(400, "Losing score must stay below the target score.")
    else:
        if winner_score < target_score:
            raise HTTPException(400, f"Winning score must be at least {target_score}.")
        if winner_score - loser_score < 2:
            raise HTTPException(400, "Win-by-two scoring requires at least a two-point lead.")
        if loser_score < target_score - 1 and winner_score != target_score:
            raise HTTPException(400, f"This game should have ended at {target_score}.")
        if loser_score >= target_score - 1 and winner_score != loser_score + 2:
            raise HTTPException(400, "Extended win-by-two games must end on the first valid two-point lead.")

    return winner_side, body.side1_score, body.side2_score


def _validate_session_config(match_format: str, queue_mode: str, rotation_mode: str) -> None:
    if match_format not in VALID_MATCH_FORMATS:
        raise HTTPException(400, "Invalid match_format.")
    if queue_mode not in VALID_QUEUE_MODES:
        raise HTTPException(400, "Invalid queue_mode.")
    if rotation_mode not in VALID_ROTATION_MODES:
        raise HTTPException(400, "Invalid rotation_mode.")


def _validate_court_role(court_role: str) -> None:
    if court_role not in VALID_COURT_ROLES:
        raise HTTPException(400, "Invalid court_role.")


def _open_play_channel(session_id: str) -> str:
    return f"isms:open_play:{session_id}"


def _publish_open_play_event(session_id: str, event: str = "session_update", reason: str | None = None) -> None:
    if _open_play_redis_pub is None:
        return
    payload = {"event": event, "session_id": session_id}
    if reason:
        payload["reason"] = reason
    try:
        _open_play_redis_pub.publish(_open_play_channel(session_id), json.dumps(payload))
    except Exception as exc:
        logger.warning(f"[open_play] Failed to publish SSE event for session {session_id}: {exc}")


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


def _is_session_admin(session: OpenPlaySession, user_id: str, db: Session) -> bool:
    return str(session.created_by) == user_id or _is_club_admin(str(session.club_id), user_id, db)


def _get_session_or_404(session_id: str, db: Session) -> OpenPlaySession:
    session = db.query(OpenPlaySession).filter(OpenPlaySession.id == session_id).first()
    if session is None:
        raise HTTPException(404, "Session not found.")
    return session


def _get_confirmed_participant_ids(session_id: str, db: Session) -> set[str]:
    return {
        str(participant.user_id)
        for participant in db.query(OpenPlayParticipant).filter(
            OpenPlayParticipant.session_id == session_id,
            OpenPlayParticipant.status == "confirmed",
        ).all()
    }


def _get_user_queue_entry(session_id: str, user_id: str, db: Session) -> OpenPlayQueueEntry | None:
    return db.query(OpenPlayQueueEntry).filter(
        OpenPlayQueueEntry.session_id == session_id,
        OpenPlayQueueEntry.status != "cancelled",
        or_(
            OpenPlayQueueEntry.player1_id == user_id,
            OpenPlayQueueEntry.player2_id == user_id,
        ),
    ).first()


def _ensure_session_courts(session: OpenPlaySession, db: Session) -> bool:
    existing = db.query(OpenPlaySessionCourt).filter(OpenPlaySessionCourt.session_id == session.id).all()
    if existing:
        return False

    if session.court_id is not None:
        courts = db.query(Court).filter(Court.id == session.court_id).all()
    else:
        courts = db.query(Court).filter(
            Court.club_id == session.club_id,
            or_(Court.sport == None, Court.sport == session.sport),
        ).order_by(Court.name.asc()).all()

    if not courts:
        raise HTTPException(400, "No courts are available for this open play session.")

    for index, court in enumerate(courts, start=1):
        db.add(OpenPlaySessionCourt(
            session_id=session.id,
            court_id=court.id,
            display_order=index,
            status="available",
            is_active=True,
            court_role="challenge" if index == 1 and str(session.rotation_mode) == "winners_stay_two_off" else "standard",
            max_consecutive_wins=2 if index == 1 and str(session.rotation_mode) == "winners_stay_two_off" else None,
        ))
    db.flush()
    return True


def _entry_group_ack_state(
    assignment_players: list[OpenPlayAssignmentPlayer],
) -> dict[str, bool]:
    grouped: dict[str, bool] = {}
    by_entry: dict[str, list[OpenPlayAssignmentPlayer]] = defaultdict(list)
    for assignment_player in assignment_players:
        if assignment_player.queue_entry_id is None:
            continue
        by_entry[str(assignment_player.queue_entry_id)].append(assignment_player)
    for entry_id, players in by_entry.items():
        grouped[entry_id] = all(player.acknowledged_at is not None for player in players)
    return grouped


def _court_role_value(court_state: OpenPlaySessionCourt | None) -> str:
    if court_state is None:
        return "standard"
    return _as_text_value(court_state.court_role, "standard")


def _effective_court_rotation_mode(session: OpenPlaySession, court_state: OpenPlaySessionCourt | None) -> str:
    if _court_role_value(court_state) == "challenge":
        return "winners_stay_two_off"
    return str(session.rotation_mode)


def _court_streak_cap(session: OpenPlaySession, court_state: OpenPlaySessionCourt | None) -> int:
    if court_state is not None and court_state.max_consecutive_wins is not None:
        return max(1, _as_int_value(court_state.max_consecutive_wins, 2))
    if _court_role_value(court_state) == "challenge":
        return 2
    if str(session.rotation_mode) == "winners_stay_two_off":
        return 2
    return 0


def _expire_stale_assignments(session: OpenPlaySession, db: Session) -> bool:
    now = _utcnow()
    changed = False
    assignments = db.query(OpenPlayAssignment).filter(
        OpenPlayAssignment.session_id == session.id,
        OpenPlayAssignment.status == "called",
    ).all()

    for assignment in assignments:
        deadline = _as_utc(assignment.ack_deadline_at)
        if deadline is None or deadline > now:
            continue

        assignment_players = db.query(OpenPlayAssignmentPlayer).filter(
            OpenPlayAssignmentPlayer.assignment_id == assignment.id
        ).all()
        ack_state = _entry_group_ack_state(assignment_players)
        seen_entry_ids: set[str] = set()
        for assignment_player in assignment_players:
            if assignment_player.queue_entry_id is None:
                continue
            entry_id = str(assignment_player.queue_entry_id)
            if entry_id in seen_entry_ids:
                continue
            seen_entry_ids.add(entry_id)

            entry = db.query(OpenPlayQueueEntry).filter(OpenPlayQueueEntry.id == assignment_player.queue_entry_id).first()
            if entry is None:
                continue

            if ack_state.get(entry_id):
                setattr(entry, "status", "waiting")
                setattr(entry, "is_ready", True)
                setattr(entry, "holding_court_id", None)
            else:
                setattr(entry, "status", "paused")
                setattr(entry, "is_ready", False)
                setattr(entry, "holding_court_id", None)
                setattr(entry, "skip_count", _as_int_value(entry.skip_count) + 1)
                for user_id in _entry_user_ids(entry):
                    send_notification(
                        user_id=user_id,
                        title="Open Play Turn Skipped",
                        body="Your court call expired because not everyone in your queue entry acknowledged in time.",
                        notif_type="open_play_skipped",
                        reference_id=str(session.id),
                    )

        setattr(assignment, "status", "expired")
        court_state = db.query(OpenPlaySessionCourt).filter(OpenPlaySessionCourt.id == assignment.session_court_id).first()
        if court_state is not None:
            setattr(court_state, "status", "available")
        changed = True

    return changed


def _recent_matchup_history(
    session: OpenPlaySession,
    db: Session,
) -> tuple[dict[frozenset[str], int], dict[frozenset[str], int]]:
    teammate_counts: dict[frozenset[str], int] = defaultdict(int)
    opponent_counts: dict[frozenset[str], int] = defaultdict(int)
    recent_assignments = db.query(OpenPlayAssignment).filter(
        OpenPlayAssignment.session_id == session.id,
        OpenPlayAssignment.status == "completed",
    ).order_by(OpenPlayAssignment.completed_at.desc()).limit(24).all()
    if not recent_assignments:
        return teammate_counts, opponent_counts

    assignment_players = db.query(OpenPlayAssignmentPlayer).filter(
        OpenPlayAssignmentPlayer.assignment_id.in_([assignment.id for assignment in recent_assignments])
    ).all()
    players_by_assignment: dict[str, list[OpenPlayAssignmentPlayer]] = defaultdict(list)
    for assignment_player in assignment_players:
        players_by_assignment[str(assignment_player.assignment_id)].append(assignment_player)

    for assignment in recent_assignments:
        side_users: dict[int, list[str]] = defaultdict(list)
        for assignment_player in players_by_assignment.get(str(assignment.id), []):
            side_users[_as_int_value(assignment_player.side_no, 1)].append(str(assignment_player.user_id))
        for users in side_users.values():
            for user_a, user_b in combinations(sorted(users), 2):
                teammate_counts[frozenset((user_a, user_b))] += 1
        for user_a in side_users.get(1, []):
            for user_b in side_users.get(2, []):
                opponent_counts[frozenset((user_a, user_b))] += 1

    return teammate_counts, opponent_counts


def _build_session_analytics(
    participant_rows: list[OpenPlayParticipant],
    queue_entries: list[OpenPlayQueueEntry],
    estimated_waits: Mapping[str, float | int],
    completed_assignments: list[OpenPlayAssignment],
    completed_assignment_players: list[OpenPlayAssignmentPlayer],
    profiles_map: dict[str, Profile],
    ratings_map: dict[str, PlayerRating],
) -> dict:
    confirmed_user_ids = [
        str(participant.user_id)
        for participant in participant_rows
        if _as_status_value(participant.status) == "confirmed"
    ]
    queue_entry_by_user: dict[str, OpenPlayQueueEntry] = {}
    current_wait_by_user: dict[str, float | int | None] = {}
    for entry in queue_entries:
        status = _as_status_value(entry.status)
        wait_minutes = estimated_waits.get(str(entry.id))
        if wait_minutes is None and status in {"called", "holding", "playing"}:
            wait_minutes = 0.0
        for user_id in _entry_user_ids(entry):
            queue_entry_by_user[user_id] = entry
            current_wait_by_user[user_id] = wait_minutes

    players_by_assignment: dict[str, list[OpenPlayAssignmentPlayer]] = defaultdict(list)
    for assignment_player in completed_assignment_players:
        players_by_assignment[str(assignment_player.assignment_id)].append(assignment_player)

    games_played: dict[str, int] = defaultdict(int)
    wins: dict[str, int] = defaultdict(int)
    losses: dict[str, int] = defaultdict(int)
    unique_teammates: dict[str, set[str]] = defaultdict(set)
    unique_opponents: dict[str, set[str]] = defaultdict(set)
    teammate_counts: dict[frozenset[str], int] = defaultdict(int)
    opponent_counts: dict[frozenset[str], int] = defaultdict(int)

    for assignment in completed_assignments:
        side_users: dict[int, list[str]] = defaultdict(list)
        for assignment_player in players_by_assignment.get(str(assignment.id), []):
            user_id = str(assignment_player.user_id)
            games_played[user_id] += 1
            outcome = _as_text_value(assignment_player.outcome)
            if outcome == "win":
                wins[user_id] += 1
            elif outcome == "loss":
                losses[user_id] += 1
            side_users[_as_int_value(assignment_player.side_no, 1)].append(user_id)

        for users in side_users.values():
            ordered_users = sorted(users)
            for user_id in ordered_users:
                unique_teammates[user_id].update(other_user_id for other_user_id in ordered_users if other_user_id != user_id)
            for user_a, user_b in combinations(ordered_users, 2):
                teammate_counts[frozenset((user_a, user_b))] += 1

        for user_id in side_users.get(1, []):
            unique_opponents[user_id].update(side_users.get(2, []))
        for user_id in side_users.get(2, []):
            unique_opponents[user_id].update(side_users.get(1, []))
        for user_a in side_users.get(1, []):
            for user_b in side_users.get(2, []):
                opponent_counts[frozenset((user_a, user_b))] += 1

    repeat_teammate_pairs = sum(1 for count in teammate_counts.values() if count > 1)
    repeat_opponent_pairs = sum(1 for count in opponent_counts.values() if count > 1)
    teammate_repeat_instances = sum(max(0, count - 1) for count in teammate_counts.values())
    opponent_repeat_instances = sum(max(0, count - 1) for count in opponent_counts.values())

    games_distribution = [games_played.get(user_id, 0) for user_id in confirmed_user_ids]
    play_gap = max(games_distribution, default=0) - min(games_distribution, default=0)
    play_balance_score = (
        max(0.0, 100.0 - (play_gap * 18.0))
        if games_distribution and any(games_distribution)
        else 100.0
    )

    wait_distribution = [
        wait_minutes
        for wait_minutes in current_wait_by_user.values()
        if wait_minutes is not None
    ]
    wait_gap_minutes = (
        round(max(wait_distribution, default=0.0) - min(wait_distribution, default=0.0), 1)
        if wait_distribution
        else 0.0
    )
    wait_balance_score = (
        max(0.0, 100.0 - (wait_gap_minutes * 4.0))
        if len(wait_distribution) > 1
        else 100.0
    )

    total_skips = sum(_as_int_value(entry.skip_count) for entry in queue_entries)
    skip_score = max(0.0, 100.0 - (total_skips * 8.0))
    fairness_score = round(
        (play_balance_score * 0.55) + (wait_balance_score * 0.25) + (skip_score * 0.20),
        1,
    )
    social_mix_score = round(
        max(0.0, 100.0 - (teammate_repeat_instances * 10.0) - (opponent_repeat_instances * 5.0)),
        1,
    )

    player_insights = []
    for user_id in confirmed_user_ids:
        queue_entry = queue_entry_by_user.get(user_id)
        player_insights.append({
            "user_id": user_id,
            "profile": _profile_brief(profiles_map.get(user_id), ratings_map.get(user_id)),
            "queue_status": _as_status_value(queue_entry.status) if queue_entry is not None else None,
            "games_played": games_played.get(user_id, 0),
            "wins": wins.get(user_id, 0),
            "losses": losses.get(user_id, 0),
            "current_wait_minutes": current_wait_by_user.get(user_id),
            "skip_count": _as_int_value(queue_entry.skip_count) if queue_entry is not None else 0,
            "unique_teammates": len(unique_teammates.get(user_id, set())),
            "unique_opponents": len(unique_opponents.get(user_id, set())),
        })

    player_insights.sort(
        key=lambda insight: (
            -_as_int_value(insight["games_played"]),
            -_as_int_value(insight["wins"]),
            _as_text_value(insight["profile"]["username"] if insight["profile"] else ""),
        )
    )

    return {
        "games_logged": len(completed_assignments),
        "fairness_score": fairness_score,
        "social_mix_score": social_mix_score,
        "play_gap": play_gap,
        "wait_gap_minutes": wait_gap_minutes,
        "repeat_teammate_pairs": repeat_teammate_pairs,
        "repeat_opponent_pairs": repeat_opponent_pairs,
        "max_teammate_repeat": max(teammate_counts.values(), default=0),
        "max_opponent_repeat": max(opponent_counts.values(), default=0),
        "player_insights": player_insights,
    }


def _plan_assignment_sides(
    session: OpenPlaySession,
    selected_entries: list[OpenPlayQueueEntry],
    held_entries: list[OpenPlayQueueEntry],
    player_snapshots: dict[str, dict],
) -> tuple[list[OpenPlayQueueEntry], list[OpenPlayQueueEntry], float]:
    held_ids = {str(entry.id) for entry in held_entries}

    if held_entries:
        side1_entries = held_entries
        side2_entries = [entry for entry in selected_entries if str(entry.id) not in held_ids]
    elif str(session.match_format) == "singles":
        side1_entries = selected_entries[:1]
        side2_entries = selected_entries[1:2]
    elif len(selected_entries) == 2 and all(_entry_size(entry) == 2 for entry in selected_entries):
        side1_entries = selected_entries[:1]
        side2_entries = selected_entries[1:2]
    elif len(selected_entries) == 3 and any(_entry_size(entry) == 2 for entry in selected_entries):
        pair_entry = next(entry for entry in selected_entries if _entry_size(entry) == 2)
        solo_entries = [entry for entry in selected_entries if str(entry.id) != str(pair_entry.id)]
        if str(selected_entries[0].id) == str(pair_entry.id):
            side1_entries = [pair_entry]
            side2_entries = solo_entries
        else:
            side1_entries = solo_entries[:2]
            side2_entries = [pair_entry]
    elif (
        str(session.queue_mode) == "balanced"
        and len(selected_entries) == 4
        and all(_entry_size(entry) == 1 for entry in selected_entries)
    ):
        four_players = [
            player_snapshots.get(str(entry.player1_id), {"player_id": str(entry.player1_id), "rating": 1500.0})
            for entry in selected_entries
        ]
        matchup = run_matchmaking(four_players, str(session.sport), str(session.match_format), wait_seconds=0)
        if matchup is not None:
            team_a_ids = {str(player["player_id"]) for player in matchup.get("team_a", [])}
            side1_entries = [entry for entry in selected_entries if str(entry.player1_id) in team_a_ids]
            side2_entries = [entry for entry in selected_entries if str(entry.player1_id) not in team_a_ids]
            if len(side1_entries) == 2 and len(side2_entries) == 2:
                return side1_entries, side2_entries, float(matchup.get("score", 0.0))
        side1_entries = selected_entries[:2]
        side2_entries = selected_entries[2:4]
    else:
        side1_entries = selected_entries[:2]
        side2_entries = selected_entries[2:4]

    side1_players = [
        snapshot
        for entry in side1_entries
        for snapshot in _entry_player_snapshots(entry, player_snapshots)
    ]
    side2_players = [
        snapshot
        for entry in side2_entries
        for snapshot in _entry_player_snapshots(entry, player_snapshots)
    ]
    return side1_entries, side2_entries, _score_fixed_team_matchup(side1_players, side2_players, session)


def _matchup_repeat_penalty(
    side1_entries: list[OpenPlayQueueEntry],
    side2_entries: list[OpenPlayQueueEntry],
    teammate_counts: dict[frozenset[str], int],
    opponent_counts: dict[frozenset[str], int],
) -> float:
    side1_users = [user_id for entry in side1_entries for user_id in _entry_user_ids(entry)]
    side2_users = [user_id for entry in side2_entries for user_id in _entry_user_ids(entry)]
    penalty = 0.0
    for users in (side1_users, side2_users):
        for user_a, user_b in combinations(sorted(users), 2):
            penalty += teammate_counts.get(frozenset((user_a, user_b)), 0)
    for user_a in side1_users:
        for user_b in side2_users:
            penalty += opponent_counts.get(frozenset((user_a, user_b)), 0) * 0.5
    return penalty


def _candidate_entry_combinations(
    waiting_entries: list[OpenPlayQueueEntry],
    required_players: int,
) -> list[list[OpenPlayQueueEntry]]:
    pool = waiting_entries[: min(len(waiting_entries), 10 if required_players <= 2 else 8)]
    combinations_found: list[list[OpenPlayQueueEntry]] = []

    def backtrack(start_index: int, selected: list[OpenPlayQueueEntry], total_players: int) -> None:
        if total_players == required_players:
            combinations_found.append(selected.copy())
            return
        if total_players > required_players:
            return
        for index in range(start_index, len(pool)):
            entry = pool[index]
            size = _entry_size(entry)
            if total_players + size > required_players:
                continue
            selected.append(entry)
            backtrack(index + 1, selected, total_players + size)
            selected.pop()

    backtrack(0, [], 0)
    return combinations_found


def _select_waiting_entries_for_slots(
    session: OpenPlaySession,
    waiting_entries: list[OpenPlayQueueEntry],
    required_players: int,
    held_entries: list[OpenPlayQueueEntry],
    player_snapshots: dict[str, dict],
    teammate_counts: dict[frozenset[str], int],
    opponent_counts: dict[frozenset[str], int],
) -> list[OpenPlayQueueEntry]:
    if str(session.queue_mode) != "balanced":
        selected: list[OpenPlayQueueEntry] = []
        total_players = 0
        for entry in waiting_entries:
            size = _entry_size(entry)
            if total_players + size > required_players:
                continue
            selected.append(entry)
            total_players += size
            if total_players == required_players:
                break
        return selected if total_players == required_players else []

    queue_rank_map = {str(entry.id): index for index, entry in enumerate(waiting_entries)}
    candidate_combos = _candidate_entry_combinations(waiting_entries, required_players)
    best_combo: list[OpenPlayQueueEntry] = []
    best_score = float("-inf")
    best_rank_sum = 10**9

    for combo in candidate_combos:
        side1_entries, side2_entries, quality = _plan_assignment_sides(
            session,
            held_entries + combo,
            held_entries,
            player_snapshots,
        )
        repeat_penalty = _matchup_repeat_penalty(side1_entries, side2_entries, teammate_counts, opponent_counts)
        queue_rank_sum = sum(queue_rank_map.get(str(entry.id), len(waiting_entries)) for entry in combo)
        overall_score = (quality * 100.0) - (repeat_penalty * 14.0) - (queue_rank_sum * 0.75)
        if overall_score > best_score or (overall_score == best_score and queue_rank_sum < best_rank_sum):
            best_combo = combo
            best_score = overall_score
            best_rank_sum = queue_rank_sum

    return best_combo


def _create_assignment(
    session: OpenPlaySession,
    court_state: OpenPlaySessionCourt,
    selected_entries: list[OpenPlayQueueEntry],
    held_entries: list[OpenPlayQueueEntry],
    player_snapshots: dict[str, dict],
    db: Session,
) -> bool:
    now = _utcnow()
    ack_deadline = now + timedelta(seconds=_as_int_value(session.ack_timeout_seconds, 60))
    assignment = OpenPlayAssignment(
        session_id=session.id,
        session_court_id=court_state.id,
        status="called",
        assigned_at=now,
        ack_deadline_at=ack_deadline,
    )
    db.add(assignment)
    db.flush()

    side1_entries, side2_entries, _ = _plan_assignment_sides(
        session,
        selected_entries,
        held_entries,
        player_snapshots,
    )

    side_entries = {1: side1_entries, 2: side2_entries}
    player_notif_ids: set[str] = set()
    for side_no, entries in side_entries.items():
        seat_no = 1
        for entry in entries:
            setattr(entry, "status", "called")
            setattr(entry, "last_called_at", now)
            if entry not in held_entries:
                setattr(entry, "holding_court_id", None)
            for user_id in _entry_user_ids(entry):
                db.add(OpenPlayAssignmentPlayer(
                    assignment_id=assignment.id,
                    queue_entry_id=entry.id,
                    user_id=user_id,
                    side_no=side_no,
                    seat_no=seat_no,
                ))
                player_notif_ids.add(user_id)
                seat_no += 1

    setattr(court_state, "status", "awaiting_ack")
    court = db.query(Court).filter(Court.id == court_state.court_id).first()
    court_name = court.name if court is not None else "your assigned court"
    send_bulk_notifications(
        sorted(player_notif_ids),
        title="Open Play Court Call",
        body=f"You are up next. Please acknowledge and proceed to {court_name}.",
        notif_type="open_play_call",
        reference_id=str(session.id),
    )
    return True


def _auto_assign_available_courts(session: OpenPlaySession, db: Session) -> bool:
    if _as_status_value(session.status) != "ongoing" or not _as_bool_value(session.auto_assign_enabled):
        return False

    changed = False
    court_size = _session_court_size(session)
    session_courts = db.query(OpenPlaySessionCourt).filter(
        OpenPlaySessionCourt.session_id == session.id,
        OpenPlaySessionCourt.is_active == True,
        OpenPlaySessionCourt.status != "closed",
    ).order_by(OpenPlaySessionCourt.display_order.asc()).all()
    session_courts.sort(
        key=lambda court_state: (
            0 if _court_role_value(court_state) == "challenge" else 1,
            _as_int_value(court_state.display_order, 1),
        )
    )

    if not session_courts:
        return False

    queue_entries = db.query(OpenPlayQueueEntry).filter(
        OpenPlayQueueEntry.session_id == session.id,
        OpenPlayQueueEntry.status.in_(["waiting", "holding"]),
    ).all()
    queue_user_ids = {user_id for entry in queue_entries for user_id in _entry_user_ids(entry)}
    profiles = db.query(Profile).filter(Profile.id.in_(list(queue_user_ids))).all() if queue_user_ids else []
    profiles_map = {str(profile.id): profile for profile in profiles}
    ratings_map = _load_ratings_map(queue_user_ids, session, db)
    player_snapshots = {
        user_id: _player_snapshot(user_id, profiles_map, ratings_map)
        for user_id in queue_user_ids
    }
    teammate_counts, opponent_counts = _recent_matchup_history(session, db)
    waiting_entries = sorted(
        [entry for entry in queue_entries if _as_status_value(entry.status) == "waiting" and _as_bool_value(entry.is_ready)],
        key=lambda entry: _queue_sort_key(session, entry),
    )
    waiting_ids = {str(entry.id) for entry in waiting_entries}

    for court_state in session_courts:
        active_assignment = db.query(OpenPlayAssignment).filter(
            OpenPlayAssignment.session_court_id == court_state.id,
            OpenPlayAssignment.status.in_(list(ACTIVE_ASSIGNMENT_STATUSES)),
        ).first()
        if active_assignment is not None:
            continue
        if _as_status_value(court_state.status) in ("paused", "closed"):
            continue

        held_entries = sorted(
            [
                entry for entry in queue_entries
                if _as_status_value(entry.status) == "holding"
                and entry.holding_court_id is not None
                and str(entry.holding_court_id) == str(court_state.id)
            ],
            key=lambda entry: _queue_sort_key(session, entry),
        )
        held_player_count = sum(_entry_size(entry) for entry in held_entries)
        if held_player_count >= court_size:
            continue

        challengers = _select_waiting_entries_for_slots(
            session,
            [entry for entry in waiting_entries if str(entry.id) in waiting_ids],
            court_size - held_player_count,
            held_entries,
            player_snapshots,
            teammate_counts,
            opponent_counts,
        )
        if not challengers and held_player_count < court_size:
            continue

        for challenger in challengers:
            waiting_ids.discard(str(challenger.id))

        if _create_assignment(session, court_state, held_entries + challengers, held_entries, player_snapshots, db):
            changed = True

    return changed


def _sync_session_runtime(session: OpenPlaySession, db: Session) -> bool:
    if _as_status_value(session.status) != "ongoing":
        return False
    changed = _ensure_session_courts(session, db)
    changed = _expire_stale_assignments(session, db) or changed
    changed = _auto_assign_available_courts(session, db) or changed
    return changed


def _serialize_queue_entry(
    entry: OpenPlayQueueEntry,
    profiles_map: dict[str, Profile],
    ratings_map: dict[str, PlayerRating],
    queue_positions: dict[str, int],
    estimated_waits: Mapping[str, float | int],
    current_user_id: str,
) -> dict:
    player1 = profiles_map.get(str(entry.player1_id))
    player2 = profiles_map.get(str(entry.player2_id)) if entry.player2_id is not None else None
    return {
        "id": str(entry.id),
        "created_by": str(entry.created_by),
        "entry_kind": entry.entry_kind,
        "status": entry.status,
        "is_ready": _as_bool_value(entry.is_ready),
        "skip_count": _as_int_value(entry.skip_count),
        "queued_at": _iso_utc(entry.queued_at),
        "last_called_at": _iso_utc(entry.last_called_at),
        "last_played_at": _iso_utc(entry.last_played_at),
        "holding_court_id": str(entry.holding_court_id) if entry.holding_court_id is not None else None,
        "queue_position": queue_positions.get(str(entry.id)),
        "estimated_wait_minutes": estimated_waits.get(str(entry.id)),
        "player_count": _entry_size(entry),
        "players": [
            _profile_brief(player1, ratings_map.get(str(entry.player1_id))),
            _profile_brief(player2, ratings_map.get(str(entry.player2_id))) if entry.player2_id is not None else None,
        ],
        "is_my_entry": _entry_contains_user(entry, current_user_id),
    }


def _serialize_assignment(
    assignment: OpenPlayAssignment,
    court_state: OpenPlaySessionCourt | None,
    court: Court | None,
    assignment_players: list[OpenPlayAssignmentPlayer],
    profiles_map: dict[str, Profile],
    ratings_map: dict[str, PlayerRating],
    current_user_id: str,
) -> dict:
    sides: dict[int, list[dict]] = {1: [], 2: []}
    for assignment_player in sorted(assignment_players, key=lambda item: (item.side_no, item.seat_no, str(item.id))):
        profile = profiles_map.get(str(assignment_player.user_id))
        side_no = _as_int_value(assignment_player.side_no, 1)
        sides.setdefault(side_no, []).append({
            "user_id": str(assignment_player.user_id),
            "profile": _profile_brief(profile, ratings_map.get(str(assignment_player.user_id))),
            "acknowledged_at": _iso_utc(assignment_player.acknowledged_at),
            "is_me": str(assignment_player.user_id) == current_user_id,
        })

    return {
        "id": str(assignment.id),
        "session_court_id": str(assignment.session_court_id),
        "status": assignment.status,
        "assigned_at": _iso_utc(assignment.assigned_at),
        "ack_deadline_at": _iso_utc(assignment.ack_deadline_at),
        "started_at": _iso_utc(assignment.started_at),
        "completed_at": _iso_utc(assignment.completed_at),
        "winner_side": assignment.winner_side,
        "side1_score": _as_int_value(assignment.side1_score) if assignment.side1_score is not None else None,
        "side2_score": _as_int_value(assignment.side2_score) if assignment.side2_score is not None else None,
        "court": {
            "id": str(court.id) if court is not None else str(court_state.court_id) if court_state is not None else None,
            "name": court.name if court is not None else None,
            "status": court_state.status if court_state is not None else None,
        },
        "all_acknowledged": all(player.acknowledged_at is not None for player in assignment_players) if assignment_players else False,
        "sides": [
            {"side_no": 1, "players": sides.get(1, [])},
            {"side_no": 2, "players": sides.get(2, [])},
        ],
    }


def _serialize_runtime_state(session: OpenPlaySession, current_user_id: str, db: Session) -> dict:
    participant_rows = db.query(OpenPlayParticipant).filter(
        OpenPlayParticipant.session_id == session.id,
        OpenPlayParticipant.status != "cancelled",
    ).all()
    queue_entries = db.query(OpenPlayQueueEntry).filter(
        OpenPlayQueueEntry.session_id == session.id,
        OpenPlayQueueEntry.status != "cancelled",
    ).all()
    session_courts = db.query(OpenPlaySessionCourt).filter(
        OpenPlaySessionCourt.session_id == session.id
    ).order_by(OpenPlaySessionCourt.display_order.asc()).all()
    assignments = db.query(OpenPlayAssignment).filter(
        OpenPlayAssignment.session_id == session.id
    ).order_by(OpenPlayAssignment.assigned_at.desc()).limit(20).all()
    completed_history = db.query(OpenPlayAssignment).filter(
        OpenPlayAssignment.session_id == session.id,
        OpenPlayAssignment.status == "completed",
    ).order_by(OpenPlayAssignment.completed_at.desc()).limit(80).all()

    assignment_ids = [assignment.id for assignment in assignments]
    assignment_players = db.query(OpenPlayAssignmentPlayer).filter(
        OpenPlayAssignmentPlayer.assignment_id.in_(assignment_ids)
    ).all() if assignment_ids else []
    completed_history_ids = [assignment.id for assignment in completed_history]
    completed_history_players = db.query(OpenPlayAssignmentPlayer).filter(
        OpenPlayAssignmentPlayer.assignment_id.in_(completed_history_ids)
    ).all() if completed_history_ids else []

    profile_ids: set[str] = set()
    for participant in participant_rows:
        profile_ids.add(str(participant.user_id))
    for entry in queue_entries:
        profile_ids.update(_entry_user_ids(entry))
    for assignment_player in assignment_players:
        profile_ids.add(str(assignment_player.user_id))

    profiles = db.query(Profile).filter(Profile.id.in_(list(profile_ids))).all() if profile_ids else []
    profiles_map = {str(profile.id): profile for profile in profiles}
    ratings_map = _load_ratings_map(profile_ids, session, db)
    courts_map = {
        str(court.id): court
        for court in db.query(Court).filter(
            Court.id.in_([court_state.court_id for court_state in session_courts])
        ).all()
    } if session_courts else {}

    queue_entries_sorted = sorted(
        queue_entries,
        key=lambda entry: (
            {"holding": 0, "called": 1, "playing": 2, "waiting": 3, "paused": 4}.get(str(entry.status), 9),
            _queue_sort_key(session, entry),
        ),
    )
    waiting_ready = sorted(
        [entry for entry in queue_entries if _as_status_value(entry.status) == "waiting" and _as_bool_value(entry.is_ready)],
        key=lambda entry: _queue_sort_key(session, entry),
    )
    queue_positions = {str(entry.id): index for index, entry in enumerate(waiting_ready, start=1)}
    estimated_cycle_minutes = _estimate_assignment_minutes(session, assignments)
    estimated_waits = _estimate_wait_minutes_by_entry(
        session,
        session_courts,
        waiting_ready,
        estimated_cycle_minutes,
    )

    participant_queue_map: dict[str, OpenPlayQueueEntry] = {}
    for entry in queue_entries:
        for user_id in _entry_user_ids(entry):
            participant_queue_map[user_id] = entry

    assignment_players_by_assignment: dict[str, list[OpenPlayAssignmentPlayer]] = defaultdict(list)
    for assignment_player in assignment_players:
        assignment_players_by_assignment[str(assignment_player.assignment_id)].append(assignment_player)

    serialized_assignments = []
    current_assignment_by_court: dict[str, dict] = {}
    my_assignment = None
    for assignment in assignments:
        court_state = next(
            (session_court for session_court in session_courts if str(session_court.id) == str(assignment.session_court_id)),
            None,
        )
        court = courts_map.get(str(court_state.court_id)) if court_state is not None else None
        serialized = _serialize_assignment(
            assignment,
            court_state,
            court,
            assignment_players_by_assignment.get(str(assignment.id), []),
            profiles_map,
            ratings_map,
            current_user_id,
        )
        serialized_assignments.append(serialized)
        if serialized["status"] in ACTIVE_ASSIGNMENT_STATUSES:
            current_assignment_by_court[serialized["session_court_id"]] = serialized
        if my_assignment is None and serialized["status"] in ACTIVE_ASSIGNMENT_STATUSES:
            for side in serialized["sides"]:
                if any(player["is_me"] for player in side["players"]):
                    my_assignment = serialized
                    break

    serialized_courts = []
    for court_state in session_courts:
        court = courts_map.get(str(court_state.court_id))
        serialized_courts.append({
            "id": str(court_state.id),
            "status": court_state.status,
            "display_order": court_state.display_order,
            "court_role": _court_role_value(court_state),
            "consecutive_wins": _as_int_value(court_state.consecutive_wins),
            "max_consecutive_wins": _as_int_value(court_state.max_consecutive_wins) if court_state.max_consecutive_wins is not None else None,
            "effective_rotation_mode": _effective_court_rotation_mode(session, court_state),
            "is_active": _as_bool_value(court_state.is_active),
            "court": {
                "id": str(court_state.court_id),
                "name": court.name if court is not None else "Court",
                "sport": court.sport if court is not None else None,
            },
            "current_assignment": current_assignment_by_court.get(str(court_state.id)),
        })

    serialized_participants = _serialize_participant_rows(
        participant_rows,
        current_user_id,
        profiles_map,
        ratings_map,
        participant_queue_map,
    )

    my_queue_entry = None
    if current_user_id in participant_queue_map:
        my_queue_entry = _serialize_queue_entry(
            participant_queue_map[current_user_id],
            profiles_map,
            ratings_map,
            queue_positions,
            estimated_waits,
            current_user_id,
        )

    completed_games = sum(1 for assignment in assignments if _as_status_value(assignment.status) == "completed")
    active_games = sum(1 for assignment in assignments if _as_status_value(assignment.status) == "in_game")
    called_games = sum(1 for assignment in assignments if _as_status_value(assignment.status) == "called")
    average_wait_minutes = round(
        sum(estimated_waits.values()) / len(estimated_waits),
        1,
    ) if estimated_waits else 0.0
    total_skips = sum(_as_int_value(entry.skip_count) for entry in queue_entries)
    longest_court_streak = max((_as_int_value(court_state.consecutive_wins) for court_state in session_courts), default=0)
    analytics = _build_session_analytics(
        participant_rows,
        queue_entries,
        estimated_waits,
        completed_history,
        completed_history_players,
        profiles_map,
        ratings_map,
    )

    return {
        "participants": serialized_participants,
        "queue_entries": [
            _serialize_queue_entry(entry, profiles_map, ratings_map, queue_positions, estimated_waits, current_user_id)
            for entry in queue_entries_sorted
        ],
        "session_courts": serialized_courts,
        "assignments": serialized_assignments,
        "my_queue_entry": my_queue_entry,
        "my_assignment": my_assignment,
        "summary": {
            "confirmed_participants": sum(1 for participant in participant_rows if _as_status_value(participant.status) == "confirmed"),
            "waitlisted_participants": sum(1 for participant in participant_rows if _as_status_value(participant.status) == "waitlisted"),
            "ready_queue_entries": sum(1 for entry in queue_entries if _as_status_value(entry.status) == "waiting" and _as_bool_value(entry.is_ready)),
            "paused_queue_entries": sum(1 for entry in queue_entries if _as_status_value(entry.status) == "paused"),
            "active_games": active_games,
            "called_games": called_games,
            "completed_games": completed_games,
            "available_courts": sum(1 for court_state in session_courts if _as_status_value(court_state.status) == "available"),
            "estimated_cycle_minutes": estimated_cycle_minutes,
            "average_wait_minutes": average_wait_minutes,
            "total_skips": total_skips,
            "longest_court_streak": longest_court_streak,
            "challenge_courts": sum(1 for court_state in session_courts if _court_role_value(court_state) == "challenge"),
        },
        "analytics": analytics,
    }


def _serialize_session(session: OpenPlaySession, current_user_id: str, db: Session) -> dict:
    confirmed = [participant for participant in session.participants if _as_status_value(participant.status) == "confirmed"]
    waitlisted = [participant for participant in session.participants if _as_status_value(participant.status) == "waitlisted"]
    is_joined = any(
        str(participant.user_id) == current_user_id and _as_status_value(participant.status) == "confirmed"
        for participant in session.participants
    )

    club = db.query(Club).filter(Club.id == session.club_id).first()
    court_name = None
    if session.court_id is not None:
        court = db.query(Court).filter(Court.id == session.court_id).first()
        if court is not None:
            court_name = court.name

    data = {
        "id": str(session.id),
        "club_id": str(session.club_id),
        "club_name": club.name if club is not None else "",
        "title": session.title,
        "sport": str(session.sport),
        "sport_emoji": SPORT_EMOJIS.get(str(session.sport), "🏅"),
        "match_format": str(session.match_format),
        "session_date": _iso_utc(session.session_date),
        "duration_hours": _as_float_value(session.duration_hours, 1.0),
        "max_players": session.max_players,
        "confirmed_count": len(confirmed),
        "waitlisted_count": len(waitlisted),
        "price_per_head": _as_float_value(session.price_per_head, 0.0),
        "status": str(session.status),
        "is_joined": is_joined,
        "can_manage": _is_session_admin(session, current_user_id, db),
        "court_name": court_name,
        "queue_mode": str(session.queue_mode),
        "rotation_mode": str(session.rotation_mode),
        "ack_timeout_seconds": _as_int_value(session.ack_timeout_seconds, 60),
        "target_score": _as_int_value(session.target_score, 11),
        "win_by_two": _as_bool_value(session.win_by_two),
        "auto_assign_enabled": _as_bool_value(session.auto_assign_enabled),
        "skill_min": None if session.skill_min is None else _as_float_value(session.skill_min),
        "skill_max": None if session.skill_max is None else _as_float_value(session.skill_max),
        "description": session.description,
        "notes": session.notes,
        "created_at": _iso_utc(session.created_at),
    }

    participant_rows = db.query(OpenPlayParticipant).filter(
        OpenPlayParticipant.session_id == session.id,
        OpenPlayParticipant.status != "cancelled",
    ).all()
    participant_profile_ids = {str(participant.user_id) for participant in participant_rows}
    participant_profiles = db.query(Profile).filter(
        Profile.id.in_(list(participant_profile_ids))
    ).all() if participant_profile_ids else []
    participant_profiles_map = {str(profile.id): profile for profile in participant_profiles}
    participant_ratings_map = _load_ratings_map(participant_profile_ids, session, db)
    data["participants"] = _serialize_participant_rows(
        participant_rows,
        current_user_id,
        participant_profiles_map,
        participant_ratings_map,
    )

    if _as_status_value(session.status) == "ongoing":
        data.update(_serialize_runtime_state(session, current_user_id, db))
    return data


@router.get("/clubs/{club_id}/open-play")
def list_club_sessions(
    club_id: str,
    status: Optional[str] = Query(None),
    sport: Optional[str] = Query(None),
    date: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    club = db.query(Club).filter(Club.id == club_id).first()
    if club is None:
        raise HTTPException(404, "Club not found.")

    query = db.query(OpenPlaySession).filter(OpenPlaySession.club_id == club_id)
    if date is not None:
        try:
            target = dt.date.fromisoformat(date)
        except ValueError as exc:
            raise HTTPException(400, "Invalid date format. Use YYYY-MM-DD.") from exc
        day_start = dt.datetime(target.year, target.month, target.day, 0, 0, 0, tzinfo=timezone.utc)
        day_end = day_start + timedelta(days=1)
        query = query.filter(OpenPlaySession.session_date >= day_start, OpenPlaySession.session_date < day_end)
    elif status is not None:
        query = query.filter(OpenPlaySession.status == status)

    if sport is not None:
        query = query.filter(OpenPlaySession.sport == sport)

    sessions = query.order_by(OpenPlaySession.session_date.asc()).all()
    return {"sessions": [_serialize_session(session, str(current_user["id"]), db) for session in sessions]}


@router.get("/open-play/sessions")
def list_all_sessions(
    sport: Optional[str] = Query(None),
    status: Optional[str] = Query("upcoming"),
    date: Optional[str] = Query(None),
    limit: int = Query(20, le=50),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    query = db.query(OpenPlaySession)
    if date is not None:
        try:
            target = dt.date.fromisoformat(date)
        except ValueError as exc:
            raise HTTPException(400, "Invalid date format. Use YYYY-MM-DD.") from exc
        day_start = dt.datetime(target.year, target.month, target.day, 0, 0, 0, tzinfo=timezone.utc)
        day_end = day_start + timedelta(days=1)
        query = query.filter(OpenPlaySession.session_date >= day_start, OpenPlaySession.session_date < day_end)
    elif status is not None:
        query = query.filter(OpenPlaySession.status == status)

    if sport is not None:
        query = query.filter(OpenPlaySession.sport == sport)

    sessions = query.order_by(OpenPlaySession.session_date.asc()).limit(limit).all()
    return {"sessions": [_serialize_session(session, str(current_user["id"]), db) for session in sessions]}


@router.get("/open-play/{session_id}")
def get_session(
    session_id: str,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    session = _get_session_or_404(session_id, db)
    if _sync_session_runtime(session, db):
        db.commit()
        db.refresh(session)
        _publish_open_play_event(session_id, reason="runtime_sync")
    return _serialize_session(session, str(current_user["id"]), db)


@router.get("/open-play/{session_id}/stream")
async def session_event_stream(
    session_id: str,
    token: str,
    db: Session = Depends(get_db),
):
    """
    SSE endpoint for Open Play session updates.
    Token is passed via query string because EventSource cannot set auth headers.
    """
    try:
        payload = jwt.decode(token, settings.jwt_secret, algorithms=[settings.jwt_algorithm])
        user_id: str | None = payload.get("sub")
        token_version: int = int(payload.get("tv", -1))
        if not user_id:
            raise HTTPException(status_code=401, detail="Invalid token")
        profile = db.query(Profile).filter(Profile.id == user_id).first()
        if not profile or int(getattr(profile, "token_version", -1)) != token_version:
            raise HTTPException(status_code=401, detail="Session expired")
        _get_session_or_404(session_id, db)
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")
    finally:
        db.close()

    if _open_play_aredis is None:
        async def _empty():
            yield ": redis-unavailable\n\n"
        return StreamingResponse(_empty(), media_type="text/event-stream")

    redis_client = _open_play_aredis

    async def _event_generator():
        pubsub = redis_client.pubsub()
        await pubsub.subscribe(_open_play_channel(session_id))
        ping_ticks = 0
        try:
            yield 'data: {"event":"connected"}\n\n'
            while True:
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if msg and msg.get("data"):
                    yield f"data: {msg['data']}\n\n"
                ping_ticks += 1
                if ping_ticks % 30 == 0:
                    yield ": ping\n\n"
        except (asyncio.CancelledError, GeneratorExit):
            pass
        finally:
            try:
                await pubsub.unsubscribe(_open_play_channel(session_id))
                await pubsub.aclose()
            except Exception:
                pass

    return StreamingResponse(
        _event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.put("/open-play/{session_id}/courts/{session_court_id}")
def update_session_court(
    session_id: str,
    session_court_id: str,
    body: UpdateSessionCourtRequest,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    session = _get_session_or_404(session_id, db)
    if not _is_session_admin(session, str(current_user["id"]), db):
        raise HTTPException(403, "Not authorized.")

    court_state = db.query(OpenPlaySessionCourt).filter(
        OpenPlaySessionCourt.id == session_court_id,
        OpenPlaySessionCourt.session_id == session.id,
    ).first()
    if court_state is None:
        raise HTTPException(404, "Session court not found.")

    requested_role = body.court_role if body.court_role is not None else _court_role_value(court_state)
    if body.max_consecutive_wins is not None and requested_role != "challenge":
        raise HTTPException(400, "max_consecutive_wins is only supported on challenge courts.")

    if body.court_role is not None:
        _validate_court_role(body.court_role)
        setattr(court_state, "court_role", body.court_role)
        if body.court_role == "standard":
            setattr(court_state, "max_consecutive_wins", None)
            if _as_status_value(court_state.status) != "occupied":
                setattr(court_state, "consecutive_wins", 0)
    if body.max_consecutive_wins is not None:
        setattr(court_state, "max_consecutive_wins", body.max_consecutive_wins)
    if body.is_active is not None:
        setattr(court_state, "is_active", body.is_active)
        if not body.is_active:
            setattr(court_state, "status", "closed")
        elif _as_status_value(court_state.status) == "closed":
            setattr(court_state, "status", "available")

    _sync_session_runtime(session, db)
    db.commit()
    _publish_open_play_event(session_id, reason="court_updated")
    return {"message": "Session court updated."}


@router.post("/clubs/{club_id}/open-play")
def create_session(
    club_id: str,
    body: CreateOpenPlaySessionRequest,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    if not _is_club_admin(club_id, str(current_user["id"]), db):
        raise HTTPException(403, "Club admin access required.")

    _validate_session_config(body.match_format, body.queue_mode, body.rotation_mode)
    _validate_skill_window(body.skill_min, body.skill_max)

    session = OpenPlaySession(
        club_id=uuid.UUID(club_id),
        created_by=current_user["id"],
        title=body.title,
        description=body.description,
        sport=body.sport,
        match_format=body.match_format,
        session_date=body.session_date,
        duration_hours=body.duration_hours,
        max_players=body.max_players,
        price_per_head=body.price_per_head,
        court_id=uuid.UUID(body.court_id) if body.court_id is not None else None,
        skill_min=body.skill_min,
        skill_max=body.skill_max,
        notes=body.notes,
        queue_mode=body.queue_mode,
        rotation_mode=body.rotation_mode,
        ack_timeout_seconds=body.ack_timeout_seconds,
        target_score=body.target_score,
        win_by_two=body.win_by_two,
        auto_assign_enabled=body.auto_assign_enabled,
        status="upcoming",
    )
    db.add(session)
    db.flush()

    club = db.query(Club).filter(Club.id == club_id).first()
    members = db.query(ClubMember).filter(ClubMember.club_id == club_id).all()
    member_ids = [str(member.user_id) for member in members if str(member.user_id) != str(current_user["id"])]
    if member_ids:
        creator = db.query(Profile).filter(Profile.id == current_user["id"]).first()
        creator_name = f"{creator.first_name or ''} {creator.last_name or ''}".strip() if creator is not None else "Someone"
        send_bulk_notifications(
            user_ids=member_ids,
            notif_type="open_play_session",
            title="New Open Play Session",
            body=f"{creator_name} scheduled '{body.title}' at {club.name if club else ''}",
            reference_id=str(session.id),
        )

    db.commit()
    _publish_open_play_event(str(session.id), reason="session_created")
    return {"session_id": str(session.id), "message": "Session created."}


@router.put("/open-play/{session_id}")
def update_session(
    session_id: str,
    body: UpdateOpenPlaySessionRequest,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    session = _get_session_or_404(session_id, db)
    if not _is_session_admin(session, str(current_user["id"]), db):
        raise HTTPException(403, "Not authorized.")

    if body.match_format is not None or body.queue_mode is not None or body.rotation_mode is not None:
        _validate_session_config(
            body.match_format or str(session.match_format),
            body.queue_mode or str(session.queue_mode),
            body.rotation_mode or str(session.rotation_mode),
        )
    _validate_skill_window(
        body.skill_min if body.skill_min is not None else (_as_float_value(session.skill_min) if session.skill_min is not None else None),
        body.skill_max if body.skill_max is not None else (_as_float_value(session.skill_max) if session.skill_max is not None else None),
    )

    if body.title is not None:
        setattr(session, "title", body.title)
    if body.description is not None:
        setattr(session, "description", body.description)
    if body.session_date is not None:
        setattr(session, "session_date", body.session_date)
    if body.duration_hours is not None:
        setattr(session, "duration_hours", body.duration_hours)
    if body.max_players is not None:
        setattr(session, "max_players", body.max_players)
    if body.price_per_head is not None:
        setattr(session, "price_per_head", body.price_per_head)
    if body.skill_min is not None:
        setattr(session, "skill_min", body.skill_min)
    if body.skill_max is not None:
        setattr(session, "skill_max", body.skill_max)
    if body.notes is not None:
        setattr(session, "notes", body.notes)
    if body.status is not None:
        setattr(session, "status", body.status)
    if body.match_format is not None:
        setattr(session, "match_format", body.match_format)
    if body.queue_mode is not None:
        setattr(session, "queue_mode", body.queue_mode)
    if body.rotation_mode is not None:
        setattr(session, "rotation_mode", body.rotation_mode)
    if body.ack_timeout_seconds is not None:
        setattr(session, "ack_timeout_seconds", body.ack_timeout_seconds)
    if body.target_score is not None:
        setattr(session, "target_score", body.target_score)
    if body.win_by_two is not None:
        setattr(session, "win_by_two", body.win_by_two)
    if body.auto_assign_enabled is not None:
        setattr(session, "auto_assign_enabled", body.auto_assign_enabled)

    if _as_status_value(session.status) == "ongoing":
        _sync_session_runtime(session, db)

    db.commit()
    _publish_open_play_event(session_id, reason="session_updated")
    return {"message": "Session updated."}


@router.post("/open-play/{session_id}/start")
def start_session(
    session_id: str,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    session = _get_session_or_404(session_id, db)
    if not _is_session_admin(session, str(current_user["id"]), db):
        raise HTTPException(403, "Not authorized.")
    if _as_status_value(session.status) == "ongoing":
        return {"message": "Session already started."}
    if _as_status_value(session.status) in ("completed", "cancelled"):
        raise HTTPException(400, "This session can no longer be started.")

    setattr(session, "status", "ongoing")
    _ensure_session_courts(session, db)
    _sync_session_runtime(session, db)

    participant_ids = [
        str(participant.user_id)
        for participant in session.participants
        if _as_status_value(participant.status) == "confirmed" and str(participant.user_id) != str(current_user["id"])
    ]
    if participant_ids:
        send_bulk_notifications(
            participant_ids,
            title="Open Play Session Is Live",
            body=f"'{session.title}' is now live. Join the queue when you're ready.",
            notif_type="open_play_live",
            reference_id=str(session.id),
        )

    db.commit()
    _publish_open_play_event(session_id, reason="session_started")
    return {"message": "Session started."}


@router.post("/open-play/{session_id}/finish")
def finish_session(
    session_id: str,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    session = _get_session_or_404(session_id, db)
    if not _is_session_admin(session, str(current_user["id"]), db):
        raise HTTPException(403, "Not authorized.")

    setattr(session, "status", "completed")
    queue_entries = db.query(OpenPlayQueueEntry).filter(
        OpenPlayQueueEntry.session_id == session.id,
        OpenPlayQueueEntry.status != "cancelled",
    ).all()
    for entry in queue_entries:
        setattr(entry, "status", "cancelled")
        setattr(entry, "holding_court_id", None)

    assignments = db.query(OpenPlayAssignment).filter(
        OpenPlayAssignment.session_id == session.id,
        OpenPlayAssignment.status.in_(["called", "in_game"]),
    ).all()
    for assignment in assignments:
        setattr(assignment, "status", "cancelled")
        court_state = db.query(OpenPlaySessionCourt).filter(OpenPlaySessionCourt.id == assignment.session_court_id).first()
        if court_state is not None:
            setattr(court_state, "status", "available")

    db.commit()
    _publish_open_play_event(session_id, reason="session_finished")
    return {"message": "Session completed."}


@router.post("/open-play/{session_id}/join")
def join_session(
    session_id: str,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    session = _get_session_or_404(session_id, db)
    if _as_status_value(session.status) not in ("upcoming", "ongoing"):
        raise HTTPException(400, "Session is not open for joining.")

    existing = db.query(OpenPlayParticipant).filter(
        OpenPlayParticipant.session_id == session_id,
        OpenPlayParticipant.user_id == current_user["id"],
        OpenPlayParticipant.status != "cancelled",
    ).first()
    if existing is not None:
        raise HTTPException(400, "Already joined this session.")

    ratings_map = _load_ratings_map({str(current_user["id"])}, session, db)
    _require_skill_eligible_player(session, str(current_user["id"]), ratings_map)

    confirmed_count = sum(1 for participant in session.participants if _as_status_value(participant.status) == "confirmed")
    participant_status = "confirmed" if confirmed_count < _as_int_value(session.max_players) else "waitlisted"

    participant = OpenPlayParticipant(
        session_id=uuid.UUID(session_id),
        user_id=current_user["id"],
        status=participant_status,
    )
    db.add(participant)
    db.flush()

    joiner = db.query(Profile).filter(Profile.id == current_user["id"]).first()
    joiner_name = f"{joiner.first_name or ''} {joiner.last_name or ''}".strip() if joiner is not None else "Someone"
    send_notification(
        user_id=str(session.created_by),
        notif_type="open_play_join",
        title="New Player Joined" if participant_status == "confirmed" else "Player on Waitlist",
        body=f"{joiner_name} joined '{session.title}'" if participant_status == "confirmed" else f"{joiner_name} is waitlisted for '{session.title}'",
        reference_id=str(session.id),
    )

    db.commit()
    _publish_open_play_event(session_id, reason="participant_joined")
    return {"status": participant_status, "message": f"You are {participant_status}."}


@router.post("/open-play/{session_id}/leave")
def leave_session(
    session_id: str,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    participant = db.query(OpenPlayParticipant).filter(
        OpenPlayParticipant.session_id == session_id,
        OpenPlayParticipant.user_id == current_user["id"],
        OpenPlayParticipant.status != "cancelled",
    ).first()
    if participant is None:
        raise HTTPException(404, "You are not in this session.")

    active_entry = _get_user_queue_entry(session_id, str(current_user["id"]), db)
    if active_entry is not None and _as_status_value(active_entry.status) in ("called", "playing", "holding"):
        raise HTTPException(400, "Leave the active queue/game state before leaving this session.")
    if active_entry is not None:
        setattr(active_entry, "status", "cancelled")
        setattr(active_entry, "holding_court_id", None)

    was_confirmed = _as_status_value(participant.status) == "confirmed"
    setattr(participant, "status", "cancelled")
    db.flush()

    if was_confirmed:
        session = _get_session_or_404(session_id, db)
        waitlisted = sorted(
            [
                queued_participant
                for queued_participant in session.participants
                if _as_status_value(queued_participant.status) == "waitlisted"
            ],
            key=lambda queued_participant: _ts(queued_participant.joined_at),
        )
        if waitlisted:
            promoted = waitlisted[0]
            setattr(promoted, "status", "confirmed")
            send_notification(
                user_id=str(promoted.user_id),
                notif_type="open_play_promoted",
                title="You're In!",
                body=f"A spot opened up — you've been confirmed for '{session.title}'.",
                reference_id=str(session.id),
            )

    db.commit()
    _publish_open_play_event(session_id, reason="participant_left")
    return {"message": "You have left the session."}


@router.post("/open-play/{session_id}/queue/join")
def join_live_queue(
    session_id: str,
    body: JoinQueueRequest,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    session = _get_session_or_404(session_id, db)
    if _as_status_value(session.status) != "ongoing":
        raise HTTPException(400, "This open play session is not live yet.")

    confirmed_ids = _get_confirmed_participant_ids(session_id, db)
    user_id = str(current_user["id"])
    if user_id not in confirmed_ids:
        raise HTTPException(403, "Join the session first before entering the live queue.")
    if _get_user_queue_entry(session_id, user_id, db) is not None:
        raise HTTPException(400, "You already have an active queue entry.")

    rating_subject_ids = {user_id}

    partner_user_id = body.partner_user_id
    if partner_user_id is not None:
        if str(session.match_format) == "singles":
            raise HTTPException(400, "Singles sessions do not support pair queue entries.")
        if partner_user_id == user_id:
            raise HTTPException(400, "You cannot pair with yourself.")
        if partner_user_id not in confirmed_ids:
            raise HTTPException(400, "Selected partner is not a confirmed session participant.")
        if _get_user_queue_entry(session_id, partner_user_id, db) is not None:
            raise HTTPException(400, "Selected partner is already in the queue.")
        rating_subject_ids.add(partner_user_id)

    ratings_map = _load_ratings_map(rating_subject_ids, session, db)
    _require_skill_eligible_player(session, user_id, ratings_map)
    if partner_user_id is not None:
        _require_skill_eligible_player(session, partner_user_id, ratings_map)

    entry = OpenPlayQueueEntry(
        session_id=session.id,
        created_by=current_user["id"],
        player1_id=current_user["id"],
        player2_id=uuid.UUID(partner_user_id) if partner_user_id is not None else None,
        entry_kind="pair" if partner_user_id is not None else "solo",
        status="waiting",
        is_ready=body.ready,
        queued_at=_utcnow(),
    )
    db.add(entry)
    db.flush()
    _sync_session_runtime(session, db)
    db.commit()
    _publish_open_play_event(session_id, reason="queue_joined")
    return {"message": "You joined the live queue."}


@router.post("/open-play/{session_id}/queue/me")
def update_my_queue_entry(
    session_id: str,
    body: QueueEntryActionRequest,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    session = _get_session_or_404(session_id, db)
    entry = _get_user_queue_entry(session_id, str(current_user["id"]), db)
    if entry is None:
        raise HTTPException(404, "You do not have an active queue entry.")

    action = body.action.strip().lower()
    if action == "ready":
        if _as_status_value(entry.status) in ("called", "playing"):
            raise HTTPException(400, "You are already assigned to a court.")
        setattr(entry, "status", "waiting")
        setattr(entry, "is_ready", True)
        setattr(entry, "holding_court_id", None)
        setattr(entry, "queued_at", _utcnow())
    elif action == "pause":
        if _as_status_value(entry.status) in ("called", "playing", "holding"):
            raise HTTPException(400, "This queue entry cannot be paused right now.")
        setattr(entry, "status", "paused")
        setattr(entry, "is_ready", False)
        setattr(entry, "holding_court_id", None)
    elif action == "leave":
        if _as_status_value(entry.status) in ("called", "playing", "holding"):
            raise HTTPException(400, "This queue entry cannot leave right now.")
        setattr(entry, "status", "cancelled")
        setattr(entry, "is_ready", False)
        setattr(entry, "holding_court_id", None)
    else:
        raise HTTPException(400, "Action must be ready, pause, or leave.")

    _sync_session_runtime(session, db)
    db.commit()
    _publish_open_play_event(session_id, reason=f"queue_{action}")
    return {"message": "Queue entry updated."}


@router.post("/open-play/{session_id}/assignments/{assignment_id}/ack")
def acknowledge_assignment(
    session_id: str,
    assignment_id: str,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    session = _get_session_or_404(session_id, db)
    assignment = db.query(OpenPlayAssignment).filter(
        OpenPlayAssignment.id == assignment_id,
        OpenPlayAssignment.session_id == session.id,
    ).first()
    if assignment is None:
        raise HTTPException(404, "Assignment not found.")
    if _as_status_value(assignment.status) != "called":
        raise HTTPException(400, "This assignment is no longer awaiting acknowledgement.")

    deadline = _as_utc(assignment.ack_deadline_at)
    if deadline is not None and deadline < _utcnow():
        _sync_session_runtime(session, db)
        db.commit()
        raise HTTPException(400, "The acknowledgement window has already expired.")

    assignment_player = db.query(OpenPlayAssignmentPlayer).filter(
        OpenPlayAssignmentPlayer.assignment_id == assignment.id,
        OpenPlayAssignmentPlayer.user_id == current_user["id"],
    ).first()
    if assignment_player is None:
        raise HTTPException(403, "You are not assigned to this court call.")

    setattr(assignment_player, "acknowledged_at", _utcnow())

    assignment_players = db.query(OpenPlayAssignmentPlayer).filter(
        OpenPlayAssignmentPlayer.assignment_id == assignment.id
    ).all()
    if assignment_players and all(player.acknowledged_at is not None for player in assignment_players):
        setattr(assignment, "status", "in_game")
        setattr(assignment, "started_at", _utcnow())

        queue_entry_ids = {
            str(player.queue_entry_id)
            for player in assignment_players
            if player.queue_entry_id is not None
        }
        for entry in db.query(OpenPlayQueueEntry).filter(OpenPlayQueueEntry.id.in_(list(queue_entry_ids))).all():
            setattr(entry, "status", "playing")
            setattr(entry, "last_played_at", _utcnow())

        court_state = db.query(OpenPlaySessionCourt).filter(OpenPlaySessionCourt.id == assignment.session_court_id).first()
        if court_state is not None:
            setattr(court_state, "status", "occupied")
            court = db.query(Court).filter(Court.id == court_state.court_id).first()
            if court is not None:
                setattr(court, "status", "occupied")

    db.commit()
    _publish_open_play_event(session_id, reason="assignment_acknowledged")
    return {"message": "Acknowledged."}


@router.post("/open-play/{session_id}/assignments/{assignment_id}/complete")
def complete_assignment(
    session_id: str,
    assignment_id: str,
    body: CompleteAssignmentRequest,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    session = _get_session_or_404(session_id, db)
    if not _is_session_admin(session, str(current_user["id"]), db):
        raise HTTPException(403, "Not authorized.")

    assignment = db.query(OpenPlayAssignment).filter(
        OpenPlayAssignment.id == assignment_id,
        OpenPlayAssignment.session_id == session.id,
    ).first()
    if assignment is None:
        raise HTTPException(404, "Assignment not found.")
    if _as_status_value(assignment.status) != "in_game":
        raise HTTPException(400, "Only in-game assignments can be completed.")

    winner_side, side1_score, side2_score = _resolve_assignment_completion(session, body)

    court_state = db.query(OpenPlaySessionCourt).filter(OpenPlaySessionCourt.id == assignment.session_court_id).first()
    assignment_players = db.query(OpenPlayAssignmentPlayer).filter(
        OpenPlayAssignmentPlayer.assignment_id == assignment.id
    ).all()

    queue_entries_by_id = {
        str(entry.id): entry
        for entry in db.query(OpenPlayQueueEntry).filter(
            OpenPlayQueueEntry.id.in_([
                player.queue_entry_id for player in assignment_players if player.queue_entry_id is not None
            ])
        ).all()
    } if assignment_players else {}

    winner_entry_ids: set[str] = set()
    losing_entry_ids: set[str] = set()
    for player in assignment_players:
        if player.queue_entry_id is None:
            continue
        if _as_int_value(player.side_no) == winner_side:
            winner_entry_ids.add(str(player.queue_entry_id))
            setattr(player, "outcome", "win")
        else:
            losing_entry_ids.add(str(player.queue_entry_id))
            setattr(player, "outcome", "loss")

    now = _utcnow()
    rotation_mode = _effective_court_rotation_mode(session, court_state)
    streak_cap = _court_streak_cap(session, court_state)
    if rotation_mode == "winners_stay_two_off" and court_state is not None:
        winner_entries = [queue_entries_by_id[entry_id] for entry_id in winner_entry_ids if entry_id in queue_entries_by_id]
        loser_entries = [queue_entries_by_id[entry_id] for entry_id in losing_entry_ids if entry_id in queue_entries_by_id]
        continuing_streak = any(
            winner_entry.holding_court_id is not None and str(winner_entry.holding_court_id) == str(court_state.id)
            for winner_entry in winner_entries
        )
        next_streak = _as_int_value(court_state.consecutive_wins) + 1 if continuing_streak else 1
        if next_streak >= max(1, streak_cap):
            for entry in winner_entries + loser_entries:
                setattr(entry, "status", "waiting")
                setattr(entry, "is_ready", True)
                setattr(entry, "holding_court_id", None)
                setattr(entry, "queued_at", now)
            setattr(court_state, "consecutive_wins", 0)
            if winner_entries:
                send_bulk_notifications(
                    sorted({user_id for entry in winner_entries for user_id in _entry_user_ids(entry)}),
                    title="Challenge Court Rotated",
                    body="Your streak reached the configured limit, so the challenge court has rotated.",
                    notif_type="open_play_challenge_rotated",
                    reference_id=str(session.id),
                )
        else:
            for entry in winner_entries:
                setattr(entry, "status", "holding")
                setattr(entry, "is_ready", True)
                setattr(entry, "holding_court_id", court_state.id)
                setattr(entry, "queued_at", now)
            for entry in loser_entries:
                setattr(entry, "status", "waiting")
                setattr(entry, "is_ready", True)
                setattr(entry, "holding_court_id", None)
                setattr(entry, "queued_at", now)
            setattr(court_state, "consecutive_wins", next_streak)
            if _court_role_value(court_state) == "challenge" and winner_entries:
                send_bulk_notifications(
                    sorted({user_id for entry in winner_entries for user_id in _entry_user_ids(entry)}),
                    title="Challenge Court Defended",
                    body=f"Stay on the challenge court. Current streak: {next_streak}/{max(1, streak_cap)}.",
                    notif_type="open_play_challenge_streak",
                    reference_id=str(session.id),
                )
    else:
        for entry in queue_entries_by_id.values():
            setattr(entry, "status", "waiting")
            setattr(entry, "is_ready", True)
            setattr(entry, "holding_court_id", None)
            setattr(entry, "queued_at", now)
        if court_state is not None:
            setattr(court_state, "consecutive_wins", 0)

    setattr(assignment, "status", "completed")
    setattr(assignment, "completed_at", now)
    setattr(assignment, "winner_side", winner_side)
    setattr(assignment, "side1_score", side1_score)
    setattr(assignment, "side2_score", side2_score)

    if court_state is not None:
        setattr(court_state, "status", "available")
        court = db.query(Court).filter(Court.id == court_state.court_id).first()
        if court is not None:
            setattr(court, "status", "available")

    _sync_session_runtime(session, db)
    db.commit()
    _publish_open_play_event(session_id, reason="assignment_completed")
    return {"message": "Game completed."}


@router.delete("/open-play/{session_id}")
def cancel_session(
    session_id: str,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    session = _get_session_or_404(session_id, db)
    if not _is_session_admin(session, str(current_user["id"]), db):
        raise HTTPException(403, "Club admin access required.")

    setattr(session, "status", "cancelled")
    confirmed_ids = [
        str(participant.user_id)
        for participant in session.participants
        if _as_status_value(participant.status) == "confirmed" and str(participant.user_id) != str(current_user["id"])
    ]
    if confirmed_ids:
        send_bulk_notifications(
            user_ids=confirmed_ids,
            notif_type="open_play_cancelled",
            title="Session Cancelled",
            body=f"'{session.title}' has been cancelled.",
            reference_id=str(session.id),
        )

    db.commit()
    _publish_open_play_event(session_id, reason="session_cancelled")
    return {"message": "Session cancelled."}
