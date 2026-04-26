from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from math import sqrt
from typing import TYPE_CHECKING

from sqlalchemy import or_
from sqlalchemy.orm import Session

from app.services.rating_policy import (
    enum_value,
    leaderboard_eligible,
    match_side_ids,
    matchmaking_eligible,
)
from app.services.performance_rating import (
    rebuild_all_performance_metrics,
    redistribute_match_ratings_by_performance,
    reset_performance_fields,
)
from app.utils.glicko2 import (
    DEFAULT_VOLATILITY,
    MAX_RD,
    MAX_VOLATILITY,
    MIN_RD,
    MIN_VOLATILITY,
    RATING_CEILING,
    RATING_FLOOR,
    update as glicko_update,
)

if TYPE_CHECKING:
    from app.models.models import Match, PlayerRating

RATING_REBUILD_STATE_KEY = "rating_history_rebuild_version"
RATING_REBUILD_VERSION = "glicko2-split-performance-v4"


@dataclass
class RatingSnapshot:
    user_id: str
    sport: str
    match_format: str
    rating: float = 1500.0
    rating_deviation: float = 350.0
    volatility: float = DEFAULT_VOLATILITY
    matches_played: int = 0
    wins: int = 0
    losses: int = 0
    current_win_streak: int = 0
    current_loss_streak: int = 0
    rating_status: str = "CALIBRATING"
    calibration_matches_played: int = 0
    distinct_opponents: set[str] | None = None
    is_matchmaking_eligible: bool = False
    is_leaderboard_eligible: bool = False
    calibration_completed_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.distinct_opponents is None:
            self.distinct_opponents = set()


@dataclass
class RatingRebuildSummary:
    matches_seen: int = 0
    matches_replayed: int = 0
    matches_skipped: int = 0
    force_completed_skipped: int = 0
    ratings_created: int = 0
    ratings_updated: int = 0
    ratings_reset_to_default: int = 0

    def to_dict(self) -> dict:
        return asdict(self)


def _rating_key(user_id: str, sport: str, match_format: str) -> tuple[str, str, str]:
    return (user_id, sport, match_format)


def _snapshot(
    snapshots: dict[tuple[str, str, str], RatingSnapshot],
    user_id: str,
    sport: str,
    match_format: str,
) -> RatingSnapshot:
    key = _rating_key(user_id, sport, match_format)
    if key not in snapshots:
        snapshots[key] = RatingSnapshot(user_id=user_id, sport=sport, match_format=match_format)
    return snapshots[key]


def _team_average(snapshots: list[RatingSnapshot]) -> tuple[float, float]:
    rating = sum(snapshot.rating for snapshot in snapshots) / len(snapshots)
    rd = sqrt(sum(snapshot.rating_deviation**2 for snapshot in snapshots) / len(snapshots))
    return rating, rd


def _apply_snapshot_result(
    snapshot: RatingSnapshot,
    *,
    won: bool,
    rating: float,
    rating_deviation: float,
    volatility: float,
    completed_at: datetime | None,
) -> None:
    snapshot.rating = rating
    snapshot.rating_deviation = rating_deviation
    snapshot.volatility = volatility
    snapshot.matches_played += 1

    if won:
        snapshot.wins += 1
        snapshot.current_win_streak += 1
        snapshot.current_loss_streak = 0
    else:
        snapshot.losses += 1
        snapshot.current_win_streak = 0
        snapshot.current_loss_streak += 1

    snapshot.calibration_matches_played += 1
    snapshot.is_matchmaking_eligible = matchmaking_eligible(snapshot.matches_played)
    if (
        snapshot.rating_status == "CALIBRATING"
        and leaderboard_eligible(
            snapshot.matches_played,
            len(snapshot.distinct_opponents or set()),
            snapshot.rating_deviation,
        )
    ):
        snapshot.rating_status = "RATED"
        snapshot.is_leaderboard_eligible = True
        snapshot.calibration_completed_at = completed_at or datetime.now(timezone.utc)


def replay_match_into_snapshots(
    match: "Match",
    snapshots: dict[tuple[str, str, str], RatingSnapshot],
    history_rows: list | None = None,
) -> bool:
    if enum_value(match.status) != "completed" or match.winner_id is None:
        return False

    sides = match_side_ids(match)
    if sides is None:
        return False

    team1_ids, team2_ids = sides
    winner_id = str(match.winner_id)
    if winner_id in team1_ids:
        team1_wins = True
    elif winner_id in team2_ids:
        team1_wins = False
    else:
        return False

    sport = enum_value(match.sport)
    match_format = enum_value(match.match_format)
    team1 = [_snapshot(snapshots, user_id, sport, match_format) for user_id in team1_ids]
    team2 = [_snapshot(snapshots, user_id, sport, match_format) for user_id in team2_ids]
    team1_rating, team1_rd = _team_average(team1)
    team2_rating, team2_rd = _team_average(team2)

    for snapshot in team1:
        snapshot.distinct_opponents = (snapshot.distinct_opponents or set()) | set(team2_ids)
    for snapshot in team2:
        snapshot.distinct_opponents = (snapshot.distinct_opponents or set()) | set(team1_ids)

    updates: list[tuple[RatingSnapshot, bool, tuple[float, float, float]]] = []
    for snapshot in team1:
        updates.append(
            (
                snapshot,
                team1_wins,
                glicko_update(
                    rating=snapshot.rating,
                    rd=snapshot.rating_deviation,
                    volatility=snapshot.volatility,
                    opp_rating=team2_rating,
                    opp_rd=team2_rd,
                    score=1.0 if team1_wins else 0.0,
                ),
            )
        )
    for snapshot in team2:
        updates.append(
            (
                snapshot,
                not team1_wins,
                glicko_update(
                    rating=snapshot.rating,
                    rd=snapshot.rating_deviation,
                    volatility=snapshot.volatility,
                    opp_rating=team1_rating,
                    opp_rd=team1_rd,
                    score=0.0 if team1_wins else 1.0,
                ),
            )
        )

    if history_rows and len(team1) == 2 and len(team2) == 2:
        old_ratings = {snapshot.user_id: snapshot.rating for snapshot in team1 + team2}
        adjusted_ratings = redistribute_match_ratings_by_performance(
            match,
            history_rows,
            old_ratings,
            {
                snapshot.user_id: rating
                for snapshot, _, (rating, _, _) in updates
            },
            winner_id=winner_id,
        )
        updates = [
            (
                snapshot,
                won,
                (
                    adjusted_ratings.get(snapshot.user_id, rating),
                    rating_deviation,
                    volatility,
                ),
            )
            for snapshot, won, (rating, rating_deviation, volatility) in updates
        ]

    for snapshot, won, (rating, rating_deviation, volatility) in updates:
        _apply_snapshot_result(
            snapshot,
            won=won,
            rating=rating,
            rating_deviation=rating_deviation,
            volatility=volatility,
            completed_at=match.completed_at,
        )

    return True


def _safe_rating_value(value) -> float:
    try:
        rating = float(value)
    except (TypeError, ValueError):
        return 1500.0
    return max(RATING_FLOOR, min(RATING_CEILING, rating))


def _safe_rd_value(value) -> float:
    try:
        rd = float(value)
    except (TypeError, ValueError):
        return 350.0
    return max(MIN_RD, min(MAX_RD, rd))


def _safe_volatility_value(value) -> float:
    try:
        volatility = float(value)
    except (TypeError, ValueError):
        return DEFAULT_VOLATILITY
    return max(MIN_VOLATILITY, min(MAX_VOLATILITY, volatility))


def _reset_rating_row(row: "PlayerRating") -> None:
    row.rating = 1500
    row.rating_deviation = 350
    row.volatility = DEFAULT_VOLATILITY
    row.matches_played = 0
    row.wins = 0
    row.losses = 0
    row.current_win_streak = 0
    row.current_loss_streak = 0
    row.rating_status = "CALIBRATING"
    row.calibration_matches_played = 0
    row.distinct_opponents_count = 0
    row.is_matchmaking_eligible = False
    row.is_leaderboard_eligible = False
    row.calibration_completed_at = None
    reset_performance_fields(row)
    row.updated_at = datetime.now(timezone.utc)


def _apply_snapshot_to_row(row: "PlayerRating", snapshot: RatingSnapshot) -> None:
    row.rating = _safe_rating_value(snapshot.rating)
    row.rating_deviation = _safe_rd_value(snapshot.rating_deviation)
    row.volatility = _safe_volatility_value(snapshot.volatility)
    row.matches_played = snapshot.matches_played
    row.wins = snapshot.wins
    row.losses = snapshot.losses
    row.current_win_streak = snapshot.current_win_streak
    row.current_loss_streak = snapshot.current_loss_streak
    row.rating_status = snapshot.rating_status
    row.calibration_matches_played = snapshot.calibration_matches_played
    row.distinct_opponents_count = len(snapshot.distinct_opponents or set())
    row.is_matchmaking_eligible = snapshot.is_matchmaking_eligible
    row.is_leaderboard_eligible = snapshot.is_leaderboard_eligible
    row.calibration_completed_at = snapshot.calibration_completed_at
    row.updated_at = datetime.now(timezone.utc)


def _force_completed_match_ids(db: Session) -> set[str]:
    from app.models.models import MatchHistory

    rows = (
        db.query(MatchHistory.match_id)
        .filter(MatchHistory.event_type == "admin_force_complete")
        .all()
    )
    return {str(row[0]) for row in rows if row[0] is not None}


def _histories_by_match_id(db: Session, match_ids: list) -> dict[str, list]:
    from app.models.models import MatchHistory

    if not match_ids:
        return {}

    rows = (
        db.query(MatchHistory)
        .filter(
            MatchHistory.match_id.in_(match_ids),
            MatchHistory.event_type.in_(("point", "violation", "serve_change")),
        )
        .order_by(MatchHistory.created_at.asc(), MatchHistory.id.asc())
        .all()
    )

    histories: dict[str, list] = {}
    for row in rows:
        histories.setdefault(str(row.match_id), []).append(row)
    return histories


def rebuild_all_ratings_from_history(db: Session) -> RatingRebuildSummary:
    from app.models.models import Match, PlayerRating

    summary = RatingRebuildSummary()
    snapshots: dict[tuple[str, str, str], RatingSnapshot] = {}
    force_completed_ids = _force_completed_match_ids(db)

    matches = (
        db.query(Match)
        .filter(
            Match.status == "completed",
            Match.winner_id.isnot(None),
            or_(Match.player1_id.isnot(None), Match.team1_player1.isnot(None)),
        )
        .order_by(Match.completed_at.asc().nulls_last(), Match.created_at.asc().nulls_last(), Match.id.asc())
        .all()
    )
    histories_by_match = _histories_by_match_id(db, [match.id for match in matches])

    for match in matches:
        summary.matches_seen += 1
        if str(match.id) in force_completed_ids:
            summary.force_completed_skipped += 1
            summary.matches_skipped += 1
            continue
        if replay_match_into_snapshots(match, snapshots, histories_by_match.get(str(match.id), [])):
            summary.matches_replayed += 1
        else:
            summary.matches_skipped += 1

    existing_rows = db.query(PlayerRating).all()
    existing_by_key = {
        _rating_key(str(row.user_id), enum_value(row.sport), str(row.match_format)): row
        for row in existing_rows
    }

    for row in existing_rows:
        _reset_rating_row(row)
        summary.ratings_reset_to_default += 1

    for key, snapshot in snapshots.items():
        row = existing_by_key.get(key)
        if row is None:
            row = PlayerRating(
                user_id=snapshot.user_id,
                sport=snapshot.sport,
                match_format=snapshot.match_format,
            )
            db.add(row)
            summary.ratings_created += 1
        else:
            summary.ratings_updated += 1
        _apply_snapshot_to_row(row, snapshot)

    rebuild_all_performance_metrics(db)
    db.flush()
    return summary
