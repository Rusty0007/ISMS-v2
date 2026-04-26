from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

from sqlalchemy import or_
from sqlalchemy.orm import Session

from app.services.rating_policy import enum_value, match_side_ids
from app.services.sport_rulesets import get_ruleset

PERFORMANCE_BASELINE = 50.0
PERFORMANCE_MIN_COVERAGE = 0.35
PERFORMANCE_FULL_COVERAGE = 0.80
PERFORMANCE_MIN_RELIABLE_EVENTS = 12.0
PERFORMANCE_FULL_EVENTS = 36.0
PERFORMANCE_FULL_MATCHES = 6.0
PERFORMANCE_TEAMMATE_MIN_CONFIDENCE = 20.0
PERFORMANCE_TEAMMATE_MIN_GAP = 5.0
PERFORMANCE_TEAMMATE_FULL_GAP = 20.0
PERFORMANCE_TEAMMATE_TRANSFER_CAP = 8.0
PERFORMANCE_RESULT_FLOOR = 1.0


@dataclass
class PerformanceAccumulator:
    matches_with_events: int = 0
    total_points: int = 0
    attributed_points: int = 0
    winning_shots: float = 0.0
    forced_errors_drawn: float = 0.0
    errors_committed: float = 0.0
    serve_faults: float = 0.0
    violations: float = 0.0
    clutch_points_won: float = 0.0
    clutch_errors: float = 0.0


@dataclass
class PerformanceSnapshot:
    performance_rating: float = PERFORMANCE_BASELINE
    performance_confidence: float = 0.0
    performance_coverage_pct: float = 0.0
    performance_reliable: bool = False
    performance_matches_with_events: int = 0
    performance_total_points: int = 0
    performance_attributed_points: int = 0
    performance_winning_shots: float = 0.0
    performance_forced_errors_drawn: float = 0.0
    performance_errors_committed: float = 0.0
    performance_serve_faults: float = 0.0
    performance_violations: float = 0.0
    performance_clutch_points_won: float = 0.0
    performance_clutch_errors: float = 0.0
    performance_last_calculated_at: datetime | None = None


def build_match_performance_snapshots(match, history_rows: list) -> dict[str, PerformanceSnapshot]:
    snapshots = build_performance_snapshots([match], {str(match.id): history_rows or []})
    sport = enum_value(match.sport)
    match_format = enum_value(match.match_format)
    return {
        user_id: snapshot
        for (user_id, snapshot_sport, snapshot_format), snapshot in snapshots.items()
        if snapshot_sport == sport and snapshot_format == match_format
    }


def _performance_key(user_id: str, sport: str, match_format: str) -> tuple[str, str, str]:
    return (str(user_id), sport, match_format)


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def _player_match_filter(user_ids: Iterable[str]):
    from app.models.models import Match

    unique_user_ids = [str(user_id) for user_id in user_ids if user_id]
    return or_(
        Match.player1_id.in_(unique_user_ids),
        Match.player2_id.in_(unique_user_ids),
        Match.player3_id.in_(unique_user_ids),
        Match.player4_id.in_(unique_user_ids),
        Match.team1_player1.in_(unique_user_ids),
        Match.team1_player2.in_(unique_user_ids),
        Match.team2_player1.in_(unique_user_ids),
        Match.team2_player2.in_(unique_user_ids),
    )


def _match_team_lookup(match) -> tuple[list[str], list[str]] | None:
    sides = match_side_ids(match)
    if sides is None:
        return None
    team1_ids, team2_ids = sides
    return (team1_ids, team2_ids)


def _team_ids_for_label(team1_ids: list[str], team2_ids: list[str], team_label: str | None) -> list[str]:
    if team_label == "team1":
        return team1_ids
    if team_label == "team2":
        return team2_ids
    return []


def _team_label_for_user(team1_ids: list[str], team2_ids: list[str], user_id: str) -> str | None:
    if user_id in team1_ids:
        return "team1"
    if user_id in team2_ids:
        return "team2"
    return None


def _score_target(match) -> tuple[int, int]:
    ruleset = get_ruleset(enum_value(match.sport)) or {}
    target = int(getattr(match, "score_limit", None) or ruleset.get("points_per_set") or ruleset.get("games_per_set") or 21)
    win_by = int(ruleset.get("win_by", 2))
    return target, max(2, win_by)


def _is_clutch_score_event(match, row) -> bool:
    if row.team1_score is None and row.team2_score is None:
        return False

    target, win_by = _score_target(match)
    left = int(row.team1_score or 0)
    right = int(row.team2_score or 0)
    return max(left, right) >= max(1, target - 1) and abs(left - right) <= win_by


def _finalize_snapshot(accumulator: PerformanceAccumulator) -> PerformanceSnapshot:
    total_points = int(accumulator.total_points)
    attributed_points = int(accumulator.attributed_points)
    coverage = (attributed_points / total_points) if total_points > 0 else 0.0

    individual_event_volume = (
        accumulator.winning_shots
        + accumulator.forced_errors_drawn
        + accumulator.errors_committed
        + accumulator.serve_faults
        + accumulator.violations
    )
    positive_total = (
        accumulator.winning_shots
        + (0.75 * accumulator.forced_errors_drawn)
        + (0.5 * accumulator.clutch_points_won)
    )
    negative_total = (
        accumulator.errors_committed
        + (0.75 * accumulator.serve_faults)
        + (0.75 * accumulator.violations)
        + (0.5 * accumulator.clutch_errors)
    )
    total_weight = positive_total + negative_total
    balance = ((positive_total - negative_total) / total_weight) if total_weight > 0 else 0.0

    sample_factor = _clamp(individual_event_volume / PERFORMANCE_FULL_EVENTS, 0.0, 1.0)
    coverage_factor = _clamp(coverage / PERFORMANCE_FULL_COVERAGE, 0.0, 1.0)
    match_factor = _clamp(accumulator.matches_with_events / PERFORMANCE_FULL_MATCHES, 0.0, 1.0)
    confidence = _clamp(
        (0.5 * sample_factor) + (0.3 * coverage_factor) + (0.2 * match_factor),
        0.0,
        1.0,
    )
    reliable = (
        individual_event_volume >= PERFORMANCE_MIN_RELIABLE_EVENTS
        and coverage >= PERFORMANCE_MIN_COVERAGE
        and accumulator.matches_with_events > 0
    )

    raw_rating = PERFORMANCE_BASELINE + (balance * 50.0)
    performance_rating = PERFORMANCE_BASELINE + ((raw_rating - PERFORMANCE_BASELINE) * confidence)

    return PerformanceSnapshot(
        performance_rating=round(_clamp(performance_rating, 0.0, 100.0), 1),
        performance_confidence=round(confidence * 100.0, 1),
        performance_coverage_pct=round(coverage * 100.0, 1),
        performance_reliable=reliable,
        performance_matches_with_events=int(accumulator.matches_with_events),
        performance_total_points=total_points,
        performance_attributed_points=attributed_points,
        performance_winning_shots=round(accumulator.winning_shots, 1),
        performance_forced_errors_drawn=round(accumulator.forced_errors_drawn, 1),
        performance_errors_committed=round(accumulator.errors_committed, 1),
        performance_serve_faults=round(accumulator.serve_faults, 1),
        performance_violations=round(accumulator.violations, 1),
        performance_clutch_points_won=round(accumulator.clutch_points_won, 1),
        performance_clutch_errors=round(accumulator.clutch_errors, 1),
        performance_last_calculated_at=datetime.now(timezone.utc),
    )


def build_performance_snapshots(matches: list, histories_by_match: dict[str, list]) -> dict[tuple[str, str, str], PerformanceSnapshot]:
    accumulators: dict[tuple[str, str, str], PerformanceAccumulator] = {}

    for match in matches:
        lookup = _match_team_lookup(match)
        if lookup is None:
            continue

        team1_ids, team2_ids = lookup
        participants = team1_ids + team2_ids
        if not participants:
            continue

        sport = enum_value(match.sport)
        match_format = enum_value(match.match_format)
        rows = histories_by_match.get(str(match.id), [])

        for user_id in participants:
            accumulators.setdefault(_performance_key(user_id, sport, match_format), PerformanceAccumulator())

        relevant_rows = [row for row in rows if row.event_type in ("point", "violation", "serve_change")]
        point_rows = [row for row in relevant_rows if row.event_type == "point"]
        if relevant_rows:
            attributed_points = sum(
                1
                for row in point_rows
                if row.player_id is not None or ((row.meta or {}).get("actor_player_id") is not None)
            )
            for user_id in participants:
                accumulator = accumulators[_performance_key(user_id, sport, match_format)]
                accumulator.matches_with_events += 1
                accumulator.total_points += len(point_rows)
                accumulator.attributed_points += attributed_points

        for row in relevant_rows:
            meta = row.meta or {}
            team_label = str(row.team) if row.team is not None else None
            scoring_team_ids = _team_ids_for_label(team1_ids, team2_ids, team_label)
            share = (1.0 / len(scoring_team_ids)) if scoring_team_ids else 0.0

            if row.event_type == "point":
                clutch = _is_clutch_score_event(match, row)
                attribution_type = str(meta.get("attribution_type") or "other")
                scorer_id = str(row.player_id) if row.player_id is not None else None
                actor_id = str(meta.get("actor_player_id")) if meta.get("actor_player_id") is not None else None

                if attribution_type == "winning_shot" and scorer_id:
                    key = _performance_key(scorer_id, sport, match_format)
                    if key in accumulators:
                        accumulators[key].winning_shots += 1.0
                        if clutch:
                            accumulators[key].clutch_points_won += 1.0
                elif attribution_type == "opponent_error":
                    if actor_id:
                        key = _performance_key(actor_id, sport, match_format)
                        if key in accumulators:
                            accumulators[key].errors_committed += 1.0
                            if clutch:
                                accumulators[key].clutch_errors += 1.0
                    if share > 0.0:
                        for user_id in scoring_team_ids:
                            key = _performance_key(user_id, sport, match_format)
                            if key not in accumulators:
                                continue
                            accumulators[key].forced_errors_drawn += share
                            if clutch:
                                accumulators[key].clutch_points_won += share

            elif row.event_type == "violation":
                violator_id = str(row.player_id) if row.player_id is not None else None
                if not violator_id:
                    continue

                key = _performance_key(violator_id, sport, match_format)
                if key not in accumulators:
                    continue

                accumulators[key].violations += 1.0

                violator_team = _team_label_for_user(team1_ids, team2_ids, violator_id)
                if team_label and violator_team and team_label != violator_team and _is_clutch_score_event(match, row):
                    accumulators[key].clutch_errors += 1.0

            elif row.event_type == "serve_change":
                fault_player_id = meta.get("fault_player_id")
                if fault_player_id is None and row.player_id is not None:
                    fault_player_id = str(row.player_id)
                if fault_player_id is None:
                    continue

                key = _performance_key(str(fault_player_id), sport, match_format)
                if key in accumulators:
                    accumulators[key].serve_faults += 1.0

    return {
        key: _finalize_snapshot(accumulator)
        for key, accumulator in accumulators.items()
    }


def _redistribute_team_rating_delta(
    team_ids: list[str],
    *,
    team_won: bool,
    old_ratings: dict[str, float],
    adjusted_ratings: dict[str, float],
    performance_snapshots: dict[str, PerformanceSnapshot],
) -> None:
    if len(team_ids) != 2:
        return

    first_id, second_id = [str(user_id) for user_id in team_ids]
    first_snapshot = performance_snapshots.get(first_id)
    second_snapshot = performance_snapshots.get(second_id)
    if first_snapshot is None or second_snapshot is None:
        return

    confidence = min(
        float(first_snapshot.performance_confidence or 0.0),
        float(second_snapshot.performance_confidence or 0.0),
    )
    if confidence < PERFORMANCE_TEAMMATE_MIN_CONFIDENCE:
        return

    gap = float(first_snapshot.performance_rating or PERFORMANCE_BASELINE) - float(
        second_snapshot.performance_rating or PERFORMANCE_BASELINE
    )
    if abs(gap) < PERFORMANCE_TEAMMATE_MIN_GAP:
        return

    better_id = first_id if gap > 0 else second_id
    worse_id = second_id if gap > 0 else first_id
    better_delta = float(adjusted_ratings.get(better_id, old_ratings.get(better_id, 1500.0))) - float(
        old_ratings.get(better_id, 1500.0)
    )
    worse_delta = float(adjusted_ratings.get(worse_id, old_ratings.get(worse_id, 1500.0))) - float(
        old_ratings.get(worse_id, 1500.0)
    )

    if team_won:
        if better_delta <= 0.0 or worse_delta <= 0.0:
            return
    else:
        if better_delta >= 0.0 or worse_delta >= 0.0:
            return

    gap_factor = _clamp(abs(gap) / PERFORMANCE_TEAMMATE_FULL_GAP, 0.0, 1.0)
    transfer = PERFORMANCE_TEAMMATE_TRANSFER_CAP * (confidence / 100.0) * gap_factor

    if team_won:
        transfer = min(transfer, max(0.0, worse_delta - PERFORMANCE_RESULT_FLOOR))
    else:
        transfer = min(transfer, max(0.0, abs(better_delta) - PERFORMANCE_RESULT_FLOOR))

    if transfer <= 0.0:
        return

    adjusted_ratings[better_id] = float(adjusted_ratings[better_id]) + transfer
    adjusted_ratings[worse_id] = float(adjusted_ratings[worse_id]) - transfer


def redistribute_match_ratings_by_performance(
    match,
    history_rows: list,
    old_ratings: dict[str, float],
    new_ratings: dict[str, float],
    *,
    winner_id: str | None = None,
) -> dict[str, float]:
    adjusted_ratings = {str(user_id): float(rating) for user_id, rating in new_ratings.items()}
    sides = match_side_ids(match)
    if sides is None:
        return adjusted_ratings

    team1_ids, team2_ids = sides
    if len(team1_ids) != 2 or len(team2_ids) != 2:
        return adjusted_ratings

    resolved_winner_id = str(winner_id) if winner_id else (str(match.winner_id) if getattr(match, "winner_id", None) is not None else None)
    if resolved_winner_id is None:
        return adjusted_ratings

    performance_snapshots = build_match_performance_snapshots(match, history_rows)
    if not performance_snapshots:
        return adjusted_ratings

    _redistribute_team_rating_delta(
        team1_ids,
        team_won=resolved_winner_id in team1_ids,
        old_ratings=old_ratings,
        adjusted_ratings=adjusted_ratings,
        performance_snapshots=performance_snapshots,
    )
    _redistribute_team_rating_delta(
        team2_ids,
        team_won=resolved_winner_id in team2_ids,
        old_ratings=old_ratings,
        adjusted_ratings=adjusted_ratings,
        performance_snapshots=performance_snapshots,
    )
    return adjusted_ratings


def _query_completed_matches(db: Session, user_ids: list[str] | None = None, sport: str | None = None, match_format: str | None = None) -> list:
    from app.models.models import Match

    query = db.query(Match).filter(Match.status == "completed")
    if user_ids:
        query = query.filter(_player_match_filter(user_ids))
    if sport is not None:
        query = query.filter(Match.sport == sport)
    if match_format is not None:
        query = query.filter(Match.match_format == match_format)
    return query.all()


def _query_histories_by_match(db: Session, match_ids: list) -> dict[str, list]:
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

    histories_by_match: dict[str, list] = {}
    for row in rows:
        histories_by_match.setdefault(str(row.match_id), []).append(row)
    return histories_by_match


def reset_performance_fields(row) -> None:
    row.performance_rating = PERFORMANCE_BASELINE
    row.performance_confidence = 0.0
    row.performance_coverage_pct = 0.0
    row.performance_reliable = False
    row.performance_matches_with_events = 0
    row.performance_total_points = 0
    row.performance_attributed_points = 0
    row.performance_winning_shots = 0.0
    row.performance_forced_errors_drawn = 0.0
    row.performance_errors_committed = 0.0
    row.performance_serve_faults = 0.0
    row.performance_violations = 0.0
    row.performance_clutch_points_won = 0.0
    row.performance_clutch_errors = 0.0
    row.performance_last_calculated_at = datetime.now(timezone.utc)


def apply_performance_snapshot_to_row(row, snapshot: PerformanceSnapshot) -> None:
    row.performance_rating = snapshot.performance_rating
    row.performance_confidence = snapshot.performance_confidence
    row.performance_coverage_pct = snapshot.performance_coverage_pct
    row.performance_reliable = snapshot.performance_reliable
    row.performance_matches_with_events = snapshot.performance_matches_with_events
    row.performance_total_points = snapshot.performance_total_points
    row.performance_attributed_points = snapshot.performance_attributed_points
    row.performance_winning_shots = snapshot.performance_winning_shots
    row.performance_forced_errors_drawn = snapshot.performance_forced_errors_drawn
    row.performance_errors_committed = snapshot.performance_errors_committed
    row.performance_serve_faults = snapshot.performance_serve_faults
    row.performance_violations = snapshot.performance_violations
    row.performance_clutch_points_won = snapshot.performance_clutch_points_won
    row.performance_clutch_errors = snapshot.performance_clutch_errors
    row.performance_last_calculated_at = snapshot.performance_last_calculated_at or datetime.now(timezone.utc)


def refresh_performance_metrics(
    db: Session,
    user_ids: list[str],
    sport: str | None = None,
    match_format: str | None = None,
) -> None:
    from app.models.models import PlayerRating

    normalized_user_ids = sorted({str(user_id) for user_id in user_ids if user_id})
    if not normalized_user_ids:
        return

    rows_query = db.query(PlayerRating).filter(PlayerRating.user_id.in_(normalized_user_ids))
    if sport is not None:
        rows_query = rows_query.filter(PlayerRating.sport == sport)
    if match_format is not None:
        rows_query = rows_query.filter(PlayerRating.match_format == match_format)
    rows = rows_query.all()

    matches = _query_completed_matches(db, normalized_user_ids, sport=sport, match_format=match_format)
    histories_by_match = _query_histories_by_match(db, [match.id for match in matches])
    snapshots = build_performance_snapshots(matches, histories_by_match)

    for row in rows:
        reset_performance_fields(row)
        key = _performance_key(str(row.user_id), enum_value(row.sport), str(row.match_format))
        snapshot = snapshots.get(key)
        if snapshot is not None:
            apply_performance_snapshot_to_row(row, snapshot)

    db.flush()


def rebuild_all_performance_metrics(db: Session) -> None:
    from app.models.models import PlayerRating

    rows = db.query(PlayerRating).all()
    matches = _query_completed_matches(db)
    histories_by_match = _query_histories_by_match(db, [match.id for match in matches])
    snapshots = build_performance_snapshots(matches, histories_by_match)

    for row in rows:
        reset_performance_fields(row)
        key = _performance_key(str(row.user_id), enum_value(row.sport), str(row.match_format))
        snapshot = snapshots.get(key)
        if snapshot is not None:
            apply_performance_snapshot_to_row(row, snapshot)

    db.flush()
