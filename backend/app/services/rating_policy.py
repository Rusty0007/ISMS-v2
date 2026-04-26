from __future__ import annotations

from typing import Iterable

ML_MATCHMAKING_MIN_MATCHES = 10
LEADERBOARD_MIN_MATCHES = 20
LEADERBOARD_MIN_DISTINCT_OPPONENTS = 3
LEADERBOARD_RD_THRESHOLD = 200.0


def enum_value(value) -> str:
    return value.value if hasattr(value, "value") else str(value)


def _id(value) -> str | None:
    return str(value) if value is not None else None


def unique_ids(values: Iterable) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        value_id = _id(value)
        if value_id is None or value_id in seen:
            continue
        seen.add(value_id)
        result.append(value_id)
    return result


def match_side_ids(match) -> tuple[list[str], list[str]] | None:
    match_format = enum_value(match.match_format)
    if match_format == "singles":
        team1 = unique_ids([match.player1_id])
        team2 = unique_ids([match.player2_id])
        return (team1, team2) if team1 and team2 else None

    has_team_slots = any(
        [
            match.team1_player1,
            match.team1_player2,
            match.team2_player1,
            match.team2_player2,
        ]
    )
    if has_team_slots:
        team1 = unique_ids(
            [
                match.team1_player1 or match.player1_id,
                match.team1_player2 or match.player3_id,
            ]
        )
        team2 = unique_ids(
            [
                match.team2_player1 or match.player2_id,
                match.team2_player2 or match.player4_id,
            ]
        )
    elif match.player3_id is not None and match.player4_id is not None:
        # Legacy doubles queue layout: player1/player2 vs player3/player4.
        team1 = unique_ids([match.player1_id, match.player2_id])
        team2 = unique_ids([match.player3_id, match.player4_id])
    else:
        # Canonical doubles layout: player1/player3 vs player2/player4.
        team1 = unique_ids([match.player1_id, match.player3_id])
        team2 = unique_ids([match.player2_id, match.player4_id])

    return (team1, team2) if team1 and team2 else None


def opponent_ids_for_user(match, user_id: str) -> set[str]:
    sides = match_side_ids(match)
    if sides is None:
        return set()

    team1, team2 = sides
    user_id = str(user_id)
    if user_id in team1:
        return set(team2)
    if user_id in team2:
        return set(team1)
    return set()


def matchmaking_eligible(matches_played: int) -> bool:
    return int(matches_played or 0) >= ML_MATCHMAKING_MIN_MATCHES


def leaderboard_eligible(
    matches_played: int,
    distinct_opponents: int,
    rating_deviation: float,
) -> bool:
    return (
        int(matches_played or 0) >= LEADERBOARD_MIN_MATCHES
        and int(distinct_opponents or 0) >= LEADERBOARD_MIN_DISTINCT_OPPONENTS
        and float(rating_deviation or 999.0) <= LEADERBOARD_RD_THRESHOLD
    )
