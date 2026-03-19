"""
Player Assessment — scores a player's suitability for a tournament.

Signals used:
  • rating          — Glicko-2 rating (from PlayerRating)
  • activity        — completed matches in the last 60 days
  • win_rate        — wins / completed matches (lifetime)
  • win_streak      — current consecutive win streak
  • recent_win_rate — wins in last 30 days / matches in last 30 days

Output: a dict with each signal + a combined 0-100 "readiness_score".
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from sqlalchemy.orm import Session
from sqlalchemy import or_

from app.models.models import Match, PlayerRating, SportType


# Weights must sum to 1.0
_WEIGHTS = {
    "rating":          0.35,
    "activity":        0.20,
    "win_rate":        0.25,
    "win_streak":      0.10,
    "recent_win_rate": 0.10,
}

_ACTIVITY_CAP   = 20   # 20+ matches in 60 days = full activity score
_WIN_STREAK_CAP = 10   # 10+ consecutive wins = full streak score


def _player_matches(db: Session, player_id: str, sport: str):
    """All completed matches for a player in a given sport."""
    return db.query(Match).filter(
        or_(
            Match.player1_id == player_id,
            Match.player2_id == player_id,
            Match.team1_player1 == player_id,
            Match.team1_player2 == player_id,
            Match.team2_player1 == player_id,
            Match.team2_player2 == player_id,
        ),
        Match.sport == sport,
        Match.status == "completed",
    ).order_by(Match.completed_at.desc()).all()


def _is_winner(match: Match, player_id: str) -> bool:
    return match.winner_id is not None and str(match.winner_id) == player_id


def assess_player(
    db: Session,
    player_id: str,
    sport: str | SportType,
    min_rating: float | None = None,
    max_rating: float | None = None,
) -> dict:
    # Normalise sport to enum value string so DB comparisons work correctly
    if isinstance(sport, SportType):
        sport = sport.value
    elif hasattr(sport, "value"):
        sport = sport.value
    """
    Returns assessment dict:
      rating, activity_score, win_rate, win_streak, recent_win_rate,
      readiness_score (0-100), meets_rating_requirement (bool),
      flags: list of human-readable observations.
    """
    # ── Rating ────────────────────────────────────────────────────────────────
    rating_row = db.query(PlayerRating).filter(
        PlayerRating.user_id == player_id,
        PlayerRating.sport   == sport,
    ).first()
    rating = float(rating_row.rating) if rating_row else 1500.0

    # Normalise rating 1000–2500 → 0–100
    rating_score = max(0.0, min(100.0, (rating - 1000.0) / 15.0))

    # ── Match history ─────────────────────────────────────────────────────────
    all_matches = _player_matches(db, player_id, sport)
    total = len(all_matches)

    now = datetime.now(timezone.utc)
    cutoff_60 = now - timedelta(days=60)
    cutoff_30 = now - timedelta(days=30)

    recent_60 = [
        m for m in all_matches
        if m.completed_at and m.completed_at.replace(tzinfo=timezone.utc) >= cutoff_60
    ]
    recent_30 = [
        m for m in all_matches
        if m.completed_at and m.completed_at.replace(tzinfo=timezone.utc) >= cutoff_30
    ]

    # Activity: matches in last 60 days, capped
    activity_score = min(100.0, (len(recent_60) / _ACTIVITY_CAP) * 100.0)

    # Win rate (lifetime)
    wins = sum(1 for m in all_matches if _is_winner(m, player_id))
    win_rate = (wins / total) if total > 0 else 0.0
    win_rate_score = win_rate * 100.0

    # Recent win rate (last 30 days)
    recent_wins = sum(1 for m in recent_30 if _is_winner(m, player_id))
    recent_win_rate = (recent_wins / len(recent_30)) if recent_30 else win_rate
    recent_win_rate_score = recent_win_rate * 100.0

    # Win streak: walk matches newest-first and count consecutive wins
    streak = 0
    for m in all_matches:
        if _is_winner(m, player_id):
            streak += 1
        else:
            break
    win_streak_score = min(100.0, (streak / _WIN_STREAK_CAP) * 100.0)

    # ── Combined readiness score ──────────────────────────────────────────────
    readiness_score = (
        rating_score          * _WEIGHTS["rating"]
        + activity_score      * _WEIGHTS["activity"]
        + win_rate_score      * _WEIGHTS["win_rate"]
        + win_streak_score    * _WEIGHTS["win_streak"]
        + recent_win_rate_score * _WEIGHTS["recent_win_rate"]
    )

    # ── Rating eligibility ────────────────────────────────────────────────────
    meets_rating = True
    if min_rating is not None and rating < min_rating:
        meets_rating = False
    if max_rating is not None and rating > max_rating:
        meets_rating = False

    # ── Human-readable flags ──────────────────────────────────────────────────
    flags: list[str] = []
    if total == 0:
        flags.append("No match history")
    elif len(recent_60) == 0:
        flags.append("Inactive (60+ days)")
    if streak >= 3:
        flags.append(f"{streak}-match win streak")
    if win_rate >= 0.7 and total >= 5:
        flags.append("High win rate")
    if not meets_rating:
        if min_rating and rating < min_rating:
            flags.append(f"Rating below minimum ({int(min_rating)})")
        if max_rating and rating > max_rating:
            flags.append(f"Rating above maximum ({int(max_rating)})")

    return {
        "rating":              round(rating, 1),
        "rating_score":        round(rating_score, 1),
        "total_matches":       total,
        "activity_score":      round(activity_score, 1),
        "win_rate":            round(win_rate, 3),
        "win_rate_score":      round(win_rate_score, 1),
        "win_streak":          streak,
        "win_streak_score":    round(win_streak_score, 1),
        "recent_win_rate":     round(recent_win_rate, 3),
        "recent_win_rate_score": round(recent_win_rate_score, 1),
        "readiness_score":     round(readiness_score, 1),
        "meets_rating_requirement": meets_rating,
        "flags":               flags,
    }
