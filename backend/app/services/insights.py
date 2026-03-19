import logging
from sqlalchemy.orm import Session
from app.models.models import PlayerRating, Match, MatchHistory, Profile
from app.config import settings
from sqlalchemy import or_

logger = logging.getLogger(__name__)


def _collect_player_stats(user_id: str, db: Session) -> dict:
    """Aggregate all stats needed for LLM prompt."""
    ratings = db.query(PlayerRating).filter(PlayerRating.user_id == user_id).all()

    # Recent completed matches (last 20)
    matches = db.query(Match).filter(
        or_(Match.player1_id == user_id, Match.player2_id == user_id),
        Match.status == "completed",
    ).order_by(Match.created_at.desc()).limit(20).all()

    total_matches  = sum(r.matches_played or 0 for r in ratings)
    total_wins     = sum(r.wins or 0 for r in ratings)
    total_losses   = sum(r.losses or 0 for r in ratings)
    win_rate       = round(total_wins / total_matches * 100, 1) if total_matches > 0 else 0

    # Per-sport breakdown
    sport_stats = []
    for r in ratings:
        if (r.matches_played or 0) == 0:
            continue
        wr = round((r.wins or 0) / (r.matches_played or 1) * 100, 1)
        sport_stats.append({
            "sport":         r.sport.value,
            "format":        r.match_format,
            "rating":        round(float(r.rating or 1500), 0),
            "matches":       r.matches_played,
            "wins":          r.wins,
            "losses":        r.losses,
            "win_rate":      wr,
            "win_streak":    r.current_win_streak,
            "loss_streak":   r.current_loss_streak,
        })

    # Event attribution stats from match history
    history_rows = db.query(MatchHistory).filter(
        MatchHistory.player_id == user_id,
    ).all()

    points_contributed = sum(1 for h in history_rows if h.event_type == "point")
    violations_caused  = sum(1 for h in history_rows if h.event_type == "violation")

    # Violation breakdown
    violation_counts: dict = {}
    for h in history_rows:
        if h.event_type == "violation" and h.meta:
            code = h.meta.get("violation_code", "unknown")
            violation_counts[code] = violation_counts.get(code, 0) + 1

    top_violations = sorted(violation_counts.items(), key=lambda x: x[1], reverse=True)[:3]

    return {
        "total_matches":      total_matches,
        "total_wins":         total_wins,
        "total_losses":       total_losses,
        "win_rate":           win_rate,
        "points_contributed": points_contributed,
        "violations_caused":  violations_caused,
        "top_violations":     [{"code": k, "count": v} for k, v in top_violations],
        "sport_breakdown":    sport_stats,
    }


def _build_prompt(username: str, stats: dict) -> str:
    sport_lines = "\n".join(
        f"  - {s['sport']} ({s['format']}): Rating {s['rating']}, "
        f"{s['matches']} matches, {s['win_rate']}% win rate, "
        f"streak: +{s['win_streak']}W / -{s['loss_streak']}L"
        for s in stats["sport_breakdown"]
    ) or "  - No completed sport data yet"

    violation_lines = "\n".join(
        f"  - {v['code']}: {v['count']}x"
        for v in stats["top_violations"]
    ) or "  - None recorded"

    return f"""You are a sports performance coach analyzing a player's match data for ISMS (Indoor Sports Management System).

Player: @{username}
Overall: {stats['total_matches']} matches | {stats['total_wins']}W {stats['total_losses']}L | {stats['win_rate']}% win rate
Points contributed (attributed): {stats['points_contributed']}
Violations caused: {stats['violations_caused']}

Sport breakdown:
{sport_lines}

Top violations:
{violation_lines}

Write a concise, personalized performance insight (3–5 sentences). Include:
1. One strength to acknowledge
2. One clear area to improve with a specific, actionable tip
3. One motivational closing line

Keep it direct and conversational. Do not use headers or bullet points — write in plain paragraphs."""


def generate_insight(user_id: str, db: Session) -> dict:
    """Generate LLM insight for a player and save to DB."""
    from app.models.models import PlayerInsight

    if not settings.anthropic_api_key:
        return {"error": "Anthropic API key not configured."}

    profile = db.query(Profile).filter(Profile.id == user_id).first()
    if not profile:
        return {"error": "Profile not found."}

    stats = _collect_player_stats(user_id, db)

    if stats["total_matches"] == 0:
        return {"error": "Play at least one match before generating insights."}

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=400,
            messages=[{"role": "user", "content": _build_prompt(profile.username, stats)}],
        )
        insight_text = message.content[0].text.strip()

    except Exception as e:
        logger.error(f"Anthropic API error for user {user_id}: {e}")
        return {"error": f"LLM call failed: {str(e)}"}

    # Save insight
    row = PlayerInsight(
        user_id=user_id,
        sport=None,
        insight_text=insight_text,
        stats_snapshot=stats,
    )
    db.add(row)
    db.commit()
    db.refresh(row)

    return {
        "id":           str(row.id),
        "insight_text": row.insight_text,
        "stats":        stats,
        "generated_at": str(row.generated_at),
    }


def get_latest_insight(user_id: str, db: Session) -> dict | None:
    """Return the most recent saved insight for a player."""
    from app.models.models import PlayerInsight

    row = db.query(PlayerInsight).filter(
        PlayerInsight.user_id == user_id,
    ).order_by(PlayerInsight.generated_at.desc()).first()

    if not row:
        return None

    return {
        "id":           str(row.id),
        "insight_text": row.insight_text,
        "stats":        row.stats_snapshot,
        "generated_at": str(row.generated_at),
    }
