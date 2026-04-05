import logging
from datetime import datetime, timezone

import httpx
from sqlalchemy import or_
from sqlalchemy.orm import Session

from app.config import settings
from app.models.models import Match, MatchHistory, PlayerRating, Profile

logger = logging.getLogger(__name__)

_OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"


def _enum_value(value) -> str:
    return str(getattr(value, "value", value))


def _player_match_filter(user_id: str):
    return or_(
        Match.player1_id == user_id,
        Match.player2_id == user_id,
        Match.player3_id == user_id,
        Match.player4_id == user_id,
        Match.team1_player1 == user_id,
        Match.team1_player2 == user_id,
        Match.team2_player1 == user_id,
        Match.team2_player2 == user_id,
    )


def _is_player_winner(match: Match, user_id: str) -> bool:
    return match.winner_id is not None and str(match.winner_id) == user_id


def _score_summary(match: Match) -> str:
    completed_sets = [s for s in sorted(match.sets or [], key=lambda row: row.set_number) if s.is_completed]
    if not completed_sets:
        return "No set scores recorded"

    is_team_match = _enum_value(match.match_format) in {"doubles", "mixed_doubles"}
    parts: list[str] = []
    for item in completed_sets:
        left = item.team1_score if is_team_match else item.player1_score
        right = item.team2_score if is_team_match else item.player2_score
        if left is None or right is None:
            continue
        parts.append(f"{left}-{right}")

    return ", ".join(parts) if parts else "No set scores recorded"


def _opponent_ids(match: Match, user_id: str) -> list[str]:
    user = str(user_id)
    fmt = _enum_value(match.match_format)

    if fmt in {"doubles", "mixed_doubles"}:
        team1 = [str(pid) for pid in [match.team1_player1, match.team1_player2] if pid]
        team2 = [str(pid) for pid in [match.team2_player1, match.team2_player2] if pid]
        if user in team1:
            return team2
        if user in team2:
            return team1
        return []

    players = [
        str(pid)
        for pid in [match.player1_id, match.player2_id, match.player3_id, match.player4_id]
        if pid
    ]
    return [pid for pid in players if pid != user]


def _best_and_weakest_sports(sport_stats: list[dict]) -> tuple[dict | None, dict | None]:
    eligible = [item for item in sport_stats if (item.get("matches") or 0) > 0]
    if not eligible:
        return None, None

    best = max(eligible, key=lambda item: (item["win_rate"], item["rating"], item["matches"]))
    weakest = min(eligible, key=lambda item: (item["win_rate"], item["rating"], -item["matches"]))
    return best, weakest


def _collect_player_stats(user_id: str, db: Session) -> dict:
    """Aggregate player match, gameplay, and discipline data for the LLM prompt."""
    ratings = db.query(PlayerRating).filter(PlayerRating.user_id == user_id).all()

    matches = db.query(Match).filter(
        _player_match_filter(user_id),
        Match.status == "completed",
    ).order_by(Match.completed_at.desc(), Match.created_at.desc()).limit(20).all()

    total_matches = sum(r.matches_played or 0 for r in ratings)
    total_wins = sum(r.wins or 0 for r in ratings)
    total_losses = sum(r.losses or 0 for r in ratings)
    win_rate = round(total_wins / total_matches * 100, 1) if total_matches > 0 else 0.0

    sport_stats: list[dict] = []
    for rating in ratings:
        if (rating.matches_played or 0) == 0:
            continue
        matches_played = int(rating.matches_played or 0)
        wins = int(rating.wins or 0)
        losses = int(rating.losses or 0)
        sport_stats.append({
            "sport": _enum_value(rating.sport),
            "format": rating.match_format,
            "rating": round(float(rating.rating or 1500), 0),
            "matches": matches_played,
            "wins": wins,
            "losses": losses,
            "win_rate": round((wins / matches_played) * 100, 1) if matches_played else 0.0,
            "win_streak": int(rating.current_win_streak or 0),
            "loss_streak": int(rating.current_loss_streak or 0),
        })

    history_rows = db.query(MatchHistory).filter(MatchHistory.player_id == user_id).all()
    points_contributed = sum(1 for item in history_rows if item.event_type == "point")
    violations_caused = sum(1 for item in history_rows if item.event_type == "violation")

    violation_counts: dict[str, int] = {}
    for item in history_rows:
        if item.event_type == "violation" and item.meta:
            code = item.meta.get("violation_code", "unknown")
            violation_counts[code] = violation_counts.get(code, 0) + 1

    top_violations = sorted(violation_counts.items(), key=lambda row: row[1], reverse=True)[:3]

    opponent_ids = {
        opponent_id
        for match in matches[:10]
        for opponent_id in _opponent_ids(match, user_id)
    }
    opponent_map: dict[str, str] = {}
    if opponent_ids:
        opponent_profiles = db.query(Profile).filter(Profile.id.in_(opponent_ids)).all()
        opponent_map = {str(profile.id): f"@{profile.username}" for profile in opponent_profiles}

    recent_matches: list[dict] = []
    recent_form_tokens: list[str] = []
    for match in matches[:10]:
        outcome = "win" if _is_player_winner(match, user_id) else "loss"
        recent_form_tokens.append("W" if outcome == "win" else "L")

        opponents = [opponent_map.get(pid, "Unknown opponent") for pid in _opponent_ids(match, user_id)]
        recent_matches.append({
            "sport": _enum_value(match.sport),
            "format": _enum_value(match.match_format),
            "outcome": outcome,
            "score": _score_summary(match),
            "opponents": opponents,
            "completed_at": (
                match.completed_at.astimezone(timezone.utc).isoformat()
                if match.completed_at else None
            ),
        })

    recent_match_count = len(recent_matches)
    recent_wins = sum(1 for item in recent_matches if item["outcome"] == "win")
    recent_win_rate = round((recent_wins / recent_match_count) * 100, 1) if recent_match_count else 0.0

    best_sport, weakest_sport = _best_and_weakest_sports(sport_stats)

    return {
        "total_matches": total_matches,
        "total_wins": total_wins,
        "total_losses": total_losses,
        "win_rate": win_rate,
        "recent_form": " / ".join(recent_form_tokens) if recent_form_tokens else "No recent results",
        "recent_match_count": recent_match_count,
        "recent_win_rate": recent_win_rate,
        "points_contributed": points_contributed,
        "violations_caused": violations_caused,
        "point_to_violation_ratio": round(points_contributed / violations_caused, 2) if violations_caused else None,
        "top_violations": [{"code": code, "count": count} for code, count in top_violations],
        "sport_breakdown": sport_stats,
        "best_sport": best_sport,
        "needs_attention_sport": weakest_sport,
        "recent_matches": recent_matches,
    }


def _build_prompt(username: str, stats: dict) -> str:
    sport_lines = "\n".join(
        f"- {item['sport']} ({item['format']}): rating {item['rating']}, "
        f"{item['matches']} matches, {item['wins']}W-{item['losses']}L, "
        f"{item['win_rate']}% win rate, streak +{item['win_streak']}W/-{item['loss_streak']}L"
        for item in stats["sport_breakdown"]
    ) or "- No completed sport data yet"

    violation_lines = "\n".join(
        f"- {item['code']}: {item['count']} times"
        for item in stats["top_violations"]
    ) or "- None recorded"

    recent_match_lines = "\n".join(
        f"- {item['completed_at'] or 'Unknown date'} | {item['sport']} {item['format']} | "
        f"{item['outcome'].upper()} | vs {', '.join(item['opponents']) or 'Unknown opponents'} | "
        f"score {item['score']}"
        for item in stats["recent_matches"]
    ) or "- No recent completed matches"

    best_sport = stats.get("best_sport")
    weakest_sport = stats.get("needs_attention_sport")

    best_line = (
        f"{best_sport['sport']} ({best_sport['format']}) with {best_sport['win_rate']}% win rate "
        f"across {best_sport['matches']} matches"
        if best_sport else "No standout sport yet"
    )
    weakest_line = (
        f"{weakest_sport['sport']} ({weakest_sport['format']}) with {weakest_sport['win_rate']}% win rate "
        f"across {weakest_sport['matches']} matches"
        if weakest_sport else "No weak-sport signal yet"
    )

    ratio_text = (
        f"{stats['point_to_violation_ratio']} points per violation"
        if stats["point_to_violation_ratio"] is not None
        else "No violations recorded"
    )

    return f"""You are the ISMS AI Performance Coach. Analyze the player's actual match history and give a short, evidence-based coaching insight.

Player: @{username}
Overall record: {stats['total_matches']} matches | {stats['total_wins']} wins | {stats['total_losses']} losses | {stats['win_rate']}% win rate
Recent form: {stats['recent_form']} ({stats['recent_win_rate']}% win rate over last {stats['recent_match_count']} completed matches)
Points contributed: {stats['points_contributed']}
Violations caused: {stats['violations_caused']} ({ratio_text})
Strongest current sport: {best_line}
Needs attention most: {weakest_line}

Sport breakdown:
{sport_lines}

Top violations:
{violation_lines}

Recent completed matches:
{recent_match_lines}

Write 4 to 6 sentences in plain text, no bullets and no markdown.
Requirements:
- Mention one clear strength backed by the data above.
- Mention one performance risk or weakness backed by the data above.
- Recommend one concrete training focus for the next 2 weeks.
- Recommend one match-day habit or decision-making adjustment.
- Keep the tone supportive, practical, and specific. Avoid generic filler."""


def _extract_openrouter_text(payload: dict) -> str:
    choices = payload.get("choices") or []
    if not choices:
        raise ValueError("OpenRouter returned no choices.")

    message = choices[0].get("message") or {}
    content = message.get("content", "")

    if isinstance(content, str):
        return content.strip()

    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict) and item.get("type") == "text":
                parts.append(item.get("text", ""))
        text = "".join(parts).strip()
        if text:
            return text

    raise ValueError("OpenRouter returned an unsupported response format.")


def _generate_with_openrouter(prompt: str) -> tuple[str, str, str]:
    headers = {
        "Authorization": f"Bearer {settings.openrouter_api_key}",
        "Content-Type": "application/json",
    }
    if settings.openrouter_site_url:
        headers["HTTP-Referer"] = settings.openrouter_site_url
    if settings.openrouter_app_name:
        headers["X-Title"] = settings.openrouter_app_name

    payload = {
        "model": settings.openrouter_model,
        "temperature": 0.4,
        "max_tokens": 450,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are an expert racket-sport performance analyst. "
                    "Base every claim only on the supplied data and avoid inventing facts."
                ),
            },
            {"role": "user", "content": prompt},
        ],
    }

    with httpx.Client(timeout=45.0) as client:
        response = client.post(_OPENROUTER_URL, headers=headers, json=payload)

    if not response.is_success:
        raise RuntimeError(f"OpenRouter request failed ({response.status_code}): {response.text}")

    return _extract_openrouter_text(response.json()), "openrouter", settings.openrouter_model


def _generate_with_anthropic(prompt: str) -> tuple[str, str, str]:
    import anthropic

    model = "claude-sonnet-4-6"
    client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
    message = client.messages.create(
        model=model,
        max_tokens=450,
        system=(
            "You are an expert racket-sport performance analyst. "
            "Base every claim only on the supplied data and avoid inventing facts."
        ),
        messages=[{"role": "user", "content": prompt}],
    )
    return message.content[0].text.strip(), "anthropic", model


def _generate_insight_text(prompt: str) -> tuple[str, str, str]:
    if settings.openrouter_api_key:
        return _generate_with_openrouter(prompt)
    if settings.anthropic_api_key:
        return _generate_with_anthropic(prompt)
    raise RuntimeError("No LLM provider configured. Set OPENROUTER_API_KEY or ANTHROPIC_API_KEY.")


def generate_insight(user_id: str, db: Session) -> dict:
    """Generate an AI coaching insight for a player and save it to the database."""
    from app.models.models import PlayerInsight

    profile = db.query(Profile).filter(Profile.id == user_id).first()
    if not profile:
        return {"error": "Profile not found."}

    stats = _collect_player_stats(user_id, db)
    if stats["total_matches"] == 0:
        return {"error": "Play at least one match before generating insights."}

    prompt = _build_prompt(profile.username, stats)

    try:
        insight_text, provider, model = _generate_insight_text(prompt)
    except Exception as exc:
        logger.error("Insight generation failed for user %s: %s", user_id, exc)
        return {"error": f"LLM call failed: {str(exc)}"}

    snapshot = {
        **stats,
        "_llm": {
            "provider": provider,
            "model": model,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        },
    }

    row = PlayerInsight(
        user_id=user_id,
        sport=None,
        insight_text=insight_text,
        stats_snapshot=snapshot,
    )
    db.add(row)
    db.commit()
    db.refresh(row)

    return {
        "id": str(row.id),
        "insight_text": row.insight_text,
        "stats": snapshot,
        "generated_at": str(row.generated_at),
        "provider": provider,
        "model": model,
    }


def get_latest_insight(user_id: str, db: Session) -> dict | None:
    """Return the most recent saved insight for a player."""
    from app.models.models import PlayerInsight

    row = db.query(PlayerInsight).filter(
        PlayerInsight.user_id == user_id,
    ).order_by(PlayerInsight.generated_at.desc()).first()

    if not row:
        return None

    meta = (row.stats_snapshot or {}).get("_llm", {})
    return {
        "id": str(row.id),
        "insight_text": row.insight_text,
        "stats": row.stats_snapshot,
        "generated_at": str(row.generated_at),
        "provider": meta.get("provider"),
        "model": meta.get("model"),
    }
