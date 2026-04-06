import logging
import re
from datetime import datetime, timezone

import httpx
from sqlalchemy import or_
from sqlalchemy.orm import Session

from app.config import settings
from app.models.models import Match, MatchHistory, PlayerRating, Profile
from app.services.sport_rulesets import get_ruleset

logger = logging.getLogger(__name__)

_OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
_INSIGHT_QUOTE_CHARS = "\"'“”‘’"


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


def _team_member_ids(match: Match, team: str) -> list[str]:
    if team == "team1":
        members = [str(pid) for pid in [match.team1_player1, match.team1_player2] if pid]
        if members:
            return members
        return [str(pid) for pid in [match.player1_id, match.player3_id] if pid]

    members = [str(pid) for pid in [match.team2_player1, match.team2_player2] if pid]
    if members:
        return members
    return [str(pid) for pid in [match.player2_id, match.player4_id] if pid]


def _is_player_winner(match: Match, user_id: str) -> bool:
    if match.winner_id is None:
        return False

    winner_id = str(match.winner_id)
    player_id = str(user_id)
    match_format = _enum_value(match.match_format)

    if match_format in {"doubles", "mixed_doubles"}:
        team1 = _team_member_ids(match, "team1")
        team2 = _team_member_ids(match, "team2")
        if winner_id in team1:
            return player_id in team1
        if winner_id in team2:
            return player_id in team2
        return False

    return winner_id == player_id


def _resolve_set_rules(match: Match) -> tuple[int, int, int | None]:
    ruleset = get_ruleset(_enum_value(match.sport)) or {}
    target = int(getattr(match, "score_limit", None) or ruleset.get("points_per_set") or ruleset.get("games_per_set") or 21)
    win_by = int(ruleset.get("win_by", 2))
    max_points = ruleset.get("max_points")
    effective_max = int(max_points) if max_points and int(max_points) > target else None
    return target, win_by, effective_max


def _score_values(match: Match, match_set) -> tuple[int, int]:
    is_team_match = _enum_value(match.match_format) in {"doubles", "mixed_doubles"}
    left = match_set.team1_score if is_team_match and match_set.team1_score is not None else match_set.player1_score
    right = match_set.team2_score if is_team_match and match_set.team2_score is not None else match_set.player2_score
    return int(left or 0), int(right or 0)


def _is_completed_score(left: int, right: int, target: int, win_by: int, effective_max: int | None) -> bool:
    if left >= target and left - right >= win_by:
        return True
    if right >= target and right - left >= win_by:
        return True
    if effective_max and (left >= effective_max or right >= effective_max):
        return left != right
    return False


def _score_summary(match: Match) -> str:
    target, win_by, effective_max = _resolve_set_rules(match)
    completed_scores: list[str] = []

    for match_set in sorted(match.sets or [], key=lambda row: row.set_number):
        left, right = _score_values(match, match_set)
        if getattr(match_set, "is_completed", False) or _is_completed_score(left, right, target, win_by, effective_max):
            completed_scores.append(f"{left}-{right}")

    if not completed_scores:
        return "No set scores recorded"

    return ", ".join(completed_scores)


def _opponent_ids(match: Match, user_id: str) -> list[str]:
    user = str(user_id)
    fmt = _enum_value(match.match_format)

    if fmt in {"doubles", "mixed_doubles"}:
        team1 = _team_member_ids(match, "team1")
        team2 = _team_member_ids(match, "team2")
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


def _normalize_insight_text(text: str, username: str | None = None) -> str:
    cleaned = " ".join((text or "").split()).strip()
    while len(cleaned) >= 2 and cleaned[0] in _INSIGHT_QUOTE_CHARS and cleaned[-1] in _INSIGHT_QUOTE_CHARS:
        cleaned = cleaned[1:-1].strip()

    if not cleaned or not username:
        return cleaned

    escaped_username = re.escape(username.lstrip("@"))
    cleaned = re.sub(rf"^@?{escaped_username}\s+demonstrates\b", "You demonstrate", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(rf"^@?{escaped_username}\s+has\b", "You have", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(rf"^@?{escaped_username}\s+have\b", "You have", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(rf"^@?{escaped_username}\s+are\b", "You are", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(rf"^@?{escaped_username}\s+need(?:s)?\b", "You need", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(rf"^@?{escaped_username}\s+should\b", "You should", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(rf"^@?{escaped_username}\s+can\b", "You can", cleaned, flags=re.IGNORECASE)
    return cleaned


def _collect_player_stats(user_id: str, db: Session) -> dict:
    """Aggregate player match, gameplay, and discipline data for the LLM prompt."""
    ratings = db.query(PlayerRating).filter(PlayerRating.user_id == user_id).all()

    completed_matches = db.query(Match).filter(
        _player_match_filter(user_id),
        Match.status == "completed",
    ).order_by(Match.completed_at.desc(), Match.created_at.desc()).all()

    total_matches = len(completed_matches)
    total_wins = sum(1 for match in completed_matches if _is_player_winner(match, user_id))
    total_losses = max(total_matches - total_wins, 0)
    win_rate = round(total_wins / total_matches * 100, 1) if total_matches > 0 else 0.0

    rating_lookup = {
        (_enum_value(rating.sport), _enum_value(rating.match_format)): rating
        for rating in ratings
    }
    sport_summary: dict[tuple[str, str], dict] = {}
    for match in completed_matches:
        sport_key = _enum_value(match.sport)
        format_key = _enum_value(match.match_format)
        key = (sport_key, format_key)
        entry = sport_summary.setdefault(key, {
            "sport": sport_key,
            "format": format_key,
            "rating": 1500,
            "matches": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": 0.0,
            "win_streak": 0,
            "loss_streak": 0,
        })
        entry["matches"] += 1
        if _is_player_winner(match, user_id):
            entry["wins"] += 1
        else:
            entry["losses"] += 1

    sport_stats: list[dict] = []
    for key, entry in sport_summary.items():
        rating = rating_lookup.get(key)
        if rating:
            entry["rating"] = round(float(rating.rating or 1500), 0)
            entry["win_streak"] = int(rating.current_win_streak or 0)
            entry["loss_streak"] = int(rating.current_loss_streak or 0)
        matches_played = entry["matches"]
        wins = entry["wins"]
        entry["win_rate"] = round((wins / matches_played) * 100, 1) if matches_played else 0.0
        sport_stats.append(entry)

    sport_stats.sort(key=lambda item: (-item["matches"], item["sport"], item["format"]))

    history_rows = db.query(MatchHistory).filter(MatchHistory.player_id == user_id).all()
    points_contributed = sum(1 for item in history_rows if item.event_type == "point")
    violations_caused = sum(1 for item in history_rows if item.event_type == "violation")

    violation_counts: dict[str, int] = {}
    for item in history_rows:
        if item.event_type == "violation" and item.meta:
            code = item.meta.get("violation_code", "unknown")
            violation_counts[code] = violation_counts.get(code, 0) + 1

    top_violations = sorted(violation_counts.items(), key=lambda row: row[1], reverse=True)[:3]

    recent_matches_source = completed_matches[:10]
    opponent_ids = {
        opponent_id
        for match in recent_matches_source
        for opponent_id in _opponent_ids(match, user_id)
    }
    opponent_map: dict[str, str] = {}
    if opponent_ids:
        opponent_profiles = db.query(Profile).filter(Profile.id.in_(opponent_ids)).all()
        opponent_map = {
            str(profile.id): f"@{profile.username}" if profile.username else "Unknown opponent"
            for profile in opponent_profiles
        }

    recent_match_ids = [match.id for match in recent_matches_source]
    recent_action_rows = []
    if recent_match_ids:
        recent_action_rows = db.query(MatchHistory).filter(
            MatchHistory.match_id.in_(recent_match_ids),
            MatchHistory.event_type.in_(["point", "violation", "serve_change"]),
        ).order_by(MatchHistory.created_at.desc()).limit(120).all()

    direct_point_breakdown: dict[str, int] = {}
    winning_shot_breakdown: dict[str, int] = {}
    committed_error_breakdown: dict[str, int] = {}
    serve_fault_breakdown: dict[str, int] = {}
    action_samples: list[str] = []

    for row in recent_action_rows:
        meta = row.meta or {}
        player_involved = row.player_id is not None and str(row.player_id) == user_id
        actor_involved = meta.get("actor_player_id") is not None and str(meta.get("actor_player_id")) == user_id
        fault_player_involved = meta.get("fault_player_id") is not None and str(meta.get("fault_player_id")) == user_id

        if row.event_type == "point":
            if player_involved:
                attribution = str(meta.get("attribution_type") or "other")
                direct_point_breakdown[attribution] = direct_point_breakdown.get(attribution, 0) + 1
                if attribution == "winning_shot":
                    cause = str(meta.get("cause") or "unknown")
                    winning_shot_breakdown[cause] = winning_shot_breakdown.get(cause, 0) + 1
            if actor_involved:
                reason = str(meta.get("reason_code") or "unknown_error")
                committed_error_breakdown[reason] = committed_error_breakdown.get(reason, 0) + 1

        if row.event_type == "violation" and player_involved:
            code = str(meta.get("violation_code") or "unknown")
            committed_error_breakdown[code] = committed_error_breakdown.get(code, 0) + 1

        if row.event_type == "serve_change" and fault_player_involved:
            event_code = str(meta.get("event_type") or "serve_change")
            serve_fault_breakdown[event_code] = serve_fault_breakdown.get(event_code, 0) + 1

        if len(action_samples) < 8 and (player_involved or actor_involved or fault_player_involved):
            action_samples.append(
                f"{row.created_at.astimezone(timezone.utc).isoformat() if row.created_at else 'Unknown time'}"
                f" | Set {row.set_number or '?'} | {row.description}"
            )

    recent_matches: list[dict] = []
    recent_form_tokens: list[str] = []
    for match in recent_matches_source:
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
    total_direct_points = sum(direct_point_breakdown.values())
    total_winning_shots = winning_shot_breakdown and sum(winning_shot_breakdown.values()) or 0
    total_committed_errors = sum(committed_error_breakdown.values())
    total_serve_faults = sum(serve_fault_breakdown.values())

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
        "recent_action_point_breakdown": direct_point_breakdown,
        "recent_action_winning_shots": winning_shot_breakdown,
        "recent_action_errors_committed": committed_error_breakdown,
        "recent_action_serve_faults": serve_fault_breakdown,
        "recent_action_samples": action_samples,
        "recent_action_totals": {
            "direct_points": total_direct_points,
            "winning_shots": total_winning_shots,
            "errors_committed": total_committed_errors,
            "serve_faults": total_serve_faults,
        },
        "point_to_violation_ratio": round(points_contributed / violations_caused, 2) if violations_caused else None,
        "top_violations": [{"code": code, "count": count} for code, count in top_violations],
        "sport_breakdown": sport_stats,
        "best_sport": best_sport,
        "needs_attention_sport": weakest_sport,
        "recent_matches": recent_matches,
    }


def _build_fallback_insight(username: str, stats: dict) -> str:
    recent_count = stats.get("recent_match_count") or 0
    recent_rate = stats.get("recent_win_rate") or 0.0
    best_sport = stats.get("best_sport")
    weakest_sport = stats.get("needs_attention_sport")
    violations = stats.get("violations_caused") or 0
    ratio = stats.get("point_to_violation_ratio")
    action_totals = stats.get("recent_action_totals") or {}
    winning_shots = action_totals.get("winning_shots", 0)
    committed_errors = action_totals.get("errors_committed", 0)
    serve_faults = action_totals.get("serve_faults", 0)

    intro = (
        f"You have completed {stats['total_matches']} matches so far with a {stats['win_rate']}% overall win rate."
        if stats["total_matches"] > 0
        else "You need at least one completed match before we can coach from real performance data."
    )
    form_line = (
        f"Your recent form is {stats['recent_form']} with a {recent_rate}% win rate over your last {recent_count} completed matches."
        if recent_count > 0
        else "There are no recent completed matches yet, so the coaching signal is still limited."
    )
    strength_line = (
        f"Your clearest strength right now is {best_sport['sport']} {best_sport['format']}, where you are winning {best_sport['win_rate']}% of {best_sport['matches']} matches."
        if best_sport
        else "Your strongest signal so far is simply match participation, which gives us enough data to start building targeted recommendations."
    )
    risk_line = (
        f"The biggest performance risk is {weakest_sport['sport']} {weakest_sport['format']}, where your win rate drops to {weakest_sport['win_rate']}% across {weakest_sport['matches']} matches."
        if weakest_sport
        else "The biggest performance risk right now is inconsistency, so the next step is repeating the same high-percentage patterns under pressure."
    )
    training_line = (
        f"For the next 2 weeks, focus training on the patterns that carry over into {weakest_sport['sport']} {weakest_sport['format']}: controlled rally tolerance, serve quality, and finishing only when the opening is clear."
        if weakest_sport
        else "For the next 2 weeks, focus on one repeatable training block: serve consistency, one reliable attacking pattern, and one defensive reset pattern."
    )
    if committed_errors > winning_shots and committed_errors > 0:
        habit_line = (
            f"On match day, simplify your decision-making under pressure, because your recent action log shows {committed_errors} player-linked errors or violations against {winning_shots} recorded winning-shot finishes."
        )
    elif serve_faults > 0:
        habit_line = (
            f"On match day, protect momentum in service games, because {serve_faults} recent serve-fault or side-out records suggest avoidable pressure swings."
        )
    elif violations > 0:
        habit_line = (
            f"On match day, reduce free points by tightening your discipline, because {violations} recorded violations"
            + (f" and a {ratio} points-per-violation ratio" if ratio is not None else "")
            + " suggest avoidable errors are still costing momentum."
        )
    else:
        habit_line = "On match day, stay patient in neutral rallies and avoid forcing low-percentage winners before you have created a real advantage."

    return " ".join([intro, form_line, strength_line, risk_line, training_line, habit_line])


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

    point_breakdown_lines = "\n".join(
        f"- {kind.replace('_', ' ')}: {count}"
        for kind, count in sorted(
            stats.get("recent_action_point_breakdown", {}).items(),
            key=lambda item: (-item[1], item[0]),
        )
    ) or "- No direct point attribution records yet"

    winning_shot_lines = "\n".join(
        f"- {cause}: {count}"
        for cause, count in sorted(
            stats.get("recent_action_winning_shots", {}).items(),
            key=lambda item: (-item[1], item[0]),
        )
    ) or "- No winning-shot causes recorded yet"

    committed_error_lines = "\n".join(
        f"- {code.replace('_', ' ')}: {count}"
        for code, count in sorted(
            stats.get("recent_action_errors_committed", {}).items(),
            key=lambda item: (-item[1], item[0]),
        )
    ) or "- No player-linked error records yet"

    serve_fault_lines = "\n".join(
        f"- {code.replace('_', ' ')}: {count}"
        for code, count in sorted(
            stats.get("recent_action_serve_faults", {}).items(),
            key=lambda item: (-item[1], item[0]),
        )
    ) or "- No player-linked serve-fault records yet"

    recent_action_lines = "\n".join(
        f"- {line}"
        for line in stats.get("recent_action_samples", [])
    ) or "- No recent player-linked action samples"

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

Player reference: @{username} (for context only; do not mention the player's username in the final response)
Overall record: {stats['total_matches']} matches | {stats['total_wins']} wins | {stats['total_losses']} losses | {stats['win_rate']}% win rate
Recent form: {stats['recent_form']} ({stats['recent_win_rate']}% win rate over last {stats['recent_match_count']} completed matches)
Points contributed: {stats['points_contributed']}
Violations caused: {stats['violations_caused']} ({ratio_text})
Strongest current sport: {best_line}
Needs attention most: {weakest_line}
Recent player-linked action totals: {stats.get('recent_action_totals', {}).get('direct_points', 0)} directly attributed points, {stats.get('recent_action_totals', {}).get('winning_shots', 0)} winning-shot records, {stats.get('recent_action_totals', {}).get('errors_committed', 0)} player-linked errors/violations, {stats.get('recent_action_totals', {}).get('serve_faults', 0)} serve-fault records

Sport breakdown:
{sport_lines}

Top violations:
{violation_lines}

Recent action point breakdown:
{point_breakdown_lines}

Recent winning shot causes:
{winning_shot_lines}

Recent committed errors / violations:
{committed_error_lines}

Recent serve-fault / side-out records:
{serve_fault_lines}

Recent completed matches:
{recent_match_lines}

Recent player-linked action samples:
{recent_action_lines}

Write 4 to 6 sentences in plain text, no bullets and no markdown.
Requirements:
- Write in direct second-person voice using "you" and "your".
- Do not mention the player's username in the final response.
- Do not wrap the response in quotation marks.
- Make the opening sentence sound like a coach's observation, such as "You demonstrate a clear strength..."
- Mention one clear strength backed by the data above.
- Mention one performance risk or weakness backed by the data above.
- Use the recent action data when it gives a clearer signal than win/loss alone.
- Recommend one concrete training focus for the next 2 weeks.
- Recommend one match-day habit or decision-making adjustment.
- Keep the tone supportive, practical, and specific. Avoid generic filler and avoid sounding like an AI report template."""


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

    username = profile.username or "player"
    stats = _collect_player_stats(user_id, db)
    if stats["total_matches"] == 0:
        return {"error": "Play at least one match before generating insights."}

    prompt = _build_prompt(username, stats)

    try:
        insight_text, provider, model = _generate_insight_text(prompt)
    except Exception as exc:
        logger.warning("Insight generation failed for user %s, using fallback: %s", user_id, exc)
        insight_text = _build_fallback_insight(username, stats)
        provider = "local-fallback"
        model = "rules-based"
    insight_text = _normalize_insight_text(insight_text, username)
    if not insight_text.strip():
        insight_text = _normalize_insight_text(_build_fallback_insight(username, stats), username)
        provider = "local-fallback"
        model = "rules-based"

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
