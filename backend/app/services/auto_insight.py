"""
Auto-insight trigger: fires after a match completes and schedules LLM coaching
generation in a background daemon thread if the player meets the trigger rules.
No Celery required — uses a plain thread with its own DB session.
"""
import logging
import threading
import uuid
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

# ── Trigger thresholds ────────────────────────────────────────────────────────
_MIN_MATCHES_TOTAL    = 3    # need at least this many completed matches overall
_COOLDOWN_HOURS       = 72   # don't re-generate within 3 days
_RECENT_WINDOW_DAYS   = 7    # "recent activity" window
_RECENT_MATCHES_FLOOR = 3    # ≥ this many matches in window → trigger
_WIN_RATE_SWING       = 10.0 # % swing between last-5 and prior-5 → trigger


def _should_generate(user_id: str, db) -> bool:
    """Return True if this player qualifies for an auto-generated insight."""
    from sqlalchemy import or_
    from app.models.models import Match, PlayerInsight

    uid = uuid.UUID(user_id) if isinstance(user_id, str) else user_id

    # 1. Cooldown check — skip if insight generated within the last 72 h
    cutoff = datetime.now(timezone.utc) - timedelta(hours=_COOLDOWN_HOURS)
    recent_insight = (
        db.query(PlayerInsight)
        .filter(PlayerInsight.user_id == uid, PlayerInsight.generated_at >= cutoff)
        .first()
    )
    if recent_insight:
        return False

    # 2. Collect completed matches for the player
    player_filter = or_(
        Match.player1_id == uid,
        Match.player2_id == uid,
        Match.team1_player1 == uid,
        Match.team1_player2 == uid,
        Match.team2_player1 == uid,
        Match.team2_player2 == uid,
    )
    matches = (
        db.query(Match)
        .filter(player_filter, Match.status == "completed")
        .order_by(Match.completed_at.desc())
        .all()
    )

    total = len(matches)
    if total < _MIN_MATCHES_TOTAL:
        return False

    # 3. Recent activity rule — ≥ 3 matches in the last 7 days
    window_start = datetime.now(timezone.utc) - timedelta(days=_RECENT_WINDOW_DAYS)
    recent_count = sum(
        1 for m in matches
        if m.completed_at and m.completed_at.replace(tzinfo=timezone.utc) >= window_start
    )
    if recent_count >= _RECENT_MATCHES_FLOOR:
        return True

    # 4. Win-rate swing rule — compare last 5 vs prior 5
    if total >= 10:
        def _won(m) -> bool:
            if not m.winner_id:
                return False
            wid = str(m.winner_id)
            uid_s = str(uid)
            for fld in ("player1_id", "player2_id",
                        "team1_player1", "team1_player2",
                        "team2_player1", "team2_player2"):
                if getattr(m, fld) and str(getattr(m, fld)) == wid and str(getattr(m, fld)) == uid_s:
                    return True
            # simpler: winner team membership
            team1 = [str(x) for x in [m.team1_player1, m.team1_player2, m.player1_id] if x]
            team2 = [str(x) for x in [m.team2_player1, m.team2_player2, m.player2_id] if x]
            if wid in team1:
                return uid_s in team1
            if wid in team2:
                return uid_s in team2
            return wid == uid_s

        last5  = matches[:5]
        prior5 = matches[5:10]
        rate_last  = sum(1 for m in last5  if _won(m)) / 5 * 100
        rate_prior = sum(1 for m in prior5 if _won(m)) / 5 * 100
        if abs(rate_last - rate_prior) >= _WIN_RATE_SWING:
            return True

    return False


def schedule_auto_insight(*user_ids: str) -> None:
    """
    Enqueue a Celery auto-insight task for each player ID.
    Falls back to a daemon thread if Celery / Redis is unavailable
    (e.g. local dev without a running worker).
    """
    from app.database import SessionLocal
    from app.services.insights import generate_insight
    from app.services.notifications import send_notification

    unique = [uid for uid in dict.fromkeys(user_ids) if uid]
    for uid in unique:
        try:
            from app.tasks.insights import auto_generate_insight
            auto_generate_insight.delay(uid)
        except Exception as exc:
            # Celery unavailable — fall back to daemon thread so local dev still works
            logger.warning("[auto_insight] Celery unavailable (%s), falling back to thread", exc)

            def _thread_run(user_id: str) -> None:
                db = SessionLocal()
                try:
                    if not _should_generate(user_id, db):
                        return
                    result = generate_insight(user_id, db)
                    if "error" not in result:
                        send_notification(
                            user_id=user_id,
                            title="New AI Coaching Insight",
                            body="Your performance coach has a new recommendation for you.",
                            notif_type="ai_insight",
                            reference_id=result.get("id"),
                        )
                except Exception as inner:
                    logger.warning("[auto_insight] Thread fallback failed for %s: %s", user_id, inner)
                finally:
                    db.close()

            t = threading.Thread(target=_thread_run, args=(uid,), daemon=True)
            t.start()
