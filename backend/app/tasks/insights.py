import logging

from app.celery_app import app

logger = logging.getLogger(__name__)


@app.task(
    bind=True,
    name="app.tasks.insights.auto_generate_insight",
    max_retries=3,
    default_retry_delay=60,   # 60 s between retries
    queue="insights",
)
def auto_generate_insight(self, user_id: str):
    """
    Check trigger rules for the given player and, if they qualify,
    call the LLM to generate a coaching insight then push a notification.
    Retried up to 3× on transient failures (network, LLM timeout, etc.).
    """
    from app.database import SessionLocal
    from app.services.auto_insight import _should_generate
    from app.services.insights import generate_insight
    from app.services.notifications import send_notification

    db = SessionLocal()
    try:
        if not _should_generate(user_id, db):
            logger.info("[insights] Skipping auto-insight for %s — trigger rules not met", user_id)
            return {"skipped": True}

        logger.info("[insights] Generating auto-insight for %s", user_id)
        result = generate_insight(user_id, db)

        if "error" in result:
            logger.info("[insights] Skipped — %s", result["error"])
            return {"skipped": True, "reason": result["error"]}

        send_notification(
            user_id=user_id,
            title="New AI Coaching Insight",
            body="Your performance coach has a new recommendation based on your latest matches.",
            notif_type="ai_insight",
            reference_id=result.get("id"),
        )
        logger.info("[insights] Insight delivered for %s (id=%s)", user_id, result.get("id"))
        return {"generated": True, "id": result.get("id")}

    except Exception as exc:
        db.rollback()
        logger.warning("[insights] Task failed for %s: %s — retrying", user_id, exc)
        raise self.retry(exc=exc)
    finally:
        db.close()
