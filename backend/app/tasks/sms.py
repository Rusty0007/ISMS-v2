import logging

from app.celery_app import app

logger = logging.getLogger(__name__)

# Notification types that warrant an SMS (time-critical events where a
# missed push notification could cause a player to forfeit their slot).
SMS_NOTIF_TYPES = {
    "tournament_match_called",      # organiser calls the match — show up now
    "tournament_match_reminder",    # 5 / 10 min pre-match reminder
    "tournament_match_ready",       # all players checked into lobby
    "open_play_call",               # court called in open play — ACK window open
    "party_match_found",            # match found for a doubles party
    "open_play_session_reminder",   # 30-min heads-up before open-play starts
    "open_play_next",               # player is next in queue — get ready
}


@app.task(
    bind=True,
    name="app.tasks.sms.send_sms_notification",
    max_retries=3,
    default_retry_delay=30,   # 30 s between retries
    queue="sms",
)
def send_sms_notification(self, user_id: str, message: str, notif_type: str):
    """
    Look up the player's phone number, check SMS opt-in, and send via UniSMS.
    Retried up to 3× on transient network failures.
    Silently skips if the player has no phone number or SMS disabled.
    """
    from app.database import SessionLocal
    from app.models.models import Profile
    from app.services.sms import send_sms

    db = SessionLocal()
    try:
        profile = db.query(Profile).filter(Profile.id == user_id).first()
        if not profile:
            return {"skipped": True, "reason": "profile_not_found"}

        phone = getattr(profile, "phone_number", None)
        sms_enabled = bool(getattr(profile, "sms_notifications_enabled", False))

        if not phone:
            return {"skipped": True, "reason": "no_phone_number"}
        if not sms_enabled:
            return {"skipped": True, "reason": "sms_disabled"}

        if send_sms(phone, message):
            return {"sent": True, "notif_type": notif_type}

        raise RuntimeError("UniSMS send failed")

    except self.MaxRetriesExceededError:
        logger.error("[sms] Max retries exceeded for user %s (%s)", user_id, notif_type)
        return {"failed": True}
    except Exception as exc:
        db.rollback()
        try:
            raise self.retry(exc=exc)
        except self.MaxRetriesExceededError:
            logger.error("[sms] Max retries exceeded for user %s (%s)", user_id, notif_type)
            return {"failed": True}
    finally:
        db.close()


def maybe_send_sms(user_id: str, body: str, notif_type: str) -> None:
    """
    Enqueue an SMS task if this notification type warrants one.
    Call this from send_notification() — it's a no-op for non-SMS types.
    """
    if notif_type not in SMS_NOTIF_TYPES:
        return
    try:
        send_sms_notification.delay(user_id, body, notif_type)
        logger.info("[sms] Queued SMS notification for user %s (%s)", user_id, notif_type)
    except Exception as exc:
        # Celery/Redis unavailable — best-effort inline fallback
        logger.warning("[sms] Celery unavailable, attempting inline SMS: %s", exc)
        try:
            from app.database import SessionLocal
            from app.models.models import Profile
            from app.services.sms import send_sms

            db = SessionLocal()
            try:
                profile = db.query(Profile).filter(Profile.id == user_id).first()
                phone = getattr(profile, "phone_number", None) if profile else None
                enabled = bool(getattr(profile, "sms_notifications_enabled", False)) if profile else False
                if phone and enabled:
                    send_sms(phone, body)
            finally:
                db.close()
        except Exception as inner:
            logger.warning("[sms] Inline SMS fallback also failed: %s", inner)
