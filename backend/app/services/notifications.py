import json
import logging
import os
from sqlalchemy.orm import Session
from app.database import SessionLocal
from app.models.models import Notification, Profile
from typing import Any

logger = logging.getLogger(__name__)

# ── Redis pub/sub for real-time notification SSE ──────────────────────────────
_NOTIF_PREFIX = "isms:notif:"

try:
    import redis as _redis_sync
    from app.config import settings as _settings
    _redis_notif = _redis_sync.from_url(_settings.redis_url, decode_responses=True)
    _redis_notif.ping()
    logger.info("[notifications] Redis pub/sub enabled — real-time SSE notifications active.")
except Exception as _e:
    _redis_notif = None
    logger.warning(f"[notifications] Redis unavailable — SSE notifications disabled. Reason: {_e}")


def _publish_notif(user_id: str) -> None:
    """Signal the user's SSE stream that a new notification is waiting."""
    if _redis_notif is None:
        return
    try:
        _redis_notif.publish(f"{_NOTIF_PREFIX}{user_id}", "new")
    except Exception as e:
        logger.warning(f"[notifications] Redis publish failed for {user_id}: {e}")


def publish_feed_unread(user_id: str, count: int) -> None:
    """Push the current feed unread count to the user's SSE stream."""
    if _redis_notif is None:
        return
    try:
        _redis_notif.publish(
            f"{_NOTIF_PREFIX}{user_id}",
            json.dumps({"type": "feed_unread_count", "count": count}),
        )
    except Exception as e:
        logger.warning(f"[notifications] feed_unread_count publish failed for {user_id}: {e}")


_GLOBAL_CHANNEL = "isms:feed:global"


def broadcast_global_announcement(
    announcement_type: str,
    id: str,
    name: str,
    sport: str,
    description: str | None,
    post_id: str,
    creator_name: str,
) -> None:
    """Broadcast a new-club or new-tournament announcement to every connected SSE client."""
    if _redis_notif is None:
        return
    try:
        _redis_notif.publish(
            _GLOBAL_CHANNEL,
            json.dumps({
                "type":              "global_announcement",
                "announcement_type": announcement_type,
                "id":                id,
                "name":              name,
                "sport":             sport,
                "description":       description or "",
                "post_id":           post_id,
                "creator_name":      creator_name,
            }),
        )
    except Exception as e:
        logger.warning(f"[notifications] global_announcement broadcast failed: {e}")


# ── Firebase Admin SDK (optional — only initialised if credentials file exists) ─
_fcm_ready = False
try:
    import firebase_admin  # type: ignore[import-untyped]
    from firebase_admin import credentials as fb_creds, messaging as fb_messaging  # type: ignore[import-untyped]
    from app.config import settings

    _cred_path = settings.firebase_credentials_path
    if os.path.isfile(_cred_path):
        _app = firebase_admin.initialize_app(fb_creds.Certificate(_cred_path))
        _fcm_ready = True
        logger.info("Firebase Admin SDK initialised — FCM push enabled.")
    else:
        logger.warning(
            f"firebase-credentials.json not found at '{_cred_path}'. "
            "FCM push notifications are disabled. "
            "Place the file there and restart to enable push."
        )
except ImportError:
    logger.warning("firebase-admin not installed. FCM push notifications disabled.")
except Exception as _e:
    logger.warning(f"Firebase Admin init failed: {_e}. FCM push disabled.")


def _send_fcm(
    token: str,
    title: str,
    body: str,
    notif_type: str,
    reference_id: str | None,
    extra_data: dict[str, Any] | None = None,
):
    """Fire-and-forget FCM push. Errors are logged, never raised."""
    if not _fcm_ready:
        return
    try:
        from firebase_admin import messaging as fb_messaging  # type: ignore[import-untyped]  # noqa: F811
        payload_data = {
            "type":         notif_type,
            "reference_id": reference_id or "",
        }
        if extra_data:
            payload_data.update({
                str(key): "" if value is None else str(value)
                for key, value in extra_data.items()
            })
        msg = fb_messaging.Message(
            notification=fb_messaging.Notification(title=title, body=body),
            data=payload_data,
            token=token,
        )
        fb_messaging.send(msg)
    except Exception as e:
        logger.warning(f"FCM push failed (token={token[:10]}…): {e}")


def send_notification(
    user_id: str,
    title: str,
    body: str,
    notif_type: str = "general",
    reference_id: str | None = None,
    extra_data: dict[str, Any] | None = None,
):
    db: Session = SessionLocal()
    committed = False
    try:
        notif_data: dict[str, Any] = {}
        if reference_id:
            notif_data["reference_id"] = reference_id
        if extra_data:
            notif_data.update(extra_data)
        notif = Notification(
            user_id=user_id,
            title=title,
            body=body,
            type=notif_type,
            is_read=False,
            data=notif_data or None,
        )
        db.add(notif)
        db.commit()
        committed = True

        # Notification is committed — signal real-time SSE stream first so the
        # badge lights up immediately, then attempt FCM as a bonus delivery.
        _publish_notif(user_id)

        profile = db.query(Profile).filter(Profile.id == user_id).first()
        fcm_token = str(profile.fcm_token) if profile and profile.fcm_token is not None else None
        if fcm_token:
            _send_fcm(fcm_token, title, body, notif_type, reference_id, extra_data)

        # SMS — only for time-critical event types, only if player opted in
        try:
            from app.tasks.sms import maybe_send_sms
            maybe_send_sms(user_id, body, notif_type)
        except Exception as sms_exc:
            logger.warning(f"[sms] Failed to enqueue SMS for {user_id}: {sms_exc}")

    except Exception as e:
        if not committed:
            logger.error(f"Failed to persist notification to {user_id}: {e}")
            db.rollback()
        else:
            logger.warning(f"Post-commit step failed for notification to {user_id}: {e}")
    finally:
        db.close()


def send_bulk_notifications(
    user_ids: list,
    title: str,
    body: str,
    notif_type: str = "general",
    reference_id: str | None = None,
    extra_data: dict[str, Any] | None = None,
):
    for user_id in user_ids:
        send_notification(user_id, title, body, notif_type, reference_id, extra_data)
