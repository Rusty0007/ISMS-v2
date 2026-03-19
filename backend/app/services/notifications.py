import logging
import os
from sqlalchemy.orm import Session
from app.database import SessionLocal
from app.models.models import Notification, Profile

logger = logging.getLogger(__name__)

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


def _send_fcm(token: str, title: str, body: str, notif_type: str, reference_id: str | None):
    """Fire-and-forget FCM push. Errors are logged, never raised."""
    if not _fcm_ready:
        return
    try:
        from firebase_admin import messaging as fb_messaging  # type: ignore[import-untyped]  # noqa: F811
        msg = fb_messaging.Message(
            notification=fb_messaging.Notification(title=title, body=body),
            data={
                "type":         notif_type,
                "reference_id": reference_id or "",
            },
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
):
    db: Session = SessionLocal()
    try:
        notif = Notification(
            user_id=user_id,
            title=title,
            body=body,
            type=notif_type,
            is_read=False,
            data={"reference_id": reference_id} if reference_id else None,
        )
        db.add(notif)
        db.commit()

        # Push via FCM if user has a registered device token
        profile = db.query(Profile).filter(Profile.id == user_id).first()
        fcm_token = str(profile.fcm_token) if profile and profile.fcm_token is not None else None
        if fcm_token:
            _send_fcm(fcm_token, title, body, notif_type, reference_id)

    except Exception as e:
        logger.error(f"Failed to send notification to {user_id}: {e}")
        db.rollback()
    finally:
        db.close()


def send_bulk_notifications(
    user_ids: list,
    title: str,
    body: str,
    notif_type: str = "general",
    reference_id: str | None = None,
):
    for user_id in user_ids:
        send_notification(user_id, title, body, notif_type, reference_id)
