import logging
import uuid

from app.celery_app import app

logger = logging.getLogger(__name__)


@app.task(
    bind=True,
    name="app.tasks.feed.dispatch_feed_notifications",
    max_retries=2,
    default_retry_delay=30,
    queue="feed",
)
def dispatch_feed_notifications(self, post_id: str):
    """
    Fan-out feed notifications for a newly created post.
    Runs after the post transaction has committed, so the post is guaranteed
    to exist in the DB when the worker picks this up.
    """
    from app.database import SessionLocal
    from app.models.models import FeedNotification, FeedPost
    from app.services.notifications import publish_feed_unread

    db = SessionLocal()
    try:
        post = db.query(FeedPost).filter(FeedPost.id == uuid.UUID(post_id)).first()
        if not post:
            logger.warning("[feed] Post %s not found — skipping notification dispatch", post_id)
            return

        # Import targeting logic from feed route
        from app.routes.feed import _target_users_for_post

        author_id = str(post.author_id)
        target_ids = _target_users_for_post(post, author_id, db)

        if not target_ids:
            return

        post_uuid = post.id
        for uid in target_ids:
            notif = FeedNotification(
                id=uuid.uuid4(),
                user_id=uuid.UUID(uid),
                post_id=post_uuid,
                is_read=False,
            )
            db.merge(notif)
        db.commit()

        # Push real-time unread count to each user's SSE stream
        for uid in target_ids:
            count = db.query(FeedNotification).filter(
                FeedNotification.user_id == uuid.UUID(uid),
                FeedNotification.is_read == False,  # noqa: E712
            ).count()
            publish_feed_unread(uid, count)

        logger.info("[feed] Dispatched feed notifications for post %s to %d users", post_id, len(target_ids))

    except Exception as exc:
        db.rollback()
        logger.warning("[feed] Dispatch failed for post %s: %s — retrying", post_id, exc)
        raise self.retry(exc=exc)
    finally:
        db.close()
