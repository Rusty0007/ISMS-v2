import json
import logging
from app.config import settings

logger = logging.getLogger(__name__)

try:
    import redis as _redis_sync
    _redis_pub = _redis_sync.from_url(settings.redis_url, decode_responses=True)
    logger.info("[broadcast] Redis connection established.")
except Exception as e:
    _redis_pub = None
    logger.warning(f"[broadcast] Redis unavailable — broadcast disabled. Reason: {e}")


def broadcast_match(match_id: str, payload: dict):
    """Publish a match event to Redis; WebSocket subscribers pick it up instantly."""
    if _redis_pub is None:
        logger.debug(f"[broadcast] Skipping broadcast for match {match_id} — Redis not connected.")
        return
    try:
        _redis_pub.publish(f"match:{match_id}", json.dumps(payload))
        logger.debug(f"[broadcast] Published '{payload.get('type')}' to match:{match_id}.")
    except Exception as e:
        logger.error(f"[broadcast] Failed to publish '{payload.get('type')}' to match:{match_id} — {e}")
