import json
import logging

from app.config import settings

logger = logging.getLogger(__name__)

try:
    import redis as _redis_sync

    # connection_pool=True + retry_on_timeout means the client auto-reconnects
    # after a Redis restart instead of holding a dead socket forever.
    _redis_pub = _redis_sync.from_url(
        settings.redis_url,
        decode_responses=True,
        retry_on_timeout=True,
        socket_keepalive=True,
        health_check_interval=30,
    )
    logger.info("[broadcast] Redis connection established.")
except Exception as e:
    _redis_pub = None
    logger.warning(f"[broadcast] Redis unavailable - broadcast disabled. Reason: {e}")


def _publish(channel: str, payload: dict):
    if _redis_pub is None:
        logger.debug(f"[broadcast] Skipping publish to {channel} - Redis not connected.")
        return
    try:
        _redis_pub.publish(channel, json.dumps(payload))
        logger.debug(
            f"[broadcast] Published '{payload.get('type') or payload.get('event')}' to {channel}."
        )
    except Exception as e:
        logger.error(
            f"[broadcast] Failed to publish '{payload.get('type') or payload.get('event')}' to {channel} - {e}"
        )
        # Attempt a single reconnect on the next call; don't raise to caller
        try:
            _redis_pub.ping()
        except Exception:
            pass


def broadcast_match(match_id: str, payload: dict):
    """Publish a match event to Redis; WebSocket subscribers pick it up instantly."""
    _publish(f"match:{match_id}", payload)


def broadcast_tournament(tournament_id: str, payload: dict):
    """Publish a tournament event to Redis; SSE subscribers pick it up instantly."""
    _publish(f"isms:tournament:{tournament_id}", payload)
