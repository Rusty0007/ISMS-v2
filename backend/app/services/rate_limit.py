"""
Lightweight Redis-based rate limiter for the scoring endpoints.

Uses a sliding window counter per key. If Redis is unavailable the check
is bypassed (fail-open), so a Redis outage never blocks legitimate scoring.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

try:
    import redis as _redis_sync
    from app.config import settings as _settings

    _rl_redis = _redis_sync.from_url(
        _settings.redis_url,
        decode_responses=True,
        socket_connect_timeout=2,
        socket_timeout=2,
    )
    _rl_redis.ping()
    logger.info("[rate_limit] Redis rate-limiter ready.")
except Exception as _e:
    _rl_redis = None
    logger.warning(f"[rate_limit] Redis unavailable — rate limiting disabled. Reason: {_e}")


def check_rate_limit(key: str, max_calls: int, window_seconds: int) -> bool:
    """
    Increment the counter for `key` inside a sliding window.

    Returns True if the request is allowed, False if the limit is exceeded.
    On any Redis error returns True (fail-open).
    """
    if _rl_redis is None:
        return True
    redis_key = f"isms:rl:{key}"
    try:
        pipe = _rl_redis.pipeline()
        pipe.incr(redis_key)
        pipe.expire(redis_key, window_seconds)
        results = pipe.execute()
        current_count: int = int(results[0])
        return current_count <= max_calls
    except Exception as exc:
        logger.warning(f"[rate_limit] Redis error for key '{key}': {exc} — allowing request")
        return True


def scoring_rate_limit_key(match_id: str, user_id: str) -> str:
    """Sliding-window key: per referee per match."""
    return f"score:{match_id}:{user_id}"
