from celery import Celery
from celery.schedules import crontab  # noqa: F401 — available for beat overrides

from app.config import settings

app = Celery(
    "isms",
    broker=settings.redis_url,
    backend=settings.redis_url,
    include=[
        "app.tasks.insights",
        "app.tasks.feed",
        "app.tasks.reminders",
        "app.tasks.sms",
    ],
)

app.conf.update(
    # Serialisation
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    # Time
    timezone="UTC",
    enable_utc=True,
    # Reliability
    task_track_started=True,
    task_acks_late=True,           # ack only after the task finishes (safe retries)
    worker_prefetch_multiplier=1,  # one task at a time per worker slot (prevents overload)
    # Results TTL — keep results for 1 hour then discard (we don't query results)
    result_expires=3600,
    # Queues
    task_default_queue="default",
    task_routes={
        "app.tasks.insights.*": {"queue": "insights"},   # LLM tasks isolated
        "app.tasks.feed.*":     {"queue": "feed"},
        "app.tasks.reminders.*": {"queue": "default"},
        "app.tasks.sms.*":      {"queue": "sms"},
    },
)

# ── Beat (periodic task) schedule ─────────────────────────────────────────────
app.conf.beat_schedule = {
    # Tournament match reminders: fire every 60 seconds.
    # Previously only triggered when someone opened the tournament detail page.
    "tournament-match-reminders": {
        "task": "app.tasks.reminders.dispatch_tournament_reminders",
        "schedule": 60.0,
    },
    # Expire stale open-play ACK assignments every 2 minutes.
    "expire-open-play-assignments": {
        "task": "app.tasks.reminders.expire_open_play_assignments",
        "schedule": 120.0,
    },
    # Auto-transition warming_up → in_game when timer elapses (every 30 s).
    "transition-warmup-assignments": {
        "task": "app.tasks.reminders.transition_warmup_assignments",
        "schedule": 30.0,
    },
    # 30-minute heads-up reminder for checked-in open-play participants (every 5 min).
    "open-play-session-reminders": {
        "task": "app.tasks.reminders.send_open_play_session_reminders",
        "schedule": 300.0,
    },
}
