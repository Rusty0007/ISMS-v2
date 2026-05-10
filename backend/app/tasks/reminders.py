import logging

from app.celery_app import app

logger = logging.getLogger(__name__)


@app.task(name="app.tasks.reminders.dispatch_tournament_reminders")
def dispatch_tournament_reminders():
    """
    Send 10-minute and 5-minute pre-match notifications for all scheduled
    tournament matches across every ongoing tournament.

    Previously this only ran when someone opened a tournament detail page.
    Running every 60 s via Celery beat means reminders fire reliably even
    when no one is actively viewing the UI.
    """
    from app.database import SessionLocal
    from app.services.tournament_runtime import dispatch_due_tournament_match_reminders

    db = SessionLocal()
    try:
        sent = dispatch_due_tournament_match_reminders(db)
        if sent:
            logger.info("[reminders] Sent %d tournament match reminder(s)", sent)
        return {"sent": sent}
    except Exception as exc:
        logger.error("[reminders] Tournament reminder dispatch failed: %s", exc)
        raise
    finally:
        db.close()


@app.task(name="app.tasks.reminders.expire_open_play_assignments")
def expire_open_play_assignments():
    """
    Expire stale open-play ACK assignments (called status, past deadline).

    Previously this only ran when a player or organiser interacted with
    a session endpoint.  Running every 2 min via Celery beat means timeouts
    are enforced even on idle sessions.
    """
    from app.database import SessionLocal
    from app.models.models import OpenPlaySession

    db = SessionLocal()
    expired_total = 0
    try:
        # Only process sessions that are currently ongoing
        active_sessions = db.query(OpenPlaySession).filter(
            OpenPlaySession.status == "ongoing",
        ).all()

        for session in active_sessions:
            try:
                # Import the internal helper from the open_play route
                from app.routes.open_play import _expire_stale_assignments
                changed = _expire_stale_assignments(session, db)
                if changed:
                    db.commit()
                    expired_total += 1
            except Exception as exc:
                db.rollback()
                logger.warning(
                    "[reminders] Failed to expire assignments for session %s: %s",
                    session.id, exc,
                )

        if expired_total:
            logger.info("[reminders] Expired stale assignments in %d session(s)", expired_total)
        return {"sessions_processed": len(active_sessions), "sessions_changed": expired_total}

    except Exception as exc:
        logger.error("[reminders] Open-play expiry sweep failed: %s", exc)
        raise
    finally:
        db.close()


@app.task(name="app.tasks.reminders.transition_warmup_assignments")
def transition_warmup_assignments():
    """
    Auto-transition warming_up assignments to in_game when the warm-up
    timer has elapsed.  Runs every 30 seconds via Celery beat.
    """
    import datetime as _dt
    from app.database import SessionLocal
    from app.models.models import OpenPlayAssignment, OpenPlaySession
    from app.routes.open_play import _publish_open_play_event

    db = SessionLocal()
    transitioned = 0
    try:
        now = _dt.datetime.now(_dt.timezone.utc)

        warming = (
            db.query(OpenPlayAssignment)
            .join(OpenPlaySession, OpenPlaySession.id == OpenPlayAssignment.session_id)
            .filter(
                OpenPlayAssignment.status == "warming_up",
                OpenPlayAssignment.warmup_started_at.isnot(None),
                OpenPlaySession.status == "ongoing",
            )
            .all()
        )

        for assignment in warming:
            session = db.query(OpenPlaySession).filter(OpenPlaySession.id == assignment.session_id).first()
            if not session:
                continue
            warm_up_secs = int(getattr(session, "warm_up_duration_seconds", 0) or 0)
            if warm_up_secs <= 0:
                continue
            warmup_started = assignment.warmup_started_at
            if warmup_started and warmup_started.replace(tzinfo=_dt.timezone.utc) + _dt.timedelta(seconds=warm_up_secs) <= now:
                setattr(assignment, "status", "in_game")
                setattr(assignment, "started_at", now)
                db.commit()
                _publish_open_play_event(str(assignment.session_id), reason="warmup_completed")
                transitioned += 1
                logger.info("[reminders] Warm-up completed for assignment %s", assignment.id)

        return {"transitioned": transitioned}
    except Exception as exc:
        db.rollback()
        logger.error("[reminders] Warm-up transition sweep failed: %s", exc)
        raise
    finally:
        db.close()


@app.task(name="app.tasks.reminders.send_open_play_session_reminders")
def send_open_play_session_reminders():
    """
    Send a 30-minute heads-up SMS + push notification to:
    - All checked-in confirmed participants
    - All assigned court managers

    Runs every 5 minutes via Celery beat. Uses checkin_reminder_sent flag
    so each player only receives the reminder once per session.
    """
    import datetime as _dt
    from app.database import SessionLocal
    from app.models.models import (
        OpenPlaySession, OpenPlayParticipant, OpenPlayCourtManager,
    )
    from app.services.notifications import send_notification

    db = SessionLocal()
    sent_total = 0
    try:
        now = _dt.datetime.now(_dt.timezone.utc)
        window_start = now + _dt.timedelta(minutes=25)
        window_end   = now + _dt.timedelta(minutes=35)

        sessions = db.query(OpenPlaySession).filter(
            OpenPlaySession.status == "upcoming",
            OpenPlaySession.session_date >= window_start,
            OpenPlaySession.session_date <= window_end,
        ).all()

        for session in sessions:
            start_str = session.session_date.strftime("%I:%M %p") if session.session_date else "soon"
            body = (
                f"Heads up! '{session.title}' open-play session starts at {start_str}. "
                "Please make your way to the venue now."
            )

            # Checked-in participants who haven't received the reminder yet
            participants = db.query(OpenPlayParticipant).filter(
                OpenPlayParticipant.session_id == session.id,
                OpenPlayParticipant.status == "confirmed",
                OpenPlayParticipant.checked_in == True,     # noqa: E712
                OpenPlayParticipant.checkin_reminder_sent == False,  # noqa: E712
            ).all()

            notified_ids: set[str] = set()
            for p in participants:
                send_notification(
                    user_id=str(p.user_id),
                    title="Open Play Starting Soon",
                    body=body,
                    notif_type="open_play_session_reminder",
                    reference_id=str(session.id),
                )
                p.checkin_reminder_sent = True
                notified_ids.add(str(p.user_id))
                sent_total += 1

            # Court managers for this session
            session_court_ids = [
                str(sc.id)
                for sc in session.session_courts
            ] if hasattr(session, "session_courts") else []

            if session_court_ids:
                managers = db.query(OpenPlayCourtManager).filter(
                    OpenPlayCourtManager.session_court_id.in_(session_court_ids),
                ).all()
                for m in managers:
                    if str(m.user_id) not in notified_ids:
                        send_notification(
                            user_id=str(m.user_id),
                            title="Open Play Starting Soon",
                            body=(
                                f"Court manager reminder: '{session.title}' starts at {start_str}. "
                                "Please be at your assigned court."
                            ),
                            notif_type="open_play_session_reminder",
                            reference_id=str(session.id),
                        )
                        notified_ids.add(str(m.user_id))
                        sent_total += 1

            db.commit()
            logger.info(
                "[reminders] Session reminder sent for '%s' to %d people",
                session.title, len(notified_ids),
            )

        return {"sent": sent_total, "sessions": len(sessions)}

    except Exception as exc:
        db.rollback()
        logger.error("[reminders] Open-play session reminder failed: %s", exc)
        raise
    finally:
        db.close()
