from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import Session

from app.models.models import Court, Match, MatchHistory, Profile, Tournament
from app.services.broadcast import broadcast_tournament
from app.services.notifications import send_bulk_notifications


def tournament_channel(tournament_id: str) -> str:
    return f"isms:tournament:{tournament_id}"


def publish_tournament_event(tournament_id: str, event: str, **payload) -> None:
    broadcast_tournament(
        tournament_id,
        {
            "event": event,
            "tournament_id": tournament_id,
            "sent_at": datetime.now(timezone.utc).isoformat(),
            **payload,
        },
    )


def _match_status_value(match: Match) -> str:
    return str(match.status.value if hasattr(match.status, "value") else match.status)


def _display_name(profile: Profile | None) -> str:
    if profile is None:
        return "A tournament official"
    full_name = f"{profile.first_name or ''} {profile.last_name or ''}".strip()
    return full_name or f"@{profile.username}" if profile.username else "A tournament official"


def _participant_ids(match: Match) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    raw_ids = [
        match.player1_id,
        match.player2_id,
        getattr(match, "player3_id", None),
        getattr(match, "player4_id", None),
        getattr(match, "team1_player1", None),
        getattr(match, "team1_player2", None),
        getattr(match, "team2_player1", None),
        getattr(match, "team2_player2", None),
        match.referee_id,
    ]
    for raw_id in raw_ids:
        if raw_id is None:
            continue
        user_id = str(raw_id)
        if user_id in seen:
            continue
        seen.add(user_id)
        result.append(user_id)
    return result


def _normalize_dt(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def dispatch_due_tournament_match_reminders(db: Session, tournament_id: str | None = None) -> int:
    now = datetime.now(timezone.utc)
    query = (
        db.query(Match)
        .join(Tournament, Tournament.id == Match.tournament_id)
        .filter(
            Match.tournament_id.isnot(None),
            Match.scheduled_at.isnot(None),
            Tournament.status == "ongoing",
        )
    )
    if tournament_id is not None:
        query = query.filter(Match.tournament_id == tournament_id)

    sent_count = 0
    for match in query.all():
        status = _match_status_value(match)
        if status in ("ongoing", "completed", "cancelled", "invalidated"):
            continue

        scheduled_at = _normalize_dt(match.scheduled_at)
        if scheduled_at is None:
            continue

        delta = scheduled_at - now
        if delta <= timedelta(0):
            continue

        minutes: int | None = None
        column_name: str | None = None
        if delta <= timedelta(minutes=5):
            minutes = 5
            column_name = "upcoming_reminder_5_sent_at"
        elif delta <= timedelta(minutes=10):
            minutes = 10
            column_name = "upcoming_reminder_10_sent_at"

        if minutes is None or column_name is None:
            continue
        if getattr(match, column_name, None) is not None:
            continue

        court = db.query(Court).filter(Court.id == match.court_id).first() if match.court_id is not None else None
        referee = db.query(Profile).filter(Profile.id == match.referee_id).first() if match.referee_id is not None else None

        scheduled_text = scheduled_at.astimezone(timezone.utc).strftime("%I:%M %p UTC")
        court_text = f" Court: {court.name}." if court is not None else ""
        referee_text = (
            f" Referee: {_display_name(referee)}."
            if referee is not None
            else ""
        )
        body = (
            f"Your tournament match is scheduled to begin in about {minutes} minutes"
            f" at {scheduled_text}.{court_text}{referee_text}"
        )

        setattr(match, column_name, now)
        db.add(
            MatchHistory(
                match_id=match.id,
                event_type=f"match_reminder_{minutes}m",
                description=f"Upcoming match reminder sent ({minutes} minutes).",
                meta={"minutes": minutes, "scheduled_at": scheduled_at.isoformat()},
            )
        )
        db.commit()

        participants = _participant_ids(match)
        if participants:
            send_bulk_notifications(
                participants,
                title="Upcoming Tournament Match",
                body=body,
                notif_type="tournament_match_reminder",
                reference_id=str(match.id),
            )

        publish_tournament_event(
            str(match.tournament_id),
            "tournament_match_reminder",
            match_id=str(match.id),
            minutes=minutes,
            scheduled_at=scheduled_at.isoformat(),
        )
        sent_count += 1

    return sent_count
