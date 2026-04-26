"""
Match Lobby - pre-match readiness checkpoint for queue and tournament matches.
"""

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.database import get_db
from app.middleware.auth import get_current_user
from app.models.models import Match, MatchHistory, MatchLobbyPlayer, Party, Profile, Tournament
from app.services.broadcast import broadcast_match
from app.services.match_lobby import ensure_initial_match_set, ensure_match_lobby_rows, expected_lobby_assignments
from app.services.notifications import send_bulk_notifications
from app.services.tournament_runtime import publish_tournament_event

router = APIRouter()


def _aggregate_lobby_status(rows: list[MatchLobbyPlayer]) -> tuple[int, int, bool]:
    status_by_user: dict[str, str] = {}
    for row in rows:
        uid = str(row.user_id)
        prev = status_by_user.get(uid)
        if prev is None or (prev != "entered" and row.status == "entered"):
            status_by_user[uid] = row.status
    total = len(status_by_user)
    entered_count = sum(1 for status in status_by_user.values() if status == "entered")
    return entered_count, total, (total > 0 and entered_count == total)


def _match_status(match: Match) -> str:
    return str(match.status.value if hasattr(match.status, "value") else match.status)


def _tournament_phase(match: Match) -> str | None:
    value = getattr(match, "tournament_phase", None)
    return str(value) if value is not None else None


def _is_tournament_lobby(match: Match) -> bool:
    return getattr(match, "tournament_id", None) is not None


def _can_enter_lobby(match: Match) -> bool:
    status = _match_status(match)
    if status in ("completed", "cancelled", "invalidated"):
        return False
    if _is_tournament_lobby(match):
        return _tournament_phase(match) in ("called", "ready") or status == "ongoing"
    return status in ("awaiting_players", "ongoing")


def _effective_rows_by_user(rows: list[MatchLobbyPlayer]) -> dict[str, MatchLobbyPlayer]:
    deduped: dict[str, MatchLobbyPlayer] = {}
    for row in rows:
        uid = str(row.user_id)
        prev = deduped.get(uid)
        if prev is None or (prev.status != "entered" and row.status == "entered"):
            deduped[uid] = row
    return deduped


def _team_ready_flags(match: Match, rows: list[MatchLobbyPlayer]) -> tuple[bool, bool]:
    rows_by_user = _effective_rows_by_user(rows)
    entered_users = {uid for uid, row in rows_by_user.items() if row.status == "entered"}
    team1_ids = [user_id for user_id, team_no in expected_lobby_assignments(match) if team_no == 1]
    team2_ids = [user_id for user_id, team_no in expected_lobby_assignments(match) if team_no == 2]
    team1_ready = bool(team1_ids) and all(user_id in entered_users for user_id in team1_ids)
    team2_ready = bool(team2_ids) and all(user_id in entered_users for user_id in team2_ids)
    return team1_ready, team2_ready


def _return_route(match: Match, linked_party_exists: bool) -> str:
    if _is_tournament_lobby(match):
        return f"/matches/{match.id}"
    return "/matches/party" if linked_party_exists else "/matches/queue"


def _player_sort_name(player: dict) -> tuple[str, str, str]:
    return (
        (player.get("first_name") or "").strip().lower(),
        (player.get("last_name") or "").strip().lower(),
        (player.get("user_id") or "").strip().lower(),
    )


@router.get("/matches/{match_id}/lobby")
def get_lobby(
    match_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    match = db.query(Match).filter(Match.id == match_id).first()
    if not match:
        raise HTTPException(404, "Match not found")

    if _can_enter_lobby(match):
        ensure_match_lobby_rows(db, match)
        db.flush()

    rows = db.query(MatchLobbyPlayer).filter(MatchLobbyPlayer.match_id == match_id).all()
    players_by_user: dict[str, dict] = {}
    for row in rows:
        uid = str(row.user_id)
        profile = db.query(Profile).filter(Profile.id == row.user_id).first()
        prev = players_by_user.get(uid)
        incoming = {
            "user_id": uid,
            "first_name": profile.first_name if profile else None,
            "last_name": profile.last_name if profile else None,
            "avatar_url": profile.avatar_url if profile else None,
            "team_no": row.team_no,
            "status": row.status,
            "entered_at": row.entered_at.isoformat() if row.entered_at else None,
        }
        if prev is None or (prev["status"] != "entered" and incoming["status"] == "entered"):
            players_by_user[uid] = incoming

    players = sorted(
        players_by_user.values(),
        key=lambda player: (player["team_no"], *_player_sort_name(player)),
    )
    entered_count, total, all_entered = _aggregate_lobby_status(rows)
    linked_party = db.query(Party.id).filter(Party.match_id == match.id).first()

    return {
        "match_id": match_id,
        "match_status": _match_status(match),
        "sport": match.sport.value if hasattr(match.sport, "value") else str(match.sport),
        "match_format": match.match_format.value if hasattr(match.match_format, "value") else str(match.match_format),
        "players": players,
        "entered_count": entered_count,
        "total_players": total,
        "all_entered": all_entered,
        "return_route": _return_route(match, linked_party is not None),
        "lobby_mode": "tournament" if _is_tournament_lobby(match) else "queue",
        "tournament_id": str(match.tournament_id) if getattr(match, "tournament_id", None) is not None else None,
        "tournament_phase": _tournament_phase(match),
        "entry_open": _can_enter_lobby(match),
        "can_cancel": (not _is_tournament_lobby(match)) and _match_status(match) == "awaiting_players",
        "deadline_at": match.checkin_deadline_at.isoformat() if getattr(match, "checkin_deadline_at", None) is not None else None,
        "referee_id": str(match.referee_id) if getattr(match, "referee_id", None) is not None else None,
        "referee_ready_at": match.referee_ready_at.isoformat() if getattr(match, "referee_ready_at", None) is not None else None,
    }


@router.post("/matches/{match_id}/lobby/enter")
def enter_lobby(
    match_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]

    match = db.query(Match).filter(Match.id == match_id).first()
    if not match:
        raise HTTPException(404, "Match not found")
    if not _can_enter_lobby(match):
        raise HTTPException(400, f"Match is not in lobby state (status: {_match_status(match)})")

    ensure_match_lobby_rows(db, match)
    db.flush()

    before_rows = db.query(MatchLobbyPlayer).filter(MatchLobbyPlayer.match_id == match_id).all()
    _, _, was_all_entered = _aggregate_lobby_status(before_rows)

    my_rows = db.query(MatchLobbyPlayer).filter(
        MatchLobbyPlayer.match_id == match_id,
        MatchLobbyPlayer.user_id == user_id,
    ).all()

    now = datetime.now(timezone.utc)

    # Referee path: not a player slot, but can still enter to signal readiness
    is_referee = (
        _is_tournament_lobby(match)
        and match.referee_id is not None
        and str(match.referee_id) == user_id
    )
    if not my_rows and not is_referee:
        raise HTTPException(403, "You are not in this lobby")

    row_changed = False
    for row in my_rows:
        if row.status != "entered":
            setattr(row, "status", "entered")
            setattr(row, "entered_at", now)
            row_changed = True
    db.flush()

    all_rows = db.query(MatchLobbyPlayer).filter(MatchLobbyPlayer.match_id == match_id).all()
    entered_count, total, all_entered = _aggregate_lobby_status(all_rows)

    if _is_tournament_lobby(match):
        team1_ready, team2_ready = _team_ready_flags(match, all_rows)

        if team1_ready and match.team1_ready_at is None:
            setattr(match, "team1_ready_at", now)
        if team2_ready and match.team2_ready_at is None:
            setattr(match, "team2_ready_at", now)
        if is_referee and match.referee_ready_at is None:
            setattr(match, "referee_ready_at", now)

        if match.team1_ready_at and match.team2_ready_at and ((match.referee_id is None) or match.referee_ready_at):
            setattr(match, "tournament_phase", "ready")
        elif getattr(match, "called_at", None) is not None:
            setattr(match, "tournament_phase", "called")
        else:
            setattr(match, "tournament_phase", "scheduled")

        db.commit()

        if all_entered and not was_all_entered:
            notify_targets: set[str] = set()
            if match.referee_id is not None:
                notify_targets.add(str(match.referee_id))
            tournament = db.query(Tournament).filter(Tournament.id == match.tournament_id).first()
            if tournament is not None:
                notify_targets.add(str(tournament.organizer_id))
            notify_targets.discard(user_id)
            if notify_targets:
                send_bulk_notifications(
                    user_ids=list(notify_targets),
                    notif_type="tournament_match_ready",
                    title="Tournament Match Lobby Ready",
                    body="All players have entered the lobby. The referee can start the match now.",
                    reference_id=match_id,
                )

        broadcast_match(
            match_id,
            {
                "type": "lobby_update",
                "user_id": user_id,
                "entered_count": entered_count,
                "total": total,
                "tournament_phase": getattr(match, "tournament_phase", None),
            },
        )
        broadcast_match(
            match_id,
            {
                "type": "tournament_match_ready",
                "match_id": match_id,
                "team1_ready_at": match.team1_ready_at.isoformat() if match.team1_ready_at is not None else None,
                "team2_ready_at": match.team2_ready_at.isoformat() if match.team2_ready_at is not None else None,
                "referee_ready_at": match.referee_ready_at.isoformat() if match.referee_ready_at is not None else None,
                "tournament_phase": getattr(match, "tournament_phase", None),
                "all_entered": all_entered,
            },
        )
        publish_tournament_event(
            str(match.tournament_id),
            "tournament_lobby_update",
            match_id=match_id,
            entered_count=entered_count,
            total=total,
            all_entered=all_entered,
            tournament_phase=getattr(match, "tournament_phase", None),
        )
        return {
            "status": "ready" if all_entered else "entered",
            "entered_count": entered_count,
            "total": total,
            "all_entered": all_entered,
            "tournament_phase": getattr(match, "tournament_phase", None),
        }

    if all_entered and _match_status(match) == "awaiting_players":
        setattr(match, "status", "ongoing")
        setattr(match, "started_at", now)
        ensure_initial_match_set(db, match)
        db.commit()
        broadcast_match(match_id, {"type": "match_live", "match_id": match_id})
        return {"status": "match_started", "match_id": match_id}

    db.commit()
    broadcast_match(
        match_id,
        {
            "type": "lobby_update",
            "user_id": user_id,
            "entered_count": entered_count,
            "total": total,
        },
    )
    return {"status": "entered", "entered_count": entered_count, "total": total}


@router.post("/matches/{match_id}/lobby/cancel")
def cancel_lobby(
    match_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]

    match = db.query(Match).filter(Match.id == match_id).first()
    if not match:
        raise HTTPException(404, "Match not found")
    if _is_tournament_lobby(match):
        raise HTTPException(400, "Tournament lobbies cannot be cancelled by players.")

    match_status = _match_status(match)
    if match_status != "awaiting_players":
        raise HTTPException(400, "Lobby is no longer cancellable")

    ensure_match_lobby_rows(db, match)
    db.flush()
    rows = db.query(MatchLobbyPlayer).filter(MatchLobbyPlayer.match_id == match_id).all()
    player_ids = [str(row.user_id) for row in rows]
    if user_id not in player_ids:
        raise HTTPException(403, "You are not in this lobby")

    setattr(match, "status", "invalidated")
    db.query(MatchHistory).filter(MatchHistory.match_id == match_id).delete(synchronize_session=False)

    parties = db.query(Party).filter(Party.match_id == match.id).all()
    for party in parties:
        setattr(party, "status", "ready")
        setattr(party, "match_id", None)
        setattr(party, "queue_started_at", None)

    db.commit()

    others = [player_id for player_id in player_ids if player_id != user_id]
    canceller = db.query(Profile).filter(Profile.id == user_id).first()
    name = f"{canceller.first_name or ''} {canceller.last_name or ''}".strip() if canceller else "A player"
    if others:
        send_bulk_notifications(
            user_ids=others,
            notif_type="lobby_cancelled",
            title="Lobby Cancelled",
            body=f"{name} cancelled the lobby. Your party is back to ready - requeue when you're set.",
            reference_id=match_id,
        )

    broadcast_match(match_id, {"type": "match_invalidated", "reason": "lobby_cancelled", "by": user_id})
    return {"status": "cancelled"}


@router.post("/matches/{match_id}/lobby/leave")
def leave_lobby(
    match_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Tournament lobby only: reset the calling player's slot to pending without cancelling the match."""
    user_id = current_user["id"]

    match = db.query(Match).filter(Match.id == match_id).first()
    if not match:
        raise HTTPException(404, "Match not found")
    if not _is_tournament_lobby(match):
        raise HTTPException(400, "Use cancel for non-tournament lobbies.")

    my_rows = db.query(MatchLobbyPlayer).filter(
        MatchLobbyPlayer.match_id == match_id,
        MatchLobbyPlayer.user_id == user_id,
    ).all()
    if not my_rows:
        # Player has no committed slot — already effectively out of the lobby.
        return {"status": "left"}

    for row in my_rows:
        if row.status == "entered":
            setattr(row, "status", "pending")
            setattr(row, "entered_at", None)
    db.flush()

    # Recalculate team readiness and update phase
    all_rows = db.query(MatchLobbyPlayer).filter(MatchLobbyPlayer.match_id == match_id).all()
    team1_ready, team2_ready = _team_ready_flags(match, all_rows)

    if not team1_ready:
        setattr(match, "team1_ready_at", None)
    if not team2_ready:
        setattr(match, "team2_ready_at", None)
    if not (team1_ready and team2_ready) and getattr(match, "called_at", None) is not None:
        setattr(match, "tournament_phase", "called")

    entered_count, total, all_entered = _aggregate_lobby_status(all_rows)
    db.commit()

    broadcast_match(
        match_id,
        {
            "type": "lobby_update",
            "user_id": user_id,
            "entered_count": entered_count,
            "total": total,
            "tournament_phase": getattr(match, "tournament_phase", None),
        },
    )
    return {"status": "left"}
