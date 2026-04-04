"""
Match Lobby — pre-match readiness checkpoint for queue doubles matches.
"""
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.database import get_db
from app.middleware.auth import get_current_user
from app.models.models import Match, MatchLobbyPlayer, Profile, Party, MatchHistory
from app.services.broadcast import broadcast_match
from app.services.notifications import send_bulk_notifications

router = APIRouter()


def _aggregate_lobby_status(rows: list[MatchLobbyPlayer]) -> tuple[int, int, bool]:
    """
    Collapse duplicate (match_id, user_id) rows into a single effective status
    per user where 'entered' wins over 'pending'.
    """
    status_by_user: dict[str, str] = {}
    for row in rows:
        uid = str(row.user_id)
        prev = status_by_user.get(uid)
        if prev is None or (prev != "entered" and row.status == "entered"):
            status_by_user[uid] = row.status
    total = len(status_by_user)
    entered_count = sum(1 for s in status_by_user.values() if s == "entered")
    return entered_count, total, (total > 0 and entered_count == total)


@router.get("/matches/{match_id}/lobby")
def get_lobby(
    match_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Return lobby state — list of players and their entry status."""
    match = db.query(Match).filter(Match.id == match_id).first()
    if not match:
        raise HTTPException(404, "Match not found")

    rows = (
        db.query(MatchLobbyPlayer)
        .filter(MatchLobbyPlayer.match_id == match_id)
        .all()
    )

    # De-duplicate by user_id for API response stability.
    players_by_user: dict[str, dict] = {}
    for row in rows:
        uid = str(row.user_id)
        profile = db.query(Profile).filter(Profile.id == row.user_id).first()
        prev = players_by_user.get(uid)
        incoming = {
            "user_id":    uid,
            "username":   profile.username if profile else None,
            "team_no":    row.team_no,
            "status":     row.status,
            "entered_at": row.entered_at.isoformat() if row.entered_at else None,
        }
        if prev is None or (prev["status"] != "entered" and incoming["status"] == "entered"):
            players_by_user[uid] = incoming

    players = sorted(players_by_user.values(), key=lambda p: (p["team_no"], p["username"] or ""))
    _, _, all_entered = _aggregate_lobby_status(rows)

    match_status = match.status.value if hasattr(match.status, "value") else str(match.status)
    return {
        "match_id":     match_id,
        "match_status": match_status,
        "sport":        match.sport.value if hasattr(match.sport, "value") else str(match.sport),
        "match_format": match.match_format.value if hasattr(match.match_format, "value") else str(match.match_format),
        "players":      players,
        "all_entered":  all_entered,
    }


@router.post("/matches/{match_id}/lobby/enter")
def enter_lobby(
    match_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Mark the calling player as entered. If all players entered, start the match."""
    user_id = current_user["id"]

    match = db.query(Match).filter(Match.id == match_id).first()
    if not match:
        raise HTTPException(404, "Match not found")

    match_status = match.status.value if hasattr(match.status, "value") else str(match.status)
    if match_status not in ("awaiting_players", "ongoing"):
        raise HTTPException(400, f"Match is not in lobby state (status: {match_status})")

    # Mark this player's lobby rows as entered (idempotent, duplicate-safe).
    my_rows = db.query(MatchLobbyPlayer).filter(
        MatchLobbyPlayer.match_id == match_id,
        MatchLobbyPlayer.user_id == user_id,
    ).all()
    if not my_rows:
        raise HTTPException(403, "You are not in this lobby")

    now = datetime.now(timezone.utc)
    for row in my_rows:
        if row.status != "entered":
            setattr(row, "status", "entered")
            setattr(row, "entered_at", now)
    db.flush()

    # Check if all players have entered
    all_rows = db.query(MatchLobbyPlayer).filter(MatchLobbyPlayer.match_id == match_id).all()
    entered_count, total, all_entered = _aggregate_lobby_status(all_rows)

    if all_entered and match_status == "awaiting_players":
        setattr(match, "status", "ongoing")
        setattr(match, "started_at", now)
        db.commit()
        broadcast_match(match_id, {"type": "match_live", "match_id": match_id})
        return {"status": "match_started", "match_id": match_id}

    db.commit()

    # Broadcast lobby update so all players see the updated checklist
    broadcast_match(match_id, {
        "type":          "lobby_update",
        "user_id":       user_id,
        "entered_count": entered_count,
        "total":         total,
    })

    return {"status": "entered", "entered_count": entered_count, "total": total}


@router.post("/matches/{match_id}/lobby/cancel")
def cancel_lobby(
    match_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Any lobby player can cancel — invalidates the match, resets both parties to 'ready',
    and notifies all other players.
    """
    user_id = current_user["id"]

    match = db.query(Match).filter(Match.id == match_id).first()
    if not match:
        raise HTTPException(404, "Match not found")

    match_status = match.status.value if hasattr(match.status, "value") else str(match.status)
    if match_status != "awaiting_players":
        raise HTTPException(400, "Lobby is no longer cancellable")

    # Verify caller is in this lobby
    rows = db.query(MatchLobbyPlayer).filter(MatchLobbyPlayer.match_id == match_id).all()
    player_ids = [str(r.user_id) for r in rows]
    if user_id not in player_ids:
        raise HTTPException(403, "You are not in this lobby")

    # Invalidate the match
    setattr(match, "status", "invalidated")
    db.query(MatchHistory).filter(MatchHistory.match_id == match_id).delete(synchronize_session=False)

    # Reset both parties linked to this match back to 'ready' so they can requeue
    parties = db.query(Party).filter(Party.match_id == match.id).all()
    for party in parties:
        setattr(party, "status", "ready")
        setattr(party, "match_id", None)

    db.commit()

    # Notify everyone else
    others = [pid for pid in player_ids if pid != user_id]
    canceller = db.query(Profile).filter(Profile.id == user_id).first()
    name = f"@{canceller.username}" if canceller and canceller.username else "A player"
    if others:
        send_bulk_notifications(
            user_ids=others,
            notif_type="lobby_cancelled",
            title="Lobby Cancelled",
            body=f"{name} cancelled the lobby. Your party is back to ready — requeue when you're set.",
            reference_id=match_id,
        )

    broadcast_match(match_id, {"type": "match_invalidated", "reason": "lobby_cancelled", "by": user_id})
    return {"status": "cancelled"}
