"""
Party System — doubles/mixed-doubles duo queue.

Flow:
  1. Player A creates a party (sport + match_format).
  2. Player A invites Player B → B gets a "party_invite" notification.
  3. B accepts → party status = "ready"; both are listed as members.
  4. Either player calls POST /parties/{id}/queue → party queues as one unit.
  5. Matchmaking (existing /matches/queue/join logic) fills the other team slot.
  6. POST /parties/{id}/disband → cancels the party and any active queue entry.
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import or_
from pydantic import BaseModel
from typing import Optional
import datetime as dt
from datetime import timezone

from app.database import get_db
from app.models.models import Party, PartyMember, PartyInvitation, Match, MatchLobbyPlayer, Profile
from app.middleware.auth import get_current_user
from app.services.notifications import send_notification

router = APIRouter()


# ── Request models ─────────────────────────────────────────────────────────────

class CreatePartyRequest(BaseModel):
    sport:        str
    match_format: str = "doubles"  # doubles | mixed_doubles


class InviteToPartyRequest(BaseModel):
    invitee_id: str


# ── Helpers ────────────────────────────────────────────────────────────────────

def _party_response(party: Party, db: Session) -> dict:
    members = []
    for m in party.members:
        p = db.query(Profile).filter(Profile.id == m.user_id).first()
        members.append({
            "user_id":  str(m.user_id),
            "username": p.username if p else None,
            "role":     m.role,
        })

    pending_invites = []
    for inv in party.invitations:
        if inv.status == "pending":
            p = db.query(Profile).filter(Profile.id == inv.invitee_id).first()
            pending_invites.append({
                "invitation_id": str(inv.id),
                "invitee_id":    str(inv.invitee_id),
                "invitee_username": p.username if p else None,
            })

    return {
        "id":               str(party.id),
        "sport":            party.sport,
        "match_format":     party.match_format,
        "status":           party.status,
        "leader_id":        str(party.leader_id),
        "members":          members,
        "pending_invites":  pending_invites,
        "match_id":         str(party.match_id) if party.match_id else None,
        "queue_started_at": party.queue_started_at.isoformat() if getattr(party, "queue_started_at", None) else None,
        "created_at":       party.created_at.isoformat() if party.created_at else None,
    }


def _get_active_party_for_user(user_id: str, db: Session) -> Optional[Party]:
    """Return the active party this user belongs to, if any."""
    parties = (
        db.query(Party)
        .join(PartyMember, PartyMember.party_id == Party.id)
        .filter(
            PartyMember.user_id == user_id,
            Party.status.in_(["forming", "ready", "in_queue", "match_found"]),
        )
        .all()
    )
    if not parties:
        return None

    priority = {"match_found": 4, "in_queue": 3, "ready": 2, "forming": 1}
    parties.sort(
        key=lambda p: (
            priority.get(str(p.status), 0),
            1 if getattr(p, "match_id", None) is not None else 0,
            p.created_at or dt.datetime.min.replace(tzinfo=timezone.utc),
        ),
        reverse=True,
    )
    return parties[0]


# ── Endpoints ──────────────────────────────────────────────────────────────────

@router.post("/parties", status_code=201)
def create_party(
    body: CreatePartyRequest,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Create a new party. Fails if the user is already in an active party."""
    user_id = current_user["id"]

    if body.match_format not in ("doubles", "mixed_doubles"):
        raise HTTPException(400, "match_format must be 'doubles' or 'mixed_doubles'")

    existing = _get_active_party_for_user(user_id, db)
    if existing:
        raise HTTPException(409, "You are already in an active party. Disband it first.")

    party = Party(
        leader_id=user_id,
        sport=body.sport,
        match_format=body.match_format,
        status="forming",
    )
    db.add(party)
    db.flush()

    db.add(PartyMember(party_id=party.id, user_id=user_id, role="leader"))
    db.commit()
    db.refresh(party)
    return _party_response(party, db)


@router.get("/parties/me")
def get_my_party(
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Return the calling user's active party, or null."""
    user_id = current_user["id"]
    party = _get_active_party_for_user(user_id, db)
    if not party:
        return {"party": None}

    # If the party was matched but the match was later invalidated, auto-disband.
    party_status = party.status.value if hasattr(party.status, "value") else str(party.status)
    if party_status == "match_found" and party.match_id:
        linked_match = db.query(Match).filter(Match.id == party.match_id).first()
        match_status = (linked_match.status.value if hasattr(linked_match.status, "value") else str(linked_match.status)) if linked_match else "not_found"
        if match_status in ("completed", "invalidated", "cancelled", "not_found"):
            setattr(party, "status", "disbanded")
            setattr(party, "match_id", None)
            db.commit()
            return {"party": None}

    # Self-heal: if party is still marked in_queue but its match is already
    # awaiting players or live, promote to match_found so members redirect.
    if party_status == "in_queue" and party.match_id:
        linked_match = db.query(Match).filter(Match.id == party.match_id).first()
        match_status = (linked_match.status.value if hasattr(linked_match.status, "value") else str(linked_match.status)) if linked_match else "not_found"
        if match_status in ("awaiting_players", "ongoing", "pending_approval"):
            setattr(party, "status", "match_found")
            db.commit()
            db.refresh(party)

    return {"party": _party_response(party, db)}


@router.get("/parties/{party_id}")
def get_party(
    party_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    party = db.query(Party).filter(Party.id == party_id).first()
    if not party:
        raise HTTPException(404, "Party not found")
    return _party_response(party, db)


@router.post("/parties/{party_id}/invite")
def invite_to_party(
    party_id: str,
    body: InviteToPartyRequest,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Leader invites a player. Party must be 'forming' with only 1 member."""
    user_id = current_user["id"]

    party = db.query(Party).filter(Party.id == party_id).first()
    if not party:
        raise HTTPException(404, "Party not found")
    if str(party.leader_id) != user_id:
        raise HTTPException(403, "Only the party leader can invite players")
    if party.status != "forming":
        raise HTTPException(400, f"Cannot invite while party is '{party.status}'")
    if len(party.members) >= 2:
        raise HTTPException(400, "Party is already full")

    invitee_id = body.invitee_id
    if invitee_id == user_id:
        raise HTTPException(400, "You cannot invite yourself")

    # Check invitee isn't already in a party
    invitee_party = _get_active_party_for_user(invitee_id, db)
    if invitee_party:
        # Auto-disband if they're alone in a forming party (solo lobby, no partner yet)
        solo_forming = (
            invitee_party.status == "forming"
            and len(invitee_party.members) == 1
        )
        if solo_forming:
            invitee_party.status = "disbanded"
            db.flush()
        else:
            raise HTTPException(409, "That player is already in an active party. Ask them to disband their party first.")

    # If there's already a pending invite to this player, return idempotently
    existing_inv = db.query(PartyInvitation).filter(
        PartyInvitation.party_id == party_id,
        PartyInvitation.invitee_id == invitee_id,
        PartyInvitation.status == "pending",
    ).first()
    if existing_inv:
        db.refresh(party)
        return _party_response(party, db)

    inv = PartyInvitation(
        party_id=party_id,
        inviter_id=user_id,
        invitee_id=invitee_id,
    )
    db.add(inv)
    db.flush()

    inviter = db.query(Profile).filter(Profile.id == user_id).first()
    send_notification(
        user_id=invitee_id,
        title="Party Invite",
        body=f"@{inviter.username if inviter else 'Someone'} invited you to join their party for {party.sport} ({party.match_format.replace('_', ' ')})",
        notif_type="party_invite",
        reference_id=str(inv.id),
    )
    db.commit()
    db.refresh(party)
    return _party_response(party, db)


@router.post("/parties/invitation/{invitation_id}/accept")
def accept_party_invite(
    invitation_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]

    inv = db.query(PartyInvitation).filter(PartyInvitation.id == invitation_id).first()
    if not inv:
        raise HTTPException(404, "Invitation not found")
    if str(inv.invitee_id) != user_id:
        raise HTTPException(403, "Not your invitation")

    party = db.query(Party).filter(Party.id == inv.party_id).first()

    # Idempotent: already accepted and already a member → just return current party state
    if inv.status == "accepted":
        already_member = any(str(m.user_id) == user_id for m in (party.members if party else []))
        if already_member and party:
            db.refresh(party)
            return _party_response(party, db)
        raise HTTPException(400, "Invitation already accepted")

    if inv.status != "pending":
        raise HTTPException(400, f"Invitation already {inv.status}")

    if not party or party.status not in ("forming",):
        raise HTTPException(400, "Party is no longer open")
    if len(party.members) >= 2:
        raise HTTPException(409, "Party is already full")

    # Auto-disband invitee's own solo forming party (same logic as the invite endpoint)
    existing = _get_active_party_for_user(user_id, db)
    if existing:
        solo_forming = existing.status == "forming" and len(existing.members) == 1
        if solo_forming:
            existing.status = "disbanded"
            db.flush()
        else:
            raise HTTPException(409, "You are already in an active party")

    inv.status = "accepted"
    db.add(PartyMember(party_id=party.id, user_id=user_id, role="member"))
    party.status = "ready"

    inviter = db.query(Profile).filter(Profile.id == inv.inviter_id).first()
    invitee = db.query(Profile).filter(Profile.id == user_id).first()
    send_notification(
        user_id=str(inv.inviter_id),
        title="Party Accepted",
        body=f"@{invitee.username if invitee else 'Your partner'} accepted your party invite! You are now ready to queue.",
        notif_type="party_accepted",
        reference_id=str(party.id),
    )
    db.commit()
    db.refresh(party)
    return _party_response(party, db)


@router.post("/parties/invitation/{invitation_id}/decline")
def decline_party_invite(
    invitation_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]

    inv = db.query(PartyInvitation).filter(PartyInvitation.id == invitation_id).first()
    if not inv:
        raise HTTPException(404, "Invitation not found")
    if str(inv.invitee_id) != user_id:
        raise HTTPException(403, "Not your invitation")
    if inv.status != "pending":
        raise HTTPException(400, f"Invitation already {inv.status}")

    inv.status = "declined"

    invitee = db.query(Profile).filter(Profile.id == user_id).first()
    send_notification(
        user_id=str(inv.inviter_id),
        title="Party Declined",
        body=f"@{invitee.username if invitee else 'Your invitee'} declined your party invite.",
        notif_type="party_declined",
        reference_id=str(inv.party_id),
    )
    db.commit()
    return {"message": "Invitation declined"}


@router.delete("/parties/invitation/{invitation_id}")
def cancel_party_invite(
    invitation_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Leader cancels a pending invite so they can invite a different player."""
    user_id = current_user["id"]

    inv = db.query(PartyInvitation).filter(PartyInvitation.id == invitation_id).first()
    if not inv:
        raise HTTPException(404, "Invitation not found")

    party = db.query(Party).filter(Party.id == inv.party_id).first()
    if not party or str(party.leader_id) != user_id:
        raise HTTPException(403, "Only the party leader can cancel an invite")
    if inv.status != "pending":
        raise HTTPException(400, f"Invitation already {inv.status}")

    inv.status = "cancelled"
    db.commit()
    db.refresh(party)
    return _party_response(party, db)


@router.post("/parties/{party_id}/queue")
def party_join_queue(
    party_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Enter the matchmaking queue as a duo.
    Creates/joins a Match in 'assembling' status with team1_player1 + team1_player2 filled.
    """
    user_id = current_user["id"]

    party = db.query(Party).filter(Party.id == party_id).first()
    if not party:
        raise HTTPException(404, "Party not found")
    if str(party.leader_id) != user_id:
        raise HTTPException(403, "Only the party leader can start the queue")
    if party.status != "ready":
        raise HTTPException(400, f"Party must be 'ready' before queuing (current: {party.status})")

    member_ids = [str(m.user_id) for m in party.members]
    if len(member_ids) != 2:
        raise HTTPException(400, "Party must have exactly 2 members to queue")

    # Check neither member is already in another queue
    for mid in member_ids:
        in_q = db.query(Match).filter(
            Match.match_type == "queue",
            Match.status.in_(["pending", "assembling"]),
            or_(
                Match.player1_id == mid, Match.player2_id == mid,
                Match.player3_id == mid, Match.player4_id == mid,
                Match.team1_player1 == mid, Match.team1_player2 == mid,
                Match.team2_player1 == mid, Match.team2_player2 == mid,
            ),
        ).first()
        if in_q:
            raise HTTPException(409, f"A party member is already in queue (match {in_q.id})")

    p1_id, p2_id = member_ids[0], member_ids[1]

    # Look for an existing assembling match that has slot for a full team
    # (team2_player1 is None — the opposing team slot is open)
    candidate = db.query(Match).filter(
        Match.match_type == "queue",
        Match.sport == party.sport,
        Match.match_format == party.match_format,
        Match.status == "assembling",
        Match.team1_player1.isnot(None),
        Match.team1_player2.isnot(None),
        Match.team2_player1.is_(None),
    ).first()

    if candidate:
        # Remember Team 1 player IDs before overwriting
        team1_p1 = str(candidate.team1_player1) if candidate.team1_player1 else None
        team1_p2 = str(candidate.team1_player2) if candidate.team1_player2 else None

        # Fill team 2
        setattr(candidate, "team2_player1", p1_id)
        setattr(candidate, "team2_player2", p2_id)
        # Canonical doubles slot mapping used by match pages + rating logic:
        # team1 = player1 + player3, team2 = player2 + player4.
        setattr(candidate, "player1_id",    team1_p1)
        setattr(candidate, "player2_id",    p1_id)
        setattr(candidate, "player3_id",    team1_p2)
        setattr(candidate, "player4_id",    p2_id)
        setattr(candidate, "status",        "awaiting_players")
        setattr(candidate, "party_id",      party.id)
        party.status = "match_found"
        party.match_id = candidate.id

        # Also mark the opposing party (Team 1) as match_found so their polling
        # redirect fires even if old party-member rows exist for that user.
        team1_party = (
            db.query(Party)
            .filter(
                Party.match_id == candidate.id,
                Party.status == "in_queue",
            )
            .first()
        )
        if not team1_party and team1_p1:
            from app.models.models import PartyMember as _PM
            team1_party = (
                db.query(Party)
                .join(_PM, _PM.party_id == Party.id)
                .filter(
                    Party.status == "in_queue",
                    _PM.user_id.in_([pid for pid in [team1_p1, team1_p2] if pid]),
                )
                .order_by(Party.created_at.desc())
                .first()
            )
        if team1_party:
            team1_party.status = "match_found"
            team1_party.match_id = candidate.id

        # Create lobby player rows for all 4 players
        lobby_assignments = [
            (team1_p1, 1), (team1_p2, 1), (p1_id, 2), (p2_id, 2)
        ]
        for (pid, team_no) in lobby_assignments:
            if pid:
                existing_row = db.query(MatchLobbyPlayer).filter(
                    MatchLobbyPlayer.match_id == candidate.id,
                    MatchLobbyPlayer.user_id == pid,
                ).first()
                if existing_row:
                    # Keep row id stable; update team assignment if needed.
                    existing_row.team_no = team_no
                else:
                    db.add(MatchLobbyPlayer(
                        match_id=candidate.id,
                        user_id=pid,
                        team_no=team_no,
                    ))

        # Notify all 4 players
        all_pids = [pid for pid in [team1_p1, team1_p2, p1_id, p2_id] if pid]
        for pid in all_pids:
            send_notification(
                user_id=pid,
                title="Match Found!",
                body="Your doubles game is starting. Enter the lobby to confirm.",
                notif_type="party_match_found",
                reference_id=str(candidate.id),
            )

        db.commit()
        return {
            "status":   "matched",
            "match_id": str(candidate.id),
            "message":  "Match found! Your doubles game is starting.",
        }

    # No open match — create one with team 1 filled
    new_match = Match(
        sport=party.sport,
        match_type="queue",
        match_format=party.match_format,
        status="assembling",
        team1_player1=p1_id,
        team1_player2=p2_id,
        player1_id=p1_id,
        player3_id=p2_id,
    )
    db.add(new_match)
    db.flush()

    party.status = "in_queue"
    party.match_id = new_match.id
    setattr(party, "queue_started_at", dt.datetime.now(timezone.utc))
    db.commit()

    # Notify the non-leader partner that the queue has started
    leader_profile = db.query(Profile).filter(Profile.id == user_id).first()
    leader_name = f"@{leader_profile.username}" if leader_profile and leader_profile.username else "Your leader"
    partner_id = next((mid for mid in member_ids if mid != user_id), None)
    if partner_id:
        send_notification(
            user_id=partner_id,
            title="Queue Started!",
            body=f"{leader_name} started searching for a match. Head to the party page to follow along.",
            notif_type="party_in_queue",
            reference_id=str(party.id),
        )

    return {
        "status":   "in_queue",
        "match_id": str(new_match.id),
        "message":  "In queue — waiting for an opponent duo.",
    }


@router.post("/parties/{party_id}/leave-queue")
def party_leave_queue(
    party_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Leave the queue and reset party status back to 'ready'."""
    user_id = current_user["id"]

    party = db.query(Party).filter(Party.id == party_id).first()
    if not party:
        raise HTTPException(404, "Party not found")
    if str(party.leader_id) != user_id:
        raise HTTPException(403, "Only the leader can leave the queue")
    if party.status != "in_queue":
        raise HTTPException(400, "Party is not currently in queue")

    # Cancel the assembling match if it's still open
    if party.match_id:
        m = db.query(Match).filter(Match.id == party.match_id).first()
        if m and m.status == "assembling":
            m.status = "cancelled"  # type: ignore[assignment]

    member_ids = [str(m.user_id) for m in party.members]
    party.status = "ready"
    party.match_id = None
    setattr(party, "queue_started_at", None)
    db.commit()
    db.refresh(party)

    # Notify the non-leader partner that queue was cancelled
    leader_profile = db.query(Profile).filter(Profile.id == user_id).first()
    leader_name = f"@{leader_profile.username}" if leader_profile and leader_profile.username else "Your leader"
    partner_id = next((mid for mid in member_ids if mid != user_id), None)
    if partner_id:
        send_notification(
            user_id=partner_id,
            title="Queue Cancelled",
            body=f"{leader_name} left the queue. You're back to ready.",
            notif_type="party_queue_left",
            reference_id=str(party.id),
        )

    return _party_response(party, db)


@router.post("/parties/{party_id}/disband")
def disband_party(
    party_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Disband the party. Notifies the partner if one was present."""
    user_id = current_user["id"]

    party = db.query(Party).filter(Party.id == party_id).first()
    if not party:
        raise HTTPException(404, "Party not found")

    member_ids = [str(m.user_id) for m in party.members]
    if user_id not in member_ids:
        raise HTTPException(403, "You are not in this party")

    # Cancel queue entry if active
    if party.match_id and party.status == "in_queue":
        m = db.query(Match).filter(Match.id == party.match_id).first()
        if m and m.status == "assembling":
            m.status = "cancelled"  # type: ignore[assignment]

    leader = db.query(Profile).filter(Profile.id == party.leader_id).first()
    for mid in member_ids:
        if mid != user_id:
            send_notification(
                user_id=mid,
                title="Party Disbanded",
                body=f"@{leader.username if leader else 'Your partner'} has disbanded the party.",
                notif_type="party_disbanded",
                reference_id=str(party.id),
            )

    party.status = "disbanded"
    db.commit()
    return {"message": "Party disbanded"}
