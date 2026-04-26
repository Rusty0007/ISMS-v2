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
from app.models.models import ClubMember, Match, MatchLobbyPlayer, Party, PartyInvitation, PartyMember, PlayerRating, Profile
from app.middleware.auth import get_current_user
from app.services.matchmaking import (
    can_join_doubles_lobby,
    is_mixed_doubles_team,
    normalize_gender,
    score_doubles_entry,
    team_avg,
)
from app.services.notifications import send_notification
from app.services.rating_policy import ML_MATCHMAKING_MIN_MATCHES

router = APIRouter()

PARTY_QUEUE_TIMEOUT_SECONDS = 180


# ── Request models ─────────────────────────────────────────────────────────────

class CreatePartyRequest(BaseModel):
    sport:        str
    match_format: str = "doubles"  # doubles | mixed_doubles


class InviteToPartyRequest(BaseModel):
    invitee_id: str


class PartyQueueRequest(BaseModel):
    preferred_club_id: Optional[str] = None
    match_mode: Optional[str] = "ranked"  # ranked | normal


# ── Helpers ────────────────────────────────────────────────────────────────────

def _party_response(party: Party, db: Session) -> dict:
    members = []
    member_ids  = [str(m.user_id) for m in party.members]
    invitee_ids = [str(inv.invitee_id) for inv in party.invitations if inv.status == "pending"]
    all_ids     = list(set(member_ids + invitee_ids))
    prof_map    = {str(p.id): p for p in db.query(Profile).filter(Profile.id.in_(all_ids)).all()}

    for m in party.members:
        p = prof_map.get(str(m.user_id))
        members.append({
            "user_id":    str(m.user_id),
            "first_name": getattr(p, "first_name", None),
            "last_name":  getattr(p, "last_name", None),
            "role":       m.role,
        })

    pending_invites = []
    for inv in party.invitations:
        if inv.status == "pending":
            p = prof_map.get(str(inv.invitee_id))
            pending_invites.append({
                "invitation_id":    str(inv.id),
                "invitee_id":       str(inv.invitee_id),
                "invitee_first_name": getattr(p, "first_name", None),
                "invitee_last_name":  getattr(p, "last_name", None),
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


def _pause_party_queue_if_expired(party: Party, db: Session) -> bool:
    party_status = party.status.value if hasattr(party.status, "value") else str(party.status)
    if party_status != "in_queue" or not getattr(party, "queue_started_at", None):
        return False

    started_at = party.queue_started_at
    if started_at.tzinfo is None:
        started_at = started_at.replace(tzinfo=timezone.utc)
    elapsed = (dt.datetime.now(timezone.utc) - started_at).total_seconds()
    if elapsed < PARTY_QUEUE_TIMEOUT_SECONDS:
        return False

    if party.match_id:
        match = db.query(Match).filter(Match.id == party.match_id).first()
        match_status = (match.status.value if hasattr(match.status, "value") else str(match.status)) if match else None
        if match and match_status == "assembling":
            setattr(match, "status", "cancelled")

    setattr(party, "status", "ready")
    setattr(party, "match_id", None)
    setattr(party, "queue_started_at", None)
    db.commit()
    db.refresh(party)
    return True


def _party_queue_mode(raw_mode: Optional[str]) -> str:
    mode = (raw_mode or "ranked").strip().lower()
    if mode in ("normal", "quick", "friendly"):
        return "normal"
    return "ranked"


def _party_queue_match_type(preferred_club_id: Optional[str], match_mode: Optional[str]) -> str:
    # A club preference scopes the venue/opponent pool only; club-scoped
    # matchmaking stays ranked to preserve club leaderboard integrity.
    if preferred_club_id:
        return "ranked"
    return "queue" if _party_queue_mode(match_mode) == "normal" else "ranked"


def _profile_gender_key(profile: Profile | None) -> str | None:
    return normalize_gender(getattr(profile, "gender", None) if profile else None)


def _profiles_for_user_ids(db: Session, user_ids: list[str]) -> list[Profile]:
    if not user_ids:
        return []
    return db.query(Profile).filter(Profile.id.in_(user_ids)).all()


def _mixed_doubles_player_entries(profiles: list[Profile]) -> list[dict]:
    return [
        {
            "player_id": str(profile.id),
            "gender": _profile_gender_key(profile),
        }
        for profile in profiles
    ]


def _require_mixed_doubles_team(db: Session, user_ids: list[str]) -> None:
    profiles = _profiles_for_user_ids(db, user_ids)
    if len(profiles) != len(set(user_ids)) or not is_mixed_doubles_team(_mixed_doubles_player_entries(profiles)):
        raise HTTPException(
            400,
            "Mixed doubles requires one male and one female player in the party.",
        )


def _is_mixed_doubles_team_ids(db: Session, user_ids: list[str]) -> bool:
    profiles = _profiles_for_user_ids(db, user_ids)
    return len(profiles) == len(set(user_ids)) and is_mixed_doubles_team(
        _mixed_doubles_player_entries(profiles)
    )


def _player_stats(db: Session, user_id: str, sport: str, match_format: str) -> dict:
    rating = db.query(PlayerRating).filter(
        PlayerRating.user_id == user_id,
        PlayerRating.sport == sport,
        PlayerRating.match_format == match_format,
    ).first()
    profile = db.query(Profile).filter(Profile.id == user_id).first()
    matches = int(getattr(rating, "matches_played", 0) or 0) if rating else 0
    wins = int(getattr(rating, "wins", 0) or 0) if rating else 0
    return {
        "player_id": user_id,
        "rating": float(rating.rating) if rating else 1500.0,  # type: ignore[arg-type]
        "rating_deviation": float(rating.rating_deviation) if rating else 200.0,  # type: ignore[arg-type]
        "win_rate": (wins / matches) if matches > 0 else 0.5,
        "activeness_score": float(rating.activeness_score) if rating else 0.5,  # type: ignore[arg-type]
        "current_streak": int(rating.current_win_streak) if rating else 0,  # type: ignore[arg-type]
        "city_code": profile.city_mun_code if profile else None,
        "province_code": profile.province_code if profile else None,
        "region_code": profile.region_code if profile else None,
        "gender": _profile_gender_key(profile),
    }


def _team_stats(db: Session, user_ids: list[str], sport: str, match_format: str) -> list[dict]:
    return [_player_stats(db, uid, sport, match_format) for uid in user_ids if uid]


def _team_average_entry(players: list[dict]) -> dict:
    return {
        "rating": team_avg(players, "rating", 1500.0),
        "rating_deviation": team_avg(players, "rating_deviation", 200.0),
        "win_rate": team_avg(players, "win_rate", 0.5),
        "activeness_score": team_avg(players, "activeness_score", 0.5),
        "current_streak": int(team_avg(players, "current_streak", 0)),
        "city_code": players[0].get("city_code") if players else None,
        "province_code": players[0].get("province_code") if players else None,
        "region_code": players[0].get("region_code") if players else None,
    }


def _require_ranked_party_eligibility(db: Session, member_ids: list[str], sport: str, match_format: str) -> None:
    rows = db.query(PlayerRating).filter(
        PlayerRating.user_id.in_(member_ids),
        PlayerRating.sport == sport,
        PlayerRating.match_format == match_format,
    ).all()
    ready_ids = {str(row.user_id) for row in rows if bool(getattr(row, "is_matchmaking_eligible", False))}
    missing = [uid for uid in member_ids if uid not in ready_ids]
    if missing:
        format_label = match_format.replace("_", " ")
        raise HTTPException(
            400,
            f"Both party members must complete ML matchmaking calibration ({ML_MATCHMAKING_MIN_MATCHES} {format_label} matches) before starting random ranked matchmaking.",
        )


def _ranked_party_team_score(
    db: Session,
    incoming_ids: list[str],
    existing_ids: list[str],
    sport: str,
    match_format: str,
) -> float:
    incoming_stats = _team_stats(db, incoming_ids, sport, match_format)
    existing_stats = _team_stats(db, existing_ids, sport, match_format)
    incoming_team = _team_average_entry(incoming_stats)
    if not can_join_doubles_lobby(
        incoming=incoming_team,
        lobby_players=existing_stats,
        sport=sport,
        match_format=match_format,
        mode="ranked",
    ):
        return 0.0
    return score_doubles_entry(
        incoming=incoming_team,
        lobby_players=existing_stats,
        sport=sport,
        match_format=match_format,
        mode="ranked",
    )


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
    if body.match_format == "mixed_doubles":
        leader_profile = db.query(Profile).filter(Profile.id == user_id).first()
        if _profile_gender_key(leader_profile) is None:
            raise HTTPException(
                400,
                "Mixed doubles requires your profile gender to be set to male or female.",
            )

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

    queue_timed_out = _pause_party_queue_if_expired(party, db)

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

    return {"party": _party_response(party, db), "queue_timed_out": queue_timed_out}


@router.get("/parties/{party_id}")
def get_party(
    party_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    party = db.query(Party).filter(Party.id == party_id).first()
    if not party:
        raise HTTPException(404, "Party not found")
    queue_timed_out = _pause_party_queue_if_expired(party, db)
    response = _party_response(party, db)
    response["queue_timed_out"] = queue_timed_out
    return response


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
    if party.match_format == "mixed_doubles":
        leader_profile = db.query(Profile).filter(Profile.id == user_id).first()
        invitee_profile = db.query(Profile).filter(Profile.id == invitee_id).first()
        if invitee_profile is None:
            raise HTTPException(404, "Invitee not found")
        if not is_mixed_doubles_team(_mixed_doubles_player_entries([p for p in [leader_profile, invitee_profile] if p])):
            raise HTTPException(
                400,
                "Mixed doubles parties must be one male and one female player.",
            )

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
    inviter_name = "Someone"
    if inviter:
        inviter_name = f"{inviter.first_name or ''} {inviter.last_name or ''}".strip() or "Someone"
    send_notification(
        user_id=invitee_id,
        title="Party Invite",
        body=f"{inviter_name} invited you to join their party for {party.sport} ({party.match_format.replace('_', ' ')})",
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
    if party.match_format == "mixed_doubles":
        existing_member_ids = [str(member.user_id) for member in party.members]
        _require_mixed_doubles_team(db, existing_member_ids + [user_id])

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

    invitee = db.query(Profile).filter(Profile.id == user_id).first()
    invitee_name = "Your partner"
    if invitee:
        invitee_name = f"{invitee.first_name or ''} {invitee.last_name or ''}".strip() or "Your partner"
    send_notification(
        user_id=str(inv.inviter_id),
        title="Party Accepted",
        body=f"{invitee_name} accepted your party invite! You are now ready to queue.",
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
    invitee_name = "Your invitee"
    if invitee:
        invitee_name = f"{invitee.first_name or ''} {invitee.last_name or ''}".strip() or "Your invitee"
    send_notification(
        user_id=str(inv.inviter_id),
        title="Party Declined",
        body=f"{invitee_name} declined your party invite.",
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
    body: Optional[PartyQueueRequest] = None,
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

    preferred_club_id = None
    if body and body.preferred_club_id:
        club_uuid = body.preferred_club_id
        membership = db.query(ClubMember).filter(
            ClubMember.club_id == club_uuid,
            ClubMember.user_id == user_id,
        ).first()
        if not membership:
            raise HTTPException(403, "You must belong to the selected club before starting a party match there.")
        preferred_club_id = club_uuid

    member_ids = [str(m.user_id) for m in party.members]
    if len(member_ids) != 2:
        raise HTTPException(400, "Party must have exactly 2 members to queue")
    if party.match_format == "mixed_doubles":
        _require_mixed_doubles_team(db, member_ids)

    queue_mode = _party_queue_mode(body.match_mode if body else None)
    queue_match_type = _party_queue_match_type(preferred_club_id, queue_mode)
    if queue_match_type == "ranked":
        _require_ranked_party_eligibility(db, member_ids, party.sport, party.match_format)

    # Check neither member is already in another queue
    for mid in member_ids:
        in_q = db.query(Match).filter(
            Match.match_type.in_(["queue", "ranked"]),
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

    def queue_party_without_match(message: str = "In queue - waiting for an opponent duo.") -> dict:
        new_match = Match(
            sport=party.sport,
            match_type=queue_match_type,
            match_format=party.match_format,
            status="assembling",
            club_id=preferred_club_id,
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

        leader_profile = db.query(Profile).filter(Profile.id == user_id).first()
        leader_name = f"{leader_profile.first_name or ''} {leader_profile.last_name or ''}".strip() if leader_profile else "Your leader"
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
            "message":  message,
        }

    # Look for an existing assembling match that has slot for a full team
    # (team2_player1 is None — the opposing team slot is open)
    candidate = db.query(Match).filter(
        Match.match_type == queue_match_type,
        Match.sport == party.sport,
        Match.match_format == party.match_format,
        Match.status == "assembling",
        Match.team1_player1.isnot(None),
        Match.team1_player2.isnot(None),
        Match.team2_player1.is_(None),
        Match.club_id == preferred_club_id if preferred_club_id else Match.club_id.is_(None),
    ).first()

    if candidate and party.match_format == "mixed_doubles":
        existing_team_ids = [
            str(pid)
            for pid in [candidate.team1_player1, candidate.team1_player2]
            if pid is not None
        ]
        if not _is_mixed_doubles_team_ids(db, existing_team_ids):
            candidate = None

    if candidate:
        # Remember Team 1 player IDs before overwriting
        team1_p1 = str(candidate.team1_player1) if candidate.team1_player1 else None
        team1_p2 = str(candidate.team1_player2) if candidate.team1_player2 else None
        team_score = None
        if queue_match_type == "ranked":
            team_score = _ranked_party_team_score(
                db,
                incoming_ids=[p1_id, p2_id],
                existing_ids=[pid for pid in [team1_p1, team1_p2] if pid],
                sport=party.sport,
                match_format=party.match_format,
            )
            if team_score <= 0.0:
                return queue_party_without_match(
                    "Your duo is queued and still searching for a compatible opponent team.",
                )

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
        setattr(candidate, "called_at",     dt.datetime.now(timezone.utc))
        setattr(candidate, "party_id",      party.id)
        if team_score is not None:
            setattr(candidate, "ml_match_score", team_score)
        if preferred_club_id:
            setattr(candidate, "club_id", preferred_club_id)
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
        match_type=queue_match_type,
        match_format=party.match_format,
        status="assembling",
        club_id=preferred_club_id,
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
    leader_name = f"@{getattr(leader_profile, 'username', None)}" if leader_profile and getattr(leader_profile, 'username', None) else "Your leader"
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
    leader_name = f"@{getattr(leader_profile, 'username', None)}" if leader_profile and getattr(leader_profile, 'username', None) else "Your leader"
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
    leader_name = "Your partner"
    if leader:
        leader_name = f"{leader.first_name or ''} {leader.last_name or ''}".strip() or "Your partner"
    for mid in member_ids:
        if mid != user_id:
            send_notification(
                user_id=mid,
                title="Party Disbanded",
                body=f"{leader_name} has disbanded the party.",
                notif_type="party_disbanded",
                reference_id=str(party.id),
            )

    party.status = "disbanded"
    db.commit()
    return {"message": "Party disbanded"}
