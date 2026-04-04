import logging
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, timezone, timedelta
from sqlalchemy.orm import Session
from app.database import get_db
from app.middleware.auth import get_current_user
from app.models.models import (
    Match, MatchStatus, Profile, UserRoleModel, ClubCheckin,
    RefereeInvite, RefereeOpenRequest, Notification, Court, Party, MatchHistory
)
from app.services.notifications import send_notification, send_bulk_notifications
from app.services.broadcast import broadcast_match

router = APIRouter()
logger = logging.getLogger(__name__)

INVITE_EXPIRY_MINUTES = 10

# ── Request Models ────────────────────────────────────────

class DirectInviteRequest(BaseModel):
    match_id:     str
    invited_user: str
    message:      Optional[str] = None

class RespondInviteRequest(BaseModel):
    response: str  # 'accepted' | 'declined'

class OpenRefereeRequest(BaseModel):
    match_id: str
    club_id:  str


# ── Direct Invite ─────────────────────────────────────────

@router.post("/referee/invite", status_code=201)
def send_referee_invite(
    data: DirectInviteRequest,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    invited_by = current_user["id"]

    if invited_by == data.invited_user:
        raise HTTPException(400, "You cannot invite yourself as referee.")

    match = db.query(Match).filter(Match.id == data.match_id).first()
    if not match:
        raise HTTPException(404, "Match not found.")
    if match.referee_id is not None:
        raise HTTPException(400, "This match already has a referee assigned.")
    if match.status.value not in ("pending", "ongoing"):
        raise HTTPException(400, "Cannot assign a referee to this match.")

    # Prevent the same user being invited twice for the same match
    already = db.query(RefereeInvite).filter(
        RefereeInvite.match_id == data.match_id,
        RefereeInvite.invited_user == data.invited_user,
        RefereeInvite.status == "pending",
    ).first()
    if already:
        raise HTTPException(400, "This user already has a pending invite for this match.")

    expires_at = datetime.now(timezone.utc) + timedelta(minutes=INVITE_EXPIRY_MINUTES)

    invite = RefereeInvite(
        match_id=data.match_id,
        invited_by=invited_by,
        invited_user=data.invited_user,
        status="pending",
        expires_at=expires_at,
    )
    db.add(invite)
    db.commit()

    inviter = db.query(Profile).filter(Profile.id == invited_by).first()
    inviter_name = f"{inviter.first_name} {inviter.last_name}" if inviter else "A player"
    invitee = db.query(Profile).filter(Profile.id == data.invited_user).first()
    invitee_username = str(invitee.username) if invitee else "someone"

    send_notification(
        user_id=data.invited_user,
        title="Referee Invite 🟡",
        body=(
            f"{inviter_name} is inviting you to referee their "
            f"{match.sport.value.replace('_', ' ').title()} {match.match_format.value} match. "
            f"This invite expires in {INVITE_EXPIRY_MINUTES} minutes."
            + (f" Message: {data.message}" if data.message else "")
        ),
        notif_type="referee_invite",
        reference_id=str(invite.id),
    )

    # Broadcast to all match participants so their consoles update in real-time
    broadcast_match(str(data.match_id), {
        "type":              "referee_invite_sent",
        "invite_id":         str(invite.id),
        "invited_by":        invited_by,
        "invited_by_name":   inviter_name,
        "invited_user":      data.invited_user,
        "invited_username":  invitee_username,
        "expires_at":        expires_at.isoformat(),
    })

    return {
        "message": f"Referee invite sent. Expires in {INVITE_EXPIRY_MINUTES} minutes.",
        "invite_id": str(invite.id),
        "expires_at": expires_at.isoformat(),
    }


@router.post("/referee/invite/{invite_id}/respond")
def respond_to_invite(
    invite_id: str,
    data: RespondInviteRequest,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]

    if data.response not in ("accepted", "declined"):
        raise HTTPException(400, "Response must be 'accepted' or 'declined'.")

    invite = db.query(RefereeInvite).filter(RefereeInvite.id == invite_id).first()
    if not invite:
        raise HTTPException(404, "Invite not found.")
    if str(invite.invited_user) != user_id:
        raise HTTPException(403, "This invite was not sent to you.")
    if str(invite.status) != "pending":
        raise HTTPException(400, f"Invite is already {invite.status}.")

    # Check expiry
    if invite.expires_at is not None and datetime.now(timezone.utc) > invite.expires_at:  # type: ignore[operator]
        setattr(invite, "status", "expired")
        db.commit()
        raise HTTPException(400, "This invite has expired.")

    setattr(invite, "status", data.response)
    setattr(invite, "responded_at", datetime.now(timezone.utc))

    # Mark the corresponding referee_invite notification as read and update its type
    # so the Accept/Decline buttons don't reappear after fetchNotifications() is called.
    notif = db.query(Notification).filter(
        Notification.user_id == user_id,
        Notification.type == "referee_invite",
        Notification.data["reference_id"].astext == invite_id,
    ).first()
    if notif:
        new_type = "referee_accepted" if data.response == "accepted" else "referee_declined"
        setattr(notif, "type", new_type)
        setattr(notif, "is_read", True)

    db.commit()

    responder = db.query(Profile).filter(Profile.id == user_id).first()
    responder_name = f"{responder.first_name} {responder.last_name}" if responder else "The player"

    if data.response == "accepted":
        # Assign referee to match
        match = db.query(Match).filter(Match.id == invite.match_id).first()
        if match:
            setattr(match, "referee_id", user_id)

        # Add referee role if not already assigned
        existing_role = db.query(UserRoleModel).filter(
            UserRoleModel.user_id == user_id,
            UserRoleModel.role == "referee",
        ).first()
        if not existing_role:
            db.add(UserRoleModel(user_id=user_id, role="referee"))

        # Update checkin status
        checkin = db.query(ClubCheckin).filter(
            ClubCheckin.user_id == user_id,
            ClubCheckin.status != "checked_out",
        ).first()
        if checkin:
            setattr(checkin, "status", "playing")

        # Cancel all other pending invites for this match (race condition: first accept wins)
        other_pending = db.query(RefereeInvite).filter(
            RefereeInvite.match_id == invite.match_id,
            RefereeInvite.id != invite.id,
            RefereeInvite.status == "pending",
        ).all()
        cancelled_ids = []
        for other in other_pending:
            setattr(other, "status", "cancelled")
            cancelled_ids.append(str(other.id))

        db.commit()

        send_notification(
            user_id=str(invite.invited_by),
            title="Referee Accepted ✅",
            body=f"{responder_name} has accepted to referee your match!",
            notif_type="referee_accepted",
            reference_id=str(invite.match_id),
        )

        broadcast_match(str(invite.match_id), {
            "type":              "referee_assigned",
            "referee_id":        user_id,
            "referee_username":  str(responder.username) if responder else "",
            "invite_id":         str(invite.id),
            "cancelled_invites": cancelled_ids,
        })

        broadcast_match(str(invite.match_id), {
            "type":    "match_announcement",
            "message": f"🏅 {responder_name} has joined as referee. Let the game begin!",
        })

        return {
            "message": "You are now assigned as referee for this match.",
            "match_id": str(invite.match_id),
            "referee_id": user_id,
        }

    else:
        send_notification(
            user_id=str(invite.invited_by),
            title="Referee Declined ❌",
            body=f"{responder_name} declined your referee invite. You can invite someone else.",
            notif_type="referee_declined",
            reference_id=str(invite.match_id),
        )

        broadcast_match(str(invite.match_id), {
            "type":      "referee_invite_declined",
            "invite_id": str(invite.id),
            "declined_username": str(responder.username) if responder else "",
        })

        return {"message": "Invite declined. The match organizer has been notified."}


@router.get("/referee/invite/{invite_id}")
def get_invite(
    invite_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    invite = db.query(RefereeInvite).filter(RefereeInvite.id == invite_id).first()
    if not invite:
        raise HTTPException(404, "Invite not found.")
    return {
        "invite": {
            "id": str(invite.id),
            "match_id": str(invite.match_id),
            "invited_by": str(invite.invited_by),
            "invited_user": str(invite.invited_user),
            "status": invite.status,
            "expires_at": str(invite.expires_at) if invite.expires_at is not None else None,
        }
    }


# ── Pending invites for a match ──────────────────────────

@router.get("/matches/{match_id}/referee-invites")
def get_match_referee_invites(
    match_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Returns all non-expired pending/declined/cancelled referee invites for a match."""
    now = datetime.now(timezone.utc)

    invites = db.query(RefereeInvite).filter(
        RefereeInvite.match_id == match_id,
        RefereeInvite.status.in_(["pending", "declined", "cancelled"]),
    ).order_by(RefereeInvite.id.asc()).all()

    # Batch-fetch profiles for inviters and invitees
    user_ids = set()
    for inv in invites:
        user_ids.add(str(inv.invited_by))
        user_ids.add(str(inv.invited_user))
    profiles = db.query(Profile).filter(Profile.id.in_(list(user_ids))).all()
    prof_map: dict = {str(p.id): p for p in profiles}

    def _name(uid: str) -> str:
        p = prof_map.get(uid)
        if not p:
            return uid[:8]
        return f"{p.first_name} {p.last_name}".strip() or str(p.username)  # type: ignore[arg-type]

    def _username(uid: str) -> str:
        p = prof_map.get(uid)
        return str(p.username) if p else uid[:8]

    result = []
    for inv in invites:
        # Auto-expire stale pending invites in the response (don't mutate DB here)
        status = str(inv.status)
        if status == "pending" and inv.expires_at is not None:
            if now > inv.expires_at:  # type: ignore[operator]
                status = "expired"
        result.append({
            "invite_id":         str(inv.id),
            "invited_by":        str(inv.invited_by),
            "invited_by_name":   _name(str(inv.invited_by)),
            "invited_user":      str(inv.invited_user),
            "invited_username":  _username(str(inv.invited_user)),
            "status":            status,
            "expires_at":        inv.expires_at.isoformat() if inv.expires_at is not None else None,
        })

    return {"invites": result}


# ── Open Referee Request ──────────────────────────────────

@router.post("/referee/open-request", status_code=201)
def post_open_referee_request(
    data: OpenRefereeRequest,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]

    match = db.query(Match).filter(Match.id == data.match_id).first()
    if not match:
        raise HTTPException(404, "Match not found.")
    if match.referee_id is not None:
        raise HTTPException(400, "Match already has a referee.")

    existing = db.query(RefereeOpenRequest).filter(
        RefereeOpenRequest.match_id == data.match_id,
        RefereeOpenRequest.status == "open",
    ).first()
    if existing:
        raise HTTPException(400, "An open referee request already exists for this match.")

    req = RefereeOpenRequest(
        match_id=data.match_id,
        club_id=data.club_id,
        posted_by=user_id,
        status="open",
    )
    db.add(req)
    db.commit()

    # Notify present club members
    checkins = db.query(ClubCheckin).filter(
        ClubCheckin.club_id == data.club_id,
        ClubCheckin.status.in_(["present", "available_to_ref"]),
    ).all()

    present_ids = [str(c.user_id) for c in checkins if str(c.user_id) != user_id]

    if present_ids:
        send_bulk_notifications(
            user_ids=present_ids,
            title="Referee Needed 🟡",
            body=(
                f"A {match.sport.value.replace('_', ' ').title()} {match.match_format.value} "
                f"match needs a referee. Can you help?"
            ),
            notif_type="referee_request",
            reference_id=str(req.id),
        )

    return {
        "message": f"Open referee request posted. {len(present_ids)} members notified.",
        "request_id": str(req.id),
        "notified": len(present_ids),
    }


@router.post("/referee/open-request/{request_id}/volunteer")
def volunteer_as_referee(
    request_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]

    req = db.query(RefereeOpenRequest).filter(RefereeOpenRequest.id == request_id).first()
    if not req:
        raise HTTPException(404, "Request not found.")
    if str(req.status) != "open":
        raise HTTPException(400, f"This request is already {req.status}.")
    if str(req.posted_by) == user_id:
        raise HTTPException(400, "You cannot volunteer to referee your own match.")

    match = db.query(Match).filter(Match.id == req.match_id).first()
    if match:
        setattr(match, "referee_id", user_id)

    setattr(req, "status", "filled")
    setattr(req, "filled_by", user_id)

    existing_role = db.query(UserRoleModel).filter(
        UserRoleModel.user_id == user_id,
        UserRoleModel.role == "referee",
    ).first()
    if not existing_role:
        db.add(UserRoleModel(user_id=user_id, role="referee"))

    db.commit()

    volunteer = db.query(Profile).filter(Profile.id == user_id).first()
    vol_name = f"{volunteer.first_name} {volunteer.last_name}" if volunteer else "A player"

    send_notification(
        user_id=str(req.posted_by),
        title="Referee Found ✅",
        body=f"{vol_name} has volunteered to referee your match!",
        notif_type="referee_request",
        reference_id=str(req.match_id),
    )

    return {
        "message": "You are now assigned as referee for this match.",
        "match_id": str(req.match_id),
        "referee_id": user_id,
    }


@router.get("/referee/open-requests/{club_id}")
def get_open_requests(
    club_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    requests = db.query(RefereeOpenRequest).filter(
        RefereeOpenRequest.club_id == club_id,
        RefereeOpenRequest.status == "open",
    ).order_by(RefereeOpenRequest.created_at.desc()).all()

    return {"open_requests": [
        {
            "id": str(r.id),
            "match_id": str(r.match_id),
            "posted_by": str(r.posted_by),
            "status": r.status,
            "created_at": str(r.created_at),
        } for r in requests
    ]}


# ── Referee Dashboard ─────────────────────────────────────

@router.get("/referee/my-invites")
def get_my_referee_invites(
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]

    invites = db.query(RefereeInvite).filter(
        RefereeInvite.invited_user == user_id,
        RefereeInvite.status == "pending",
    ).order_by(RefereeInvite.created_at.desc()).all()

    result = []
    for inv in invites:
        match = db.query(Match).filter(Match.id == inv.match_id).first()
        inviter = db.query(Profile).filter(Profile.id == inv.invited_by).first()
        if not match:
            continue
        name = ""
        if inviter:
            name = f"{inviter.first_name or ''} {inviter.last_name or ''}".strip() or inviter.username or "A player"
        result.append({
            "id": str(inv.id),
            "match_id": str(inv.match_id),
            "sport": match.sport.value,
            "match_format": match.match_format.value,
            "match_type": match.match_type.value,
            "match_status": match.status.value,
            "invited_by_id": str(inv.invited_by),
            "invited_by_username": inviter.username if inviter else None,
            "invited_by_name": name or "A player",
            "expires_at": inv.expires_at.isoformat() if inv.expires_at is not None else None,
            "created_at": str(inv.created_at),
        })

    return {"invites": result, "count": len(result)}


@router.get("/referee/my-matches")
def get_my_referee_matches(
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]

    matches = db.query(Match).filter(
        Match.referee_id == user_id
    ).order_by(Match.created_at.desc()).all()

    def fmt(m):
        return {
            "id": str(m.id), "sport": m.sport.value,
            "match_format": m.match_format.value,
            "match_type": m.match_type.value,
            "status": m.status.value,
            "scheduled_at": str(m.scheduled_at) if m.scheduled_at else None,
            "started_at": str(m.started_at) if m.started_at else None,
            "completed_at": str(m.completed_at) if m.completed_at else None,
        }

    return {
        "upcoming":  [fmt(m) for m in matches if m.status.value == "pending"],
        "ongoing":   [fmt(m) for m in matches if m.status.value == "ongoing"],
        "completed": [fmt(m) for m in matches if m.status.value == "completed"],
        "total":     len(matches),
    }


# ── Referee Leave ─────────────────────────────────────────

@router.post("/referee/{match_id}/leave")
def referee_leave_match(
    match_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Called when the assigned referee voluntarily leaves a pending or ongoing match.
    The match is immediately invalidated and all players are notified.
    """
    user_id = current_user["id"]

    match = db.query(Match).filter(Match.id == match_id).first()
    if not match:
        raise HTTPException(status_code=404, detail="Match not found.")
    if match.referee_id is None or str(match.referee_id) != user_id:
        raise HTTPException(status_code=403, detail="You are not the referee for this match.")
    if match.status.value not in ("pending", "assembling", "ongoing"):
        raise HTTPException(status_code=400, detail="Match is not in an active state.")

    # Invalidate the match
    setattr(match, "status", MatchStatus.invalidated)

    # Invalidated matches should not retain user-facing timelines.
    db.query(MatchHistory).filter(MatchHistory.match_id == match_id).delete(synchronize_session=False)

    # Release the court if one was assigned
    if match.court_id is not None:
        court = db.query(Court).filter(Court.id == match.court_id).first()
        if court is not None:
            setattr(court, "status", "available")

    db.flush()

    # Collect all player IDs to notify
    player_ids = [
        str(pid) for pid in [
            match.player1_id, match.player2_id,
            match.player3_id, match.player4_id,
        ]
        if pid is not None and str(pid) != user_id
    ]

    referee = db.query(Profile).filter(Profile.id == user_id).first()
    ref_name = f"{referee.first_name} {referee.last_name}".strip() if referee else "The referee"

    phase = "live match" if match.status.value == "ongoing" else "lobby"

    if player_ids:
        send_bulk_notifications(
            user_ids    = player_ids,
            notif_type  = "referee_left",
            title       = "Match Invalidated",
            body        = f"{ref_name} left the {phase}. The match has been invalidated.",
            reference_id= match_id,
        )

    # Disband any party whose match_id points to this now-invalidated match
    linked_parties = db.query(Party).filter(Party.match_id == match.id).all()
    for linked_party in linked_parties:
        setattr(linked_party, "status", "disbanded")
        setattr(linked_party, "match_id", None)

    db.commit()

    # Broadcast so the match page updates in real-time
    broadcast_match(match_id, {"type": "match_invalidated", "reason": "referee_left"})

    return {"message": "You have left the match. The match has been invalidated."}


# ── Notifications ─────────────────────────────────────────

@router.get("/notifications")
def get_notifications(
    current_user: dict = Depends(get_current_user),
    limit: int = 20,
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]

    notifications = db.query(Notification).filter(
        Notification.user_id == user_id
    ).order_by(Notification.created_at.desc()).limit(limit).all()

    unread_count = db.query(Notification).filter(
        Notification.user_id == user_id,
        Notification.is_read == False,
    ).count()

    return {
        "notifications": [
            {
                "id": str(n.id), "type": n.type,
                "title": n.title, "body": n.body,
                "is_read": n.is_read, "data": n.data,
                "reference_id": (n.data or {}).get("reference_id") if isinstance(n.data, dict) else None,
                "created_at": str(n.created_at),
            } for n in notifications
        ],
        "unread_count": unread_count,
    }


@router.put("/notifications/{notif_id}/read")
def mark_read(
    notif_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]

    notif = db.query(Notification).filter(
        Notification.id == notif_id,
        Notification.user_id == user_id,
    ).first()

    if notif:
        setattr(notif, "is_read", True)
        db.commit()

    return {"message": "Marked as read."}


@router.put("/notifications/read-all")
def mark_all_read(
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]

    db.query(Notification).filter(
        Notification.user_id == user_id,
        Notification.is_read == False,
    ).update({"is_read": True})

    db.commit()
    return {"message": "All notifications marked as read."}
