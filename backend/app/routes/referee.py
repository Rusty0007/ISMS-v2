import logging
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, timezone, timedelta
from sqlalchemy.orm import Session
from app.database import get_db
from app.middleware.auth import get_current_user
from app.models.models import (
    Match, Profile, UserRoleModel, ClubCheckin,
    RefereeInvite, RefereeOpenRequest, Notification
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

    # Check no pending invite already
    pending = db.query(RefereeInvite).filter(
        RefereeInvite.match_id == data.match_id,
        RefereeInvite.status == "pending",
    ).first()
    if pending:
        raise HTTPException(400, "There is already a pending referee invite for this match.")

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

        db.commit()

        send_notification(
            user_id=str(invite.invited_by),
            title="Referee Accepted ✅",
            body=f"{responder_name} has accepted to referee your match!",
            notif_type="referee_invite",
            reference_id=str(invite.match_id),
        )

        broadcast_match(str(invite.match_id), {
            "type": "referee_assigned",
            "referee_id": user_id,
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
            notif_type="referee_invite",
            reference_id=str(invite.match_id),
        )
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