from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import or_
from app.database import get_db
from app.middleware.auth import get_current_user
from app.models.models import Friendship, Profile, ClubCheckin
from app.services.notifications import send_notification
from datetime import datetime, timezone, timedelta

router = APIRouter()


def _friendship_user_ids(f: Friendship, current_id: str) -> str:
    """Return the OTHER user's id from a friendship row."""
    return str(f.addressee_id) if str(f.requester_id) == current_id else str(f.requester_id)


def _profile_summary(p: Profile) -> dict:
    return {
        "id":         str(p.id),
        "username":   p.username,
        "first_name": p.first_name,
        "last_name":  p.last_name,
        "avatar_url": p.avatar_url,
    }


# ── Send friend request ───────────────────────────────────────────────────────

@router.post("/request/{target_id}", status_code=201)
def send_friend_request(
    target_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]
    if user_id == target_id:
        raise HTTPException(400, "Cannot add yourself.")

    target = db.query(Profile).filter(Profile.id == target_id).first()
    if not target:
        raise HTTPException(404, "Player not found.")

    existing = db.query(Friendship).filter(
        or_(
            (Friendship.requester_id == user_id) & (Friendship.addressee_id == target_id),
            (Friendship.requester_id == target_id) & (Friendship.addressee_id == user_id),
        )
    ).first()

    if existing:
        if str(existing.status) == "accepted":
            raise HTTPException(400, "Already friends.")
        if str(existing.status) == "pending":
            raise HTTPException(400, "Request already sent.")

    new_friendship = Friendship(requester_id=user_id, addressee_id=target_id, status="pending")
    db.add(new_friendship)
    db.commit()
    db.refresh(new_friendship)

    sender = db.query(Profile).filter(Profile.id == user_id).first()
    sender_name = sender.username if sender else "Someone"
    send_notification(
        user_id=target_id,
        title="New Friend Request",
        body=f"@{sender_name} sent you a friend request.",
        notif_type="friend_request",
        reference_id=str(new_friendship.id),
    )

    return {"message": "Friend request sent."}


# ── Accept request ────────────────────────────────────────────────────────────

@router.post("/{friendship_id}/accept")
def accept_friend_request(
    friendship_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]
    f = db.query(Friendship).filter(Friendship.id == friendship_id).first()
    if not f:
        raise HTTPException(404, "Request not found.")
    if str(f.addressee_id) != user_id:
        raise HTTPException(403, "Not your request to accept.")
    if str(f.status) != "pending":
        raise HTTPException(400, "Request is not pending.")

    setattr(f, "status", "accepted")
    db.commit()
    return {"message": "Friend request accepted."}


# ── Decline / unfriend ────────────────────────────────────────────────────────

@router.delete("/{friendship_id}")
def remove_friend(
    friendship_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]
    f = db.query(Friendship).filter(Friendship.id == friendship_id).first()
    if not f:
        raise HTTPException(404, "Not found.")
    if user_id not in (str(f.requester_id), str(f.addressee_id)):
        raise HTTPException(403, "Not your friendship.")

    db.delete(f)
    db.commit()
    return {"message": "Removed."}


# ── List accepted friends ─────────────────────────────────────────────────────

@router.get("")
def get_friends(
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]
    friendships = db.query(Friendship).filter(
        or_(Friendship.requester_id == user_id, Friendship.addressee_id == user_id),
        Friendship.status == "accepted",
    ).all()

    result = []
    for f in friendships:
        other_id = _friendship_user_ids(f, user_id)
        profile  = db.query(Profile).filter(Profile.id == other_id).first()
        if profile:
            result.append({
                "friendship_id": str(f.id),
                "since":         str(f.created_at),
                **_profile_summary(profile),
            })

    return {"friends": result, "count": len(result)}


# ── Pending incoming requests ─────────────────────────────────────────────────

@router.get("/requests")
def get_friend_requests(
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]
    pending = db.query(Friendship).filter(
        Friendship.addressee_id == user_id,
        Friendship.status == "pending",
    ).order_by(Friendship.created_at.desc()).all()

    result = []
    for f in pending:
        profile = db.query(Profile).filter(Profile.id == f.requester_id).first()
        if profile:
            result.append({
                "friendship_id": str(f.id),
                "sent_at":       str(f.created_at),
                **_profile_summary(profile),
            })

    return {"requests": result, "count": len(result)}


# ── Nearby players (same club check-in, active within 6 hours) ───────────────

@router.get("/nearby")
def get_nearby_players(
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id  = current_user["id"]
    cutoff   = datetime.now(timezone.utc) - timedelta(hours=6)

    # Find clubs the current user is checked into
    my_checkins = db.query(ClubCheckin).filter(
        ClubCheckin.user_id == user_id,
        ClubCheckin.status == "present",
        ClubCheckin.checked_in_at >= cutoff,
    ).all()

    if not my_checkins:
        return {"nearby": [], "club_ids": [], "message": "Check in to a club to see nearby players."}

    my_club_ids = [str(c.club_id) for c in my_checkins]

    # Find other players checked into the same clubs
    others = db.query(ClubCheckin).filter(
        ClubCheckin.club_id.in_(my_club_ids),
        ClubCheckin.user_id != user_id,
        ClubCheckin.status == "present",
        ClubCheckin.checked_in_at >= cutoff,
    ).all()

    seen: set = set()
    result = []
    for c in others:
        pid = str(c.user_id)
        if pid in seen:
            continue
        seen.add(pid)
        profile = db.query(Profile).filter(Profile.id == pid).first()
        if profile:
            result.append({
                "club_id":      str(c.club_id),
                "checked_in_at": str(c.checked_in_at),
                **_profile_summary(profile),
            })

    return {"nearby": result, "count": len(result), "club_ids": my_club_ids}
