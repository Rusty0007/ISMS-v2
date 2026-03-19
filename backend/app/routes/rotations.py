from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional
from sqlalchemy.orm import Session
from app.database import get_db
from app.middleware.auth import get_current_user
from app.models.models import CourtRotation, CourtRotationMember, Profile

router = APIRouter()

VALID_SPORTS  = ["pickleball", "badminton", "lawn_tennis", "table_tennis"]
VALID_FORMATS = ["singles", "doubles"]


# ── Request models ────────────────────────────────────────────────────────────

class CreateRotationRequest(BaseModel):
    sport:    str
    format:   str = "singles"
    club_id:  Optional[str] = None
    court_id: Optional[str] = None


class AddMemberRequest(BaseModel):
    display_name: str
    user_id:      Optional[str] = None


class AdvanceRequest(BaseModel):
    winner_ids: list[str]   # list of CourtRotationMember IDs


# ── Helpers ───────────────────────────────────────────────────────────────────

def _member_dict(m: CourtRotationMember) -> dict:
    return {
        "id":             str(m.id),
        "user_id":        str(m.user_id) if m.user_id else None,
        "display_name":   m.display_name,
        "queue_position": m.queue_position,
        "games_played":   m.games_played,
        "wins":           m.wins,
        "joined_at":      str(m.joined_at),
    }


def _rotation_dict(r: CourtRotation) -> dict:
    court_size = 2 if r.format == "singles" else 4
    return {
        "id":         str(r.id),
        "sport":      r.sport,
        "format":     r.format,
        "club_id":    str(r.club_id)  if r.club_id  else None,
        "court_id":   str(r.court_id) if r.court_id else None,
        "created_by": str(r.created_by),
        "status":     r.status,
        "court_size": court_size,
        "created_at": str(r.created_at),
        "members":    [_member_dict(m) for m in r.members],
    }


def _repack_positions(members: list) -> None:
    """Reassign queue_position 1..N in the given ordered list."""
    for i, m in enumerate(members, start=1):
        setattr(m, "queue_position", i)


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.post("", status_code=201)
def create_rotation(
    data: CreateRotationRequest,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if data.sport not in VALID_SPORTS:
        raise HTTPException(400, "Invalid sport.")
    if data.format not in VALID_FORMATS:
        raise HTTPException(400, "Invalid format. Use 'singles' or 'doubles'.")

    rotation = CourtRotation(
        sport=data.sport,
        format=data.format,
        club_id=data.club_id,
        court_id=data.court_id,
        created_by=current_user["id"],
        status="active",
    )
    db.add(rotation)
    db.commit()
    db.refresh(rotation)
    return {"rotation_id": str(rotation.id), "message": "Rotation session created."}


@router.get("/mine")
def get_my_rotations(
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    rotations = db.query(CourtRotation).filter(
        CourtRotation.created_by == current_user["id"],
        CourtRotation.status == "active",
    ).order_by(CourtRotation.created_at.desc()).all()
    return {"rotations": [_rotation_dict(r) for r in rotations]}


@router.get("/{rotation_id}")
def get_rotation(
    rotation_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    rotation = db.query(CourtRotation).filter(CourtRotation.id == rotation_id).first()
    if not rotation:
        raise HTTPException(404, "Rotation not found.")
    return _rotation_dict(rotation)


@router.post("/{rotation_id}/members", status_code=201)
def add_member(
    rotation_id: str,
    data: AddMemberRequest,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    rotation = db.query(CourtRotation).filter(CourtRotation.id == rotation_id).first()
    if not rotation:
        raise HTTPException(404, "Rotation not found.")
    if rotation.status != "active":
        raise HTTPException(400, "Rotation session has ended.")

    # If user_id provided, resolve display_name from profile
    display_name = data.display_name
    user_id = data.user_id
    if user_id:
        profile = db.query(Profile).filter(Profile.id == user_id).first()
        if not profile:
            raise HTTPException(404, "Player not found.")
        display_name = profile.username

    # Check duplicate user
    if user_id:
        dup = next((m for m in rotation.members if m.user_id and str(m.user_id) == user_id), None)
        if dup:
            raise HTTPException(400, "Player is already in this rotation.")

    next_pos = len(rotation.members) + 1
    member = CourtRotationMember(
        rotation_id=rotation_id,
        user_id=user_id,
        display_name=display_name,
        queue_position=next_pos,
    )
    db.add(member)
    db.commit()
    db.refresh(rotation)
    return {"message": "Player added.", "member_id": str(member.id), "queue_position": next_pos}


@router.delete("/{rotation_id}/members/{member_id}")
def remove_member(
    rotation_id: str,
    member_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    rotation = db.query(CourtRotation).filter(CourtRotation.id == rotation_id).first()
    if not rotation:
        raise HTTPException(404, "Rotation not found.")
    if str(rotation.created_by) != current_user["id"]:
        raise HTTPException(403, "Only the session creator can remove players.")

    member = db.query(CourtRotationMember).filter(
        CourtRotationMember.id == member_id,
        CourtRotationMember.rotation_id == rotation_id,
    ).first()
    if not member:
        raise HTTPException(404, "Member not found.")

    db.delete(member)
    db.flush()

    # Re-fetch and repack remaining members
    remaining = db.query(CourtRotationMember).filter(
        CourtRotationMember.rotation_id == rotation_id,
    ).order_by(CourtRotationMember.queue_position).all()
    _repack_positions(remaining)
    db.commit()
    return {"message": "Player removed."}


@router.post("/{rotation_id}/advance")
def advance_rotation(
    rotation_id: str,
    data: AdvanceRequest,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    rotation = db.query(CourtRotation).filter(CourtRotation.id == rotation_id).first()
    if not rotation:
        raise HTTPException(404, "Rotation not found.")
    if str(rotation.created_by) != current_user["id"]:
        raise HTTPException(403, "Only the session creator can advance the rotation.")
    if rotation.status != "active":
        raise HTTPException(400, "Rotation session has ended.")

    members = list(rotation.members)  # already sorted by queue_position
    if len(members) < 2:
        raise HTTPException(400, "Need at least 2 players to advance.")

    court_size = 2 if rotation.format == "singles" else 4
    playing    = members[:court_size]
    waiting    = members[court_size:]
    playing_ids = {str(m.id) for m in playing}

    # Validate winner_ids are all among on-court players
    for wid in data.winner_ids:
        if wid not in playing_ids:
            raise HTTPException(400, f"Winner {wid} is not currently on court.")

    winners = [m for m in playing if str(m.id) in set(data.winner_ids)]
    losers  = [m for m in playing if str(m.id) not in set(data.winner_ids)]

    # Increment stats for on-court players
    for m in playing:
        setattr(m, "games_played", m.games_played + 1)
    for m in winners:
        setattr(m, "wins", m.wins + 1)

    # New order: winners → waiting → losers
    new_order = winners + waiting + losers
    _repack_positions(new_order)
    db.commit()

    db.refresh(rotation)
    return {"message": "Rotation advanced.", "rotation": _rotation_dict(rotation)}


@router.delete("/{rotation_id}")
def end_rotation(
    rotation_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    rotation = db.query(CourtRotation).filter(CourtRotation.id == rotation_id).first()
    if not rotation:
        raise HTTPException(404, "Rotation not found.")
    if str(rotation.created_by) != current_user["id"]:
        raise HTTPException(403, "Only the session creator can end the session.")

    setattr(rotation, "status", "ended")
    db.commit()
    return {"message": "Rotation session ended."}
