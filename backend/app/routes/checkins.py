from fastapi import APIRouter, HTTPException, Depends
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from app.database import get_db
from app.middleware.auth import get_current_user
from app.models.models import ClubCheckin, Club

router = APIRouter()


@router.post("/clubs/{club_id}/checkin", status_code=201)
def checkin(
    club_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]

    # Already checked in?
    existing = db.query(ClubCheckin).filter(
        ClubCheckin.user_id == user_id,
        ClubCheckin.club_id == club_id,
        ClubCheckin.status != "checked_out",
    ).first()

    if existing:
        return {"message": "Already checked in.", "status": existing.status}

    # Club exists?
    club = db.query(Club).filter(Club.id == club_id).first()
    if not club:
        raise HTTPException(404, "Club not found.")

    checkin = ClubCheckin(user_id=user_id, club_id=club_id, status="present")
    db.add(checkin)
    db.commit()

    return {"message": f"Checked in to {club.name}.", "checkin_id": str(checkin.id)}


@router.post("/clubs/{club_id}/checkout")
def checkout(
    club_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]

    checkin = db.query(ClubCheckin).filter(
        ClubCheckin.user_id == user_id,
        ClubCheckin.club_id == club_id,
        ClubCheckin.status != "checked_out",
    ).first()

    if not checkin:
        raise HTTPException(400, "You are not checked in to this club.")

    setattr(checkin, "status", "checked_out")
    setattr(checkin, "checked_out_at", datetime.now(timezone.utc))
    db.commit()

    return {"message": "Checked out successfully."}


@router.put("/clubs/{club_id}/referee-available")
def set_referee_available(
    club_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]

    checkin = db.query(ClubCheckin).filter(
        ClubCheckin.user_id == user_id,
        ClubCheckin.club_id == club_id,
        ClubCheckin.status != "checked_out",
    ).first()

    if not checkin:
        raise HTTPException(400, "You must be checked in to this club first.")

    new_status = "available_to_ref" if str(checkin.status) != "available_to_ref" else "present"
    setattr(checkin, "status", new_status)
    db.commit()

    message = (
        "You are now visible as available to referee."
        if new_status == "available_to_ref"
        else "Referee availability removed."
    )
    return {"message": message, "status": new_status}


@router.put("/clubs/{club_id}/checkin-status")
def update_checkin_status(
    club_id: str,
    status: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]
    valid = ["present", "playing", "available_to_ref"]

    if status not in valid:
        raise HTTPException(400, f"Invalid status. Choose from: {valid}")

    checkin = db.query(ClubCheckin).filter(
        ClubCheckin.user_id == user_id,
        ClubCheckin.club_id == club_id,
        ClubCheckin.status != "checked_out",
    ).first()

    if not checkin:
        raise HTTPException(400, "You are not checked in to this club.")

    setattr(checkin, "status", status)
    db.commit()

    return {"message": f"Status updated to '{status}'.", "status": status}


@router.get("/clubs/{club_id}/present")
def get_present_members(
    club_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    checkins = db.query(ClubCheckin).filter(
        ClubCheckin.club_id == club_id,
        ClubCheckin.status != "checked_out",
    ).all()

    def fmt(c):
        return {
            "user_id": str(c.user_id),
            "status": c.status,
            "checked_in_at": str(c.checked_in_at),
        }

    return {
        "club_id":          club_id,
        "total_present":    len(checkins),
        "present":          [fmt(c) for c in checkins if str(c.status) == "present"],
        "playing":          [fmt(c) for c in checkins if str(c.status) == "playing"],
        "available_to_ref": [fmt(c) for c in checkins if str(c.status) == "available_to_ref"],
    }


@router.get("/clubs/{club_id}/my-checkin")
def my_checkin(
    club_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]

    checkin = db.query(ClubCheckin).filter(
        ClubCheckin.user_id == user_id,
        ClubCheckin.club_id == club_id,
        ClubCheckin.status != "checked_out",
    ).first()

    if not checkin:
        return {"checked_in": False, "status": None}

    return {
        "checked_in": True,
        "status": checkin.status,
        "checked_in_at": str(checkin.checked_in_at),
    }