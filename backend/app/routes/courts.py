from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, timezone, timedelta, date as date_type
from sqlalchemy.orm import Session
from app.database import get_db
from app.middleware.auth import get_current_user
from app.models.models import Club, Court, CourtBooking, ClubCheckin, Match, MatchSet, Profile, PlayerRating
from sqlalchemy import func
from app.services.notifications import send_notification

router = APIRouter()

# ── Request models ────────────────────────────────────────

class CreateCourtRequest(BaseModel):
    name:      str
    sport:     Optional[str] = None
    surface:   Optional[str] = None   # e.g. Wooden, Clay, Acrylic, Concrete, Modular Tiles
    is_indoor: Optional[bool] = True
    lighting:  Optional[str] = "good" # good | fair | poor
    capacity:  Optional[int] = None
    notes:     Optional[str] = None

class UpdateCourtRequest(BaseModel):
    name:      Optional[str] = None
    sport:     Optional[str] = None
    surface:   Optional[str] = None
    is_indoor: Optional[bool] = None
    lighting:  Optional[str] = None
    capacity:  Optional[int] = None
    notes:     Optional[str] = None
    status:    Optional[str] = None  # 'available' | 'reserved' | 'maintenance'

class BookCourtRequest(BaseModel):
    match_id:     str
    scheduled_at: datetime
    notes:        Optional[str] = None

class RentCourtRequest(BaseModel):
    scheduled_at:   datetime
    duration_hours: Optional[float] = 1.0
    notes:          Optional[str] = None


# ══════════════════════════════════════════════════════════
# CLUB ADMIN — COURT MANAGEMENT
# ══════════════════════════════════════════════════════════

@router.post("/clubs/{club_id}/courts", status_code=201)
def create_court(
    club_id: str,
    data: CreateCourtRequest,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]

    club = db.query(Club).filter(Club.id == club_id).first()
    if not club:
        raise HTTPException(404, "Club not found.")
    if str(club.admin_id) != user_id:
        raise HTTPException(403, "Only the club admin can manage courts.")

    court = Court(
        club_id=club_id,
        name=data.name,
        sport=data.sport,
        surface=data.surface,
        is_indoor=data.is_indoor if data.is_indoor is not None else True,
        lighting=data.lighting or "good",
        capacity=data.capacity,
        notes=data.notes,
        status="available",
    )
    db.add(court)
    db.commit()

    return {"message": "Court created.", "court": {
        "id": str(court.id), "name": court.name, "sport": court.sport,
        "surface": court.surface, "is_indoor": court.is_indoor,
        "lighting": court.lighting, "capacity": court.capacity, "status": court.status,
        "image_url": getattr(court, "image_url", None),
    }}


@router.get("/clubs/{club_id}/courts")
def get_club_courts(
    club_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    courts = db.query(Court).filter(Court.club_id == club_id).order_by(Court.name).all()

    return {"courts": [
        {
            "id": str(c.id), "name": c.name, "sport": c.sport,
            "surface": c.surface, "is_indoor": c.is_indoor,
            "lighting": c.lighting, "capacity": c.capacity,
            "notes": c.notes, "status": c.status,
            "image_url": getattr(c, "image_url", None),
        }
        for c in courts
    ]}


@router.put("/clubs/{club_id}/courts/{court_id}")
def update_court(
    club_id:  str,
    court_id: str,
    data: UpdateCourtRequest,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]

    club = db.query(Club).filter(Club.id == club_id).first()
    if not club or str(club.admin_id) != user_id:
        raise HTTPException(403, "Only the club admin can update courts.")

    court = db.query(Court).filter(Court.id == court_id, Court.club_id == club_id).first()
    if not court:
        raise HTTPException(404, "Court not found.")

    update_data = {k: v for k, v in data.model_dump().items() if v is not None}
    if not update_data:
        raise HTTPException(400, "Nothing to update.")

    for key, value in update_data.items():
        setattr(court, key, value)
    db.commit()

    return {"message": "Court updated."}


@router.delete("/clubs/{club_id}/courts/{court_id}")
def delete_court(
    club_id:  str,
    court_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]

    club = db.query(Club).filter(Club.id == club_id).first()
    if not club or str(club.admin_id) != user_id:
        raise HTTPException(403, "Only the club admin can delete courts.")

    court = db.query(Court).filter(Court.id == court_id, Court.club_id == club_id).first()
    if not court:
        raise HTTPException(404, "Court not found.")

    db.delete(court)
    db.commit()

    return {"message": "Court deleted."}


# ══════════════════════════════════════════════════════════
# LIVE COURT VIEW
# ══════════════════════════════════════════════════════════

@router.get("/clubs/{club_id}/courts/live")
def get_live_court_view(
    club_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    courts = db.query(Court).filter(Court.club_id == club_id).order_by(Court.name).all()
    all_court_ids = [c.id for c in courts]

    # Get ongoing/pending matches at any court in this club
    court_match_map = {}
    if all_court_ids:
        ongoing = db.query(Match).filter(
            Match.court_id.in_(all_court_ids),
            Match.status.in_(["ongoing", "pending"]),
        ).all()
        for match in ongoing:
            if match.court_id is not None:
                court_match_map[match.court_id] = match

    # Batch-fetch player + referee usernames for all live matches
    all_player_ids: set = set()
    all_referee_ids: set = set()
    for match in court_match_map.values():
        for pid in [match.player1_id, match.player2_id, match.player3_id, match.player4_id]:
            if pid is not None:
                all_player_ids.add(pid)
        if match.referee_id is not None:
            all_referee_ids.add(match.referee_id)

    all_profile_ids = all_player_ids | all_referee_ids
    profile_map: dict = {}   # id → username
    if all_profile_ids:
        profiles = db.query(Profile).filter(Profile.id.in_(list(all_profile_ids))).all()
        profile_map = {str(p.id): p.username for p in profiles}

    # Batch-fetch total matches_played per player (summed across all sports/formats)
    matches_played_map: dict = {}  # str(user_id) → int total
    if all_player_ids:
        rows = (
            db.query(PlayerRating.user_id, func.sum(PlayerRating.matches_played).label("total"))
            .filter(PlayerRating.user_id.in_(list(all_player_ids)))
            .group_by(PlayerRating.user_id)
            .all()
        )
        matches_played_map = {str(r.user_id): int(r.total or 0) for r in rows}

    # Get latest set score per match
    match_scores = {}
    for match in court_match_map.values():
        latest_set = db.query(MatchSet).filter(
            MatchSet.match_id == match.id,
        ).order_by(MatchSet.set_number.desc()).first()
        if latest_set:
            match_scores[match.id] = latest_set

    def _pid(fld):   return str(fld) if fld is not None else None
    def _uname(fld): return profile_map.get(str(fld)) if fld is not None else None
    def _mp(fld):    return matches_played_map.get(str(fld), 0) if fld is not None else None

    live_courts = []
    for court in courts:
        match = court_match_map.get(court.id)
        court_data = {
            "court_id":   str(court.id),
            "court_name": court.name,
            "sport":      court.sport.value if court.sport is not None else None,
            "status":     court.status,
            "match":      None,
        }

        if match:
            score = match_scores.get(match.id)
            court_data["status"] = "occupied"

            court_data["match"] = {
                "match_id":     str(match.id),
                "sport":        match.sport.value,
                "match_format": match.match_format.value,
                "match_status": match.status.value,
                # Players
                "player1_id":       _pid(match.player1_id),
                "player2_id":       _pid(match.player2_id),
                "player3_id":       _pid(match.player3_id),
                "player4_id":       _pid(match.player4_id),
                "player1_username": _uname(match.player1_id),
                "player2_username": _uname(match.player2_id),
                "player3_username": _uname(match.player3_id),
                "player4_username": _uname(match.player4_id),
                "player1_matches_played": _mp(match.player1_id),
                "player2_matches_played": _mp(match.player2_id),
                "player3_matches_played": _mp(match.player3_id),
                "player4_matches_played": _mp(match.player4_id),
                # Referee
                "referee_id":       _pid(match.referee_id),
                "referee_username": _uname(match.referee_id),
                "has_referee":      match.referee_id is not None,
                # Time
                "scheduled_at": str(match.scheduled_at) if match.scheduled_at is not None else None,
                "started_at":   str(match.started_at)   if match.started_at   is not None else None,
                # Score
                "current_set":  score.set_number if score else 1,
                "score":        f"{score.player1_score}-{score.player2_score}" if score else "0-0",
            }

        live_courts.append(court_data)

    checkins = db.query(ClubCheckin).filter(
        ClubCheckin.club_id == club_id,
        ClubCheckin.status != "checked_out",
    ).all()

    present       = [{"user_id": str(c.user_id), "status": c.status} for c in checkins if str(c.status) == "present"]
    available_ref = [{"user_id": str(c.user_id), "status": c.status} for c in checkins if str(c.status) == "available_to_ref"]

    return {
        "club_id":          club_id,
        "courts":           live_courts,
        "people_present":   len(checkins),
        "available_to_ref": available_ref,
        "present_members":  present,
        "courts_occupied":  sum(1 for c in live_courts if c["status"] == "occupied"),
        "courts_available": sum(1 for c in live_courts if c["status"] == "available"),
    }


# ══════════════════════════════════════════════════════════
# COURT BOOKING
# ══════════════════════════════════════════════════════════

@router.post("/courts/{court_id}/book", status_code=201)
def book_court(
    court_id: str,
    data: BookCourtRequest,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]

    court = db.query(Court).filter(Court.id == court_id).first()
    if not court:
        raise HTTPException(404, "Court not found.")
    if str(court.status) != "available":
        raise HTTPException(400, f"Court is currently {court.status}.")

    club = db.query(Club).filter(Club.id == court.club_id).first()
    if not club:
        raise HTTPException(404, "Club not found.")

    booking = CourtBooking(
        court_id=court_id,
        match_id=data.match_id,
        requested_by=user_id,
        club_id=str(court.club_id),
        scheduled_at=data.scheduled_at,
        status="pending_approval",
        admin_notes=data.notes,
    )
    db.add(booking)
    db.commit()

    send_notification(
        user_id      = str(club.admin_id),
        title        = "New Court Booking Request",
        body         = f"A player is requesting to book {court.name} at {data.scheduled_at.strftime('%b %d, %Y %I:%M %p')}.",
        notif_type   = "court_booking",
        reference_id = str(booking.id),
    )

    return {
        "message":    "Booking request sent. Waiting for club admin approval.",
        "booking_id": str(booking.id),
    }


@router.post("/courts/bookings/{booking_id}/approve")
def approve_booking(
    booking_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]

    booking = db.query(CourtBooking).filter(CourtBooking.id == booking_id).first()
    if not booking:
        raise HTTPException(404, "Booking not found.")

    court = db.query(Court).filter(Court.id == booking.court_id).first()
    # Authorization: club admin OR standalone court owner
    if booking.club_id is not None:
        club = db.query(Club).filter(Club.id == booking.club_id).first()
        if not club or str(club.admin_id) != user_id:
            raise HTTPException(403, "Only the club admin can approve bookings.")
    elif court is None or str(getattr(court, "created_by", None)) != user_id:
        raise HTTPException(403, "Only the court owner can approve bookings.")

    setattr(booking, "status", "approved")
    setattr(booking, "decided_at", datetime.now(timezone.utc))

    if court:
        setattr(court, "status", "reserved")

    if booking.match_id is not None:
        match = db.query(Match).filter(Match.id == booking.match_id).first()
        if match:
            match.court_id = booking.court_id

    db.commit()

    send_notification(
        user_id      = str(booking.requested_by),
        title        = "Court Booking Approved ✅",
        body         = f"Your rental of {court.name if court else 'the court'} has been approved!",
        notif_type   = "court_booking",
        reference_id = str(booking.id),
    )

    return {"message": "Booking approved."}


@router.post("/courts/bookings/{booking_id}/reject")
def reject_booking(
    booking_id: str,
    reason: Optional[str] = None,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]

    booking = db.query(CourtBooking).filter(CourtBooking.id == booking_id).first()
    if not booking:
        raise HTTPException(404, "Booking not found.")

    court = db.query(Court).filter(Court.id == booking.court_id).first()
    if booking.club_id is not None:
        club = db.query(Club).filter(Club.id == booking.club_id).first()
        if not club or str(club.admin_id) != user_id:
            raise HTTPException(403, "Only the club admin can reject bookings.")
    elif court is None or str(getattr(court, "created_by", None)) != user_id:
        raise HTTPException(403, "Only the court owner can reject bookings.")

    setattr(booking, "status", "rejected")
    setattr(booking, "admin_notes", reason)
    setattr(booking, "decided_at", datetime.now(timezone.utc))
    db.commit()

    send_notification(
        user_id      = str(booking.requested_by),
        title        = "Court Booking Rejected",
        body         = f"Your court rental was declined. {reason or ''}".strip(),
        notif_type   = "court_booking",
        reference_id = str(booking.id),
    )

    return {"message": "Booking rejected."}


@router.get("/clubs/{club_id}/courts/bookings")
def get_club_bookings(
    club_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]

    club = db.query(Club).filter(Club.id == club_id).first()
    if not club or str(club.admin_id) != user_id:
        raise HTTPException(403, "Only the club admin can view bookings.")

    bookings = db.query(CourtBooking).filter(
        CourtBooking.club_id == club_id,
    ).order_by(CourtBooking.created_at.desc()).all()

    return {"bookings": [
        {
            "id":            str(b.id),
            "court_id":      str(b.court_id),
            "match_id":      str(b.match_id) if b.match_id is not None else None,
            "booking_type":  getattr(b, "booking_type", "match") or "match",
            "requested_by":  str(b.requested_by),
            "scheduled_at":  str(b.scheduled_at) if b.scheduled_at is not None else None,
            "duration_hours": getattr(b, "duration_hours", 1),
            "status":        b.status,
            "admin_notes":   b.admin_notes,
            "decided_at":    str(b.decided_at) if b.decided_at is not None else None,
            "created_at":    str(b.created_at),
        }
        for b in bookings
    ]}


# ══════════════════════════════════════════════════════════
# COURT RENTAL (standalone — no match required)
# ══════════════════════════════════════════════════════════

@router.post("/courts/{court_id}/rent", status_code=201)
def rent_court(
    court_id: str,
    data: RentCourtRequest,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]

    court = db.query(Court).filter(Court.id == court_id).first()
    if not court:
        raise HTTPException(404, "Court not found.")
    if str(court.status) == "maintenance":
        raise HTTPException(400, "Court is under maintenance.")

    is_standalone = court.club_id is None
    club = None
    if not is_standalone:
        club = db.query(Club).filter(Club.id == court.club_id).first()
        if not club:
            raise HTTPException(404, "Club not found.")

    # Check for overlapping approved/pending bookings
    duration = data.duration_hours or 1.0
    end_time = data.scheduled_at + timedelta(hours=duration)
    overlapping = db.query(CourtBooking).filter(
        CourtBooking.court_id == court_id,
        CourtBooking.status.in_(["pending_approval", "approved"]),
        CourtBooking.scheduled_at < end_time,
    ).all()
    for b in overlapping:
        if b.scheduled_at is not None:
            b_duration = float(getattr(b, "duration_hours", 1) or 1)
            b_end = b.scheduled_at + timedelta(hours=b_duration)
            if b_end > data.scheduled_at:  # type: ignore[operator]
                raise HTTPException(400, "This time slot is already booked.")

    booking = CourtBooking(
        court_id     = court_id,
        match_id     = None,
        requested_by = user_id,
        club_id      = None if is_standalone else str(court.club_id),
        scheduled_at = data.scheduled_at,
        status       = "pending_approval",
        admin_notes  = data.notes,
    )
    setattr(booking, "booking_type",  "rental")
    setattr(booking, "duration_hours", duration)
    db.add(booking)
    db.commit()

    # Notify the court owner (standalone) or club admin
    court_owner_id = str(getattr(court, "created_by")) if is_standalone else str(club.admin_id)  # type: ignore[union-attr]
    ref_id = str(court.id) if is_standalone else str(club.id)  # type: ignore[union-attr]
    send_notification(
        user_id      = court_owner_id,
        title        = "Court Rental Request",
        body         = f"A player wants to rent {court.name} on {data.scheduled_at.strftime('%b %d, %Y %I:%M %p')} for {duration}h.",
        notif_type   = "court_rental",
        reference_id = ref_id,
    )

    return {"message": "Rental request sent. Waiting for approval.", "booking_id": str(booking.id)}


@router.get("/clubs/{club_id}/courts/availability")
def get_court_availability(
    club_id: str,
    date: str = Query(..., description="Date in YYYY-MM-DD format"),
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    try:
        target_date = date_type.fromisoformat(date)
    except ValueError:
        raise HTTPException(400, "Invalid date format. Use YYYY-MM-DD.")

    courts = db.query(Court).filter(Court.club_id == club_id).order_by(Court.name).all()
    if not courts:
        return {"courts": []}

    # Generate 1-hour slots 06:00–22:00
    slot_hours = list(range(6, 22))  # 06:00 to 21:00 start times

    result = []
    for court in courts:
        # Fetch all bookings for this court on this date
        day_start = datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0, tzinfo=timezone.utc)
        day_end   = day_start + timedelta(days=1)

        bookings = db.query(CourtBooking).filter(
            CourtBooking.court_id == court.id,
            CourtBooking.status.in_(["pending_approval", "approved"]),
            CourtBooking.scheduled_at >= day_start,
            CourtBooking.scheduled_at < day_end,
        ).all()

        slots = []
        for hour in slot_hours:
            slot_start = datetime(target_date.year, target_date.month, target_date.day, hour, 0, 0, tzinfo=timezone.utc)
            slot_end   = slot_start + timedelta(hours=1)

            is_available = True
            booking_id   = None
            for b in bookings:
                if b.scheduled_at is None:
                    continue
                b_start    = b.scheduled_at.replace(tzinfo=timezone.utc) if b.scheduled_at.tzinfo is None else b.scheduled_at
                b_duration = float(getattr(b, "duration_hours", 1) or 1)
                b_end      = b_start + timedelta(hours=b_duration)
                # Overlap check
                if b_start < slot_end and b_end > slot_start:  # type: ignore[operator]
                    is_available = False
                    booking_id   = str(b.id)
                    break

            slots.append({
                "start":        slot_start.isoformat(),
                "end":          slot_end.isoformat(),
                "is_available": is_available,
                "booking_id":   booking_id,
            })

        result.append({
            "court_id":       str(court.id),
            "court_name":     court.name,
            "sport":          court.sport,
            "surface":        court.surface,
            "is_indoor":      court.is_indoor,
            "price_per_hour": float(getattr(court, "price_per_hour", None) or 0) or None,
            "slots":          slots,
        })

    return {"courts": result}


# ══════════════════════════════════════════════════════════
# STANDALONE COURTS (no club)
# ══════════════════════════════════════════════════════════

class CreateStandaloneCourtRequest(BaseModel):
    name:           str
    sport:          Optional[str]   = None
    surface:        Optional[str]   = None
    is_indoor:      Optional[bool]  = True
    lighting:       Optional[str]   = "good"
    capacity:       Optional[int]   = None
    notes:          Optional[str]   = None
    address:        Optional[str]   = None
    region_code:    Optional[str]   = None
    province_code:  Optional[str]   = None
    city_mun_code:  Optional[str]   = None
    price_per_hour: Optional[float] = None


def _serialize_standalone(c: Court, owner_name: str | None = None) -> dict:
    raw_pph = getattr(c, "price_per_hour", None)
    return {
        "id":             str(c.id),
        "name":           c.name,
        "sport":          c.sport,
        "surface":        c.surface,
        "is_indoor":      c.is_indoor,
        "lighting":       c.lighting,
        "capacity":       c.capacity,
        "notes":          c.notes,
        "status":         c.status,
        "image_url":      getattr(c, "image_url", None),
        "address":        getattr(c, "address", None),
        "region_code":    getattr(c, "region_code", None),
        "province_code":  getattr(c, "province_code", None),
        "city_mun_code":  getattr(c, "city_mun_code", None),
        "price_per_hour": float(raw_pph) if raw_pph is not None else None,
        "created_by":     str(c.created_by) if getattr(c, "created_by", None) else None,
        "owner_name":     owner_name,
    }


@router.post("/courts", status_code=201)
def create_standalone_court(
    data: CreateStandaloneCourtRequest,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]

    court = Court(
        club_id=None,
        name=data.name,
        sport=data.sport,
        surface=data.surface,
        is_indoor=data.is_indoor if data.is_indoor is not None else True,
        lighting=data.lighting or "good",
        capacity=data.capacity,
        notes=data.notes,
        status="available",
    )
    setattr(court, "created_by",     user_id)
    setattr(court, "address",        data.address)
    setattr(court, "region_code",    data.region_code)
    setattr(court, "province_code",  data.province_code)
    setattr(court, "city_mun_code",  data.city_mun_code)
    setattr(court, "price_per_hour", data.price_per_hour)

    db.add(court)
    db.commit()
    db.refresh(court)

    return {"message": "Court registered.", "court": _serialize_standalone(court)}


@router.get("/courts")
def list_standalone_courts(
    sport: Optional[str] = None,
    q:     Optional[str] = None,
    db: Session = Depends(get_db),
):
    query = db.query(Court).filter(Court.club_id.is_(None))
    if sport:
        query = query.filter(Court.sport == sport)
    if q:
        query = query.filter(Court.name.ilike(f"%{q}%"))
    courts = query.order_by(Court.created_at.desc()).all()
    return {"courts": [_serialize_standalone(c) for c in courts]}


@router.put("/courts/{court_id}")
def update_standalone_court(
    court_id: str,
    data: CreateStandaloneCourtRequest,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]

    court = db.query(Court).filter(Court.id == court_id).first()
    if not court:
        raise HTTPException(404, "Court not found.")
    if court.club_id is not None:
        raise HTTPException(400, "Use the club court update endpoint for club-owned courts.")
    creator = getattr(court, "created_by", None)
    if not creator or str(creator) != user_id:
        raise HTTPException(403, "Only the court creator can update this court.")

    for field in ("name", "sport", "surface", "is_indoor", "lighting",
                  "capacity", "notes", "address", "region_code",
                  "province_code", "city_mun_code", "price_per_hour"):
        val = getattr(data, field, None)
        if val is not None:
            setattr(court, field, val)

    db.commit()
    db.refresh(court)
    return {"message": "Court updated.", "court": _serialize_standalone(court)}


# ── GET /courts/{court_id} — public detail ────────────────────────────────────

@router.get("/courts/{court_id}")
def get_standalone_court(
    court_id: str,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    court = db.query(Court).filter(Court.id == court_id).first()
    if not court:
        raise HTTPException(404, "Court not found.")

    owner_name: str | None = None
    creator = getattr(court, "created_by", None)
    if creator:
        profile = db.query(Profile).filter(Profile.id == creator).first()
        if profile:
            owner_name = (f"{profile.first_name} {profile.last_name}".strip()  # type: ignore[arg-type]
                          or str(profile.username))

    return _serialize_standalone(court, owner_name=owner_name)


# ── GET /courts/{court_id}/availability?date=YYYY-MM-DD ──────────────────────

@router.get("/courts/{court_id}/availability")
def get_court_availability_standalone(
    court_id: str,
    date: str = Query(..., description="Date in YYYY-MM-DD format"),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    try:
        target_date = date_type.fromisoformat(date)
    except ValueError:
        raise HTTPException(400, "Invalid date format. Use YYYY-MM-DD.")

    court = db.query(Court).filter(Court.id == court_id).first()
    if not court:
        raise HTTPException(404, "Court not found.")

    day_start = datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0, tzinfo=timezone.utc)
    day_end   = day_start + timedelta(days=1)

    bookings = db.query(CourtBooking).filter(
        CourtBooking.court_id == court_id,
        CourtBooking.status.in_(["pending_approval", "approved"]),
        CourtBooking.scheduled_at >= day_start,
        CourtBooking.scheduled_at < day_end,
    ).all()

    slot_hours = list(range(6, 22))
    slots = []
    for hour in slot_hours:
        slot_start = datetime(target_date.year, target_date.month, target_date.day, hour, 0, 0, tzinfo=timezone.utc)
        slot_end   = slot_start + timedelta(hours=1)
        is_available = True
        booking_id   = None
        for b in bookings:
            if b.scheduled_at is None:
                continue
            b_start    = b.scheduled_at.replace(tzinfo=timezone.utc) if b.scheduled_at.tzinfo is None else b.scheduled_at
            b_duration = float(getattr(b, "duration_hours", 1) or 1)
            b_end      = b_start + timedelta(hours=b_duration)
            if b_start < slot_end and b_end > slot_start:  # type: ignore[operator]
                is_available = False
                booking_id   = str(b.id)
                break
        slots.append({
            "start":        slot_start.isoformat(),
            "end":          slot_end.isoformat(),
            "is_available": is_available,
            "booking_id":   booking_id,
        })

    raw_pph = getattr(court, "price_per_hour", None)
    return {
        "court_id":       court_id,
        "court_name":     court.name,
        "price_per_hour": float(raw_pph) if raw_pph is not None else None,
        "slots":          slots,
    }


# ── GET /courts/{court_id}/bookings — owner views & manages bookings ──────────

@router.get("/courts/{court_id}/bookings")
def get_court_bookings(
    court_id: str,
    status: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]

    court = db.query(Court).filter(Court.id == court_id).first()
    if not court:
        raise HTTPException(404, "Court not found.")

    # Only the court owner can see this court's booking management list
    creator = getattr(court, "created_by", None)
    if not creator or str(creator) != user_id:
        raise HTTPException(403, "Only the court owner can view bookings.")

    q = db.query(CourtBooking).filter(CourtBooking.court_id == court_id)
    if status:
        q = q.filter(CourtBooking.status == status)
    bookings = q.order_by(CourtBooking.created_at.desc()).all()

    # Batch-fetch requester names
    requester_ids = list({str(b.requested_by) for b in bookings if b.requested_by is not None})
    profiles = db.query(Profile).filter(Profile.id.in_(requester_ids)).all()
    profile_map = {str(p.id): (f"{p.first_name} {p.last_name}".strip() or p.username) for p in profiles}

    return {"bookings": [
        {
            "id":             str(b.id),
            "requested_by":  str(b.requested_by),
            "requester_name": profile_map.get(str(b.requested_by), "Unknown"),
            "scheduled_at":   b.scheduled_at.isoformat() if b.scheduled_at is not None else None,
            "duration_hours": float(getattr(b, "duration_hours", 1) or 1),
            "booking_type":   getattr(b, "booking_type", "rental") or "rental",
            "status":         b.status,
            "admin_notes":    b.admin_notes,
            "decided_at":     b.decided_at.isoformat() if b.decided_at is not None else None,
            "created_at":     b.created_at.isoformat() if b.created_at is not None else None,
        }
        for b in bookings
    ]}
