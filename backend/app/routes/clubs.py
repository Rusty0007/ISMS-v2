from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text as sql_text
from pydantic import BaseModel
from typing import Optional
import uuid

from datetime import date, timedelta, timezone
import datetime as dt

from app.database import get_db
from app.models.models import Club, ClubMember, ClubInvite, ClubCheckin, Court, CourtBooking, Profile, Match, PlayerRating
from sqlalchemy import func
from app.middleware.auth import get_current_user
from app.services.notifications import send_notification

router = APIRouter()


# ── Request models ─────────────────────────────────────────────────────────

class CreateClubRequest(BaseModel):
    name: str
    description: Optional[str] = None
    sport: Optional[str] = None
    category: Optional[str] = None          # community | school | private | municipal | barangay | academy | venue
    membership_type: Optional[str] = "open" # open | invite_only
    address: Optional[str] = None
    region_code: Optional[str] = None
    province_code: Optional[str] = None
    city_mun_code: Optional[str] = None

class UpdateClubRequest(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    sport: Optional[str] = None
    category: Optional[str] = None
    membership_type: Optional[str] = None
    approval_mode: Optional[str] = None   # auto | manual
    opening_time: Optional[str] = None   # HH:MM 24-hour
    closing_time: Optional[str] = None   # HH:MM 24-hour
    address: Optional[str] = None
    region_code: Optional[str] = None
    province_code: Optional[str] = None
    city_mun_code: Optional[str] = None
    logo_url: Optional[str] = None
    cover_url: Optional[str] = None

class SetMemberRoleRequest(BaseModel):
    role: str                              # member | admin | assistant
    duty_date: Optional[str] = None        # ISO date string, for assistants

class CreateCourtRequest(BaseModel):
    name: str
    sport: Optional[str] = None

class ClubInviteRequest(BaseModel):
    user_id: str
    message: Optional[str] = None

class RespondClubInviteRequest(BaseModel):
    response: str  # "accepted" | "declined"


# ── Helpers ────────────────────────────────────────────────────────────────

def _club_or_404(db: Session, club_id: str) -> Club:
    club = db.query(Club).filter(Club.id == uuid.UUID(club_id)).first()
    if not club:
        raise HTTPException(404, "Club not found.")
    return club

def _require_admin(club: Club, user_id: uuid.UUID):
    if str(club.admin_id) != str(user_id):
        raise HTTPException(403, "Club admin access required.")


# ── Club CRUD ──────────────────────────────────────────────────────────────

@router.post("", status_code=201)
def create_club(
    data: CreateClubRequest,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    user_id = uuid.UUID(current_user["id"])

    if db.query(Club).filter(Club.name == data.name).first():
        raise HTTPException(400, "Club name already taken.")

    club = Club(
        name=data.name,
        description=data.description,
        admin_id=user_id,
        sport=data.sport,
        category=data.category,
        membership_type=data.membership_type or "open",
        address=data.address,
        region_code=data.region_code,
        province_code=data.province_code,
        city_mun_code=data.city_mun_code,
    )
    db.add(club)
    db.flush()

    # Auto-enroll creator as owner
    db.add(ClubMember(club_id=club.id, user_id=user_id, role="owner"))
    db.commit()
    db.refresh(club)

    return {"club_id": str(club.id), "message": "Club created."}


@router.get("")
def list_clubs(
    sport: Optional[str] = None,
    mode: Optional[str] = None,  # "nearby" | "explore" | None
    q: Optional[str] = None,      # name search
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    user_id = uuid.UUID(current_user["id"])
    query = db.query(Club).filter(Club.is_active == True)  # noqa: E712
    if sport:
        query = query.filter(Club.sport == sport)
    if q:
        query = query.filter(Club.name.ilike(f"%{q}%"))

    def _serialize(c: Club, proximity: Optional[str] = None) -> dict:
        return {
            "id": str(c.id),
            "name": c.name,
            "description": c.description,
            "sport": c.sport.value if c.sport is not None else None,
            "category": c.category,
            "admin_id": str(c.admin_id),
            "city_mun_code": c.city_mun_code,
            "province_code": c.province_code,
            "logo_url": c.logo_url,
            "cover_url": c.cover_url,
            "member_count": db.query(ClubMember).filter(ClubMember.club_id == c.id).count(),
            "court_count": db.query(Court).filter(Court.club_id == c.id).count(),
            "proximity": proximity,
        }

    if mode == "nearby":
        profile = db.query(Profile).filter(Profile.id == user_id).first()
        proximity = None
        clubs: list = []
        if profile:
            if profile.city_mun_code is not None:
                clubs = query.filter(Club.city_mun_code == profile.city_mun_code).all()
                if clubs:
                    proximity = "city"
            if not clubs and profile.province_code is not None:
                clubs = query.filter(Club.province_code == profile.province_code).all()
                if clubs:
                    proximity = "province"
            if not clubs and profile.region_code is not None:
                clubs = query.filter(Club.region_code == profile.region_code).all()
                if clubs:
                    proximity = "region"
        if not clubs:
            clubs = query.order_by(Club.created_at.desc()).all()
        return [_serialize(c, proximity) for c in clubs]

    if mode == "explore":
        clubs = query.all()
        serialized = [_serialize(c) for c in clubs]
        return sorted(serialized, key=lambda x: (x["member_count"], x["court_count"]), reverse=True)

    # default — all clubs, newest first
    clubs = query.order_by(Club.created_at.desc()).all()
    return [_serialize(c) for c in clubs]


@router.get("/mine")
def my_clubs(
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    user_id = uuid.UUID(current_user["id"])
    admin_clubs = db.query(Club).filter(Club.admin_id == user_id, Club.is_active == True).all()  # noqa: E712
    memberships = db.query(ClubMember).filter(ClubMember.user_id == user_id).all()
    member_club_ids = [m.club_id for m in memberships]
    member_clubs = db.query(Club).filter(Club.id.in_(member_club_ids), Club.is_active == True).all() if member_club_ids else []  # noqa: E712

    admin_ids = {c.id for c in admin_clubs}

    return {
        "admin": [
            {"id": str(c.id), "name": c.name, "sport": c.sport.value if c.sport is not None else None}
            for c in admin_clubs
        ],
        "member": [
            {"id": str(c.id), "name": c.name, "sport": c.sport.value if c.sport is not None else None}
            for c in member_clubs if c.id not in admin_ids
        ],
        "member_club_ids": [str(m.club_id) for m in memberships],
    }


# ── Address search (must be declared before /{club_id} to avoid route conflict)

@router.get("/addresses")
def search_addresses(
    q: str = Query(default="", min_length=0),
    limit: int = Query(default=15, ge=1, le=30),
    db: Session = Depends(get_db),
):
    """Search Philippine locations via the search_locations() stored function."""
    if len(q.strip()) < 2:
        return []
    rows = db.execute(
        sql_text("SELECT id, full_address, city_municipality, province, region FROM search_locations(:q, :lim)"),
        {"q": q.strip(), "lim": limit},
    ).fetchall()
    return [
        {
            "id":                r.id,
            "full_address":      r.full_address,
            "city_municipality": r.city_municipality,
            "province":          r.province,
            "region":            r.region,
        }
        for r in rows
    ]


# ── Club Invites ────────────────────────────────────────────────────────────

@router.get("/my-invites")
def my_club_invites(
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    user_id = uuid.UUID(current_user["id"])
    now = dt.datetime.now(dt.timezone.utc)
    invites = (
        db.query(ClubInvite)
        .filter(
            ClubInvite.invited_user == user_id,
            ClubInvite.status == "pending",
            ClubInvite.expires_at > now,
        )
        .order_by(ClubInvite.created_at.desc())
        .all()
    )
    result = []
    for inv in invites:
        club = db.query(Club).filter(Club.id == inv.club_id).first()
        inviter = db.query(Profile).filter(Profile.id == inv.invited_by).first()
        result.append({
            "invite_id":           str(inv.id),
            "club_id":             str(inv.club_id),
            "club_name":           club.name if club else None,
            "sport":               club.sport.value if club is not None and club.sport is not None else None,
            "category":            club.category if club else None,
            "invited_by_username": inviter.username if inviter else None,
            "message":             inv.message,
            "expires_at":          str(inv.expires_at),
            "created_at":          str(inv.created_at),
        })
    return result


@router.post("/{club_id}/invite", status_code=201)
def send_club_invite(
    club_id: str,
    data: ClubInviteRequest,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    current_id = uuid.UUID(current_user["id"])
    club = _club_or_404(db, club_id)
    _require_admin(club, current_id)

    try:
        target_id = uuid.UUID(data.user_id)
    except ValueError:
        raise HTTPException(400, "Invalid user_id.")

    # Must not already be a member
    if db.query(ClubMember).filter(ClubMember.club_id == club.id, ClubMember.user_id == target_id).first():
        raise HTTPException(400, "User is already a member of this club.")

    # No duplicate pending invite
    now = dt.datetime.now(dt.timezone.utc)
    existing = db.query(ClubInvite).filter(
        ClubInvite.club_id == club.id,
        ClubInvite.invited_user == target_id,
        ClubInvite.status == "pending",
        ClubInvite.expires_at > now,
    ).first()
    if existing:
        raise HTTPException(400, "A pending invite already exists for this user.")

    inviter = db.query(Profile).filter(Profile.id == current_id).first()
    invite = ClubInvite(
        club_id      = club.id,
        invited_by   = current_id,
        invited_user = target_id,
        message      = data.message,
        expires_at   = now + timedelta(days=7),
    )
    db.add(invite)
    db.commit()
    db.refresh(invite)

    send_notification(
        user_id      = str(target_id),
        title        = "Club Invitation",
        body         = f"@{inviter.username if inviter else 'Admin'} invited you to join {club.name}.",
        notif_type   = "club_invite",
        reference_id = str(invite.id),
    )
    return {"invite_id": str(invite.id), "message": "Invite sent."}


@router.post("/invites/{invite_id}/respond")
def respond_club_invite(
    invite_id: str,
    data: RespondClubInviteRequest,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    user_id = uuid.UUID(current_user["id"])
    if data.response not in ("accepted", "declined"):
        raise HTTPException(400, "response must be 'accepted' or 'declined'.")

    invite = db.query(ClubInvite).filter(ClubInvite.id == uuid.UUID(invite_id)).first()
    if not invite:
        raise HTTPException(404, "Invite not found.")
    if str(invite.invited_user) != str(user_id):
        raise HTTPException(403, "Not your invite.")

    now = dt.datetime.now(dt.timezone.utc)
    expires_at = invite.expires_at
    if expires_at is not None and expires_at.replace(tzinfo=dt.timezone.utc) < now:
        raise HTTPException(400, "This invite has expired.")
    if str(invite.status) != "pending":
        raise HTTPException(400, "Invite already responded to.")

    invitee = db.query(Profile).filter(Profile.id == user_id).first()
    club    = db.query(Club).filter(Club.id == invite.club_id).first()

    setattr(invite, "status", data.response)
    setattr(invite, "responded_at", now)

    if data.response == "accepted":
        # Avoid duplicate membership (race condition guard)
        if not db.query(ClubMember).filter(
            ClubMember.club_id == invite.club_id,
            ClubMember.user_id == user_id,
        ).first():
            db.add(ClubMember(club_id=invite.club_id, user_id=user_id, role="member"))
        send_notification(
            user_id      = str(invite.invited_by),
            title        = "Invite Accepted",
            body         = f"@{invitee.username if invitee else 'Someone'} joined {club.name if club else 'your club'}.",
            notif_type   = "club_invite_accepted",
            reference_id = str(invite.club_id),
        )
    else:
        send_notification(
            user_id      = str(invite.invited_by),
            title        = "Invite Declined",
            body         = f"@{invitee.username if invitee else 'Someone'} declined your invite to {club.name if club else 'your club'}.",
            notif_type   = "club_invite_declined",
            reference_id = str(invite.club_id),
        )

    db.commit()
    return {"message": f"Invite {data.response}."}


@router.get("/pending-requests")
def pending_club_requests(
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    """Return count of pending match approvals across all clubs where the user is admin."""
    user_id = uuid.UUID(current_user["id"])
    admin_clubs = db.query(ClubMember).filter(
        ClubMember.user_id == user_id,
        ClubMember.role.in_(["admin", "assistant"]),
    ).all()
    club_ids = [str(m.club_id) for m in admin_clubs]

    count = 0
    if club_ids:
        count = db.query(Match).filter(
            Match.club_id.in_(club_ids),
            Match.status == "pending_approval",
        ).count()

    return {"count": count, "requests": []}


@router.get("/{club_id}")
def get_club(
    club_id: str,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    club = _club_or_404(db, club_id)
    admin_profile = db.query(Profile).filter(Profile.id == club.admin_id).first()

    return {
        "id": str(club.id),
        "name": club.name,
        "description": club.description,
        "sport": club.sport.value if club.sport is not None else None,
        "category": club.category,
        "membership_type": club.membership_type,
        "approval_mode": club.approval_mode or "auto",
        "address": club.address,
        "admin_id": str(club.admin_id),
        "admin_username": admin_profile.username if admin_profile else None,
        "region_code": club.region_code,
        "province_code": club.province_code,
        "city_mun_code": club.city_mun_code,
        "logo_url": club.logo_url,
        "cover_url": club.cover_url,
        "opening_time": getattr(club, "opening_time", None) or "06:00",
        "closing_time": getattr(club, "closing_time", None) or "22:00",
        "is_active": club.is_active,
        "created_at": str(club.created_at),
        "member_count": db.query(ClubMember).filter(ClubMember.club_id == club.id).count(),
        "court_count": db.query(Court).filter(Court.club_id == club.id).count(),
    }


@router.put("/{club_id}")
def update_club(
    club_id: str,
    data: UpdateClubRequest,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    user_id = uuid.UUID(current_user["id"])
    club = _club_or_404(db, club_id)
    _require_admin(club, user_id)

    if data.name is not None:
        if db.query(Club).filter(Club.name == data.name, Club.id != club.id).first():
            raise HTTPException(400, "Club name already taken.")
        setattr(club, "name", data.name)
    if data.description is not None:
        setattr(club, "description", data.description)
    if data.sport is not None:
        setattr(club, "sport", data.sport)
    if data.category is not None:
        setattr(club, "category", data.category)
    if data.membership_type is not None:
        setattr(club, "membership_type", data.membership_type)
    if data.address is not None:
        setattr(club, "address", data.address)
    if data.region_code is not None:
        setattr(club, "region_code", data.region_code)
    if data.province_code is not None:
        setattr(club, "province_code", data.province_code)
    if data.city_mun_code is not None:
        setattr(club, "city_mun_code", data.city_mun_code)
    if data.approval_mode is not None:
        if data.approval_mode not in ("auto", "manual"):
            raise HTTPException(400, "approval_mode must be 'auto' or 'manual'.")
        setattr(club, "approval_mode", data.approval_mode)
    if data.logo_url is not None:
        setattr(club, "logo_url", data.logo_url)
    if data.cover_url is not None:
        setattr(club, "cover_url", data.cover_url)
    if data.opening_time is not None:
        setattr(club, "opening_time", data.opening_time)
    if data.closing_time is not None:
        setattr(club, "closing_time", data.closing_time)

    db.commit()
    return {"message": "Club updated."}


#  Members 

@router.post("/{club_id}/join")
def join_club(
    club_id: str,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    user_id = uuid.UUID(current_user["id"])
    club = _club_or_404(db, club_id)

    if db.query(ClubMember).filter(ClubMember.club_id == club.id, ClubMember.user_id == user_id).first():
        raise HTTPException(400, "Already a member of this club.")

    db.add(ClubMember(club_id=club.id, user_id=user_id))
    db.commit()
    return {"message": "Joined club."}


@router.get("/{club_id}/members")
def list_members(
    club_id: str,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    club = _club_or_404(db, club_id)
    members = db.query(ClubMember).filter(ClubMember.club_id == club.id).order_by(ClubMember.joined_at).all()

    result = []
    for m in members:
        profile = db.query(Profile).filter(Profile.id == m.user_id).first()
        result.append({
            "member_id": str(m.id),
            "user_id": str(m.user_id),
            "username": profile.username if profile else None,
            "first_name": profile.first_name if profile else None,
            "last_name": profile.last_name if profile else None,
            "role": m.role or "member",
            "duty_date": str(m.duty_date) if m.duty_date is not None else None,
            "joined_at": str(m.joined_at),
            "is_admin": m.user_id == club.admin_id,
        })
    return result


@router.delete("/{club_id}/members/{user_id_str}")
def remove_member(
    club_id: str,
    user_id_str: str,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    current_id = uuid.UUID(current_user["id"])
    club = _club_or_404(db, club_id)
    _require_admin(club, current_id)

    if str(club.admin_id) == user_id_str:
        raise HTTPException(400, "Cannot remove the club admin.")

    member = db.query(ClubMember).filter(
        ClubMember.club_id == club.id,
        ClubMember.user_id == uuid.UUID(user_id_str),
    ).first()
    if not member:
        raise HTTPException(404, "Member not found.")

    db.delete(member)
    db.commit()
    return {"message": "Member removed."}


# ── Stats / Dashboard ──────────────────────────────────────────────────────

@router.get("/{club_id}/stats")
def club_stats(
    club_id: str,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    user_id = uuid.UUID(current_user["id"])
    club = _club_or_404(db, club_id)
    _require_admin(club, user_id)

    active_checkins = db.query(ClubCheckin).filter(
        ClubCheckin.club_id == club.id,
        ClubCheckin.checked_out_at.is_(None),
    ).count()

    pending_bookings = db.query(CourtBooking).filter(
        CourtBooking.club_id == club.id,
        CourtBooking.status == "pending",
    ).count()

    return {
        "member_count": db.query(ClubMember).filter(ClubMember.club_id == club.id).count(),
        "court_count": db.query(Court).filter(Court.club_id == club.id).count(),
        "active_checkins": active_checkins,
        "pending_bookings": pending_bookings,
    }


@router.get("/{club_id}/occupancy")
def club_occupancy(
    club_id: str,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    club = _club_or_404(db, club_id)
    courts = db.query(Court).filter(Court.club_id == club.id).all()

    total    = len(courts)
    occupied = sum(1 for c in courts if str(c.status) == "occupied")
    vacant   = total - occupied

    if total == 0:
        crowd_level = "Empty"
    elif occupied / total >= 0.8:
        crowd_level = "High"
    elif occupied / total >= 0.4:
        crowd_level = "Moderate"
    else:
        crowd_level = "Low"

    # Pre-fetch all ongoing matches for occupied courts in one query
    court_ids = [c.id for c in courts if str(c.status) == "occupied"]
    ongoing_matches: dict = {}
    if court_ids:
        for m in db.query(Match).filter(Match.court_id.in_(court_ids), Match.status == "ongoing").all():
            if m.court_id is not None:
                ongoing_matches[m.court_id] = m

    # Gather all player + referee IDs for batch lookups
    all_pids: set = set()
    all_rids: set = set()
    for m in ongoing_matches.values():
        for pid in [m.player1_id, m.player2_id, m.player3_id, m.player4_id]:
            if pid is not None: all_pids.add(pid)
        if m.referee_id is not None: all_rids.add(m.referee_id)

    all_lookup = all_pids | all_rids
    profile_map: dict = {}
    if all_lookup:
        for p in db.query(Profile).filter(Profile.id.in_(list(all_lookup))).all():
            profile_map[str(p.id)] = p.username

    matches_played_map: dict = {}
    if all_pids:
        for row in (db.query(PlayerRating.user_id, func.sum(PlayerRating.matches_played).label("t"))
                    .filter(PlayerRating.user_id.in_(list(all_pids)))
                    .group_by(PlayerRating.user_id).all()):
            matches_played_map[str(row.user_id)] = int(row[1] or 0)

    def _pid(v): return str(v) if v is not None else None
    def _u(v):   return profile_map.get(str(v)) if v is not None else None
    def _mp(v):  return matches_played_map.get(str(v)) if v is not None else None

    court_details = []
    for c in courts:
        m = ongoing_matches.get(c.id)
        live_match = None
        if m:
            live_match = {
                "match_id":     str(m.id),
                "sport":        m.sport.value if m.sport else None,
                "match_format": m.match_format.value if m.match_format else None,
                "player1_id":   _pid(m.player1_id), "player2_id": _pid(m.player2_id),
                "player3_id":   _pid(m.player3_id), "player4_id": _pid(m.player4_id),
                "player1_username": _u(m.player1_id), "player2_username": _u(m.player2_id),
                "player3_username": _u(m.player3_id), "player4_username": _u(m.player4_id),
                "player1_matches_played": _mp(m.player1_id), "player2_matches_played": _mp(m.player2_id),
                "player3_matches_played": _mp(m.player3_id), "player4_matches_played": _mp(m.player4_id),
                "referee_id":       _pid(m.referee_id),
                "referee_username": _u(m.referee_id),
                "scheduled_at": str(m.scheduled_at) if m.scheduled_at else None,
                "started_at":   str(m.started_at)   if m.started_at   else None,
            }
        court_details.append({
            "court_id":   str(c.id),
            "name":       c.name,
            "sport":      c.sport.value if c.sport is not None else None,
            "status":     c.status,
            "surface":    c.surface,
            "is_indoor":  c.is_indoor,
            "live_match": live_match,
        })

    return {
        "club_id":     club_id,
        "total":       total,
        "occupied":    occupied,
        "vacant":      vacant,
        "crowd_level": crowd_level,
        "courts":      court_details,
    }


# ── Member Role Management ──────────────────────────────────────────────────

@router.put("/{club_id}/members/{user_id_str}/role")
def set_member_role(
    club_id: str,
    user_id_str: str,
    data: SetMemberRoleRequest,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    current_id = uuid.UUID(current_user["id"])
    club = _club_or_404(db, club_id)
    _require_admin(club, current_id)

    if data.role not in ("member", "admin", "assistant"):
        raise HTTPException(400, "Role must be member, admin, or assistant.")

    member = db.query(ClubMember).filter(
        ClubMember.club_id == club.id,
        ClubMember.user_id == uuid.UUID(user_id_str),
    ).first()
    if not member:
        raise HTTPException(404, "Member not found.")

    setattr(member, "role", data.role)
    if data.duty_date:
        try:
            setattr(member, "duty_date", date.fromisoformat(data.duty_date))
        except ValueError:
            raise HTTPException(400, "duty_date must be a valid ISO date (YYYY-MM-DD).")
    else:
        setattr(member, "duty_date", None)

    db.commit()
    return {"message": f"Role updated to '{data.role}'."}


# ── Match Approval Workflow ─────────────────────────────────────────────────

def _can_approve(club: Club, member: ClubMember) -> bool:
    """Admin or today's assistant may approve/reject."""
    if str(member.user_id) == str(club.admin_id):
        return True
    if str(member.role) in ("admin", "owner"):
        return True
    if str(member.role) == "assistant" and str(member.duty_date) == str(date.today()):
        return True
    return False


@router.get("/{club_id}/pending-matches")
def list_pending_matches(
    club_id: str,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    user_id = uuid.UUID(current_user["id"])
    club = _club_or_404(db, club_id)

    # Must be at least a member with approval rights
    member = db.query(ClubMember).filter(
        ClubMember.club_id == club.id,
        ClubMember.user_id == user_id,
    ).first()
    if not member or not _can_approve(club, member):
        raise HTTPException(403, "Approval access required.")

    pending = db.query(Match).filter(
        Match.club_id == club.id,
        Match.status == "pending_approval",
    ).order_by(Match.created_at.asc()).all()

    result = []
    for m in pending:
        court = db.query(Court).filter(Court.id == m.court_id).first() if m.court_id is not None else None
        p1 = db.query(Profile).filter(Profile.id == m.player1_id).first() if m.player1_id is not None else None
        p2 = db.query(Profile).filter(Profile.id == m.player2_id).first() if m.player2_id is not None else None
        result.append({
            "match_id":    str(m.id),
            "sport":       m.sport.value if m.sport is not None else None,
            "format":      m.match_format.value if m.match_format is not None else None,
            "court_name":  court.name if court else None,
            "player1":     p1.username if p1 else None,
            "player2":     p2.username if p2 else None,
            "created_at":  str(m.created_at),
        })
    return {"pending_matches": result}


@router.post("/{club_id}/matches/{match_id}/approve")
def approve_match(
    club_id: str,
    match_id: str,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    user_id = uuid.UUID(current_user["id"])
    club = _club_or_404(db, club_id)

    member = db.query(ClubMember).filter(
        ClubMember.club_id == club.id,
        ClubMember.user_id == user_id,
    ).first()
    if not member or not _can_approve(club, member):
        raise HTTPException(403, "Approval access required.")

    match = db.query(Match).filter(
        Match.id == uuid.UUID(match_id),
        Match.club_id == club.id,
        Match.status == "pending_approval",
    ).first()
    if not match:
        raise HTTPException(404, "Pending match not found.")

    setattr(match, "status", "ongoing")
    setattr(match, "started_at", __import__("datetime").datetime.now(__import__("datetime").timezone.utc))
    db.commit()

    # Notify players
    for pid in [match.player1_id, match.player2_id, match.player3_id, match.player4_id]:
        if pid is not None:
            court = db.query(Court).filter(Court.id == match.court_id).first() if match.court_id is not None else None
            send_notification(
                user_id      = str(pid),
                title        = "Match Confirmed!",
                body         = f"Your match at {club.name}{f' — {court.name}' if court else ''} has been approved. You&apos;re ready to play!",
                notif_type   = "match_approved",
                reference_id = str(match.id),
            )
    return {"message": "Match approved and set to ongoing."}


@router.post("/{club_id}/matches/{match_id}/reject")
def reject_match(
    club_id: str,
    match_id: str,
    body: dict = {},
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    user_id = uuid.UUID(current_user["id"])
    club = _club_or_404(db, club_id)

    member = db.query(ClubMember).filter(
        ClubMember.club_id == club.id,
        ClubMember.user_id == user_id,
    ).first()
    if not member or not _can_approve(club, member):
        raise HTTPException(403, "Approval access required.")

    match = db.query(Match).filter(
        Match.id == uuid.UUID(match_id),
        Match.club_id == club.id,
        Match.status == "pending_approval",
    ).first()
    if not match:
        raise HTTPException(404, "Pending match not found.")

    reason = body.get("reason", "Court unavailable.")

    # Release court back to available
    if match.court_id is not None:
        court = db.query(Court).filter(Court.id == match.court_id).first()
        if court:
            setattr(court, "status", "available")

    setattr(match, "status", "cancelled")
    setattr(match, "court_id", None)
    db.commit()

    # Notify players
    for pid in [match.player1_id, match.player2_id, match.player3_id, match.player4_id]:
        if pid is not None:
            send_notification(
                user_id      = str(pid),
                title        = "Match Not Approved",
                body         = f"Your match at {club.name} was not approved: {reason}",
                notif_type   = "match_rejected",
                reference_id = str(match.id),
            )
    return {"message": "Match rejected. Court released."}


@router.get("/{club_id}/rankings")
def get_club_rankings(
    club_id: str,
    sport: str,
    match_format: str = "singles",   # singles | doubles | mixed_doubles
    gender: Optional[str] = None,    # male | female | other | None (all)
    limit: int = 10,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    club = _club_or_404(db, club_id)

    # Build query: club members → player_ratings → profiles
    q = (
        db.query(
            Profile.id,
            Profile.username,
            Profile.first_name,
            Profile.last_name,
            Profile.avatar_url,
            Profile.gender,
            PlayerRating.rating,
            PlayerRating.rating_deviation,
            PlayerRating.matches_played,
            PlayerRating.wins,
            PlayerRating.losses,
            PlayerRating.rating_status,
            PlayerRating.is_leaderboard_eligible,
        )
        .join(ClubMember, ClubMember.user_id == Profile.id)
        .join(
            PlayerRating,
            (PlayerRating.user_id == Profile.id) &
            (PlayerRating.sport == sport) &
            (PlayerRating.match_format == match_format),
        )
        .filter(ClubMember.club_id == club.id)
    )

    if gender:
        q = q.filter(Profile.gender == gender)

    rows = q.order_by(PlayerRating.rating.desc()).limit(limit).all()

    return {
        "club_id":      str(club.id),
        "sport":        sport,
        "match_format": match_format,
        "gender":       gender,
        "rankings": [
            {
                "rank":                  idx + 1,
                "id":                    str(r.id),
                "username":              r.username,
                "first_name":            r.first_name,
                "last_name":             r.last_name,
                "avatar_url":            r.avatar_url,
                "gender":                r.gender,
                "rating":                float(r.rating),
                "rating_deviation":      float(r.rating_deviation),
                "matches_played":        r.matches_played,
                "wins":                  r.wins,
                "losses":                r.losses,
                "rating_status":         r.rating_status,
                "is_leaderboard_eligible": bool(r.is_leaderboard_eligible),
            }
            for idx, r in enumerate(rows)
        ],
    }
