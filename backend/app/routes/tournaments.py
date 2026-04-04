import uuid
import math
import random
from collections import Counter
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.database import get_db
from app.middleware.auth import get_current_user
from app.models.models import (
    Tournament, TournamentRegistration, Match, MatchSet, Profile, PlayerRating,
    TournamentGroup, TournamentGroupMember, TournamentGroupStanding,
)
from app.services.notifications import send_notification
from app.services.sport_rulesets import get_ruleset
from app.services.smart_tiered import (
    generate_smart_tiered,
    entries_from_registrations,
    GroupDistribution,
)
from app.services.player_assessment import assess_player

router = APIRouter()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _is_organizer(t: Tournament, user_id: str) -> bool:
    return str(t.organizer_id) == user_id


def _is_doubles(match_format) -> bool:
    """Return True for any doubles or mixed_doubles match format."""
    val = match_format.value if hasattr(match_format, "value") else str(match_format)
    return val in ("doubles", "mixed_doubles")


def _assign_match_players(m: Match, reg1, reg2, is_doubles_fmt: bool) -> None:
    """
    Assign player IDs to a match from two registrations.
    For singles: sets player1_id / player2_id only.
    For doubles: additionally populates the four team columns so the bracket
                 correctly displays "P1 / Partner vs P3 / Partner".
    `player1_id` and `player2_id` always hold the team captains so that
    bracket-advancement logic (which uses those columns) works unchanged.
    """
    setattr(m, "player1_id", reg1.player_id)
    setattr(m, "player2_id", reg2.player_id)
    if is_doubles_fmt:
        setattr(m, "team1_player1", reg1.player_id)
        setattr(m, "team1_player2", reg1.partner_id)   # may be None
        setattr(m, "team2_player1", reg2.player_id)
        setattr(m, "team2_player2", reg2.partner_id)   # may be None


def _tournament_summary(t: Tournament, reg_count: int = 0) -> dict:
    return {
        "id":                str(t.id),
        "name":              t.name,
        "description":       t.description,
        "sport":             t.sport,
        "format":            t.format,
        "match_format":      t.match_format,
        "organizer_id":      str(t.organizer_id),
        "club_id":           str(t.club_id) if t.club_id is not None else None,
        "max_participants":  t.max_participants,
        "status":            t.status,
        "registration_open": t.registration_open,
        "starts_at":         str(t.starts_at) if t.starts_at is not None else None,
        "ends_at":           str(t.ends_at)   if t.ends_at   is not None else None,
        "region_code":          t.region_code,
        "province_code":        t.province_code,
        "draw_method":          t.draw_method,
        "smart_tiered_config":  t.smart_tiered_config,
        "min_rating":           t.min_rating,
        "max_rating":           t.max_rating,
        "requires_approval":    t.requires_approval,
        "knockout_best_of":     getattr(t, "knockout_best_of", 3) or 3,
        "created_at":           str(t.created_at),
        "participant_count":    reg_count,
    }


def _profile_mini(p: Profile | None) -> dict | None:
    if not p:
        return None
    return {
        "id":         str(p.id),
        "username":   p.username,
        "first_name": p.first_name,
        "last_name":  p.last_name,
        "avatar_url": p.avatar_url,
    }


# ── List / browse ─────────────────────────────────────────────────────────────

@router.get("")
def list_tournaments(
    sport:  str | None = None,
    status: str | None = None,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    q = db.query(Tournament)
    if sport:
        q = q.filter(Tournament.sport == sport)
    if status:
        q = q.filter(Tournament.status == status)
    tournaments = q.order_by(Tournament.starts_at.asc()).all()
    result = []
    for t in tournaments:
        count = db.query(TournamentRegistration).filter(
            TournamentRegistration.tournament_id == t.id,
            TournamentRegistration.status == "confirmed",
        ).count()
        result.append(_tournament_summary(t, count))
    return {"tournaments": result}


@router.get("/mine")
def list_my_tournaments(
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]
    tournaments = db.query(Tournament).filter(
        Tournament.organizer_id == user_id
    ).order_by(Tournament.created_at.desc()).all()
    result = []
    for t in tournaments:
        count = db.query(TournamentRegistration).filter(
            TournamentRegistration.tournament_id == t.id,
            TournamentRegistration.status == "confirmed",
        ).count()
        result.append(_tournament_summary(t, count))
    return {"tournaments": result}


# ── My invitations ───────────────────────────────────────────────────────────

@router.get("/my-invitations")
def my_tournament_invitations(
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]
    invitations = db.query(TournamentRegistration).filter(
        TournamentRegistration.player_id == user_id,
        TournamentRegistration.status == "invited",
    ).order_by(TournamentRegistration.registered_at.desc()).all()

    result = []
    for reg in invitations:
        t = db.query(Tournament).filter(Tournament.id == reg.tournament_id).first()
        if t:
            result.append({
                "registration_id": str(reg.id),
                "tournament": _tournament_summary(t),
            })

    return {"invitations": result, "count": len(result)}


# ── Get detail ────────────────────────────────────────────────────────────────

@router.get("/{tournament_id}")
def get_tournament(
    tournament_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    t = db.query(Tournament).filter(Tournament.id == tournament_id).first()
    if not t:
        raise HTTPException(404, "Tournament not found.")

    all_regs = db.query(TournamentRegistration).filter(
        TournamentRegistration.tournament_id == t.id
    ).all()

    is_org = _is_organizer(t, current_user["id"])

    reg_list = []
    for r in all_regs:
        # Public view: only show confirmed registrations
        # Organizer view: show all (so they can see invited/pending/pending_approval)
        if not is_org and r.status not in ("confirmed",):
            continue
        profile = db.query(Profile).filter(Profile.id == r.player_id).first()
        entry: dict = {
            "registration_id": str(r.id),
            "player_id":       str(r.player_id),
            "seed":            r.seed,
            "status":          r.status,
            "source":          r.source,
            "registered_at":   str(r.registered_at),
        }
        if profile:
            entry["username"]   = profile.username
            entry["first_name"] = profile.first_name
            entry["last_name"]  = profile.last_name
            entry["avatar_url"] = profile.avatar_url
        if r.partner_id is not None:
            partner = db.query(Profile).filter(Profile.id == r.partner_id).first()
            entry["partner_id"] = str(r.partner_id)
            if partner:
                entry["partner_username"] = partner.username
        # Include player assessment for organizer view on pending requests
        if is_org and r.status == "pending_approval":
            entry["assessment"] = assess_player(
                db, str(r.player_id), t.sport.value if hasattr(t.sport, "value") else str(t.sport),
                min_rating=float(t.min_rating) if t.min_rating is not None else None,
                max_rating=float(t.max_rating) if t.max_rating is not None else None,
            )
        reg_list.append(entry)

    reg_list.sort(key=lambda x: (x.get("seed") or 9999, x.get("registered_at") or ""))

    # Check if current user is registered (confirmed only)
    user_id = current_user["id"]
    my_reg = db.query(TournamentRegistration).filter(
        TournamentRegistration.tournament_id == tournament_id,
        TournamentRegistration.player_id == user_id,
        TournamentRegistration.status == "confirmed",
    ).first()

    # Check if current user has a pending organizer invitation
    my_invite = db.query(TournamentRegistration).filter(
        TournamentRegistration.tournament_id == tournament_id,
        TournamentRegistration.player_id == user_id,
        TournamentRegistration.status == "invited",
    ).first()

    # Check if current user is waiting for their partner to accept (they are the registrant)
    my_pending_partner_reg = db.query(TournamentRegistration).filter(
        TournamentRegistration.tournament_id == tournament_id,
        TournamentRegistration.player_id == user_id,
        TournamentRegistration.status == "pending_partner",
    ).first()

    # Check if current user has been invited as a partner (they are the partner_id target)
    my_partner_invite = db.query(TournamentRegistration).filter(
        TournamentRegistration.tournament_id == tournament_id,
        TournamentRegistration.partner_id == user_id,
        TournamentRegistration.status == "pending_partner",
    ).first()
    partner_invite_from: str | None = None
    if my_partner_invite:
        requester_profile = db.query(Profile).filter(Profile.id == my_partner_invite.player_id).first()
        partner_invite_from = requester_profile.username if requester_profile else None

    organizer = db.query(Profile).filter(Profile.id == t.organizer_id).first()

    confirmed_count = sum(1 for r in all_regs if r.status == "confirmed")
    return {
        "tournament":              _tournament_summary(t, confirmed_count),
        "registrations":           reg_list,
        "is_organizer":            is_org,
        "is_registered":           my_reg is not None,
        "my_reg_id":               str(my_reg.id) if my_reg else None,
        "my_invite_id":            str(my_invite.id) if my_invite else None,
        "my_pending_partner_reg":  str(my_pending_partner_reg.id) if my_pending_partner_reg else None,
        "my_partner_invite_reg":   str(my_partner_invite.id) if my_partner_invite else None,
        "partner_invite_from":     partner_invite_from,
        "organizer":               _profile_mini(organizer),
    }


# ── Create ────────────────────────────────────────────────────────────────────

@router.post("", status_code=201)
def create_tournament(
    body: dict,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]
    name  = (body.get("name") or "").strip()
    sport = body.get("sport")
    if not name:
        raise HTTPException(400, "Tournament name is required.")
    if not sport:
        raise HTTPException(400, "Sport is required.")

    starts_at = None
    ends_at   = None
    if body.get("starts_at"):
        try:
            starts_at = datetime.fromisoformat(body["starts_at"].replace("Z", "+00:00"))
        except Exception:
            pass
    if body.get("ends_at"):
        try:
            ends_at = datetime.fromisoformat(body["ends_at"].replace("Z", "+00:00"))
        except Exception:
            pass

    draw_method = body.get("draw_method", "random")
    smart_tiered_config = None
    if draw_method == "smart_tiered":
        smart_tiered_config = {
            "group_count":        int(body.get("group_count", 4)),
            "balance_by_rating":  bool(body.get("balance_by_rating", True)),
            "separate_clubs":     bool(body.get("separate_clubs", True)),
            "separate_locations": bool(body.get("separate_locations", False)),
            "num_candidates":     8,
        }

    min_rating = float(body["min_rating"]) if body.get("min_rating") not in (None, "") else None
    max_rating = float(body["max_rating"]) if body.get("max_rating") not in (None, "") else None

    t = Tournament(
        id                  = uuid.uuid4(),
        name                = name,
        description         = body.get("description"),
        sport               = sport,
        format              = body.get("format", "single_elimination"),
        match_format        = body.get("match_format", "singles"),
        organizer_id        = user_id,
        club_id             = body.get("club_id"),
        max_participants    = int(body.get("max_participants", 16)),
        status              = "upcoming",
        registration_open   = True,
        starts_at           = starts_at,
        ends_at             = ends_at,
        region_code         = body.get("region_code"),
        province_code       = body.get("province_code"),
        draw_method         = draw_method,
        smart_tiered_config = smart_tiered_config,
        min_rating          = min_rating,
        max_rating          = max_rating,
        requires_approval   = bool(body.get("requires_approval", False)),
        knockout_best_of    = int(body.get("knockout_best_of", 3)) if body.get("knockout_best_of") in (1, 3, "1", "3") else 3,
    )
    db.add(t)
    db.commit()
    db.refresh(t)
    return {"tournament": _tournament_summary(t)}


# ── Update ────────────────────────────────────────────────────────────────────

@router.patch("/{tournament_id}")
def update_tournament(
    tournament_id: str,
    body: dict,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    t = db.query(Tournament).filter(Tournament.id == tournament_id).first()
    if not t:
        raise HTTPException(404, "Tournament not found.")
    if not _is_organizer(t, current_user["id"]):
        raise HTTPException(403, "Not authorized.")
    if t.status not in ("upcoming",):
        raise HTTPException(400, "Cannot edit a started or completed tournament.")

    for field in ("name", "description", "max_participants", "registration_open"):
        if field in body:
            setattr(t, field, body[field])
    if "knockout_best_of" in body and body["knockout_best_of"] in (1, 3):
        setattr(t, "knockout_best_of", int(body["knockout_best_of"]))
    for field in ("starts_at", "ends_at"):
        if field in body and body[field]:
            try:
                setattr(t, field, datetime.fromisoformat(body[field].replace("Z", "+00:00")))
            except Exception:
                pass
    db.commit()
    count = db.query(TournamentRegistration).filter(
        TournamentRegistration.tournament_id == t.id
    ).count()
    return {"tournament": _tournament_summary(t, count)}


# ── Delete ────────────────────────────────────────────────────────────────────

@router.delete("/{tournament_id}")
def delete_tournament(
    tournament_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    t = db.query(Tournament).filter(Tournament.id == tournament_id).first()
    if not t:
        raise HTTPException(404, "Tournament not found.")
    if not _is_organizer(t, current_user["id"]):
        raise HTTPException(403, "Not authorized.")
    status_val = t.status.value if hasattr(t.status, "value") else str(t.status)
    if status_val not in ("upcoming", "completed"):
        raise HTTPException(400, "Cannot delete a tournament that is currently ongoing. End it first.")

    # Cascade-delete match sets, matches, registrations, then the tournament
    match_ids = [str(row.id) for row in db.query(Match.id).filter(Match.tournament_id == tournament_id).all()]
    if match_ids:
        db.query(MatchSet).filter(MatchSet.match_id.in_(match_ids)).delete(synchronize_session=False)
        db.query(Match).filter(Match.tournament_id == tournament_id).delete(synchronize_session=False)
    db.query(TournamentRegistration).filter(
        TournamentRegistration.tournament_id == tournament_id
    ).delete(synchronize_session=False)
    db.delete(t)
    db.commit()
    return {"message": "Tournament deleted."}


# ── Register ──────────────────────────────────────────────────────────────────

@router.post("/{tournament_id}/register", status_code=201)
def register_for_tournament(
    tournament_id: str,
    body: dict = {},
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]
    t = db.query(Tournament).filter(Tournament.id == tournament_id).first()
    if not t:
        raise HTTPException(404, "Tournament not found.")
    if t.registration_open is not True:
        raise HTTPException(400, "Registration is closed.")
    if str(t.status) != "upcoming":
        raise HTTPException(400, "Registration is not available for this tournament.")

    existing = db.query(TournamentRegistration).filter(
        TournamentRegistration.tournament_id == tournament_id,
        TournamentRegistration.player_id == user_id,
    ).first()
    if existing:
        if existing.status == "pending_approval":
            raise HTTPException(400, "Your join request is already pending approval.")
        if existing.status == "pending_partner":
            raise HTTPException(400, "You already have a pending partner invite. Cancel it first to register with a different partner.")
        raise HTTPException(400, "Already registered.")

    # Check rating eligibility
    if t.min_rating is not None or t.max_rating is not None:
        rating_row = db.query(PlayerRating).filter(
            PlayerRating.user_id == user_id,
            PlayerRating.sport   == t.sport,
        ).first()
        player_rating = float(rating_row.rating) if rating_row else 1500.0
        if t.min_rating is not None and player_rating < t.min_rating:
            raise HTTPException(400, f"Your rating ({int(player_rating)}) is below the minimum required ({int(t.min_rating)}).")
        if t.max_rating is not None and player_rating > t.max_rating:
            raise HTTPException(400, f"Your rating ({int(player_rating)}) exceeds the maximum allowed ({int(t.max_rating)}).")

    # Count confirmed + pending_partner (reserved slots) for capacity
    active_count = db.query(TournamentRegistration).filter(
        TournamentRegistration.tournament_id == tournament_id,
        TournamentRegistration.status.in_(["confirmed", "pending_partner", "pending_approval"]),
    ).count()
    if t.max_participants is not None and active_count >= t.max_participants:  # type: ignore[operator]
        raise HTTPException(400, "Tournament is full.")

    # ── Doubles / singles format enforcement ──────────────────────────────────
    is_dbl_fmt = _is_doubles(t.match_format)
    partner_id = body.get("partner_id") if isinstance(body, dict) else None
    if is_dbl_fmt:
        if not partner_id:
            raise HTTPException(
                400,
                "This is a doubles tournament. You must provide a partner_id to register.",
            )
        if str(partner_id) == str(user_id):
            raise HTTPException(400, "You cannot register yourself as your own partner.")
        # Ensure partner is not already registered in this tournament
        partner_existing = db.query(TournamentRegistration).filter(
            TournamentRegistration.tournament_id == tournament_id,
            TournamentRegistration.player_id == partner_id,
        ).first()
        if partner_existing:
            raise HTTPException(400, "Your selected partner is already registered in this tournament.")
    else:
        if partner_id:
            raise HTTPException(400, "This is a singles tournament. partner_id is not allowed.")

    needs_approval = bool(t.requires_approval)

    if is_dbl_fmt:
        # Doubles: create registrant's row as "pending_partner" until partner accepts
        reg = TournamentRegistration(
            tournament_id = tournament_id,
            player_id     = user_id,
            partner_id    = partner_id,
            status        = "pending_partner",
            source        = "self_registered",
        )
        db.add(reg)
        db.commit()

        profile = db.query(Profile).filter(Profile.id == user_id).first()
        pname = profile.username if profile else "A player"

        # Notify the invited partner
        send_notification(
            user_id      = str(partner_id),
            title        = "Doubles Partner Invite",
            body         = f"@{pname} invited you to be their doubles partner in \"{t.name}\". Accept to register as a team.",
            notif_type   = "doubles_partner_invite",
            reference_id = str(reg.id),   # reg.id is used in accept/decline endpoints
        )
        return {"message": "Partner invite sent. Waiting for your partner to accept.", "registration_id": str(reg.id)}
    else:
        reg_status = "pending_approval" if needs_approval else "confirmed"
        reg = TournamentRegistration(
            tournament_id = tournament_id,
            player_id     = user_id,
            partner_id    = None,
            status        = reg_status,
            source        = "self_registered",
        )
        db.add(reg)
        db.commit()

        profile = db.query(Profile).filter(Profile.id == user_id).first()
        pname = profile.username if profile else "A player"

        if needs_approval:
            send_notification(
                user_id      = str(t.organizer_id),
                title        = "New Join Request",
                body         = f"@{pname} requested to join {t.name}. Review in your dashboard.",
                notif_type   = "tournament_join_request",
                reference_id = str(t.id),
            )
            return {"message": "Your request has been submitted and is awaiting organizer approval."}
        else:
            send_notification(
                user_id      = str(t.organizer_id),
                title        = "New Tournament Registration",
                body         = f"@{pname} registered for {t.name}.",
                notif_type   = "tournament_registration",
                reference_id = str(t.id),
            )
            return {"message": "Registered successfully."}


# ── Withdraw ──────────────────────────────────────────────────────────────────

@router.delete("/{tournament_id}/register")
def withdraw_from_tournament(
    tournament_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]
    reg = db.query(TournamentRegistration).filter(
        TournamentRegistration.tournament_id == tournament_id,
        TournamentRegistration.player_id == user_id,
    ).first()
    if not reg:
        raise HTTPException(404, "Not registered.")
    t = db.query(Tournament).filter(Tournament.id == tournament_id).first()
    if t and str(t.status) != "upcoming":
        raise HTTPException(400, "Cannot withdraw from a started tournament.")

    # If cancelling a pending_partner invite, notify the invited partner
    if str(reg.status) == "pending_partner" and reg.partner_id:
        profile = db.query(Profile).filter(Profile.id == user_id).first()
        pname = profile.username if profile else "A player"
        send_notification(
            user_id      = str(reg.partner_id),
            title        = "Partner Invite Cancelled",
            body         = f"@{pname} cancelled their doubles partner invite for \"{t.name if t else 'a tournament'}\".",
            notif_type   = "doubles_partner_declined",
            reference_id = str(t.id) if t else None,
        )

    db.delete(reg)
    db.commit()
    return {"message": "Withdrawn successfully."}


# ── Doubles partner invite: accept / decline ──────────────────────────────────

@router.post("/partner-invite/{registration_id}/accept")
def accept_partner_invite(
    registration_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """The invited partner accepts: both registrations become confirmed."""
    user_id = current_user["id"]

    requester_reg = db.query(TournamentRegistration).filter(
        TournamentRegistration.id == registration_id,
    ).first()
    if not requester_reg:
        raise HTTPException(404, "Partner invite not found.")
    if str(requester_reg.partner_id) != str(user_id):
        raise HTTPException(403, "This invite was not sent to you.")
    if str(requester_reg.status) != "pending_partner":
        raise HTTPException(400, "This invite has already been responded to.")

    t = db.query(Tournament).filter(Tournament.id == requester_reg.tournament_id).first()
    if not t:
        raise HTTPException(404, "Tournament not found.")
    if t.registration_open is not True:
        raise HTTPException(400, "Registration is closed.")

    # Check partner is not already separately registered
    existing = db.query(TournamentRegistration).filter(
        TournamentRegistration.tournament_id == requester_reg.tournament_id,
        TournamentRegistration.player_id == user_id,
    ).first()
    if existing:
        raise HTTPException(400, "You are already registered in this tournament.")

    # Capacity check — both players must fit (they share one team slot in doubles,
    # but each gets their own registration row)
    if t.max_participants is not None:
        confirmed_count = db.query(TournamentRegistration).filter(
            TournamentRegistration.tournament_id == requester_reg.tournament_id,
            TournamentRegistration.status.in_(["confirmed", "pending_approval"]),
        ).count()
        # The requester already holds a pending_partner slot; accepting adds partner (+1)
        # so we only need room for 1 more player
        if confirmed_count + 1 > t.max_participants:
            raise HTTPException(400, "Tournament is full. Cannot confirm this doubles team.")

    needs_approval = bool(t.requires_approval)
    final_status   = "pending_approval" if needs_approval else "confirmed"

    # Confirm (or pend approval for) requester
    setattr(requester_reg, "status", final_status)

    # Create partner's own registration row
    partner_reg = TournamentRegistration(
        tournament_id = requester_reg.tournament_id,
        player_id     = user_id,
        partner_id    = requester_reg.player_id,
        status        = final_status,
        source        = "self_registered",
    )
    db.add(partner_reg)
    db.commit()

    requester_profile = db.query(Profile).filter(Profile.id == requester_reg.player_id).first()
    partner_profile   = db.query(Profile).filter(Profile.id == user_id).first()
    rname = requester_profile.username if requester_profile else "Your partner"
    pname = partner_profile.username   if partner_profile   else "A player"

    # Notify requester that partner accepted
    send_notification(
        user_id      = str(requester_reg.player_id),
        title        = "Partner Accepted",
        body         = f"@{pname} accepted your doubles partner invite for \"{t.name}\".",
        notif_type   = "doubles_partner_accepted",
        reference_id = str(t.id),
    )
    if needs_approval:
        send_notification(
            user_id      = str(t.organizer_id),
            title        = "New Doubles Team Join Request",
            body         = f"Team @{rname} / @{pname} requested to join {t.name}.",
            notif_type   = "tournament_join_request",
            reference_id = str(t.id),
        )
        return {"message": "Partner accepted. Your team is awaiting organizer approval."}
    send_notification(
        user_id      = str(t.organizer_id),
        title        = "New Team Registration",
        body         = f"Team @{rname} / @{pname} registered for {t.name}.",
        notif_type   = "tournament_registration",
        reference_id = str(t.id),
    )
    return {"message": "You have joined the tournament as a doubles team."}


@router.post("/partner-invite/{registration_id}/decline")
def decline_partner_invite(
    registration_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """The invited partner declines: requester's registration is removed."""
    user_id = current_user["id"]

    requester_reg = db.query(TournamentRegistration).filter(
        TournamentRegistration.id == registration_id,
    ).first()
    if not requester_reg:
        raise HTTPException(404, "Partner invite not found.")
    if str(requester_reg.partner_id) != str(user_id):
        raise HTTPException(403, "This invite was not sent to you.")
    if str(requester_reg.status) != "pending_partner":
        raise HTTPException(400, "This invite has already been responded to.")

    t = db.query(Tournament).filter(Tournament.id == requester_reg.tournament_id).first()

    requester_profile = db.query(Profile).filter(Profile.id == requester_reg.player_id).first()
    rname = requester_profile.username if requester_profile else "Your partner"
    partner_profile = db.query(Profile).filter(Profile.id == user_id).first()
    pname = partner_profile.username if partner_profile else "A player"

    db.delete(requester_reg)
    db.commit()

    # Notify requester that partner declined
    if t:
        send_notification(
            user_id      = str(requester_reg.player_id),
            title        = "Partner Declined",
            body         = f"@{pname} declined your doubles partner invite for \"{t.name}\". You can register again with a different partner.",
            notif_type   = "doubles_partner_declined",
            reference_id = str(t.id),
        )
    return {"message": "You declined the doubles partner invite."}


# ── Organizer invite player ───────────────────────────────────────────────────

@router.post("/{tournament_id}/invite", status_code=201)
def invite_player(
    tournament_id: str,
    body: dict,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    t = db.query(Tournament).filter(Tournament.id == tournament_id).first()
    if not t:
        raise HTTPException(404, "Tournament not found.")
    if not _is_organizer(t, current_user["id"]):
        raise HTTPException(403, "Only the organizer can invite players.")
    if str(t.status) != "upcoming":
        raise HTTPException(400, "Cannot invite players after registration closes.")

    username = (body.get("username") or "").strip()
    if not username:
        raise HTTPException(400, "username is required.")

    target = db.query(Profile).filter(Profile.username == username).first()
    if not target:
        raise HTTPException(404, f"Player @{username} not found.")
    if str(target.id) == current_user["id"]:
        raise HTTPException(400, "You cannot invite yourself.")

    existing = db.query(TournamentRegistration).filter(
        TournamentRegistration.tournament_id == tournament_id,
        TournamentRegistration.player_id == target.id,
    ).first()
    if existing:
        if existing.status == "confirmed":
            raise HTTPException(400, "Player is already registered.")
        if existing.status == "invited":
            raise HTTPException(400, "Player already has a pending invitation.")
        if existing.status == "declined":
            # Re-invite: reset to invited
            setattr(existing, "status", "invited")
            db.commit()
            send_notification(
                user_id      = str(target.id),
                title        = "Tournament Invitation",
                body         = f"You've been re-invited to {t.name}.",
                notif_type   = "tournament_invitation",
                reference_id = str(t.id),
            )
            return {"message": f"Re-invited @{username} to {t.name}."}

    confirmed_count = db.query(TournamentRegistration).filter(
        TournamentRegistration.tournament_id == tournament_id,
        TournamentRegistration.status == "confirmed",
    ).count()
    if t.max_participants is not None and confirmed_count >= t.max_participants:
        raise HTTPException(400, "Tournament is full.")

    reg = TournamentRegistration(
        tournament_id = tournament_id,
        player_id     = target.id,
        status        = "invited",
        source        = "organizer_invited",
    )
    db.add(reg)
    db.commit()

    send_notification(
        user_id      = str(target.id),
        title        = "Tournament Invitation",
        body         = f"You've been invited to join {t.name}.",
        notif_type   = "tournament_invitation",
        reference_id = str(t.id),
    )
    return {"message": f"Invited @{username} to {t.name}."}


# ── Accept invitation ──────────────────────────────────────────────────────────

@router.post("/{tournament_id}/invitations/{reg_id}/accept")
def accept_invitation(
    tournament_id: str,
    reg_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    reg = db.query(TournamentRegistration).filter(
        TournamentRegistration.id == reg_id,
        TournamentRegistration.tournament_id == tournament_id,
        TournamentRegistration.player_id == current_user["id"],
        TournamentRegistration.status == "invited",
    ).first()
    if not reg:
        raise HTTPException(404, "Invitation not found.")

    t = db.query(Tournament).filter(Tournament.id == tournament_id).first()
    if not t or str(t.status) != "upcoming":
        raise HTTPException(400, "Tournament is no longer accepting registrations.")

    confirmed_count = db.query(TournamentRegistration).filter(
        TournamentRegistration.tournament_id == tournament_id,
        TournamentRegistration.status == "confirmed",
    ).count()
    if t.max_participants is not None and confirmed_count >= t.max_participants:
        raise HTTPException(400, "Tournament is full.")

    setattr(reg, "status", "confirmed")
    db.commit()

    profile = db.query(Profile).filter(Profile.id == current_user["id"]).first()
    pname = profile.username if profile else "A player"
    send_notification(
        user_id      = str(t.organizer_id),
        title        = "Invitation Accepted",
        body         = f"@{pname} accepted your invitation to {t.name}.",
        notif_type   = "tournament_invite_accepted",
        reference_id = str(t.id),
    )
    return {"message": "Invitation accepted. You are now registered."}


# ── Decline invitation ─────────────────────────────────────────────────────────

@router.post("/{tournament_id}/invitations/{reg_id}/decline")
def decline_invitation(
    tournament_id: str,
    reg_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    reg = db.query(TournamentRegistration).filter(
        TournamentRegistration.id == reg_id,
        TournamentRegistration.tournament_id == tournament_id,
        TournamentRegistration.player_id == current_user["id"],
        TournamentRegistration.status == "invited",
    ).first()
    if not reg:
        raise HTTPException(404, "Invitation not found.")

    setattr(reg, "status", "declined")
    db.commit()
    return {"message": "Invitation declined."}


# ── Approve join request (organizer) ─────────────────────────────────────────

@router.post("/{tournament_id}/registrations/{reg_id}/approve")
def approve_registration(
    tournament_id: str,
    reg_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    t = db.query(Tournament).filter(Tournament.id == tournament_id).first()
    if not t:
        raise HTTPException(404, "Tournament not found.")
    if not _is_organizer(t, current_user["id"]):
        raise HTTPException(403, "Not authorized.")

    reg = db.query(TournamentRegistration).filter(
        TournamentRegistration.id == reg_id,
        TournamentRegistration.tournament_id == tournament_id,
        TournamentRegistration.status == "pending_approval",
    ).first()
    if not reg:
        raise HTTPException(404, "Pending request not found.")

    confirmed_count = db.query(TournamentRegistration).filter(
        TournamentRegistration.tournament_id == tournament_id,
        TournamentRegistration.status == "confirmed",
    ).count()
    if t.max_participants is not None and confirmed_count >= t.max_participants:
        raise HTTPException(400, "Tournament is full.")

    setattr(reg, "status", "confirmed")
    db.commit()

    profile = db.query(Profile).filter(Profile.id == reg.player_id).first()
    send_notification(
        user_id      = str(reg.player_id),
        title        = "Join Request Approved",
        body         = f"Your request to join {t.name} has been approved!",
        notif_type   = "tournament_update",
        reference_id = str(t.id),
    )
    pname = profile.username if profile else str(reg.player_id)
    return {"message": f"@{pname} approved and added to the tournament."}


# ── Reject join request (organizer) ──────────────────────────────────────────

@router.post("/{tournament_id}/registrations/{reg_id}/reject")
def reject_registration(
    tournament_id: str,
    reg_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    t = db.query(Tournament).filter(Tournament.id == tournament_id).first()
    if not t:
        raise HTTPException(404, "Tournament not found.")
    if not _is_organizer(t, current_user["id"]):
        raise HTTPException(403, "Not authorized.")

    reg = db.query(TournamentRegistration).filter(
        TournamentRegistration.id == reg_id,
        TournamentRegistration.tournament_id == tournament_id,
        TournamentRegistration.status == "pending_approval",
    ).first()
    if not reg:
        raise HTTPException(404, "Pending request not found.")

    setattr(reg, "status", "declined")
    db.commit()

    send_notification(
        user_id      = str(reg.player_id),
        title        = "Join Request Declined",
        body         = f"Your request to join {t.name} was not approved.",
        notif_type   = "tournament_update",
        reference_id = str(t.id),
    )
    return {"message": "Request declined."}


# ── Remove participant (organizer) ────────────────────────────────────────────

@router.delete("/{tournament_id}/registrations/{reg_id}")
def remove_participant(
    tournament_id: str,
    reg_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    t = db.query(Tournament).filter(Tournament.id == tournament_id).first()
    if not t:
        raise HTTPException(404, "Tournament not found.")
    if not _is_organizer(t, current_user["id"]):
        raise HTTPException(403, "Not authorized.")
    reg = db.query(TournamentRegistration).filter(
        TournamentRegistration.id == reg_id,
        TournamentRegistration.tournament_id == tournament_id,
    ).first()
    if not reg:
        raise HTTPException(404, "Registration not found.")
    db.delete(reg)
    db.commit()
    return {"message": "Participant removed."}


# ── Set seed ──────────────────────────────────────────────────────────────────

@router.patch("/{tournament_id}/registrations/{reg_id}/seed")
def set_seed(
    tournament_id: str,
    reg_id: str,
    body: dict,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    t = db.query(Tournament).filter(Tournament.id == tournament_id).first()
    if not t:
        raise HTTPException(404, "Tournament not found.")
    if not _is_organizer(t, current_user["id"]):
        raise HTTPException(403, "Not authorized.")
    reg = db.query(TournamentRegistration).filter(
        TournamentRegistration.id == reg_id,
        TournamentRegistration.tournament_id == tournament_id,
    ).first()
    if not reg:
        raise HTTPException(404, "Registration not found.")
    setattr(reg, "seed", body.get("seed"))
    db.commit()
    return {"message": "Seed updated."}


# ── Close registration ────────────────────────────────────────────────────────

@router.post("/{tournament_id}/close-registration")
def close_registration(
    tournament_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    t = db.query(Tournament).filter(Tournament.id == tournament_id).first()
    if not t:
        raise HTTPException(404, "Tournament not found.")
    if not _is_organizer(t, current_user["id"]):
        raise HTTPException(403, "Not authorized.")
    setattr(t, "registration_open", False)
    db.commit()
    return {"message": "Registration closed."}


# ── Smart Tiered preview (no matches created) ─────────────────────────────────

@router.get("/{tournament_id}/smart-tiered-preview")
def smart_tiered_preview(
    tournament_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    t = db.query(Tournament).filter(Tournament.id == tournament_id).first()
    if not t:
        raise HTTPException(404, "Tournament not found.")
    if not _is_organizer(t, current_user["id"]):
        raise HTTPException(403, "Not authorized.")
    if t.draw_method != "smart_tiered":
        raise HTTPException(400, "This tournament does not use Smart Tiered Draw.")

    regs = db.query(TournamentRegistration).filter(
        TournamentRegistration.tournament_id == tournament_id,
        TournamentRegistration.status == "confirmed",
    ).all()
    if len(regs) < 2:
        raise HTTPException(400, "Need at least 2 confirmed participants to preview.")

    # Build ratings map
    player_ids = [str(r.player_id) for r in regs]
    rating_rows = db.query(PlayerRating).filter(
        PlayerRating.user_id.in_(player_ids),
        PlayerRating.sport == t.sport,
    ).all()
    ratings_map = {str(r.user_id): float(r.rating) for r in rating_rows}

    profiles = db.query(Profile).filter(Profile.id.in_(player_ids)).all()
    profiles_map = {str(p.id): p for p in profiles}

    entries = entries_from_registrations(regs, profiles_map, ratings_map)
    cfg     = t.smart_tiered_config or {}
    dist    = generate_smart_tiered(entries, cfg)

    groups_out = []
    for i, group in enumerate(dist.groups):
        label = chr(ord("A") + i)
        members = []
        for e in group:
            p = profiles_map.get(e.player_id)
            members.append({
                "player_id":  e.player_id,
                "username":   p.username   if p else e.player_id[:8],
                "first_name": p.first_name if p else None,
                "last_name":  p.last_name  if p else None,
                "rating":     round(e.rating, 1),
            })
        groups_out.append({"label": label, "members": members})

    return {
        "groups": groups_out,
        "scores": dist.scores,
    }


# ── Pool-play configuration options ───────────────────────────────────────────

def _pool_sizes(n: int, num_pools: int) -> list[int]:
    """Distribute n players into num_pools pools as evenly as possible."""
    base, extra = divmod(n, num_pools)
    return [base + (1 if i < extra else 0) for i in range(num_pools)]

def _recommend_qualifiers(n: int, num_pools: int) -> int:
    """Return the cleanest power-of-2 qualifier count >= 4."""
    # Aim for each pool to contribute at least 1 qualifier; default 4/8/16
    for q in [4, 8, 16]:
        if q <= n and q >= num_pools:
            return q
    return 4

def _knockout_stage_label(qualifiers: int) -> str:
    return {2: "Final", 4: "Semifinals", 8: "Quarterfinals", 16: "Round of 16"}.get(qualifiers, f"Top {qualifiers}")

def _pool_play_options(n: int) -> list[dict]:
    """Return all valid pool configurations for n players."""
    options = []
    # Valid pool counts: any divisor-ish value where pools have 3–6 players each
    for num_pools in range(2, n // 2 + 1):
        sizes = _pool_sizes(n, num_pools)
        min_sz, max_sz = min(sizes), max(sizes)
        if min_sz < 3 or max_sz > 6:
            continue
        qualifiers = _recommend_qualifiers(n, num_pools)
        # Summarise pool sizes e.g. "4, 4, 4" or "5, 5, 4"
        cnt = Counter(sizes)
        size_summary = " · ".join(f"{cnt[s]}×{s}" if cnt[s] > 1 else str(s) for s in sorted(cnt.keys(), reverse=True))
        pool_matches = sum(s * (s - 1) // 2 for s in sizes)
        options.append({
            "num_pools":       num_pools,
            "pool_sizes":      sizes,
            "size_summary":    size_summary,
            "pool_matches":    pool_matches,
            "qualifiers":      qualifiers,
            "knockout_stage":  _knockout_stage_label(qualifiers),
            "is_recommended":  False,  # set below
        })

    # Mark the recommended option
    # Prefer the option that matches the master rule table
    rule_defaults: dict[int, int] = {12: 3, 16: 4, 18: 4, 20: 4, 24: 6}
    preferred_pools = rule_defaults.get(n)
    if preferred_pools:
        for opt in options:
            if opt["num_pools"] == preferred_pools:
                opt["is_recommended"] = True
                break
    elif options:
        # Default: pick the option with pool size closest to 4
        best = min(options, key=lambda o: abs(sum(o["pool_sizes"]) / len(o["pool_sizes"]) - 4))
        best["is_recommended"] = True

    return options


@router.get("/{tournament_id}/pool-play-options")
def get_pool_play_options(
    tournament_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    t = db.query(Tournament).filter(Tournament.id == tournament_id).first()
    if not t:
        raise HTTPException(404, "Tournament not found.")
    if not _is_organizer(t, current_user["id"]):
        raise HTTPException(403, "Not authorized.")
    regs = db.query(TournamentRegistration).filter(
        TournamentRegistration.tournament_id == tournament_id,
        TournamentRegistration.status == "confirmed",
    ).all()
    n = len(regs)
    options = _pool_play_options(n)
    return {"participant_count": n, "options": options}


# ── Generate bracket ──────────────────────────────────────────────────────────

@router.post("/{tournament_id}/generate-bracket")
def generate_bracket(
    tournament_id: str,
    body: dict = None,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    t = db.query(Tournament).filter(Tournament.id == tournament_id).first()
    if not t:
        raise HTTPException(404, "Tournament not found.")
    if not _is_organizer(t, current_user["id"]):
        raise HTTPException(403, "Not authorized.")
    if str(t.status) != "upcoming":
        raise HTTPException(400, "Bracket already generated.")
    if bool(t.registration_open):
        raise HTTPException(400, "Close registration before generating the bracket.")

    existing = db.query(Match).filter(Match.tournament_id == tournament_id).first()
    if existing:
        raise HTTPException(400, "Bracket already exists.")

    # Warn if any doubles teams are still waiting for partner to accept
    if _is_doubles(t.match_format):
        pending_teams = db.query(TournamentRegistration).filter(
            TournamentRegistration.tournament_id == tournament_id,
            TournamentRegistration.status == "pending_partner",
        ).count()
        if pending_teams > 0:
            raise HTTPException(
                400,
                f"{pending_teams} team(s) still have a pending partner invite. "
                "Ask them to accept or cancel before generating the bracket.",
            )

    regs = db.query(TournamentRegistration).filter(
        TournamentRegistration.tournament_id == tournament_id,
        TournamentRegistration.status == "confirmed",
    ).all()
    if len(regs) < 2:
        raise HTTPException(400, "Need at least 2 confirmed participants.")

    # ── Format enforcement before bracket generation ───────────────────────
    if _is_doubles(t.match_format):
        missing = [str(r.player_id) for r in regs if not r.partner_id]
        if missing:
            profiles_map = {
                str(p.id): p
                for p in db.query(Profile).filter(Profile.id.in_(missing)).all()
            }
            names = ", ".join(
                profiles_map[pid].username if pid in profiles_map else pid[:8]
                for pid in missing
            )
            raise HTTPException(
                400,
                f"This is a doubles tournament but the following registrations are missing a partner: {names}. "
                "Each team must have exactly 2 players before the bracket can be generated.",
            )
    else:
        # Singles — ensure no one slipped in with a partner
        has_partner = [r for r in regs if r.partner_id]
        if has_partner:
            for r in has_partner:
                r.partner_id = None
            db.flush()

    # ── Smart Tiered Draw ──────────────────────────────────────────────────
    if t.draw_method == "smart_tiered":
        player_ids  = [str(r.player_id) for r in regs]
        rating_rows = db.query(PlayerRating).filter(
            PlayerRating.user_id.in_(player_ids),
            PlayerRating.sport == t.sport,
        ).all()
        ratings_map  = {str(r.user_id): float(r.rating) for r in rating_rows}
        profiles     = db.query(Profile).filter(Profile.id.in_(player_ids)).all()
        profiles_map = {str(p.id): p for p in profiles}

        entries = entries_from_registrations(regs, profiles_map, ratings_map)
        cfg     = t.smart_tiered_config or {}
        dist    = generate_smart_tiered(entries, cfg)

        # Re-order regs to match smart_tiered group distribution
        # Build a player_id → reg lookup
        reg_map = {str(r.player_id): r for r in regs}
        ordered_regs = []
        for group in dist.groups:
            for entry in group:
                r = reg_map.get(entry.player_id)
                if r:
                    ordered_regs.append(r)
        # Any regs not in dist (shouldn't happen) appended last
        seen = {str(r.player_id) for r in ordered_regs}
        for r in regs:
            if str(r.player_id) not in seen:
                ordered_regs.append(r)

        return _generate_group_stage_knockout(t, ordered_regs, db,
                                              group_count=len(dist.groups),
                                              fairness_scores=dist.scores)

    if str(t.format) in ("round_robin", "TournamentFormat.round_robin"):
        return _generate_round_robin(t, regs, db)
    if str(t.format) in ("double_elimination", "TournamentFormat.double_elimination"):
        return _generate_double_elimination(t, regs, db)
    if str(t.format) in ("group_stage_knockout", "TournamentFormat.group_stage_knockout"):
        return _generate_group_stage_knockout(t, regs, db)
    if str(t.format) in ("swiss", "TournamentFormat.swiss"):
        return _generate_swiss(t, regs, db)
    if str(t.format) in ("pool_play", "TournamentFormat.pool_play"):
        num_pools = int((body or {}).get("num_pools", 0)) or None
        return _generate_pool_play(t, regs, db, num_pools=num_pools)
    return _generate_single_elimination(t, regs, db)


def _generate_single_elimination(t: Tournament, regs: list, db: Session):
    seeded   = sorted([r for r in regs if r.seed], key=lambda r: r.seed)
    unseeded = [r for r in regs if not r.seed]
    random.shuffle(unseeded)
    ordered = seeded + unseeded

    n    = len(ordered)
    size = 1
    while size < n:
        size *= 2

    slots      = ordered + [None] * (size - n)
    num_rounds = int(math.log2(size))

    # Create all placeholder matches for every round
    all_matches: dict[tuple[int, int], Match] = {}
    for r in range(1, num_rounds + 1):
        count = max(1, size // (2 ** r))
        for pos in range(1, count + 1):
            m = Match(
                id               = uuid.uuid4(),
                sport            = t.sport,
                match_type       = "tournament",
                match_format     = t.match_format,
                status           = "pending",
                tournament_id    = t.id,
                round_number     = r,
                bracket_position = pos,
            )
            db.add(m)
            all_matches[(r, pos)] = m

    db.flush()

    # Wire next_match_id
    for (r, pos), m in all_matches.items():
        if r < num_rounds:
            next_pos  = (pos + 1) // 2
            next_m    = all_matches.get((r + 1, next_pos))
            if next_m:
                setattr(m, "next_match_id", next_m.id)

    # Assign round-1 players / handle byes
    is_dbl = _is_doubles(t.match_format)
    round1_count = size // 2
    for pos in range(1, round1_count + 1):
        m     = all_matches[(1, pos)]
        p1reg = slots[2 * (pos - 1)]
        p2reg = slots[2 * pos - 1]

        if p1reg and p2reg:
            _assign_match_players(m, p1reg, p2reg, is_dbl)
        elif p1reg:  # bye for p1
            setattr(m, "player1_id", p1reg.player_id)
            if is_dbl:
                setattr(m, "team1_player1", p1reg.player_id)
                setattr(m, "team1_player2", p1reg.partner_id)
            setattr(m, "status",     "completed")
            setattr(m, "winner_id",  p1reg.player_id)
            _place_winner_in_next(pos, str(p1reg.player_id), all_matches)
        elif p2reg:  # bye for p2
            setattr(m, "player2_id", p2reg.player_id)
            if is_dbl:
                setattr(m, "team2_player1", p2reg.player_id)
                setattr(m, "team2_player2", p2reg.partner_id)
            setattr(m, "status",     "completed")
            setattr(m, "winner_id",  p2reg.player_id)
            _place_winner_in_next(pos, str(p2reg.player_id), all_matches)

    setattr(t, "status", "registration_closed")
    db.commit()
    return {"message": f"Bracket generated: {size}-player single elimination ({num_rounds} rounds)."}


def _generate_round_robin(t: Tournament, regs: list, db: Session):
    is_dbl = _is_doubles(t.match_format)
    n   = len(regs)
    pos = 1
    for i in range(n):
        for j in range(i + 1, n):
            m = Match(
                id               = uuid.uuid4(),
                sport            = t.sport,
                match_type       = "tournament",
                match_format     = t.match_format,
                status           = "pending",
                tournament_id    = t.id,
                player1_id       = regs[i].player_id,
                player2_id       = regs[j].player_id,
                round_number     = 1,
                bracket_position = pos,
            )
            if is_dbl:
                setattr(m, "team1_player1", regs[i].player_id)
                setattr(m, "team1_player2", regs[i].partner_id)
                setattr(m, "team2_player1", regs[j].player_id)
                setattr(m, "team2_player2", regs[j].partner_id)
            db.add(m)
            pos += 1
    setattr(t, "status", "registration_closed")
    db.commit()
    return {"message": f"Round robin generated: {n * (n - 1) // 2} matches."}


def _generate_swiss(t: Tournament, regs: list, db: Session):
    """
    Swiss format — generate Round 1 only.
    Subsequent rounds are generated via POST /generate-next-round after each round completes.
    """
    n          = len(regs)
    num_rounds = math.ceil(math.log2(n)) if n > 1 else 1

    seeded   = sorted([r for r in regs if r.seed], key=lambda r: r.seed)
    unseeded = [r for r in regs if not r.seed]
    random.shuffle(unseeded)
    ordered = seeded + unseeded

    # Pair 1v2, 3v4, …
    is_dbl = _is_doubles(t.match_format)
    pos = 1
    for i in range(0, len(ordered) - 1, 2):
        m = Match(
            id               = uuid.uuid4(),
            sport            = t.sport,
            match_type       = "tournament",
            match_format     = t.match_format,
            status           = "pending",
            tournament_id    = t.id,
            player1_id       = ordered[i].player_id,
            player2_id       = ordered[i + 1].player_id,
            round_number     = 1,
            bracket_position = pos,
        )
        if is_dbl:
            setattr(m, "team1_player1", ordered[i].player_id)
            setattr(m, "team1_player2", ordered[i].partner_id)
            setattr(m, "team2_player1", ordered[i + 1].player_id)
            setattr(m, "team2_player2", ordered[i + 1].partner_id)
        db.add(m)
        pos += 1

    # Odd player out gets a bye
    if len(ordered) % 2 == 1:
        bye = ordered[-1]
        m = Match(
            id               = uuid.uuid4(),
            sport            = t.sport,
            match_type       = "tournament",
            match_format     = t.match_format,
            status           = "completed",
            tournament_id    = t.id,
            player1_id       = bye.player_id,
            winner_id        = bye.player_id,
            round_number     = 1,
            bracket_position = pos,
        )
        db.add(m)

    setattr(t, "status", "registration_closed")
    db.commit()
    return {"message": f"Swiss bracket generated: Round 1 of {num_rounds}. Start the tournament, then generate each subsequent round after the previous one completes."}


def _generate_double_elimination(t: Tournament, regs: list, db: Session):
    seeded   = sorted([r for r in regs if r.seed], key=lambda r: r.seed)
    unseeded = [r for r in regs if not r.seed]
    random.shuffle(unseeded)
    ordered = seeded + unseeded

    n    = len(ordered)
    size = 1
    while size < n:
        size *= 2

    slots     = ordered + [None] * (size - n)
    wb_rounds = int(math.log2(size))

    def _make(round_num: int, pos: int, side: str) -> Match:
        m = Match(
            id               = uuid.uuid4(),
            sport            = t.sport,
            match_type       = "tournament",
            match_format     = t.match_format,
            status           = "pending",
            tournament_id    = t.id,
            round_number     = round_num,
            bracket_position = pos,
            bracket_side     = side,
        )
        db.add(m)
        return m

    # ── Winners Bracket ───────────────────────────────────────────────────────
    wb: dict[tuple[int, int], Match] = {}
    for r in range(1, wb_rounds + 1):
        count = max(1, size // (2 ** r))
        for pos in range(1, count + 1):
            wb[(r, pos)] = _make(r, pos, "W")

    for (r, pos), m in wb.items():
        if r < wb_rounds:
            next_m = wb.get((r + 1, (pos + 1) // 2))
            if next_m:
                setattr(m, "next_match_id", next_m.id)

    # ── Losers Bracket ────────────────────────────────────────────────────────
    # Alternates consolidation rounds (LB-only) and drop-in rounds (LB + WB losers).
    lb_round_num     = 0
    lb_prev: list[Match] = []  # matches whose winners feed next LB round

    for wb_r in range(1, wb_rounds + 1):
        wb_r_matches = [wb[(wb_r, pos)] for pos in range(1, size // (2 ** wb_r) + 1)]

        if wb_r == 1:
            # Pair up the WR1 losers in their own mini-bracket
            lb_round_num += 1
            new_lb: list[Match] = []
            for pos in range(1, len(wb_r_matches) // 2 + 1):
                lm = _make(lb_round_num, pos, "L")
                new_lb.append(lm)
                setattr(wb_r_matches[2 * (pos - 1)], "loser_next_match_id", lm.id)
                setattr(wb_r_matches[2 * pos - 1],   "loser_next_match_id", lm.id)
            lb_prev = new_lb
        else:
            # Consolidation round(s) until lb_prev count matches wb_r_matches count
            while len(lb_prev) > len(wb_r_matches):
                lb_round_num += 1
                new_lb = []
                for pos in range(1, len(lb_prev) // 2 + 1):
                    lm = _make(lb_round_num, pos, "L")
                    new_lb.append(lm)
                    setattr(lb_prev[2 * (pos - 1)], "next_match_id", lm.id)
                    setattr(lb_prev[2 * pos - 1],   "next_match_id", lm.id)
                lb_prev = new_lb

            # Drop-in round: LB survivors vs WB losers
            lb_round_num += 1
            new_lb = []
            for pos, wb_m in enumerate(wb_r_matches, start=1):
                lm = _make(lb_round_num, pos, "L")
                new_lb.append(lm)
                setattr(lb_prev[pos - 1], "next_match_id", lm.id)
                setattr(wb_m, "loser_next_match_id", lm.id)
            lb_prev = new_lb

    # ── Grand Final ───────────────────────────────────────────────────────────
    gf = _make(wb_rounds + lb_round_num + 1, 1, "GF")
    setattr(wb[(wb_rounds, 1)], "next_match_id", gf.id)   # WF winner → GF
    setattr(lb_prev[0],         "next_match_id", gf.id)   # LF winner → GF

    # ── Assign WB R1 players / handle byes ───────────────────────────────────
    for pos in range(1, size // 2 + 1):
        m     = wb[(1, pos)]
        p1reg = slots[2 * (pos - 1)]
        p2reg = slots[2 * pos - 1]
        if p1reg and p2reg:
            setattr(m, "player1_id", p1reg.player_id)
            setattr(m, "player2_id", p2reg.player_id)
        elif p1reg:
            setattr(m, "player1_id", p1reg.player_id)
            setattr(m, "status",     "completed")
            setattr(m, "winner_id",  p1reg.player_id)
            _place_winner_in_next(pos, str(p1reg.player_id), wb)
        elif p2reg:
            setattr(m, "player2_id", p2reg.player_id)
            setattr(m, "status",     "completed")
            setattr(m, "winner_id",  p2reg.player_id)
            _place_winner_in_next(pos, str(p2reg.player_id), wb)

    setattr(t, "status", "registration_closed")
    db.commit()
    return {"message": f"Double elimination bracket generated: {size}-player ({wb_rounds} WB rounds, {lb_round_num} LB rounds)."}


def _generate_pool_play(t: Tournament, regs: list, db: Session, num_pools: int | None = None):
    """
    Pool-play (group-stage only, no knockout).
    If num_pools is given, distribute n players into that many pools (possibly unequal sizes 3–6).
    Otherwise, auto-select a balanced option.
    """
    n = len(regs)

    # ── Resolve num_groups and per-pool sizes ────────────────────────────────
    if num_pools and num_pools >= 2:
        sizes = _pool_sizes(n, num_pools)
        if min(sizes) < 3 or max(sizes) > 6:
            raise HTTPException(
                400,
                f"With {num_pools} pools and {n} players, pool sizes would be "
                f"{min(sizes)}–{max(sizes)}. Each pool must have 3–6 players.",
            )
        num_groups = num_pools
    else:
        # Auto: try equal groups, priority 4 > 5 > 3
        num_groups = None
        for gs in [4, 5, 3]:
            if n % gs == 0 and (n // gs) >= 2:
                num_groups = n // gs
                break
        if num_groups is None:
            options = _pool_play_options(n)
            if options:
                # Pick recommended or first valid option
                rec = next((o for o in options if o["is_recommended"]), options[0])
                num_groups = rec["num_pools"]
            else:
                valid_examples = "6 (2×3), 8 (2×4), 9 (3×3), 10 (2×5), 12 (3×4), 15 (3×5), 16 (4×4), 20 (4×5)"
                raise HTTPException(
                    400,
                    f"Pool play cannot be generated: {n} confirmed entries cannot form balanced pools "
                    f"of 3–6 players. Valid counts include: {valid_examples}.",
                )
        sizes = _pool_sizes(n, num_groups)

    # ── Order entries: seeded first (by seed), then randomised unseeded ──────
    seeded   = sorted([r for r in regs if r.seed], key=lambda r: r.seed)
    unseeded = [r for r in regs if not r.seed]
    random.shuffle(unseeded)
    ordered  = seeded + unseeded

    # ── Serpentine distribution for balanced group strength ──────────────────
    # Assign each player to a pool index using snake-draft order
    group_entries: list[list] = [[] for _ in range(num_groups)]
    pool_cursors = [0] * num_groups  # how many slots filled per pool
    direction = 1
    current_pool = 0
    for reg in ordered:
        # Find next pool that still has capacity
        while pool_cursors[current_pool] >= sizes[current_pool]:
            current_pool = (current_pool + direction) % num_groups
        group_entries[current_pool].append(reg)
        pool_cursors[current_pool] += 1
        # Advance in snake direction
        next_pool = current_pool + direction
        if next_pool < 0 or next_pool >= num_groups:
            direction *= -1
            next_pool = current_pool + direction
        current_pool = next_pool % num_groups

    # ── Create TournamentGroup records ───────────────────────────────────────
    groups_db: list[TournamentGroup] = []
    for i in range(num_groups):
        g = TournamentGroup(
            id=uuid.uuid4(),
            tournament_id=t.id,
            group_name=f"Group {chr(ord('A') + i)}",
            group_order=i,
            group_size=sizes[i],
        )
        db.add(g)
        groups_db.append(g)
    db.flush()

    # ── Create members, standings, and round-robin matches per group ─────────
    is_dbl = _is_doubles(t.match_format)
    pos    = 1
    for g_idx, (g_db, members) in enumerate(zip(groups_db, group_entries)):
        side = f"G{chr(ord('A') + g_idx)}"   # GA, GB, GC… (distinct from G0/G1 used by group_stage_knockout)

        for reg in members:
            db.add(TournamentGroupMember(
                id=uuid.uuid4(),
                tournament_group_id=g_db.id,
                entry_id=reg.id,
                seed_number=reg.seed,
            ))
            db.add(TournamentGroupStanding(
                id=uuid.uuid4(),
                tournament_id=t.id,
                group_id=g_db.id,
                entry_id=reg.id,
            ))

        for i in range(len(members)):
            for j in range(i + 1, len(members)):
                m = Match(
                    id=uuid.uuid4(),
                    sport=t.sport,
                    match_type="tournament",
                    match_format=t.match_format,
                    status="pending",
                    tournament_id=t.id,
                    player1_id=members[i].player_id,
                    player2_id=members[j].player_id,
                    round_number=1,
                    bracket_position=pos,
                    bracket_side=side,
                )
                if is_dbl:
                    setattr(m, "team1_player1", members[i].player_id)
                    setattr(m, "team1_player2", members[i].partner_id)
                    setattr(m, "team2_player1", members[j].player_id)
                    setattr(m, "team2_player2", members[j].partner_id)
                db.add(m)
                pos += 1

    setattr(t, "status", "registration_closed")
    db.commit()

    total_matches = pos - 1
    qualifiers    = _recommend_qualifiers(n, num_groups)
    cnt           = Counter(sizes)
    size_summary  = " · ".join(f"{cnt[s]}×{s}" if cnt[s] > 1 else str(s) for s in sorted(cnt.keys(), reverse=True))
    return {
        "message": f"Pool play generated: {num_groups} pools ({size_summary}), {total_matches} total matches.",
        "groups": num_groups,
        "pool_sizes": sizes,
        "size_summary": size_summary,
        "total_matches": total_matches,
        "recommended_qualifiers": qualifiers,
        "knockout_stage": _knockout_stage_label(qualifiers),
    }


def _generate_group_stage_knockout(
    t: Tournament,
    regs: list,
    db: Session,
    group_count: int | None = None,
    fairness_scores: dict | None = None,
):
    seeded   = sorted([r for r in regs if r.seed], key=lambda r: r.seed)
    unseeded = [r for r in regs if not r.seed]
    random.shuffle(unseeded)
    ordered = seeded + unseeded

    n          = len(ordered)
    num_groups = group_count if group_count else max(2, round(n / 4))

    # When smart_tiered is used, `ordered` is already sorted by group assignment.
    # Re-distribute sequentially into groups in the order they arrive.
    if group_count is not None:
        # entries arrive pre-ordered: group0_entries, group1_entries, …
        # Distribute them back into groups in the same interleaved order
        per_group = math.ceil(n / num_groups)
        groups: list[list] = [[] for _ in range(num_groups)]
        for i, reg in enumerate(ordered):
            groups[i // per_group if i // per_group < num_groups else num_groups - 1].append(reg)
    else:
        # Default snake distribution
        groups = [[] for _ in range(num_groups)]
        for i, reg in enumerate(ordered):
            row = i // num_groups
            idx = i % num_groups if row % 2 == 0 else num_groups - 1 - i % num_groups
            groups[idx].append(reg)

    # Group stage — round robin within each group (bracket_side = G0, G1, …)
    is_dbl = _is_doubles(t.match_format)
    pos = 1
    for g_idx, group in enumerate(groups):
        side = f"G{g_idx}"
        for i in range(len(group)):
            for j in range(i + 1, len(group)):
                m = Match(
                    id               = uuid.uuid4(),
                    sport            = t.sport,
                    match_type       = "tournament",
                    match_format     = t.match_format,
                    status           = "pending",
                    tournament_id    = t.id,
                    player1_id       = group[i].player_id,
                    player2_id       = group[j].player_id,
                    round_number     = 1,
                    bracket_position = pos,
                    bracket_side     = side,
                )
                if is_dbl:
                    setattr(m, "team1_player1", group[i].player_id)
                    setattr(m, "team1_player2", group[i].partner_id)
                    setattr(m, "team2_player1", group[j].player_id)
                    setattr(m, "team2_player2", group[j].partner_id)
                db.add(m)
                pos += 1

    # Knockout stage — placeholder matches (top 2 per group advance)
    ko_spots = num_groups * 2
    ko_size  = 1
    while ko_size < ko_spots:
        ko_size *= 2

    ko_rounds = int(math.log2(ko_size))
    ko: dict[tuple[int, int], Match] = {}
    for r in range(1, ko_rounds + 1):
        count = max(1, ko_size // (2 ** r))
        for kpos in range(1, count + 1):
            m = Match(
                id               = uuid.uuid4(),
                sport            = t.sport,
                match_type       = "tournament",
                match_format     = t.match_format,
                status           = "pending",
                tournament_id    = t.id,
                round_number     = r,
                bracket_position = kpos,
                bracket_side     = "K",
            )
            db.add(m)
            ko[(r, kpos)] = m

    for (r, kpos), m in ko.items():
        if r < ko_rounds:
            next_m = ko.get((r + 1, (kpos + 1) // 2))
            if next_m:
                setattr(m, "next_match_id", next_m.id)

    setattr(t, "status", "registration_closed")
    db.commit()
    total_gm = sum(len(g) * (len(g) - 1) // 2 for g in groups)
    result = {"message": f"Group stage + knockout generated: {num_groups} groups, {total_gm} group matches, {ko_size}-player knockout."}
    if fairness_scores:
        result["fairness"] = fairness_scores
    return result


# ── Promote group stage winners to knockout ───────────────────────────────────

@router.post("/{tournament_id}/promote-to-knockout")
def promote_to_knockout(
    tournament_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    For group_stage_knockout tournaments:
    - Validates all group-stage matches are complete
    - Advances top 2 per group to a new randomly-seeded single-elimination knockout bracket
    - QF/SF = BO3, Championship = BO5 (per-match best_of override)
    - Deletes existing empty knockout placeholders before creating real matches
    """
    t = db.query(Tournament).filter(Tournament.id == tournament_id).first()
    if not t:
        raise HTTPException(404, "Tournament not found.")
    if not _is_organizer(t, current_user["id"]):
        raise HTTPException(403, "Not authorized.")

    fmt = str(t.format)
    if "group_stage_knockout" not in fmt:
        raise HTTPException(400, "Promote-to-knockout is only available for Group Stage + Knockout tournaments.")

    if str(t.status) not in ("ongoing", "registration_closed"):
        raise HTTPException(400, "Tournament must be ongoing or registration closed to promote to knockout.")

    # ── Fetch all group-stage matches (bracket_side starts with G, not K) ────
    all_matches = db.query(Match).filter(Match.tournament_id == tournament_id).all()
    group_matches = [m for m in all_matches if (getattr(m, "bracket_side", "") or "").startswith("G")]
    ko_matches    = [m for m in all_matches if (getattr(m, "bracket_side", "") or "") == "K"]

    if not group_matches:
        raise HTTPException(400, "No group-stage matches found. Generate the bracket first.")

    # ── Ensure all group matches are completed ───────────────────────────────
    incomplete = [
        m for m in group_matches
        if str(m.status.value if hasattr(m.status, "value") else m.status) != "completed"
    ]
    if incomplete:
        raise HTTPException(
            400,
            f"{len(incomplete)} group-stage match(es) are not yet completed. "
            "All group matches must finish before promotion."
        )

    # ── Already promoted? (knockout matches have real players) ───────────────
    ko_with_players = [m for m in ko_matches if m.player1_id is not None or m.player2_id is not None]
    if ko_with_players:
        raise HTTPException(400, "Knockout stage has already been populated.")

    # ── Gather top 2 from each group (by bracket_side G0, G1, …) ────────────
    # Group matches are keyed by their bracket_side (G0, G1, …)
    sides: set[str] = {getattr(m, "bracket_side", "") for m in group_matches}

    # Build standings per group from match results
    qualified_player_ids: list[str] = []

    for side in sorted(sides):
        side_matches = [m for m in group_matches if getattr(m, "bracket_side", "") == side]

        # Collect all player ids in this group
        player_ids_in_group: set[str] = set()
        for m in side_matches:
            if m.player1_id:
                player_ids_in_group.add(str(m.player1_id))
            if m.player2_id:
                player_ids_in_group.add(str(m.player2_id))

        # Tally wins and point_diff per player
        wins_map:  dict[str, int] = {pid: 0 for pid in player_ids_in_group}
        pdiff_map: dict[str, int] = {pid: 0 for pid in player_ids_in_group}

        for m in side_matches:
            if not m.winner_id:
                continue
            winner_str = str(m.winner_id)
            loser_str  = str(m.player1_id) if winner_str == str(m.player2_id) else str(m.player2_id)
            wins_map[winner_str] = wins_map.get(winner_str, 0) + 1

            sets = db.query(MatchSet).filter(MatchSet.match_id == m.id).all()
            p1_pts = sum((s.player1_score or 0) + (s.team1_score or 0) for s in sets)
            p2_pts = sum((s.player2_score or 0) + (s.team2_score or 0) for s in sets)
            if winner_str == str(m.player1_id):
                pdiff_map[winner_str] = pdiff_map.get(winner_str, 0) + (p1_pts - p2_pts)
                pdiff_map[loser_str]  = pdiff_map.get(loser_str,  0) + (p2_pts - p1_pts)
            else:
                pdiff_map[winner_str] = pdiff_map.get(winner_str, 0) + (p2_pts - p1_pts)
                pdiff_map[loser_str]  = pdiff_map.get(loser_str,  0) + (p1_pts - p2_pts)

        # Sort: wins desc → point_diff desc → random tiebreak
        ranked = sorted(
            player_ids_in_group,
            key=lambda pid: (-wins_map.get(pid, 0), -pdiff_map.get(pid, 0), random.random()),
        )
        # Take top 2 (or top 1 if group has only 2 players)
        advance = ranked[:2] if len(ranked) >= 2 else ranked[:1]
        qualified_player_ids.extend(advance)

    if len(qualified_player_ids) < 2:
        raise HTTPException(400, "Not enough players qualified for knockout stage.")

    # ── Delete existing empty K placeholder matches ───────────────────────────
    # First null out cross-references to avoid FK violations (next_match_id
    # references the same matches table and has no ON DELETE SET NULL)
    for m in ko_matches:
        setattr(m, "next_match_id", None)
        setattr(m, "loser_next_match_id", None)
    db.flush()
    for m in ko_matches:
        db.query(MatchSet).filter(MatchSet.match_id == m.id).delete()
        db.delete(m)
    db.flush()

    # ── Randomly reshuffle qualified players ─────────────────────────────────
    random.shuffle(qualified_player_ids)

    # ── Build single-elimination knockout bracket ─────────────────────────────
    n        = len(qualified_player_ids)
    ko_size  = 1
    while ko_size < n:
        ko_size *= 2
    ko_rounds = int(math.log2(ko_size))

    is_dbl = _is_doubles(t.match_format)

    # Determine best_of per round: championship (round ko_rounds) = BO5, others = BO3
    # Rounds go 1 = first round (QF or earlier), ko_rounds = Final
    new_ko: dict[tuple[int, int], Match] = {}

    for r in range(1, ko_rounds + 1):
        count  = max(1, ko_size // (2 ** r))
        bo     = 5 if r == ko_rounds else 3
        for kpos in range(1, count + 1):
            m = Match(
                id               = uuid.uuid4(),
                sport            = t.sport,
                match_type       = "tournament",
                match_format     = t.match_format,
                status           = "pending",
                tournament_id    = t.id,
                round_number     = r,
                bracket_position = kpos,
                bracket_side     = "K",
                best_of          = bo,
            )
            db.add(m)
            new_ko[(r, kpos)] = m

    db.flush()

    # Wire next_match_id links
    for (r, kpos), m in new_ko.items():
        if r < ko_rounds:
            next_m = new_ko.get((r + 1, (kpos + 1) // 2))
            if next_m:
                setattr(m, "next_match_id", next_m.id)

    db.flush()

    # ── Seed qualified players into round-1 slots (byes for non-power-of-2) ──
    r1_matches = sorted(
        [(kpos, m) for (r, kpos), m in new_ko.items() if r == 1],
        key=lambda x: x[0],
    )

    slot_idx = 0
    for kpos, m in r1_matches:
        # slot 1 (odd position) gets player1, slot 2 (even) gets player2
        p1 = qualified_player_ids[slot_idx] if slot_idx < len(qualified_player_ids) else None
        p2 = qualified_player_ids[slot_idx + 1] if slot_idx + 1 < len(qualified_player_ids) else None
        slot_idx += 2

        if p1:
            setattr(m, "player1_id", p1)
        if p2:
            setattr(m, "player2_id", p2)

        # If only one player (bye): auto-complete and advance
        if p1 and not p2:
            setattr(m, "status", "completed")
            setattr(m, "winner_id", p1)
            setattr(m, "completed_at", datetime.now(timezone.utc))
            if m.next_match_id is not None:
                next_m = new_ko.get((2, (kpos + 1) // 2))
                if next_m:
                    if kpos % 2 == 1:
                        setattr(next_m, "player1_id", p1)
                    else:
                        setattr(next_m, "player2_id", p1)

    db.commit()

    # ── Notify all participants ───────────────────────────────────────────────
    regs = db.query(TournamentRegistration).filter(
        TournamentRegistration.tournament_id == tournament_id,
        TournamentRegistration.status == "confirmed",
    ).all()
    for reg in regs:
        send_notification(
            user_id      = str(reg.player_id),
            title        = "Knockout Stage Begins!",
            body         = f"Group stage is over — {t.name} knockout bracket is live!",
            notif_type   = "tournament_ko_promotion",
            reference_id = str(t.id),
        )

    return {
        "message": f"Knockout bracket generated: {len(qualified_player_ids)} players, {ko_size}-slot bracket.",
        "qualified_count": len(qualified_player_ids),
        "ko_size": ko_size,
        "ko_rounds": ko_rounds,
    }


def _place_winner_in_next(
    from_pos: int,
    winner_id: str,
    all_matches: dict[tuple[int, int], "Match"],
    from_round: int = 1,
):
    """Place winner from (from_round, from_pos) into the next round's match."""
    next_pos = (from_pos + 1) // 2
    next_m   = all_matches.get((from_round + 1, next_pos))
    if not next_m:
        return
    if from_pos % 2 == 1:
        if next_m.player1_id is None:
            setattr(next_m, "player1_id", winner_id)
    else:
        if next_m.player2_id is None:
            setattr(next_m, "player2_id", winner_id)


# ── Get bracket ───────────────────────────────────────────────────────────────

@router.get("/{tournament_id}/bracket")
def get_bracket(
    tournament_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    t = db.query(Tournament).filter(Tournament.id == tournament_id).first()
    if not t:
        raise HTTPException(404, "Tournament not found.")

    matches = db.query(Match).filter(
        Match.tournament_id == tournament_id
    ).order_by(Match.bracket_side, Match.round_number, Match.bracket_position).all()

    # Batch-load profiles for all players + referees
    player_ids: set[str] = set()
    for m in matches:
        for fk in (m.player1_id, m.player2_id,
                   getattr(m, "team1_player1", None), getattr(m, "team1_player2", None),
                   getattr(m, "team2_player1", None), getattr(m, "team2_player2", None),
                   m.referee_id):
            if fk is not None:
                player_ids.add(str(fk))
    profiles_map: dict[str, Profile] = {}
    if player_ids:
        for p in db.query(Profile).filter(Profile.id.in_(player_ids)).all():
            profiles_map[str(p.id)] = p

    # Batch-load courts
    court_ids = {str(m.court_id) for m in matches if m.court_id}
    courts_map: dict[str, str] = {}
    if court_ids:
        from app.models.models import Court as CourtModel
        for c in db.query(CourtModel).filter(CourtModel.id.in_(court_ids)).all():
            courts_map[str(c.id)] = c.name

    is_dbl = _is_doubles(t.match_format)

    def _match_dict(m: Match) -> dict:
        p1 = profiles_map.get(str(m.player1_id)) if m.player1_id else None
        p2 = profiles_map.get(str(m.player2_id)) if m.player2_id else None
        ref = profiles_map.get(str(m.referee_id)) if m.referee_id else None
        sets = []
        for s in (m.sets or []):
            sets.append({
                "set_number":    s.set_number,
                "player1_score": s.team1_score if s.team1_score is not None else s.player1_score,
                "player2_score": s.team2_score if s.team2_score is not None else s.player2_score,
                "is_completed":  s.is_completed,
            })

        court_name = courts_map.get(str(m.court_id)) if m.court_id else None

        d: dict = {
            "match_id":            str(m.id),
            "bracket_position":    m.bracket_position,
            "bracket_side":        getattr(m, "bracket_side", None),
            "round_number":        m.round_number,
            "status":              str(m.status.value) if hasattr(m.status, "value") else str(m.status),
            "player1":             _profile_mini(p1),
            "player2":             _profile_mini(p2),
            "winner_id":           str(m.winner_id) if m.winner_id is not None else None,
            "next_match_id":       str(m.next_match_id) if m.next_match_id is not None else None,
            "loser_next_match_id": str(m.loser_next_match_id) if getattr(m, "loser_next_match_id", None) is not None else None,
            "scheduled_at":        str(m.scheduled_at) if m.scheduled_at is not None else None,
            "started_at":          str(m.started_at) if m.started_at is not None else None,
            "best_of":             getattr(m, "best_of", None),
            "sets":                sets,
            "is_doubles":          is_dbl,
            "court_id":            str(m.court_id) if m.court_id else None,
            "court_name":          court_name,
            "referee_id":          str(m.referee_id) if m.referee_id else None,
            "referee_username":    ref.username if ref else None,
            "referee_name":        f"{ref.first_name or ''} {ref.last_name or ''}".strip() if ref else None,
        }

        # For doubles include full team members so frontend can display "P1 / Partner"
        if is_dbl:
            t1p1 = profiles_map.get(str(m.team1_player1)) if getattr(m, "team1_player1", None) else None
            t1p2 = profiles_map.get(str(m.team1_player2)) if getattr(m, "team1_player2", None) else None
            t2p1 = profiles_map.get(str(m.team2_player1)) if getattr(m, "team2_player1", None) else None
            t2p2 = profiles_map.get(str(m.team2_player2)) if getattr(m, "team2_player2", None) else None
            d["team1"] = [_profile_mini(t1p1), _profile_mini(t1p2)]
            d["team2"] = [_profile_mini(t2p1), _profile_mini(t2p2)]

        return d

    fmt = str(t.format)

    # ── DE / Group-stage: group by bracket_side then round ───────────────────
    if fmt in ("double_elimination", "TournamentFormat.double_elimination",
               "group_stage_knockout", "TournamentFormat.group_stage_knockout"):
        sections: dict[str, dict[int, list]] = {}
        for m in matches:
            side  = getattr(m, "bracket_side", None) or "W"
            r     = m.round_number or 1
            sections.setdefault(side, {}).setdefault(r, []).append(_match_dict(m))

        section_order = {"W": 0, "GF": 999}  # L/G*/K sort naturally
        result_sections = []
        for side in sorted(sections.keys(), key=lambda s: section_order.get(s, 1)):
            wb_rounds = max(sections[side].keys())
            round_labels_de = {1: "Grand Final"} if side == "GF" else {}
            rounds_list = []
            for r in sorted(sections[side].keys()):
                if side == "W":
                    reverse_r = wb_rounds - r + 1
                    label_map  = {1: "Winners Final", 2: "Winners Semi-Final", 3: "Winners Quarter-Final"}
                    label = label_map.get(reverse_r, f"WB Round {r}")
                elif side == "L":
                    lb_total = max(sections[side].keys())
                    reverse_r = lb_total - r + 1
                    label = "Losers Final" if reverse_r == 1 else f"LB Round {r}"
                elif side == "GF":
                    label = "Grand Final"
                elif side == "K":
                    ko_total  = max(sections[side].keys())
                    reverse_r = ko_total - r + 1
                    label_map  = {1: "Final", 2: "Semi-Final", 3: "Quarter-Final"}
                    label = label_map.get(reverse_r, f"Knockout Round {r}")
                else:  # group G0, G1…
                    label = f"Group {side[1:]}"
                rounds_list.append({"round": r, "label": label, "matches": sections[side][r]})
            section_label = (
                "Winners Bracket" if side == "W" else
                "Losers Bracket"  if side == "L" else
                "Grand Final"     if side == "GF" else
                "Knockout"        if side == "K" else
                f"Group {side[1:]}"
            )
            result_sections.append({"section": side, "label": section_label, "rounds": rounds_list})

        return {
            "tournament": _tournament_summary(t),
            "sections":   result_sections,
            "format":     fmt,
        }

    # ── Single elimination / Round robin ─────────────────────────────────────
    rounds: dict[int, list] = {}
    for m in matches:
        r = m.round_number or 1
        rounds.setdefault(r, []).append(_match_dict(m))

    total_rounds = len(rounds)
    round_labels = {1: "Final", 2: "Semi-Final", 3: "Quarter-Final"}
    result = []
    for r in sorted(rounds.keys()):
        reverse_r = total_rounds - r + 1
        label = round_labels.get(reverse_r, f"Round {r}")
        result.append({"round": r, "label": label, "matches": rounds[r]})

    return {
        "tournament": _tournament_summary(t),
        "rounds":     result,
        "format":     fmt,
    }


# ── Generate next Swiss round ─────────────────────────────────────────────────

@router.post("/{tournament_id}/generate-next-round")
def generate_next_round(
    tournament_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    t = db.query(Tournament).filter(Tournament.id == tournament_id).first()
    if not t:
        raise HTTPException(404, "Tournament not found.")
    if not _is_organizer(t, current_user["id"]):
        raise HTTPException(403, "Not authorized.")
    if str(t.format) not in ("swiss", "TournamentFormat.swiss"):
        raise HTTPException(400, "Only Swiss tournaments support round-by-round generation.")
    if str(t.status) != "ongoing":
        raise HTTPException(400, "Start the tournament before generating the next round.")

    existing = db.query(Match).filter(Match.tournament_id == tournament_id).all()
    if not existing:
        raise HTTPException(400, "No matches found. Generate bracket first.")

    current_round = max(m.round_number for m in existing if m.round_number)

    # Check all matches in current round are completed
    incomplete = [m for m in existing
                  if m.round_number == current_round
                  and str(m.status.value if hasattr(m.status, "value") else m.status) != "completed"]
    if incomplete:
        raise HTTPException(400, f"Round {current_round} still has {len(incomplete)} unfinished match(es).")

    # Max rounds = ceil(log2(n))
    regs = db.query(TournamentRegistration).filter(
        TournamentRegistration.tournament_id == tournament_id,
        TournamentRegistration.status == "confirmed",
    ).all()
    n          = len(regs)
    max_rounds = math.ceil(math.log2(n)) if n > 1 else 1
    next_round = current_round + 1

    if next_round > max_rounds:
        raise HTTPException(400, f"All {max_rounds} Swiss rounds have been completed.")

    # Build win tallies from completed matches
    wins: dict[str, int] = {str(r.player_id): 0 for r in regs}
    for m in existing:
        if m.winner_id:
            wins[str(m.winner_id)] = wins.get(str(m.winner_id), 0) + 1

    # Build rematch history
    played_pairs: set[tuple] = set()
    for m in existing:
        if m.player1_id and m.player2_id:
            a, b = str(m.player1_id), str(m.player2_id)
            played_pairs.add((min(a, b), max(a, b)))

    # Sort by wins desc, break ties randomly
    players = sorted(wins.keys(), key=lambda p: (-wins[p], random.random()))

    # Greedy pairing: avoid rematches where possible
    paired: set[str] = set()
    pairings: list[tuple[str, str]] = []

    for p1 in players:
        if p1 in paired:
            continue
        best_p2 = None
        for p2 in players:
            if p2 in paired or p2 == p1:
                continue
            key = (min(p1, p2), max(p1, p2))
            if key not in played_pairs:
                best_p2 = p2
                break
        if best_p2 is None:
            # Fallback: allow rematch with lowest-wins opponent
            for p2 in players:
                if p2 not in paired and p2 != p1:
                    best_p2 = p2
                    break
        if best_p2:
            pairings.append((p1, best_p2))
            paired.add(p1)
            paired.add(best_p2)

    # Odd player gets a bye (the one unpaired)
    bye_player = next((p for p in players if p not in paired), None)

    pos = 1
    for p1, p2 in pairings:
        m = Match(
            id               = uuid.uuid4(),
            sport            = t.sport,
            match_type       = "tournament",
            match_format     = t.match_format,
            status           = "pending",
            tournament_id    = t.id,
            player1_id       = p1,
            player2_id       = p2,
            round_number     = next_round,
            bracket_position = pos,
        )
        db.add(m)
        pos += 1

    if bye_player:
        m = Match(
            id               = uuid.uuid4(),
            sport            = t.sport,
            match_type       = "tournament",
            match_format     = t.match_format,
            status           = "completed",
            tournament_id    = t.id,
            player1_id       = bye_player,
            winner_id        = bye_player,
            round_number     = next_round,
            bracket_position = pos,
        )
        db.add(m)

    db.commit()
    return {
        "message": f"Round {next_round} generated with {len(pairings)} matches.",
        "round":   next_round,
        "max_rounds": max_rounds,
    }


# ── Start tournament ──────────────────────────────────────────────────────────

@router.post("/{tournament_id}/start")
def start_tournament(
    tournament_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    t = db.query(Tournament).filter(Tournament.id == tournament_id).first()
    if not t:
        raise HTTPException(404, "Tournament not found.")
    if not _is_organizer(t, current_user["id"]):
        raise HTTPException(403, "Not authorized.")
    if str(t.status) != "registration_closed":
        raise HTTPException(400, "Generate bracket first.")

    matches_count = db.query(Match).filter(Match.tournament_id == tournament_id).count()
    if matches_count == 0:
        raise HTTPException(400, "No matches in bracket.")

    setattr(t, "status", "ongoing")
    db.commit()

    regs = db.query(TournamentRegistration).filter(
        TournamentRegistration.tournament_id == tournament_id
    ).all()
    for reg in regs:
        send_notification(
            user_id      = str(reg.player_id),
            title        = "Tournament Started!",
            body         = f"{t.name} has started. Check your bracket!",
            notif_type   = "tournament_start",
            reference_id = str(t.id),
        )
    return {"message": "Tournament started."}


# ── End tournament ────────────────────────────────────────────────────────────

@router.post("/{tournament_id}/end")
def end_tournament(
    tournament_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    t = db.query(Tournament).filter(Tournament.id == tournament_id).first()
    if not t:
        raise HTTPException(404, "Tournament not found.")
    if not _is_organizer(t, current_user["id"]):
        raise HTTPException(403, "Not authorized.")
    setattr(t, "status", "completed")
    db.commit()
    return {"message": "Tournament completed."}


# ── Manual advance winner (organizer override) ────────────────────────────────

@router.post("/{tournament_id}/advance/{match_id}")
def advance_match_winner(
    tournament_id: str,
    match_id: str,
    body: dict,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    t = db.query(Tournament).filter(Tournament.id == tournament_id).first()
    if not t:
        raise HTTPException(404, "Tournament not found.")
    if not _is_organizer(t, current_user["id"]):
        raise HTTPException(403, "Not authorized.")

    m = db.query(Match).filter(
        Match.id == match_id,
        Match.tournament_id == tournament_id,
    ).first()
    if not m:
        raise HTTPException(404, "Match not found.")
    if str(m.status.value if hasattr(m.status, "value") else m.status) != "completed":
        raise HTTPException(400, "Match is not completed yet.")

    winner_id = body.get("winner_id")
    if not winner_id:
        raise HTTPException(400, "winner_id is required.")
    if winner_id not in (str(m.player1_id), str(m.player2_id)):
        raise HTTPException(400, "winner_id must be one of the match players.")

    setattr(m, "winner_id", winner_id)

    # Advance winner
    if m.next_match_id is not None:
        next_m = db.query(Match).filter(Match.id == m.next_match_id).first()
        if next_m:
            if (m.bracket_position or 1) % 2 == 1:  # type: ignore[operator]
                setattr(next_m, "player1_id", winner_id)
            else:
                setattr(next_m, "player2_id", winner_id)

    # Route loser to losers bracket (double elimination)
    loser_next_id = getattr(m, "loser_next_match_id", None)
    if loser_next_id is not None:
        loser_id = str(m.player1_id) if winner_id == str(m.player2_id) else str(m.player2_id)
        loser_m  = db.query(Match).filter(Match.id == loser_next_id).first()
        if loser_m:
            if loser_m.player1_id is None:
                setattr(loser_m, "player1_id", loser_id)
            else:
                setattr(loser_m, "player2_id", loser_id)

    db.commit()
    return {"message": "Winner advanced to next round."}


# ── Organizer score submission ────────────────────────────────────────────────

class OrganizerSetScore(BaseModel):
    p1_score: int
    p2_score: int

class OrganizerScoreRequest(BaseModel):
    sets: list[OrganizerSetScore]
    winner_id: str


@router.post("/{tournament_id}/matches/{match_id}/submit-score")
def organizer_submit_score(
    tournament_id: str,
    match_id: str,
    body: OrganizerScoreRequest,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    t = db.query(Tournament).filter(Tournament.id == tournament_id).first()
    if not t:
        raise HTTPException(404, "Tournament not found.")
    if not _is_organizer(t, current_user["id"]):
        raise HTTPException(403, "Not authorized.")

    m = db.query(Match).filter(
        Match.id == match_id,
        Match.tournament_id == tournament_id,
    ).first()
    if not m:
        raise HTTPException(404, "Match not found.")

    status_val = str(m.status.value if hasattr(m.status, "value") else m.status)
    if status_val == "completed":
        raise HTTPException(400, "Match is already completed.")
    if m.player1_id is None or m.player2_id is None:
        raise HTTPException(400, "Both players must be assigned before submitting a score.")
    if body.winner_id not in (str(m.player1_id), str(m.player2_id)):
        raise HTTPException(400, "winner_id must be one of the two players.")
    if not body.sets:
        raise HTTPException(400, "At least one set score is required.")

    # Sport-specific validation
    sport_val = t.sport.value if hasattr(t.sport, "value") else str(t.sport)
    rules = get_ruleset(sport_val)
    if rules:
        max_sets_allowed = rules.get("max_sets", 3)
        pts_per_set = rules.get("points_per_set", 11)
        win_by = rules.get("win_by", 2)
        max_pts = rules.get("max_points")

        if len(body.sets) > max_sets_allowed:
            raise HTTPException(400, f"Maximum {max_sets_allowed} sets allowed for {rules['label']}.")

        for i, s in enumerate(body.sets, start=1):
            if s.p1_score < 0 or s.p2_score < 0:
                raise HTTPException(400, f"Set {i}: Scores cannot be negative.")
            
            # Basic validation: someone must have reached the target points
            if s.p1_score < pts_per_set and s.p2_score < pts_per_set:
                # Exception: last set in tennis might be different, but for now we follow ruleset
                pass 

            # Max points check (e.g. 30 in badminton)
            if max_pts:
                if s.p1_score > max_pts or s.p2_score > max_pts:
                    raise HTTPException(400, f"Set {i}: Scores cannot exceed {max_pts} for {rules['label']}.")
    else:
        # Fallback to generic 11-point, 3-set validation
        if len(body.sets) > 3:
            raise HTTPException(400, "Maximum 3 sets per match.")
        for i, s in enumerate(body.sets, start=1):
            if s.p1_score < 0 or s.p2_score < 0:
                raise HTTPException(400, f"Set {i}: Scores cannot be negative.")

    # Validate winner is consistent with set wins
    p1_set_wins = 0
    p2_set_wins = 0
    for s in body.sets:
        if rules:
            pts = rules.get("points_per_set", 11)
            wb  = rules.get("win_by", 2)
            m_pts = rules.get("max_points")
            
            p1_won = (s.p1_score >= pts and s.p1_score - s.p2_score >= wb) or (m_pts and s.p1_score == m_pts)
            p2_won = (s.p2_score >= pts and s.p2_score - s.p1_score >= wb) or (m_pts and s.p2_score == m_pts)
            
            if p1_won: p1_set_wins += 1
            elif p2_won: p2_set_wins += 1
        else:
            if s.p1_score > s.p2_score: p1_set_wins += 1
            elif s.p2_score > s.p1_score: p2_set_wins += 1

    if p1_set_wins != p2_set_wins:
        expected_winner = str(m.player1_id) if p1_set_wins > p2_set_wins else str(m.player2_id)
        if body.winner_id != expected_winner:
            raise HTTPException(
                400,
                f"Winner mismatch: set scores show {'Player 1' if p1_set_wins > p2_set_wins else 'Player 2'} won "
                f"{max(p1_set_wins, p2_set_wins)}-{min(p1_set_wins, p2_set_wins)} sets. "
                "Please correct the winner selection."
            )

    # Force match to ongoing so we can set scores
    if status_val == "pending":
        setattr(m, "status", "ongoing")
        setattr(m, "started_at", datetime.now(timezone.utc))
        db.flush()

    # Clear existing sets then write new ones
    db.query(MatchSet).filter(MatchSet.match_id == match_id).delete()
    for i, s in enumerate(body.sets, start=1):
        db.add(MatchSet(
            match_id=match_id,
            set_number=i,
            player1_score=s.p1_score,
            player2_score=s.p2_score,
        ))
    db.flush()

    # Complete via stored procedure (updates ratings + advances bracket)
    p1_id = str(m.player1_id)
    p2_id = str(m.player2_id)
    sport = m.sport.value if hasattr(m.sport, "value") else str(m.sport)
    match_format = m.match_format.value if hasattr(m.match_format, "value") else str(m.match_format)

    from app.models.models import PlayerRating as PR
    from app.utils.glicko2 import update as glicko_update

    is_doubles_match = _is_doubles(t.match_format)
    p3_id = str(m.team1_player2) if is_doubles_match and m.team1_player2 else None
    p4_id = str(m.team2_player2) if is_doubles_match and m.team2_player2 else None

    p1_rating = db.query(PR).filter(PR.user_id == p1_id, PR.sport == sport, PR.match_format == match_format).first()
    p2_rating = db.query(PR).filter(PR.user_id == p2_id, PR.sport == sport, PR.match_format == match_format).first()
    p3_rating = db.query(PR).filter(PR.user_id == p3_id, PR.sport == sport, PR.match_format == match_format).first() if p3_id else None
    p4_rating = db.query(PR).filter(PR.user_id == p4_id, PR.sport == sport, PR.match_format == match_format).first() if p4_id else None

    if p1_rating and p2_rating:
        p1_wins = (body.winner_id == p1_id)
        new_p1_r, new_p1_rd, new_p1_vol = glicko_update(
            rating=float(p1_rating.rating), rd=float(p1_rating.rating_deviation), volatility=float(p1_rating.volatility),
            opp_rating=float(p2_rating.rating), opp_rd=float(p2_rating.rating_deviation),
            score=1.0 if p1_wins else 0.0,
        )
        new_p2_r, new_p2_rd, new_p2_vol = glicko_update(
            rating=float(p2_rating.rating), rd=float(p2_rating.rating_deviation), volatility=float(p2_rating.volatility),
            opp_rating=float(p1_rating.rating), opp_rd=float(p1_rating.rating_deviation),
            score=0.0 if p1_wins else 1.0,
        )

        params: dict = {
            "mid":    match_id,
            "winner": body.winner_id,
            "r1":  new_p1_r,  "rd1": new_p1_rd,  "vol1": new_p1_vol,
            "r2":  new_p2_r,  "rd2": new_p2_rd,  "vol2": new_p2_vol,
        }

        # For doubles, compute and pass partner ratings too
        partner_sql_fragment = ""
        if p3_rating:
            new_p3_r, new_p3_rd, new_p3_vol = glicko_update(
                rating=float(p3_rating.rating), rd=float(p3_rating.rating_deviation), volatility=float(p3_rating.volatility),
                opp_rating=float(p2_rating.rating), opp_rd=float(p2_rating.rating_deviation),
                score=1.0 if p1_wins else 0.0,
            )
            params.update({"r3": new_p3_r, "rd3": new_p3_rd, "vol3": new_p3_vol})
            partner_sql_fragment += ", :r3, :rd3, :vol3"
        if p4_rating:
            new_p4_r, new_p4_rd, new_p4_vol = glicko_update(
                rating=float(p4_rating.rating), rd=float(p4_rating.rating_deviation), volatility=float(p4_rating.volatility),
                opp_rating=float(p1_rating.rating), opp_rd=float(p1_rating.rating_deviation),
                score=0.0 if p1_wins else 1.0,
            )
            params.update({"r4": new_p4_r, "rd4": new_p4_rd, "vol4": new_p4_vol})
            # Only append p4 params if p3 was also present (positional)
            if p3_rating:
                partner_sql_fragment += ", :r4, :rd4, :vol4"

        db.execute(text(f"""
            SELECT fn_complete_match(
                CAST(:mid AS uuid), CAST(:winner AS uuid),
                :r1, :rd1, :vol1,
                :r2, :rd2, :vol2
                {partner_sql_fragment}
            )
        """), params)
    else:
        # No rating rows — just mark complete and advance manually
        setattr(m, "status", "completed")
        setattr(m, "winner_id", body.winner_id)
        setattr(m, "completed_at", datetime.now(timezone.utc))
        db.flush()
        # Advance winner (+ partner for doubles) to next match
        if m.next_match_id is not None:
            next_m = db.query(Match).filter(Match.id == m.next_match_id).first()
            if next_m:
                slot = 1 if (m.next_match_slot or (1 if (m.bracket_position or 1) % 2 == 1 else 2)) == 1 else 2
                win_partner = getattr(m, "team1_player2", None) if body.winner_id == p1_id else getattr(m, "team2_player2", None)
                if slot == 1:
                    setattr(next_m, "player1_id", body.winner_id)
                    if is_doubles_match:
                        setattr(next_m, "team1_player1", body.winner_id)
                        setattr(next_m, "team1_player2", win_partner)
                else:
                    setattr(next_m, "player2_id", body.winner_id)
                    if is_doubles_match:
                        setattr(next_m, "team2_player1", body.winner_id)
                        setattr(next_m, "team2_player2", win_partner)
        # Route loser (+ partner for doubles) to losers bracket (double elimination)
        loser_next_id = getattr(m, "loser_next_match_id", None)
        if loser_next_id is not None:
            loser_id = p1_id if body.winner_id == p2_id else p2_id
            lose_partner = getattr(m, "team1_player2", None) if loser_id == p1_id else getattr(m, "team2_player2", None)
            loser_m = db.query(Match).filter(Match.id == loser_next_id).first()
            if loser_m:
                loser_slot = getattr(m, "loser_next_match_slot", None) or (1 if loser_m.player1_id is None else 2)
                if loser_slot == 1:
                    setattr(loser_m, "player1_id", loser_id)
                    if is_doubles_match:
                        setattr(loser_m, "team1_player1", loser_id)
                        setattr(loser_m, "team1_player2", lose_partner)
                else:
                    setattr(loser_m, "player2_id", loser_id)
                    if is_doubles_match:
                        setattr(loser_m, "team2_player1", loser_id)
                        setattr(loser_m, "team2_player2", lose_partner)

    db.commit()
    _update_pool_standing(m, body.winner_id, db)

    # ── Auto-complete tournament when final match is done ─────────────────────
    # Final match has no next_match_id and belongs to a tournament still "ongoing"
    if m.tournament_id and getattr(m, "next_match_id", None) is None:
        t = db.query(Tournament).filter(Tournament.id == m.tournament_id).first()
        t_status = str(t.status.value if hasattr(t.status, "value") else t.status) if t else None
        if t and t_status == "ongoing":
            # Confirm all tournament matches are now completed
            pending = db.query(Match).filter(
                Match.tournament_id == m.tournament_id,
                Match.status != "completed",
                Match.player1_id.isnot(None),
                Match.player2_id.isnot(None),
            ).count()
            if pending == 0:
                setattr(t, "status", "completed")
                db.commit()

    return {"message": "Score submitted and bracket advanced."}


# ── Pool play helpers + endpoints ─────────────────────────────────────────────

def _update_pool_standing(match: Match, winner_id: str, db: Session) -> None:
    """Recalculate and persist pool standings after a pool-play match completes."""
    if not match.tournament_id:
        return
    t = db.query(Tournament).filter(Tournament.id == match.tournament_id).first()
    if not t or str(t.format) not in ("pool_play", "TournamentFormat.pool_play"):
        return
    bracket_side = getattr(match, "bracket_side", None) or ""
    if not bracket_side.startswith("G"):
        return

    p1_id = str(match.player1_id) if match.player1_id else None
    p2_id = str(match.player2_id) if match.player2_id else None
    if not p1_id or not p2_id:
        return

    reg1 = db.query(TournamentRegistration).filter(
        TournamentRegistration.tournament_id == match.tournament_id,
        TournamentRegistration.player_id == p1_id,
        TournamentRegistration.status == "confirmed",
    ).first()
    reg2 = db.query(TournamentRegistration).filter(
        TournamentRegistration.tournament_id == match.tournament_id,
        TournamentRegistration.player_id == p2_id,
        TournamentRegistration.status == "confirmed",
    ).first()
    if not reg1 or not reg2:
        return

    s1 = db.query(TournamentGroupStanding).filter(TournamentGroupStanding.entry_id == reg1.id).first()
    s2 = db.query(TournamentGroupStanding).filter(TournamentGroupStanding.entry_id == reg2.id).first()
    if not s1 or not s2:
        return

    sets = db.query(MatchSet).filter(MatchSet.match_id == match.id).all()
    p1_pts = sum((s.player1_score or 0) + (s.team1_score or 0) for s in sets)
    p2_pts = sum((s.player2_score or 0) + (s.team2_score or 0) for s in sets)

    is_p1_win = winner_id == p1_id
    s1.played += 1
    s1.wins   += 1 if is_p1_win else 0
    s1.losses += 0 if is_p1_win else 1
    s1.points_for     += p1_pts
    s1.points_against += p2_pts
    s1.point_diff      = s1.points_for - s1.points_against

    s2.played += 1
    s2.wins   += 0 if is_p1_win else 1
    s2.losses += 1 if is_p1_win else 0
    s2.points_for     += p2_pts
    s2.points_against += p1_pts
    s2.point_diff      = s2.points_for - s2.points_against

    db.commit()


@router.get("/{tournament_id}/pool-play-preview")
def pool_play_preview(
    tournament_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Return pool-play format info for the current confirmed entry count — no changes made."""
    t = db.query(Tournament).filter(Tournament.id == tournament_id).first()
    if not t:
        raise HTTPException(404, "Tournament not found.")
    if not _is_organizer(t, current_user["id"]):
        raise HTTPException(403, "Not authorized.")

    n = db.query(TournamentRegistration).filter(
        TournamentRegistration.tournament_id == tournament_id,
        TournamentRegistration.status == "confirmed",
    ).count()

    for gs in [4, 5, 3]:
        if n % gs == 0 and (n // gs) >= 2:
            num_groups   = n // gs
            matches_each = gs * (gs - 1) // 2
            return {
                "valid": True,
                "confirmed_entries": n,
                "group_size": gs,
                "num_groups": num_groups,
                "matches_per_group": matches_each,
                "total_matches": matches_each * num_groups,
                "group_names": [f"Group {chr(ord('A') + i)}" for i in range(num_groups)],
                "summary": f"{num_groups} Groups of {gs}",
            }

    return {
        "valid": False,
        "confirmed_entries": n,
        "message": (
            f"Pool play cannot be auto-generated: {n} entries cannot be divided evenly "
            f"into equal groups of 3, 4, or 5. "
            f"Valid counts include: 6, 8, 9, 10, 12, 15, 16, 20, 24, 25, 27…"
        ),
    }


@router.get("/{tournament_id}/pool-groups")
def get_pool_groups(
    tournament_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Return pool groups with members and live standings."""
    t = db.query(Tournament).filter(Tournament.id == tournament_id).first()
    if not t:
        raise HTTPException(404, "Tournament not found.")

    groups = (
        db.query(TournamentGroup)
        .filter(TournamentGroup.tournament_id == tournament_id)
        .order_by(TournamentGroup.group_order)
        .all()
    )

    result = []
    for g in groups:
        members = db.query(TournamentGroupMember).filter(
            TournamentGroupMember.tournament_group_id == g.id
        ).all()

        standings_map = {
            str(s.entry_id): s
            for s in db.query(TournamentGroupStanding).filter(
                TournamentGroupStanding.group_id == g.id
            ).all()
        }

        member_data = []
        for mem in members:
            reg    = db.query(TournamentRegistration).filter(TournamentRegistration.id == mem.entry_id).first()
            player = db.query(Profile).filter(Profile.id == reg.player_id).first() if reg else None
            st     = standings_map.get(str(mem.entry_id))
            member_data.append({
                "entry_id":   str(mem.entry_id),
                "player_id":  str(reg.player_id) if reg else None,
                "username":   player.username    if player else None,
                "first_name": player.first_name  if player else None,
                "last_name":  player.last_name   if player else None,
                "seed":       mem.seed_number,
                "standing": {
                    "played":         st.played         if st else 0,
                    "wins":           st.wins           if st else 0,
                    "losses":         st.losses         if st else 0,
                    "points_for":     st.points_for     if st else 0,
                    "points_against": st.points_against if st else 0,
                    "point_diff":     st.point_diff     if st else 0,
                },
            })

        # Sort by wins desc, then point_diff desc
        member_data.sort(key=lambda m: (-m["standing"]["wins"], -m["standing"]["point_diff"]))

        result.append({
            "id":          str(g.id),
            "group_name":  g.group_name,
            "group_order": g.group_order,
            "group_size":  g.group_size,
            "members":     member_data,
        })

    return result
