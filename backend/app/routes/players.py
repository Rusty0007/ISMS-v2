import json
import logging
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from fastapi import APIRouter, HTTPException, Depends, status
from pydantic import BaseModel
from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy import func, or_
from app.config import settings
from app.database import get_db
from app.middleware.auth import get_current_user
from app.models.models import Profile, UserRoleModel, SportRegistration, PlayerRating, MatchHistory, Match
from app.services.rating_policy import LEADERBOARD_MIN_MATCHES, ML_MATCHMAKING_MIN_MATCHES
from app.services.sport_rulesets import SPORT_RULESETS
from app.utils.skill_tiers import RATING_CEILING, RATING_FLOOR, get_skill_tier_name

logger = logging.getLogger(__name__)
router = APIRouter()

_PROFILE_TTL = 60  # seconds


def _get_redis():
    try:
        import redis as _redis_lib
        r = _redis_lib.from_url(settings.redis_url, decode_responses=True, socket_timeout=1)
        r.ping()
        return r
    except Exception:
        return None


def _invalidate_profile_cache(player_id: str) -> None:
    redis = _get_redis()
    if redis:
        try:
            redis.delete(f"profile:{player_id}")
        except Exception:
            pass

VALID_SPORTS = ["pickleball", "badminton", "lawn_tennis", "table_tennis"]

# ── Skill level helper ────────────────────────────────────────────────────────

def _skill_level(rating: float, rating_status: str) -> str:
    if rating_status != "RATED":
        return "Calibrating"
    return get_skill_tier_name(rating)


def _rating_payload(r: PlayerRating) -> dict:
    raw_rating = float(r.rating)  # type: ignore[arg-type]
    safe_rating = max(RATING_FLOOR, min(RATING_CEILING, raw_rating))
    calibration_matches = int(r.calibration_matches_played or 0)
    is_skill_rating_awarded = bool(r.is_matchmaking_eligible) or calibration_matches >= ML_MATCHMAKING_MIN_MATCHES
    visible_rating = safe_rating if is_skill_rating_awarded else 1500.0
    visible_skill_level = get_skill_tier_name(safe_rating) if is_skill_rating_awarded else "Calibrating"

    return {
        "rating": safe_rating,
        "visible_rating": visible_rating,
        "rating_visible": is_skill_rating_awarded,
        "is_skill_rating_awarded": is_skill_rating_awarded,
        "skill_level": visible_skill_level,
        "calibration_matches_played": calibration_matches,
        "calibration_target": ML_MATCHMAKING_MIN_MATCHES,
        "calibration_remaining": max(0, ML_MATCHMAKING_MIN_MATCHES - calibration_matches),
    }


def _performance_payload(r: PlayerRating) -> dict:
    return {
        "performance_rating": round(float(r.performance_rating), 1) if r.performance_rating is not None else 50.0,  # type: ignore[arg-type]
        "performance_confidence": round(float(r.performance_confidence), 1) if r.performance_confidence is not None else 0.0,  # type: ignore[arg-type]
        "performance_coverage_pct": round(float(r.performance_coverage_pct), 1) if r.performance_coverage_pct is not None else 0.0,  # type: ignore[arg-type]
        "performance_reliable": bool(r.performance_reliable),
        "performance_matches_with_events": int(r.performance_matches_with_events or 0),  # type: ignore[arg-type]
        "performance_total_points": int(r.performance_total_points or 0),  # type: ignore[arg-type]
        "performance_attributed_points": int(r.performance_attributed_points or 0),  # type: ignore[arg-type]
        "performance_breakdown": {
            "winning_shots": round(float(r.performance_winning_shots), 1) if r.performance_winning_shots is not None else 0.0,  # type: ignore[arg-type]
            "forced_errors_drawn": round(float(r.performance_forced_errors_drawn), 1) if r.performance_forced_errors_drawn is not None else 0.0,  # type: ignore[arg-type]
            "errors_committed": round(float(r.performance_errors_committed), 1) if r.performance_errors_committed is not None else 0.0,  # type: ignore[arg-type]
            "serve_faults": round(float(r.performance_serve_faults), 1) if r.performance_serve_faults is not None else 0.0,  # type: ignore[arg-type]
            "violations": round(float(r.performance_violations), 1) if r.performance_violations is not None else 0.0,  # type: ignore[arg-type]
            "clutch_points_won": round(float(r.performance_clutch_points_won), 1) if r.performance_clutch_points_won is not None else 0.0,  # type: ignore[arg-type]
            "clutch_errors": round(float(r.performance_clutch_errors), 1) if r.performance_clutch_errors is not None else 0.0,  # type: ignore[arg-type]
        },
    }


def _fallback_stat_label(value: Optional[str]) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "Other"
    return raw.replace("_", " ").title()


def _ruleset_label(sport_name: str, code: Optional[str], bucket: str) -> str:
    raw = str(code or "").strip()
    if not raw:
        return "Other"

    ruleset = SPORT_RULESETS.get(sport_name) or {}
    for item in ruleset.get(bucket, []):
        if str(item.get("code", "")).lower() == raw.lower():
            return str(item.get("label") or raw)
    return _fallback_stat_label(raw)


def _serve_change_label(event_type: Optional[str]) -> str:
    raw = str(event_type or "").strip().lower()
    if raw == "loss_of_serve":
        return "Loss of Serve"
    if raw == "side_out":
        return "Side Out"
    return "Serve Fault"


def _append_stat(bucket: dict[str, int], label: str, count: int) -> None:
    if count <= 0:
        return
    bucket[label] = bucket.get(label, 0) + count

# ── Request Models ────────────────────────────────────────

class UpdateProfileRequest(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    avatar_url: Optional[str] = None
    gender: Optional[str] = None          # male | female | other
    region_code: Optional[str] = None
    province_code: Optional[str] = None
    city_mun_code: Optional[str] = None
    barangay_code: Optional[str] = None
    profile_setup_complete: Optional[bool] = None
    phone_number: Optional[str] = None
    sms_notifications_enabled: Optional[bool] = None

class SportsRegisterRequest(BaseModel):
    sport: str

# ── Routes ────────────────────────────────────────────────

@router.get("/me/performance-stats")
def get_performance_stats(
    sport: Optional[str] = None,
    period: Optional[str] = None,   # "week" | "month" | None (all-time)
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]

    # Date window for weekly / monthly filtering
    since = None
    if period == "week":
        since = datetime.now(timezone.utc) - timedelta(days=7)
    elif period == "month":
        since = datetime.now(timezone.utc) - timedelta(days=30)

    # Base query for shots won
    shots_query = db.query(
        Match.sport,
        MatchHistory.meta["cause"].astext.label("label"),
        func.count(MatchHistory.id).label("count")
    ).join(Match, Match.id == MatchHistory.match_id)\
     .filter(Match.status == "completed")\
     .filter(MatchHistory.player_id == user_id)\
     .filter(MatchHistory.event_type == "point")\
     .filter(MatchHistory.meta["attribution_type"].astext == "winning_shot")

    # Base query for errors committed
    errors_query = db.query(
        Match.sport,
        MatchHistory.meta["reason_code"].astext.label("reason_code"),
        func.count(MatchHistory.id).label("count")
    ).join(Match, Match.id == MatchHistory.match_id)\
     .filter(Match.status == "completed")\
     .filter(MatchHistory.meta["actor_player_id"].astext == str(user_id))\
     .filter(MatchHistory.event_type == "point")\
     .filter(MatchHistory.meta["attribution_type"].astext == "opponent_error")

    # Logged violations were missing from the profile error chart.
    violations_query = db.query(
        Match.sport,
        MatchHistory.meta["violation_code"].astext.label("violation_code"),
        func.count(MatchHistory.id).label("count")
    ).join(Match, Match.id == MatchHistory.match_id)\
     .filter(Match.status == "completed")\
     .filter(MatchHistory.player_id == user_id)\
     .filter(MatchHistory.event_type == "violation")

    serve_change_query = db.query(
        Match.sport,
        MatchHistory.meta["event_type"].astext.label("serve_event"),
        func.count(MatchHistory.id).label("count")
    ).join(Match, Match.id == MatchHistory.match_id)\
     .filter(Match.status == "completed")\
     .filter(MatchHistory.player_id == user_id)\
     .filter(MatchHistory.event_type == "serve_change")

    # Apply period filter across all queries
    if since is not None:
        shots_query       = shots_query.filter(Match.completed_at >= since)
        errors_query      = errors_query.filter(Match.completed_at >= since)
        violations_query  = violations_query.filter(Match.completed_at >= since)
        serve_change_query = serve_change_query.filter(Match.completed_at >= since)

    if sport:
        shots_query = shots_query.filter(Match.sport == sport)
        errors_query = errors_query.filter(Match.sport == sport)
        violations_query = violations_query.filter(Match.sport == sport)
        serve_change_query = serve_change_query.filter(Match.sport == sport)

    shots_results = shots_query.group_by(Match.sport, "label").all()
    errors_results = errors_query.group_by(Match.sport, "reason_code").all()
    violations_results = violations_query.group_by(Match.sport, "violation_code").all()
    serve_change_results = serve_change_query.group_by(Match.sport, "serve_event").all()

    # Organize by sport
    stats: dict[str, dict[str, list[dict[str, int | str]]]] = {}
    shots_by_sport: dict[str, dict[str, int]] = defaultdict(dict)
    errors_by_sport: dict[str, dict[str, int]] = defaultdict(dict)

    # Initialize sports
    sports_to_process = [sport] if sport else VALID_SPORTS
    for s in sports_to_process:
        stats[s] = {"shots": [], "errors": []}

    for row in shots_results:
        s_name = row.sport.value if hasattr(row.sport, "value") else str(row.sport)
        if s_name in stats:
            _append_stat(
                shots_by_sport[s_name],
                _fallback_stat_label(row.label),
                int(row.count or 0),
            )

    for row in errors_results:
        s_name = row.sport.value if hasattr(row.sport, "value") else str(row.sport)
        if s_name in stats:
            _append_stat(
                errors_by_sport[s_name],
                _ruleset_label(s_name, row.reason_code, "error_types"),
                int(row.count or 0),
            )

    for row in violations_results:
        s_name = row.sport.value if hasattr(row.sport, "value") else str(row.sport)
        if s_name in stats:
            _append_stat(
                errors_by_sport[s_name],
                _ruleset_label(s_name, row.violation_code, "violation_types"),
                int(row.count or 0),
            )

    for row in serve_change_results:
        s_name = row.sport.value if hasattr(row.sport, "value") else str(row.sport)
        if s_name in stats:
            _append_stat(
                errors_by_sport[s_name],
                _serve_change_label(row.serve_event),
                int(row.count or 0),
            )

    for s_name in sports_to_process:
        stats[s_name]["shots"] = [
            {"label": label, "count": count}
            for label, count in sorted(shots_by_sport[s_name].items(), key=lambda item: (-item[1], item[0]))
        ]
        stats[s_name]["errors"] = [
            {"label": label, "count": count}
            for label, count in sorted(errors_by_sport[s_name].items(), key=lambda item: (-item[1], item[0]))
        ]

    return stats


@router.get("/me")
def get_my_profile(
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]

    profile = db.query(Profile).filter(Profile.id == user_id).first()
    if not profile:
        raise HTTPException(status_code=404, detail="Profile not found")

    _raw_ratings = db.query(PlayerRating).filter(PlayerRating.user_id == user_id).all()
    # Deduplicate: keep the row with the highest matches_played (most data) per sport+format
    _seen: dict = {}
    for r in _raw_ratings:
        key = (r.sport.value, r.match_format)
        if key not in _seen or (r.matches_played or 0) > (_seen[key].matches_played or 0):  # type: ignore[operator]
            _seen[key] = r
    ratings = list(_seen.values())

    sports = db.query(SportRegistration).filter(SportRegistration.user_id == user_id).all()
    roles = db.query(UserRoleModel).filter(UserRoleModel.user_id == user_id).all()

    return {
        "profile": {
            "id": str(profile.id),
            "email": profile.email,
            "first_name": profile.first_name,
            "last_name": profile.last_name,
            "avatar_url": profile.avatar_url,
            "region_code": profile.region_code,
            "province_code": profile.province_code,
            "city_mun_code": profile.city_mun_code,
            "barangay_code": profile.barangay_code,
            "gender": profile.gender,
            "profile_setup_complete": profile.profile_setup_complete,
            "phone_number": getattr(profile, "phone_number", None),
            "phone_verified": bool(getattr(profile, "phone_verified", False)),
            "sms_notifications_enabled": bool(getattr(profile, "sms_notifications_enabled", False)),
            "created_at": str(profile.created_at),
        },
        "sports": [
            {"sport": s.sport.value, "registered_at": str(s.registered_at)}
            for s in sports
        ],
        "ratings": [
            {
                "sport":                      r.sport.value,
                "match_format":               r.match_format,
                **_rating_payload(r),
                "rating_deviation":           max(10.0,  min(500.0,  float(r.rating_deviation))),  # type: ignore[arg-type]
                "matches_played":             r.matches_played,
                "wins":                       r.wins,
                "losses":                     r.losses,
                "rating_status":              r.rating_status or "CALIBRATING",
                "distinct_opponents_count":   r.distinct_opponents_count or 0,
                "is_matchmaking_eligible":    bool(r.is_matchmaking_eligible),
                "is_leaderboard_eligible":    bool(r.is_leaderboard_eligible),
                "matchmaking_target":         ML_MATCHMAKING_MIN_MATCHES,
                "leaderboard_target":         LEADERBOARD_MIN_MATCHES,
                "updated_at":                 str(r.updated_at),
                **_performance_payload(r),
            }
            for r in ratings
        ],
        "roles": [r.role.value for r in roles],
    }


@router.put("/me")
@router.patch("/me")
def update_my_profile(
    data: UpdateProfileRequest,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]

    profile = db.query(Profile).filter(Profile.id == user_id).first()
    if not profile:
        raise HTTPException(status_code=404, detail="Profile not found")

    update_data = {k: v for k, v in data.model_dump().items() if v is not None}
    if not update_data:
        raise HTTPException(status_code=400, detail="No fields provided for update")

    # Normalise phone number to E.164 before saving
    if "phone_number" in update_data:
        from app.services.sms import normalise_ph_number
        normalised = normalise_ph_number(update_data["phone_number"])
        if not normalised:
            raise HTTPException(status_code=400, detail="Invalid phone number. Use format: 09XXXXXXXXX or +639XXXXXXXXX")
        update_data["phone_number"] = normalised

    for key, value in update_data.items():
        setattr(profile, key, value)

    db.commit()
    _invalidate_profile_cache(user_id)
    return {"message": "Profile updated successfully"}


# ── Phone OTP verification ────────────────────────────────────────────────────

def _otp_redis():
    try:
        import redis as _redis
        from app.config import settings
        r = _redis.from_url(settings.redis_url, decode_responses=True)
        r.ping()
        return r
    except Exception:
        return None


@router.post("/me/phone/request-otp")
def request_phone_otp(
    body: dict,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Send a 6-digit OTP to the supplied phone number via UniSMS."""
    from app.services.sms import normalise_ph_number, send_sms
    import random

    user_id = current_user["id"]
    raw = (body.get("phone_number") or "").strip()
    if not raw:
        raise HTTPException(400, "phone_number is required.")

    e164 = normalise_ph_number(raw)
    if not e164:
        raise HTTPException(400, "Invalid phone number. Use format: 09XXXXXXXXX or +639XXXXXXXXX")

    # Block if the number is already verified by a different account
    existing = db.query(Profile).filter(
        Profile.phone_number == e164,
        Profile.phone_verified == True,  # noqa: E712
        Profile.id != user_id,
    ).first()
    if existing:
        raise HTTPException(409, "This number is already registered to another account.")

    otp = str(random.randint(100000, 999999))

    r = _otp_redis()
    if r:
        r.setex(f"isms:phone_otp:{user_id}", 300, f"{otp}:{e164}")
    else:
        # Fallback: store temporarily on the profile row (cleared after verify)
        profile = db.query(Profile).filter(Profile.id == user_id).first()
        if profile:
            setattr(profile, "phone_number", f"_otp_{otp}_{e164}")
            db.commit()

    sent = send_sms(e164, f"Your ISMS verification code is {otp}. It expires in 5 minutes.")
    if not sent:
        raise HTTPException(503, "Failed to send OTP. Check the phone number and try again.")

    return {"message": f"OTP sent to {e164[:4]}****{e164[-4:]}"}


@router.post("/me/phone/verify")
def verify_phone_otp(
    body: dict,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Verify the OTP and save the phone number as verified."""
    user_id = current_user["id"]
    submitted_otp = (body.get("otp") or "").strip()
    if not submitted_otp:
        raise HTTPException(400, "otp is required.")

    r = _otp_redis()
    stored = None

    if r:
        stored = r.get(f"isms:phone_otp:{user_id}")
    else:
        profile = db.query(Profile).filter(Profile.id == user_id).first()
        phone_val = getattr(profile, "phone_number", None)
        if isinstance(phone_val, str) and phone_val.startswith("_otp_"):
            stored = phone_val[5:]  # strip "_otp_"

    stored_str: str | None = str(stored) if stored is not None else None
    if not stored_str:
        raise HTTPException(400, "OTP expired or not requested. Please request a new one.")

    expected_otp, e164 = stored_str.split(":", 1)
    if submitted_otp != expected_otp:
        raise HTTPException(400, "Incorrect OTP. Please try again.")

    profile = db.query(Profile).filter(Profile.id == user_id).first()
    if not profile:
        raise HTTPException(404, "Profile not found.")

    setattr(profile, "phone_number", e164)
    setattr(profile, "phone_verified", True)
    setattr(profile, "sms_notifications_enabled", True)
    db.commit()

    if r:
        r.delete(f"isms:phone_otp:{user_id}")

    return {"message": "Phone number verified successfully.", "phone_number": e164}


@router.get("/sports/me")
def get_my_sports(
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]

    sports = db.query(SportRegistration).filter(SportRegistration.user_id == user_id).all()
    ratings = db.query(PlayerRating).filter(PlayerRating.user_id == user_id).all()

    ratings_map = {r.sport.value: r for r in ratings}

    return [
        {
            "sport": s.sport.value,
            "registered_at": str(s.registered_at),
            "rating_info": {
                "rating": float(ratings_map[s.sport.value].rating),             # type: ignore[arg-type]
                "rating_deviation": float(ratings_map[s.sport.value].rating_deviation),  # type: ignore[arg-type]
                "matches_played": ratings_map[s.sport.value].matches_played,
                "wins": ratings_map[s.sport.value].wins,
                "losses": ratings_map[s.sport.value].losses,
                "skill_level": _skill_level(float(ratings_map[s.sport.value].rating), ratings_map[s.sport.value].rating_status or "CALIBRATING"),  # type: ignore[arg-type]
                **_performance_payload(ratings_map[s.sport.value]),
            } if s.sport.value in ratings_map else None,
        }
        for s in sports
    ]


@router.post("/sports/register", status_code=status.HTTP_201_CREATED)
def register_sport(
    data: SportsRegisterRequest,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if data.sport not in VALID_SPORTS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid sport '{data.sport}'. Valid options: {VALID_SPORTS}"
        )

    user_id = current_user["id"]

    # Check if already registered
    existing = db.query(SportRegistration).filter(
        SportRegistration.user_id == user_id,
        SportRegistration.sport == data.sport,
    ).first()

    if not existing:
        db.add(SportRegistration(user_id=user_id, sport=data.sport))

    # Seed rating rows per format
    for fmt in ["singles", "doubles", "mixed_doubles"]:
        existing_rating = db.query(PlayerRating).filter(
            PlayerRating.user_id == user_id,
            PlayerRating.sport == data.sport,
            PlayerRating.match_format == fmt,
        ).first()

        if not existing_rating:
            db.add(PlayerRating(
                user_id=user_id,
                sport=data.sport,
                match_format=fmt,
                rating=1500,
                rating_deviation=350,
                volatility=0.06,
                matches_played=0,
                wins=0,
                losses=0,
                rating_status="CALIBRATING",
                calibration_matches_played=0,
                distinct_opponents_count=0,
                is_matchmaking_eligible=False,
                is_leaderboard_eligible=False,
            ))

    db.commit()
    return {"message": f"Successfully registered for {data.sport}."}


@router.get("/search")
def search_players(
    q: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if len(q.strip()) < 2:
        raise HTTPException(400, "Query must be at least 2 characters.")

    query_str = f"%{q.strip()}%"
    results = db.query(Profile).filter(
        or_(
            Profile.first_name.ilike(query_str),
            Profile.last_name.ilike(query_str),
        ),
        Profile.id != current_user["id"],
    ).limit(10).all()

    return {
        "players": [
            {
                "id": str(p.id),
                "first_name": p.first_name,
                "last_name": p.last_name,
                "avatar_url": p.avatar_url,
            }
            for p in results
        ]
    }


@router.get("/{player_id}")
def get_player_profile(
    player_id: str,
    db: Session = Depends(get_db),
):
    cache_key = f"profile:{player_id}"
    redis = _get_redis()
    if redis:
        try:
            cached = redis.get(cache_key)
            if cached:
                return json.loads(str(cached))
        except Exception:
            pass

    profile = db.query(Profile).filter(Profile.id == player_id).first()
    if not profile:
        raise HTTPException(status_code=404, detail="Player not found.")

    _raw_ratings = db.query(PlayerRating).filter(PlayerRating.user_id == player_id).all()
    _seen2: dict = {}
    for r in _raw_ratings:
        key = (r.sport.value, r.match_format)
        if key not in _seen2 or (r.matches_played or 0) > (_seen2[key].matches_played or 0):  # type: ignore[operator]
            _seen2[key] = r
    ratings = list(_seen2.values())

    payload = {
        "profile": {
            "id": str(profile.id),
            "first_name": profile.first_name,
            "last_name": profile.last_name,
            "avatar_url": profile.avatar_url,
            "region_code": profile.region_code,
            "province_code": profile.province_code,
            "city_mun_code": profile.city_mun_code,
            "gender": profile.gender,
        },
        "ratings": [
            {
                "sport":                   r.sport.value,
                "match_format":            r.match_format,
                **_rating_payload(r),
                "rating_deviation":        max(10.0, min(500.0, float(r.rating_deviation))),  # type: ignore[arg-type]
                "matches_played":          r.matches_played,
                "wins":                    r.wins,
                "losses":                  r.losses,
                "rating_status":           r.rating_status or "CALIBRATING",
                "distinct_opponents_count":   r.distinct_opponents_count or 0,
                "is_matchmaking_eligible": bool(r.is_matchmaking_eligible),
                "is_leaderboard_eligible": bool(r.is_leaderboard_eligible),
                "matchmaking_target":      ML_MATCHMAKING_MIN_MATCHES,
                "leaderboard_target":      LEADERBOARD_MIN_MATCHES,
                "updated_at":              str(r.updated_at),
                **_performance_payload(r),
            }
            for r in ratings
        ],
    }
    if redis:
        try:
            redis.setex(cache_key, _PROFILE_TTL, json.dumps(payload))
        except Exception:
            pass
    return payload
