from fastapi import APIRouter, HTTPException, Depends, status
from pydantic import BaseModel
from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy import or_
from app.database import get_db
from app.middleware.auth import get_current_user
from app.models.models import Profile, UserRoleModel, SportRegistration, PlayerRating
import uuid

router = APIRouter()

VALID_SPORTS = ["pickleball", "badminton", "lawn_tennis", "table_tennis"]

# ── Request Models ────────────────────────────────────────

class UpdateProfileRequest(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    avatar_url: Optional[str] = None
    region_code: Optional[str] = None
    province_code: Optional[str] = None
    city_mun_code: Optional[str] = None
    barangay_code: Optional[str] = None
    profile_setup_complete: Optional[bool] = None

class SportsRegisterRequest(BaseModel):
    sport: str

# ── Routes ────────────────────────────────────────────────

@router.get("/me")
def get_my_profile(
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]

    profile = db.query(Profile).filter(Profile.id == user_id).first()
    if not profile:
        raise HTTPException(status_code=404, detail="Profile not found")

    ratings = db.query(PlayerRating).filter(PlayerRating.user_id == user_id).all()
    sports = db.query(SportRegistration).filter(SportRegistration.user_id == user_id).all()
    roles = db.query(UserRoleModel).filter(UserRoleModel.user_id == user_id).all()

    return {
        "profile": {
            "id": str(profile.id),
            "username": profile.username,
            "email": profile.email,
            "first_name": profile.first_name,
            "last_name": profile.last_name,
            "avatar_url": profile.avatar_url,
            "region_code": profile.region_code,
            "province_code": profile.province_code,
            "city_mun_code": profile.city_mun_code,
            "barangay_code": profile.barangay_code,
            "profile_setup_complete": profile.profile_setup_complete,
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
                "rating":                     float(r.rating),             # type: ignore[arg-type]
                "rating_deviation":           float(r.rating_deviation),  # type: ignore[arg-type]
                "matches_played":             r.matches_played,
                "wins":                       r.wins,
                "losses":                     r.losses,
                "rating_status":              r.rating_status or "CALIBRATING",
                "calibration_matches_played": r.calibration_matches_played or 0,
                "is_leaderboard_eligible":    bool(r.is_leaderboard_eligible),
                "updated_at":                 str(r.updated_at),
            }
            for r in ratings
        ],
        "roles": [r.role.value for r in roles],
    }


@router.put("/me")
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

    for key, value in update_data.items():
        setattr(profile, key, value)

    db.commit()
    return {"message": "Profile updated successfully"}


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

    results = db.query(Profile).filter(
        Profile.username.ilike(f"{q.strip()}%"),
        Profile.id != current_user["id"],
    ).limit(10).all()

    return {
        "players": [
            {
                "id": str(p.id),
                "username": p.username,
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
    profile = db.query(Profile).filter(Profile.id == player_id).first()
    if not profile:
        raise HTTPException(status_code=404, detail="Player not found.")

    ratings = db.query(PlayerRating).filter(PlayerRating.user_id == player_id).all()

    return {
        "profile": {
            "id": str(profile.id),
            "username": profile.username,
            "first_name": profile.first_name,
            "last_name": profile.last_name,
            "avatar_url": profile.avatar_url,
            "region_code": profile.region_code,
            "province_code": profile.province_code,
            "city_mun_code": profile.city_mun_code,
        },
        "ratings": [
            {
                "sport": r.sport.value,
                "rating": float(r.rating),  # type: ignore[arg-type]
                "matches_played": r.matches_played,
                "wins": r.wins,
                "losses": r.losses,
            }
            for r in ratings
        ],
    }   