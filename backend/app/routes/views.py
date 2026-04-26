# backend/app/routes/views.py
# API endpoints that query SQL views directly

from fastapi import APIRouter, Depends, Query
from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.database import get_db
from app.middleware.auth import get_current_user

router = APIRouter()


# ── Leaderboard View ──────────────────────────────────────

@router.get("/leaderboard")
def get_leaderboard(
    sport: str,
    match_format: str = "singles",
    region_code: Optional[str] = None,
    province_code: Optional[str] = None,
    city_mun_code: Optional[str] = None,
    limit: int = 50,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    filters = ["sport = :sport", "match_format = :match_format", "is_leaderboard_eligible = TRUE"]
    params  = {"sport": sport, "match_format": match_format, "limit": limit}

    if region_code:
        filters.append("region_code = :region_code")
        params["region_code"] = region_code
    if province_code:
        filters.append("province_code = :province_code")
        params["province_code"] = province_code
    if city_mun_code:
        filters.append("city_mun_code = :city_mun_code")
        params["city_mun_code"] = city_mun_code

    where = " AND ".join(filters)
    sql = text(f"SELECT * FROM leaderboard_view WHERE {where} LIMIT :limit")

    rows = db.execute(sql, params).mappings().all()
    return {"leaderboard": [dict(r) for r in rows]}


# ── Player Profile Summary View ───────────────────────────

@router.get("/profile-summary/{user_id}")
def get_profile_summary(
    user_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    rows = db.execute(
        text("SELECT * FROM player_profile_summary WHERE user_id = :uid"),
        {"uid": user_id}
    ).mappings().all()

    if not rows:
        return {"profile": None}

    # First row has profile info, group ratings by sport
    first = dict(rows[0])
    profile = {
        "user_id":               first["user_id"],
        "first_name":            first["first_name"],
        "last_name":             first["last_name"],
        "profile_setup_complete": first["profile_setup_complete"],
        "region_code":           first["region_code"],
        "province_code":         first["province_code"],
        "city_mun_code":         first["city_mun_code"],
        "barangay_code":         first["barangay_code"],
        "member_since":          str(first["member_since"]),
        "roles":                 first["roles"],
        "ratings": [
            {
                "sport":              r["sport"],
                "match_format":       r["match_format"],
                "rating":             float(r["rating"]) if r["rating"] else None,
                "rating_deviation":   float(r["rating_deviation"]) if r["rating_deviation"] else None,
                "wins":               r["wins"],
                "losses":             r["losses"],
                "matches_played":     r["matches_played"],
                "win_rate_pct":       float(r["win_rate_pct"]) if r["win_rate_pct"] else 0,
                "skill_tier":         r["skill_tier"],
                "current_win_streak": r["current_win_streak"],
                "activeness_score":   float(r["activeness_score"]) if r["activeness_score"] else 0,
            }
            for r in rows if r["sport"] is not None
        ],
    }

    return {"profile": profile}


# ── Match History View ────────────────────────────────────

@router.get("/match-history/{user_id}")
def get_match_history(
    user_id: str,
    sport: Optional[str] = None,
    limit: int = 20,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    filters = ["""(
        player1_id = :uid OR player2_id = :uid OR player3_id = :uid OR player4_id = :uid
        OR team1_player1 = :uid OR team1_player2 = :uid OR team2_player1 = :uid OR team2_player2 = :uid
    )"""]
    params  = {"uid": user_id, "limit": limit}

    if sport:
        filters.append("sport::TEXT = :sport")
        params["sport"] = sport

    where = " AND ".join(filters)
    sql = text(f"""
        SELECT * FROM match_history_view
        WHERE {where}
        ORDER BY created_at DESC
        LIMIT :limit
    """)

    rows = db.execute(sql, params).mappings().all()
    return {"matches": [dict(r) for r in rows]}


# ── Active Queue View ─────────────────────────────────────

@router.get("/active-queue")
def get_active_queue(
    sport: str,
    match_format: str = "singles",
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    rows = db.execute(
        text("""
            SELECT * FROM active_queue_view
            WHERE sport::TEXT = :sport
            AND match_format::TEXT = :match_format
        """),
        {"sport": sport, "match_format": match_format}
    ).mappings().all()

    return {
        "sport": sport,
        "match_format": match_format,
        "queue_count": len(rows),
        "queue": [dict(r) for r in rows],
    }


# ── Tournament Standings View ─────────────────────────────

@router.get("/tournament-standings/{tournament_id}")
def get_tournament_standings(
    tournament_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    rows = db.execute(
        text("SELECT * FROM tournament_standings_view WHERE tournament_id = :tid"),
        {"tid": tournament_id}
    ).mappings().all()

    return {
        "tournament_id": tournament_id,
        "standings": [dict(r) for r in rows],
    }
