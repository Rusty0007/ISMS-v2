from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.database import get_db
from app.middleware.auth import get_current_user
from app.models.models import Profile
from app.services.rating_policy import (
    LEADERBOARD_MIN_DISTINCT_OPPONENTS,
    LEADERBOARD_MIN_MATCHES,
    LEADERBOARD_RD_THRESHOLD,
    ML_MATCHMAKING_MIN_MATCHES,
)
from app.utils.skill_tiers import SKILL_TIER_DEFINITIONS

router = APIRouter()

TIER_ORDER = [tier.name for tier in SKILL_TIER_DEFINITIONS]

# Canonical level names keyed by lowercase slug (for query param → DB lookup)
_LEVEL_SLUGS = {tier.slug: tier.name for tier in SKILL_TIER_DEFINITIONS}


NATIONAL_MIN_RATING   = 1900  # Minimum rating to appear on National leaderboard
NATIONAL_MIN_MATCHES  = LEADERBOARD_MIN_MATCHES


def _build_filters(sport: str, match_format: str, geo_level: str, profile: Profile | None):
    """Return WHERE clause filters and params for leaderboard_view."""
    # Only officially rated (calibration complete) players appear on public leaderboards
    filters = [
        "sport::TEXT = :sport",
        "match_format::TEXT = :match_format",
        "is_leaderboard_eligible = TRUE",
        "matches_played >= :leaderboard_min_matches",
        "distinct_opponents_count >= :leaderboard_min_opponents",
        "rating_deviation <= :leaderboard_rd_threshold",
    ]
    params: dict = {
        "sport": sport,
        "match_format": match_format,
        "leaderboard_min_matches": LEADERBOARD_MIN_MATCHES,
        "leaderboard_min_opponents": LEADERBOARD_MIN_DISTINCT_OPPONENTS,
        "leaderboard_rd_threshold": LEADERBOARD_RD_THRESHOLD,
    }

    if geo_level == "national":
        # National board is exclusive — requires minimum rating + match count
        filters.append("rating >= :nat_min_rating")
        filters.append("matches_played >= :nat_min_matches")
        params["nat_min_rating"]  = NATIONAL_MIN_RATING
        params["nat_min_matches"] = NATIONAL_MIN_MATCHES
    elif geo_level == "regional" and profile is not None and profile.region_code is not None:
        filters.append("region_code = :region_code")
        params["region_code"] = profile.region_code
    elif geo_level == "provincial" and profile is not None and profile.province_code is not None:
        filters.append("province_code = :province_code")
        params["province_code"] = profile.province_code
    elif geo_level == "city" and profile is not None and profile.city_mun_code is not None:
        filters.append("city_mun_code = :city_mun_code")
        params["city_mun_code"] = profile.city_mun_code
    elif geo_level == "barangay" and profile is not None and profile.barangay_code is not None:
        filters.append("barangay_code = :barangay_code")
        params["barangay_code"] = profile.barangay_code

    return " AND ".join(filters), params


@router.get("")
def get_leaderboard(
    sport:        str = Query(...),
    match_format: str = Query("singles"),
    geo_level:    str = Query("national"),   # national | regional | provincial | city
    limit:        int = Query(50, le=100),
    offset:       int = Query(0, ge=0),
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_id = current_user["id"]
    profile = db.query(Profile).filter(Profile.id == user_id).first()

    where, params = _build_filters(sport, match_format, geo_level, profile)

    # Ranked query with window function
    sql = text(f"""
        SELECT
            ROW_NUMBER() OVER (ORDER BY rating DESC, wins DESC) AS rank,
            user_id, username, first_name, last_name,
            region_code, province_code, city_mun_code,
            rating, rating_deviation, wins, losses, matches_played,
            distinct_opponents_count, current_win_streak, win_rate_pct, skill_tier, activeness_score
        FROM leaderboard_view
        WHERE {where}
        ORDER BY rating DESC, wins DESC
        LIMIT :limit OFFSET :offset
    """)
    params["limit"]  = limit
    params["offset"] = offset

    rows = db.execute(sql, params).mappings().all()

    result = []
    my_rank_in_page = None
    for row in rows:
        entry = {
            "rank":             int(row["rank"]),
            "user_id":          str(row["user_id"]),
            "username":         row["username"],
            "first_name":       row["first_name"],
            "last_name":        row["last_name"],
            "rating":           round(float(row["rating"]), 1) if row["rating"] else 1500.0,
            "rating_deviation": round(float(row["rating_deviation"]), 1) if row["rating_deviation"] else 350.0,
            "wins":             row["wins"] or 0,
            "losses":           row["losses"] or 0,
            "matches_played":   row["matches_played"] or 0,
            "distinct_opponents_count": row["distinct_opponents_count"] or 0,
            "win_rate_pct":     round(float(row["win_rate_pct"]), 1) if row["win_rate_pct"] else 0.0,
            "current_win_streak": row["current_win_streak"] or 0,
            "skill_tier":       row["skill_tier"],
            "activeness_score": round(float(row["activeness_score"]), 2) if row["activeness_score"] else 0.0,
            "region_code":      row["region_code"],
            "province_code":    row["province_code"],
            "city_mun_code":    row["city_mun_code"],
            "is_me":            str(row["user_id"]) == user_id,
        }
        if entry["is_me"]:
            my_rank_in_page = entry["rank"]
        result.append(entry)

    # Count total for this filter
    count_sql = text(f"SELECT COUNT(*) FROM leaderboard_view WHERE {where}")
    count_params = {k: v for k, v in params.items() if k not in ("limit", "offset")}
    total = db.execute(count_sql, count_params).scalar() or 0

    # Get current user's rank if not in current page
    my_rank = my_rank_in_page
    if my_rank is None:
        rank_sql = text(f"""
            SELECT sub.rank FROM (
                SELECT
                    user_id,
                    ROW_NUMBER() OVER (ORDER BY rating DESC, wins DESC) AS rank
                FROM leaderboard_view
                WHERE {where}
            ) sub
            WHERE sub.user_id = :me_id
        """)
        my_rank_params = {**count_params, "me_id": user_id}
        my_rank_row = db.execute(rank_sql, my_rank_params).fetchone()
        if my_rank_row:
            my_rank = int(my_rank_row[0])

    return {
        "leaderboard":  result,
        "total":        int(total),
        "offset":       offset,
        "limit":        limit,
        "my_rank":      my_rank,
        "sport":        sport,
        "match_format": match_format,
        "geo_level":    geo_level,
    }


@router.get("/competitive")
def get_competitive_leaderboard(
    sport:        str = Query(...),
    match_format: str = Query("singles"),
    level:        str = Query(...),     # novice | advanced_beginner | competent | proficient | expert
    limit:        int = Query(50, le=100),
    offset:       int = Query(0, ge=0),
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Returns players whose Glicko-2 rating places them in the requested
    competitive tier AND who have played ≥ minimum_matches_required matches
    in the last 90 days for the given sport+format.
    Since tier ranges are non-overlapping, each player appears in exactly one tier.
    """
    level_name = _LEVEL_SLUGS.get(level.lower())
    if not level_name:
        raise HTTPException(status_code=400, detail=f"Unknown level '{level}'. Valid: {list(_LEVEL_SLUGS.keys())}")

    level_row = db.execute(text("""
        SELECT min_rating, max_rating, minimum_matches_required, COALESCE(rd_threshold, 200)
        FROM ranking_levels
        WHERE level_name = :lname AND is_active = TRUE
    """), {"lname": level_name}).fetchone()

    if not level_row:
        raise HTTPException(status_code=404, detail="Level not configured in database")

    min_r, max_r, min_matches, rd_threshold = level_row
    min_r        = float(min_r)
    max_r        = float(max_r) if max_r is not None else None
    rd_threshold = float(rd_threshold)

    user_id = current_user["id"]

    # Rating range filter
    rating_clause = "lv.rating >= :min_r AND lv.rating_deviation <= :rd_threshold"
    params: dict = {
        "sport": sport, "match_format": match_format,
        "min_r": min_r, "min_matches": int(min_matches),
        "rd_threshold": rd_threshold,
        "leaderboard_min_matches": LEADERBOARD_MIN_MATCHES,
        "leaderboard_min_opponents": LEADERBOARD_MIN_DISTINCT_OPPONENTS,
        "limit": limit, "offset": offset,
    }
    if max_r is not None:
        rating_clause += " AND lv.rating < :max_r"
        params["max_r"] = max_r

    # National → no location filter

    rows = db.execute(text(f"""
        WITH recent_activity AS (
            SELECT player1_id AS uid, COUNT(*) AS cnt
            FROM matches
            WHERE status = 'completed'
              AND sport::TEXT = :sport
              AND match_format::TEXT = :match_format
              AND created_at >= NOW() - INTERVAL '90 days'
            GROUP BY player1_id
            UNION ALL
            SELECT player2_id, COUNT(*) FROM matches
            WHERE status = 'completed'
              AND sport::TEXT = :sport
              AND match_format::TEXT = :match_format
              AND created_at >= NOW() - INTERVAL '90 days'
            GROUP BY player2_id
        ),
        activity AS (
            SELECT uid, SUM(cnt) AS recent_count FROM recent_activity GROUP BY uid
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY lv.rating DESC, lv.wins DESC) AS rank,
            lv.user_id, lv.first_name, lv.last_name,
            lv.rating, lv.rating_deviation, lv.wins, lv.losses, lv.matches_played,
            lv.distinct_opponents_count, lv.win_rate_pct, lv.current_win_streak, lv.skill_tier,
            COALESCE(a.recent_count, 0) AS recent_matches
        FROM leaderboard_view lv
        LEFT JOIN activity a ON a.uid = lv.user_id
        WHERE lv.sport::TEXT = :sport
          AND lv.match_format::TEXT = :match_format
          AND {rating_clause}
          AND COALESCE(a.recent_count, 0) >= :min_matches
          AND lv.is_leaderboard_eligible = TRUE
          AND lv.matches_played >= :leaderboard_min_matches
          AND lv.distinct_opponents_count >= :leaderboard_min_opponents
        ORDER BY lv.rating DESC, lv.wins DESC
        LIMIT :limit OFFSET :offset
    """), params).mappings().all()

    count_params = {k: v for k, v in params.items() if k not in ("limit", "offset")}
    total = db.execute(text(f"""
        WITH recent_activity AS (
            SELECT player1_id AS uid, COUNT(*) AS cnt FROM matches
            WHERE status='completed' AND sport::TEXT=:sport AND match_format::TEXT=:match_format
              AND created_at >= NOW() - INTERVAL '90 days' GROUP BY player1_id
            UNION ALL
            SELECT player2_id, COUNT(*) FROM matches
            WHERE status='completed' AND sport::TEXT=:sport AND match_format::TEXT=:match_format
              AND created_at >= NOW() - INTERVAL '90 days' GROUP BY player2_id
        ), activity AS (SELECT uid, SUM(cnt) AS recent_count FROM recent_activity GROUP BY uid)
        SELECT COUNT(*) FROM leaderboard_view lv LEFT JOIN activity a ON a.uid=lv.user_id
        WHERE lv.sport::TEXT=:sport AND lv.match_format::TEXT=:match_format
          AND {rating_clause} AND COALESCE(a.recent_count,0) >= :min_matches
          AND lv.is_leaderboard_eligible=TRUE
          AND lv.matches_played >= :leaderboard_min_matches
          AND lv.distinct_opponents_count >= :leaderboard_min_opponents
    """), count_params).scalar() or 0

    result = []
    my_rank_in_page = None
    for row in rows:
        entry = {
            "rank":               int(row["rank"]),
            "user_id":            str(row["user_id"]),
            "first_name":         row["first_name"],
            "last_name":          row["last_name"],
            "rating":             round(float(row["rating"]), 1) if row["rating"] else 1500.0,
            "rating_deviation":   round(float(row["rating_deviation"]), 1) if row["rating_deviation"] else 350.0,
            "wins":               row["wins"] or 0,
            "losses":             row["losses"] or 0,
            "matches_played":     row["matches_played"] or 0,
            "distinct_opponents_count": row["distinct_opponents_count"] or 0,
            "win_rate_pct":       round(float(row["win_rate_pct"]), 1) if row["win_rate_pct"] else 0.0,
            "current_win_streak": row["current_win_streak"] or 0,
            "skill_tier":         row["skill_tier"],
            "recent_matches":     int(row["recent_matches"]),
            "is_me":              str(row["user_id"]) == user_id,
        }
        if entry["is_me"]:
            my_rank_in_page = entry["rank"]
        result.append(entry)

    return {
        "leaderboard":   result,
        "total":         int(total),
        "offset":        offset,
        "limit":         limit,
        "my_rank":       my_rank_in_page,
        "level":         level_name,
        "sport":         sport,
        "match_format":  match_format,
        "min_rating":    min_r,
        "max_rating":    max_r,
        "min_matches":   int(min_matches),
        "rd_threshold":  rd_threshold,
    }


@router.get("/competitive/me")
def get_my_competitive_level(
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    For the current user, returns their competitive level per sport+format,
    plus how many rating points they need to reach the next tier and
    how many recent matches they have in the last 90 days.
    """
    user_id = current_user["id"]

    # Fetch all active tiers ordered by rating asc
    tiers = db.execute(text("""
        SELECT level_name, min_rating, max_rating, display_order, is_top_level,
               minimum_matches_required, COALESCE(rd_threshold, 200) AS rd_threshold
        FROM ranking_levels WHERE is_active = TRUE ORDER BY display_order ASC
    """)).mappings().all()

    # Fetch user ratings + RD + calibration state
    ratings = db.execute(text("""
        SELECT sport::TEXT, match_format, rating, rating_deviation,
               COALESCE(rating_status, 'CALIBRATING') AS rating_status,
               COALESCE(calibration_matches_played, 0) AS calibration_matches_played,
               COALESCE(distinct_opponents_count, 0) AS distinct_opponents_count,
               COALESCE(is_matchmaking_eligible, FALSE) AS is_matchmaking_eligible,
               COALESCE(is_leaderboard_eligible, FALSE) AS is_leaderboard_eligible
        FROM player_ratings WHERE user_id = :uid
    """), {"uid": user_id}).mappings().all()

    # Fetch recent match counts per sport+format
    recent = db.execute(text("""
        WITH ra AS (
            SELECT sport::TEXT AS sport, match_format::TEXT AS match_format, player1_id AS uid FROM matches
            WHERE status='completed' AND created_at >= NOW()-INTERVAL '90 days'
            UNION ALL
            SELECT sport::TEXT, match_format::TEXT, player2_id FROM matches
            WHERE status='completed' AND created_at >= NOW()-INTERVAL '90 days'
        )
        SELECT sport, match_format, COUNT(*) AS cnt FROM ra WHERE uid = :uid GROUP BY sport, match_format
    """), {"uid": user_id}).mappings().all()

    recent_map: dict[tuple, int] = {(r["sport"], r["match_format"]): int(r["cnt"]) for r in recent}

    results = []
    for r in ratings:
        sport           = r["sport"]
        fmt             = r["match_format"]
        rat             = float(r["rating"]) if r["rating"] else 1500.0
        p_rd            = float(r["rating_deviation"]) if r["rating_deviation"] else 350.0
        rc              = recent_map.get((sport, fmt), 0)
        rating_status   = r["rating_status"]
        cal_played      = int(r["calibration_matches_played"])
        distinct_opps   = int(r["distinct_opponents_count"])
        ml_ready        = bool(r["is_matchmaking_eligible"])
        is_rated        = bool(r["is_leaderboard_eligible"])
        is_calibrating  = (rating_status == "CALIBRATING")

        current_level = None
        next_level    = None
        for tier in tiers:
            lo          = float(tier["min_rating"])
            hi          = float(tier["max_rating"]) if tier["max_rating"] else None
            rd_thresh   = float(tier["rd_threshold"])
            if hi is None:
                in_tier = rat >= lo
            else:
                in_tier = lo <= rat < hi
            if in_tier:
                meets_matches = rc >= tier["minimum_matches_required"]
                meets_rd      = p_rd <= rd_thresh
                current_level = {
                    "name":            tier["level_name"],
                    "min_rating":      lo,
                    "max_rating":      hi,
                    "display_order":   tier["display_order"],
                    "is_top_level":    tier["is_top_level"],
                    "min_matches":     tier["minimum_matches_required"],
                    "rd_threshold":    rd_thresh,
                    "active":          meets_matches and meets_rd,
                    "meets_matches":   meets_matches,
                    "meets_rd":        meets_rd,
                    "recent_matches":  rc,
                    "rating_deviation": round(p_rd, 1),
                }
                break

        # Find next tier
        if current_level and not current_level["is_top_level"]:
            for tier in tiers:
                if tier["display_order"] == current_level["display_order"] + 1:
                    next_lo = float(tier["min_rating"])
                    next_level = {
                        "name":          tier["level_name"],
                        "min_rating":    next_lo,
                        "rating_needed": max(0.0, round(next_lo - rat, 1)),
                        "min_matches":   tier["minimum_matches_required"],
                        "rd_threshold":  float(tier["rd_threshold"]),
                    }
                    break

        results.append({
            "sport":                      sport,
            "match_format":               fmt,
            "rating":                     round(rat, 1),
            "recent_matches":             rc,
            "current_level":              current_level,
            "next_level":                 next_level,
            # Calibration
            "rating_status":              rating_status,
            "is_calibrating":             is_calibrating,
            "is_matchmaking_eligible":    ml_ready,
            "is_leaderboard_eligible":    is_rated,
            "calibration_matches_played": cal_played,
            "distinct_opponents_count":   distinct_opps,
            "matchmaking_target":         ML_MATCHMAKING_MIN_MATCHES,
            "matchmaking_remaining":      max(0, ML_MATCHMAKING_MIN_MATCHES - cal_played) if not ml_ready else 0,
            "calibration_target":         LEADERBOARD_MIN_MATCHES,
            "calibration_remaining":      max(0, LEADERBOARD_MIN_MATCHES - cal_played) if is_calibrating else 0,
            "leaderboard_distinct_opponents_target": LEADERBOARD_MIN_DISTINCT_OPPONENTS,
            "leaderboard_distinct_opponents_remaining": max(0, LEADERBOARD_MIN_DISTINCT_OPPONENTS - distinct_opps) if not is_rated else 0,
        })

    return {"competitive_levels": results}


@router.get("/me")
def get_my_rankings(
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Return the current user's rank and rating across all sport/format combos."""
    user_id = current_user["id"]

    sql = text("""
        SELECT
            sub.sport,
            sub.match_format,
            sub.rating,
            sub.wins,
            sub.losses,
            sub.matches_played,
            sub.win_rate_pct,
            sub.skill_tier,
            sub.current_win_streak,
            sub.rank
        FROM (
            SELECT
                user_id, sport, match_format, rating, wins, losses,
                matches_played, win_rate_pct, skill_tier, current_win_streak,
                ROW_NUMBER() OVER (PARTITION BY sport, match_format ORDER BY rating DESC, wins DESC) AS rank
            FROM leaderboard_view
            WHERE is_leaderboard_eligible = TRUE
              AND matches_played >= :leaderboard_min_matches
              AND distinct_opponents_count >= :leaderboard_min_opponents
              AND rating_deviation <= :leaderboard_rd_threshold
        ) sub
        WHERE sub.user_id = :uid
        ORDER BY sub.sport, sub.match_format
    """)

    rows = db.execute(sql, {
        "uid": user_id,
        "leaderboard_min_matches": LEADERBOARD_MIN_MATCHES,
        "leaderboard_min_opponents": LEADERBOARD_MIN_DISTINCT_OPPONENTS,
        "leaderboard_rd_threshold": LEADERBOARD_RD_THRESHOLD,
    }).mappings().all()

    return {
        "rankings": [
            {
                "sport":            r["sport"],
                "match_format":     r["match_format"],
                "rating":           round(float(r["rating"]), 1) if r["rating"] else 1500.0,
                "wins":             r["wins"] or 0,
                "losses":           r["losses"] or 0,
                "matches_played":   r["matches_played"] or 0,
                "win_rate_pct":     round(float(r["win_rate_pct"]), 1) if r["win_rate_pct"] else 0.0,
                "skill_tier":       r["skill_tier"],
                "current_win_streak": r["current_win_streak"] or 0,
                "rank":             int(r["rank"]),
            }
            for r in rows
        ]
    }
