import logging
from typing import Optional, cast
from sqlalchemy.orm import Session
from app.database import SessionLocal
from app.models.models import Match, PlayerRating, Profile
from app.utils.skill_tiers import get_skill_tier_slug

logger = logging.getLogger(__name__)

SPORT_MAX_SETS = {
    "pickleball":   3,
    "badminton":    3,
    "table_tennis": 5,
    "lawn_tennis":  3,
}

# ── Helpers ───────────────────────────────────────────────

def compute_real_match_quality(
    score_w: int, score_l: int, nb_sets: int,
    max_sets: int, rating_diff: float, upset: bool
) -> float:
    total = score_w + score_l
    closeness    = (1.0 - abs(score_w - score_l) / total) if total > 0 else 0.5
    sets_factor  = nb_sets / max_sets if max_sets > 0 else 0.5
    rating_factor = max(0.0, 1.0 - rating_diff / 600.0)
    upset_bonus  = 0.05 if upset else 0.0
    quality = (closeness * 0.40) + (sets_factor * 0.20) + (rating_factor * 0.40) + upset_bonus
    return round(min(max(quality, 0.0), 1.0), 4)


def compute_geo_score(
    city_a, city_b, province_a, province_b, region_a, region_b
) -> float:
    if city_a and city_b and city_a == city_b:
        return 1.0
    if province_a and province_b and province_a == province_b:
        return 0.7
    if region_a and region_b and region_a == region_b:
        return 0.4
    return 0.2


def get_skill_category(rating: float) -> str:
    return get_skill_tier_slug(rating)


def safe_win_rate(rating: PlayerRating | None, default: float = 0.5) -> float:
    if rating is None:
        return default
    mp: int = cast(int, rating.matches_played) or 0
    if mp <= 0:
        return default
    wins: int = cast(int, rating.wins) or 0
    return float(wins) / mp


# ── Main Collector ────────────────────────────────────────

def save_training_row(match_id: str) -> bool:
    """
    Extract features from a completed match and save
    as a training row in matchmaking_training_data.
    """
    from sqlalchemy import text

    db: Session = SessionLocal()
    try:
        match = db.query(Match).filter(Match.id == match_id).first()
        if not match:
            logger.warning(f"Match {match_id} not found")
            return False

        if match.status.value != "completed":
            logger.info(f"Match {match_id} is not completed yet, skipping")
            return False

        sport        = match.sport.value
        match_format = match.match_format.value
        match_type   = match.match_type.value
        player1_id   = str(match.player1_id) if match.player1_id is not None else None
        player2_id   = str(match.player2_id) if match.player2_id is not None else None
        winner_id    = str(match.winner_id)  if match.winner_id  is not None else None

        if not all([sport, match_format, player1_id, player2_id, winner_id]):
            logger.warning(f"Match {match_id} missing required fields")
            return False

        # Fetch ratings
        ra = db.query(PlayerRating).filter(
            PlayerRating.user_id == player1_id,
            PlayerRating.sport == sport,
            PlayerRating.match_format == match_format,
        ).first()

        rb = db.query(PlayerRating).filter(
            PlayerRating.user_id == player2_id,
            PlayerRating.sport == sport,
            PlayerRating.match_format == match_format,
        ).first()

        if not ra or not rb:
            logger.warning(f"Missing player ratings for match {match_id}")
            return False

        # Fetch profiles for geo data
        pa = db.query(Profile).filter(Profile.id == player1_id).first()
        pb = db.query(Profile).filter(Profile.id == player2_id).first()

        rating_a = float(ra.rating)           # type: ignore[arg-type]
        rating_b = float(rb.rating)           # type: ignore[arg-type]
        rd_a     = float(ra.rating_deviation) # type: ignore[arg-type]
        rd_b     = float(rb.rating_deviation) # type: ignore[arg-type]
        wr_a     = safe_win_rate(ra)
        wr_b     = safe_win_rate(rb)
        act_a    = float(ra.activeness_score) # type: ignore[arg-type]
        act_b    = float(rb.activeness_score) # type: ignore[arg-type]
        str_a    = int(ra.current_win_streak) # type: ignore[arg-type]
        str_b    = int(rb.current_win_streak) # type: ignore[arg-type]

        rating_diff     = abs(rating_a - rating_b)
        avg_rd          = (rd_a + rd_b) / 2
        winrate_diff    = abs(wr_a - wr_b)
        activeness_diff = abs(act_a - act_b)
        streak_diff     = abs(str_a - str_b)

        geo_score = compute_geo_score(
            city_a=pa.city_mun_code if pa else None,
            city_b=pb.city_mun_code if pb else None,
            province_a=pa.province_code if pa else None,
            province_b=pb.province_code if pb else None,
            region_a=pa.region_code if pa else None,
            region_b=pb.region_code if pb else None,
        )

        # H2H count
        h2h_count = db.query(Match).filter(
            Match.sport == sport,
            Match.status == "completed",
        ).filter(
            ((Match.player1_id == player1_id) & (Match.player2_id == player2_id)) |
            ((Match.player1_id == player2_id) & (Match.player2_id == player1_id))
        ).count()

        same_skill = int(get_skill_category(rating_a) == get_skill_category(rating_b))

        upset = (
            (winner_id == player1_id and rating_a < rating_b) or
            (winner_id == player2_id and rating_b < rating_a)
        )

        quality = compute_real_match_quality(
            score_w=0, score_l=0, nb_sets=1,
            max_sets=SPORT_MAX_SETS.get(sport, 3),
            rating_diff=rating_diff, upset=upset,
        )

        # Insert training row
        db.execute(text("""
            INSERT INTO matchmaking_training_data (
                match_id, sport, format, match_type,
                rating_diff, avg_rating_deviation, winrate_diff,
                activeness_diff, streak_diff, geo_score,
                h2h_matches, same_skill_category, wait_time_seconds,
                match_quality_score, used_in_training
            ) VALUES (
                :match_id, :sport, :format, :match_type,
                :rating_diff, :avg_rd, :winrate_diff,
                :activeness_diff, :streak_diff, :geo_score,
                :h2h_count, :same_skill, :wait_seconds,
                :quality, false
            )
        """), {
            "match_id": match_id, "sport": sport,
            "format": match_format, "match_type": match_type,
            "rating_diff": rating_diff, "avg_rd": avg_rd,
            "winrate_diff": winrate_diff, "activeness_diff": activeness_diff,
            "streak_diff": streak_diff, "geo_score": geo_score,
            "h2h_count": h2h_count, "same_skill": same_skill,
            "wait_seconds": 120, "quality": quality,
        })
        db.commit()

        logger.info(f"Training row saved for match {match_id} — quality: {quality}")
        return True

    except Exception as e:
        logger.error(f"Error saving training row for match {match_id}: {str(e)}")
        db.rollback()
        return False
    finally:
        db.close()


async def check_retraining_readiness(threshold: int = 500) -> dict:
    db: Session = SessionLocal()
    try:
        from sqlalchemy import text
        result = db.execute(text(
            "SELECT COUNT(*) FROM matchmaking_training_data WHERE used_in_training = false"
        )).scalar()
        count = result or 0
        ready = count >= threshold
        return {
            "unused_rows": count,
            "threshold": threshold,
            "ready_to_retrain": ready,
            "message": (
                f"Ready to retrain! {count} new matches collected."
                if ready else
                f"Need {threshold - count} more matches before retraining."
            )
        }
    except Exception as e:
        return {"unused_rows": 0, "threshold": threshold,
                "ready_to_retrain": False, "message": f"Error: {str(e)}"}
    finally:
        db.close()
