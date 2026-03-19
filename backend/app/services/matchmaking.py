import os
import pickle
import json
import logging
import numpy as np
from typing import Optional

logger = logging.getLogger(__name__)

BASE_DIR   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MODELS_DIR = os.path.join(BASE_DIR, "models")

_model      = None
_sport_enc  = None
_format_enc = None
_model_info = None




# ══════════════════════════════════════════════════════════
# MODEL LOADER
# ══════════════════════════════════════════════════════════
def load_model():
    """Load ML model and encoders into memory once at startup."""
    global _model, _sport_enc, _format_enc, _model_info
    if _model is not None:
        return
    try:
        with open(os.path.join(MODELS_DIR, "matchmaking_model.pkl"), "rb") as f:
            _model = pickle.load(f)
        with open(os.path.join(MODELS_DIR, "sport_encoder.pkl"), "rb") as f:
            _sport_enc = pickle.load(f)
        with open(os.path.join(MODELS_DIR, "format_encoder.pkl"), "rb") as f:
            _format_enc = pickle.load(f)
        with open(os.path.join(MODELS_DIR, "model_info.json"), "r") as f:
            _model_info = json.load(f)
        logger.info("Matchmaking ML model loaded successfully.")
        logger.info(f"  Model R2: {_model_info['performance']['r2']}")
        logger.info(f"  Trained on: {_model_info['total_synthetic_matches_trained_on']} matches")
    except Exception as e:
        logger.error(f"Failed to load matchmaking model: {e}")
        logger.warning("Falling back to simple rating-difference matchmaking.")
        _model = None


# ══════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════
def get_skill_category(rating: float) -> str:
    if rating < 1100:   return "beginner"
    elif rating < 1500: return "intermediate"
    elif rating < 1900: return "advanced"
    else:               return "expert"


def compute_geo_score(
    city_a: Optional[str],     city_b: Optional[str],
    province_a: Optional[str], province_b: Optional[str],
    region_a: Optional[str],   region_b: Optional[str],
) -> float:
    if city_a and city_b and city_a == city_b:                 return 1.0
    if province_a and province_b and province_a == province_b: return 0.7
    if region_a and region_b and region_a == region_b:         return 0.4
    return 0.2


def average_geo_score(team_a: list, team_b: list) -> float:
    """Average geo score across all cross-team player pairs."""
    scores = [
        compute_geo_score(
            pa.get("city_code"),     pb.get("city_code"),
            pa.get("province_code"), pb.get("province_code"),
            pa.get("region_code"),   pb.get("region_code"),
        )
        for pa in team_a for pb in team_b
    ]
    return float(np.mean(scores)) if scores else 0.5


def team_avg(players: list, field: str, default: float) -> float:
    """Average a stat field across a list of player dicts."""
    vals = [float(p.get(field, default)) for p in players]
    return float(np.mean(vals)) if vals else default


# ══════════════════════════════════════════════════════════
# CORE ML PREDICTION (shared by singles + doubles)
# ══════════════════════════════════════════════════════════
def score_candidate(
    rating_a: float, rd_a: float, win_rate_a: float,
    activeness_a: float, streak_a: int,
    city_a: Optional[str], province_a: Optional[str], region_a: Optional[str],

    rating_b: float, rd_b: float, win_rate_b: float,
    activeness_b: float, streak_b: int,
    city_b: Optional[str], province_b: Optional[str], region_b: Optional[str],

    sport: str, match_format: str, wait_seconds: int, h2h_count: int = 0,
) -> float:
    """
    Core scoring function used by both singles and doubles.
    Accepts two players (or team averages) and returns quality score 0.0-1.0.
    Falls back to rating-diff scoring if ML model not loaded.
    """
    load_model()

    rating_diff     = abs(rating_a - rating_b)
    avg_rd          = (rd_a + rd_b) / 2
    winrate_diff    = abs(win_rate_a - win_rate_b)
    activeness_diff = abs(activeness_a - activeness_b)
    streak_diff     = abs(streak_a - streak_b)
    geo_score       = compute_geo_score(city_a, city_b, province_a, province_b, region_a, region_b)
    same_skill      = int(get_skill_category(rating_a) == get_skill_category(rating_b))

    # ── ML model prediction ───────────────────────────────
    if _model is not None and _sport_enc is not None and _format_enc is not None:
        try:
            sport_classes  = list(_sport_enc.classes_)
            format_classes = list(_format_enc.classes_)
            sport_enc_val  = sport_classes.index(sport)         if sport         in sport_classes  else 0
            format_enc_val = format_classes.index(match_format) if match_format  in format_classes else 0

            features = [[
                rating_diff, avg_rd, winrate_diff, activeness_diff,
                streak_diff, geo_score, h2h_count, same_skill,
                wait_seconds, sport_enc_val, format_enc_val,
            ]]
            score = float(_model.predict(features)[0])
            return round(float(np.clip(score, 0.0, 1.0)), 4)

        except Exception as e:
            logger.warning(f"ML prediction failed, using fallback: {e}")

    # ── Fallback: simple rating proximity ─────────────────
    rating_factor = max(0.0, 1.0 - rating_diff / 600.0)
    wait_factor   = min(wait_seconds / 300.0, 0.2)
    return round(float(np.clip(rating_factor + wait_factor, 0.0, 1.0)), 4)


# ══════════════════════════════════════════════════════════
# 1 VS 1 — SINGLES MATCHMAKING
# ══════════════════════════════════════════════════════════
def find_best_opponent(
    player: dict,
    candidates: list,
    sport: str,
    match_format: str,
    wait_seconds: int = 0,
    min_score: float = 0.45,
) -> Optional[dict]:
    """
    1v1: Find the best opponent from a list of queue candidates.

    player and each candidate dict must have:
        rating, rating_deviation, win_rate, activeness_score,
        current_streak, city_code, province_code, region_code,
        player_id, h2h_count (optional), queue_wait_seconds (optional)

    Returns best candidate dict (with _ml_score attached), or None.
    """
    if not candidates:
        return None

    best_candidate, best_score = None, -1.0

    for candidate in candidates:
        try:
            effective_wait = max(wait_seconds, int(candidate.get("queue_wait_seconds", 60)))

            # ── Hard geo pre-filter (staged expansion) ────────────────────────
            # Prevent cross-region matches regardless of how good the ML score is.
            # Minimum required geo score relaxes the longer both players wait:
            #   < 5 min  → same province required  (geo >= 0.7)
            #   5-10 min → same region required     (geo >= 0.4)
            #   10+ min  → same region still        (geo >= 0.4, never cross-region)
            geo = compute_geo_score(
                player.get("city_code"),     candidate.get("city_code"),
                player.get("province_code"), candidate.get("province_code"),
                player.get("region_code"),   candidate.get("region_code"),
            )
            if effective_wait < 300:
                min_geo = 0.7   # same province
            else:
                min_geo = 0.4   # same region

            if geo < min_geo:
                logger.debug(
                    f"[1v1] Skipping candidate {candidate.get('player_id')}: "
                    f"geo={geo:.1f} < required {min_geo:.1f} (wait={effective_wait}s)"
                )
                continue
            # ──────────────────────────────────────────────────────────────────

            score = score_candidate(
                rating_a     = float(player.get("rating", 1200)),
                rd_a         = float(player.get("rating_deviation", 200)),
                win_rate_a   = float(player.get("win_rate", 0.5)),
                activeness_a = float(player.get("activeness_score", 0.5)),
                streak_a     = int(player.get("current_streak", 0)),
                city_a       = player.get("city_code"),
                province_a   = player.get("province_code"),
                region_a     = player.get("region_code"),

                rating_b     = float(candidate.get("rating", 1200)),
                rd_b         = float(candidate.get("rating_deviation", 200)),
                win_rate_b   = float(candidate.get("win_rate", 0.5)),
                activeness_b = float(candidate.get("activeness_score", 0.5)),
                streak_b     = int(candidate.get("current_streak", 0)),
                city_b       = candidate.get("city_code"),
                province_b   = candidate.get("province_code"),
                region_b     = candidate.get("region_code"),

                sport        = sport,
                match_format = match_format,
                wait_seconds = effective_wait,
                h2h_count    = int(candidate.get("h2h_count", 0)),
            )

            candidate["_ml_score"] = score

            if score > best_score:
                best_score, best_candidate = score, candidate

        except Exception as e:
            logger.warning(f"Error scoring candidate {candidate.get('player_id')}: {e}")
            continue

    # Threshold relaxes the longer players wait
    wait_bonus    = min(wait_seconds / 600.0, 0.15)
    effective_min = max(min_score - wait_bonus, 0.25)

    if best_score >= effective_min:
        logger.info(f"[1v1] Match found: score={best_score:.3f} threshold={effective_min:.3f}")
        return best_candidate

    logger.info(f"[1v1] No match. Best={best_score:.3f} < threshold={effective_min:.3f}")
    return None


# ══════════════════════════════════════════════════════════
# 2 VS 2 — DOUBLES MATCHMAKING
# ══════════════════════════════════════════════════════════
def _score_doubles_split(team_a: list, team_b: list, sport: str,
                          match_format: str, wait_seconds: int) -> float:
    """
    Score one specific team split by averaging each team's stats
    and running them through score_candidate.
    """
    return score_candidate(
        # Team A averages
        rating_a     = team_avg(team_a, "rating", 1200),
        rd_a         = team_avg(team_a, "rating_deviation", 200),
        win_rate_a   = team_avg(team_a, "win_rate", 0.5),
        activeness_a = team_avg(team_a, "activeness_score", 0.5),
        streak_a     = int(team_avg(team_a, "current_streak", 0)),
        city_a       = team_a[0].get("city_code"),
        province_a   = team_a[0].get("province_code"),
        region_a     = team_a[0].get("region_code"),

        # Team B averages
        rating_b     = team_avg(team_b, "rating", 1200),
        rd_b         = team_avg(team_b, "rating_deviation", 200),
        win_rate_b   = team_avg(team_b, "win_rate", 0.5),
        activeness_b = team_avg(team_b, "activeness_score", 0.5),
        streak_b     = int(team_avg(team_b, "current_streak", 0)),
        city_b       = team_b[0].get("city_code"),
        province_b   = team_b[0].get("province_code"),
        region_b     = team_b[0].get("region_code"),

        sport        = sport,
        match_format = match_format,
        wait_seconds = wait_seconds,
        h2h_count    = 0,
    )


def run_matchmaking(
    four_players: list,
    sport: str,
    match_format: str,
    wait_seconds: int = 0,
) -> Optional[dict]:
    """
    2v2: Given exactly 4 assembled players, try all 3 possible
    team splits and return the most balanced one.

    3 possible splits for [P0, P1, P2, P3]:
        Split 1: [P0+P1] vs [P2+P3]
        Split 2: [P0+P2] vs [P1+P3]
        Split 3: [P0+P3] vs [P1+P2]

    Each split is scored using score_candidate with team averages.
    Returns the split with the highest score.

    Returns:
    {
        "team_a": [player_dict, player_dict],
        "team_b": [player_dict, player_dict],
        "score":  0.87,
        "team_a_avg_rating": 1420.5,
        "team_b_avg_rating": 1385.0,
        "rating_diff": 35.5,
    }
    or None if something went wrong.
    """
    if len(four_players) != 4:
        logger.error(f"run_matchmaking requires exactly 4 players, got {len(four_players)}")
        return None

    p0   = four_players[0]
    rest = four_players[1:]  # [P1, P2, P3]

    # Build all 3 possible splits
    # P0 is always on team_a (fixes duplicates: P0+P1 vs P2+P3 == P2+P3 vs P0+P1)
    possible_splits = [
        (
            [p0, rest[i]],                                    # team_a
            [rest[j] for j in range(len(rest)) if j != i]    # team_b
        )
        for i in range(len(rest))
    ]

    best_split, best_score = None, -1.0

    for team_a, team_b in possible_splits:
        score = _score_doubles_split(team_a, team_b, sport, match_format, wait_seconds)

        logger.info(
            f"[2v2] Split: {[p.get('player_id', '?') for p in team_a]} "
            f"vs {[p.get('player_id', '?') for p in team_b]} "
            f"→ score={score:.3f}"
        )

        if score > best_score:
            best_score = score
            best_split = (team_a, team_b)

    if best_split is None:
        logger.error("[2v2] No valid split found.")
        return None

    team_a, team_b = best_split
    avg_a = team_avg(team_a, "rating", 1200)
    avg_b = team_avg(team_b, "rating", 1200)

    logger.info(
        f"[2v2] Best split chosen: score={best_score:.3f} "
        f"TeamA_avg={avg_a:.0f} TeamB_avg={avg_b:.0f} "
        f"diff={abs(avg_a - avg_b):.0f}"
    )

    return {
        "team_a":            team_a,
        "team_b":            team_b,
        "score":             best_score,
        "team_a_avg_rating": round(avg_a, 1),
        "team_b_avg_rating": round(avg_b, 1),
        "rating_diff":       round(abs(avg_a - avg_b), 1),
    }


# ══════════════════════════════════════════════════════════
# MODEL INFO (for admin/debug endpoint)
# ══════════════════════════════════════════════════════════
def get_model_info() -> dict:
    """Return model metadata for the admin dashboard."""
    load_model()
    if _model_info:
        return {
            "status":      "loaded",
            "model_type":  _model_info.get("model_type"),
            "r2":          _model_info["performance"]["r2"],
            "rmse":        _model_info["performance"]["rmse"],
            "trained_on":  _model_info.get("total_synthetic_matches_trained_on"),
            "features":    _model_info.get("features"),
            "top_feature": list(_model_info["feature_importances"].keys())[0],
        }
    return {"status": "fallback_mode", "reason": "Model file not found or failed to load"}

