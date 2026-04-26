import os
import pickle
import json
import logging
from typing import Any, Optional
from app.services.performance_rating import PERFORMANCE_BASELINE
from app.utils.skill_tiers import get_skill_tier_slug

try:
    import numpy as np
except ImportError:
    class _NumpyFallback:
        @staticmethod
        def mean(values):
            if not values:
                return 0.0
            return sum(float(value) for value in values) / len(values)

        @staticmethod
        def clip(value, minimum, maximum):
            return max(minimum, min(maximum, value))

    np = _NumpyFallback()

logger = logging.getLogger(__name__)

BASE_DIR   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MODELS_DIR = os.path.join(BASE_DIR, "models")

PlayerDict = dict[str, Any]


# ══════════════════════════════════════════════════════════
# MODE CONFIG  — controls per-mode geo filter, quality
#               threshold, and wait-time weight
# ══════════════════════════════════════════════════════════
#
#  geo_filter        : minimum geo proximity required before first relaxation
#                      "city" | "province" | "region" | "none"
#  geo_relaxes_after : seconds of wait until geo_filter relaxes to geo_relaxed_to
#                      None = never relaxes
#  geo_relaxed_to    : geo level after relaxation; "none" = no geo restriction
#  min_quality       : minimum ML score for a pairing to be accepted
#  wait_weight       : multiplier applied to wait_seconds before scoring
#                      (0.0 = wait time ignored; 1.0 = full weight)
#
MATCH_MODE_CONFIG: dict = {
    "quick": {
        "min_quality":       0.45,
        "geo_filter":        "province",
        "geo_relaxes_after": 300,       # relax after 5 min
        "geo_relaxed_to":    "region",
        "wait_weight":       1.0,
    },
    "ranked": {
        "min_quality":       0.65,      # stricter — protect rating integrity
        "geo_filter":        "region",
        "geo_relaxes_after": None,      # never relaxes for ranked
        "geo_relaxed_to":    None,
        "wait_weight":       0.5,       # wait time matters less than fairness
    },
    "friendly": {
        "min_quality":       0.30,      # loose — casual play
        "geo_filter":        "region",
        "geo_relaxes_after": 180,       # relax after 3 min
        "geo_relaxed_to":    "none",    # no geo restriction after relaxation
        "wait_weight":       0.8,
    },
    "club": {
        "min_quality":       0.40,
        "geo_filter":        "none",    # players are already inside the same club
        "geo_relaxes_after": None,
        "geo_relaxed_to":    None,
        "wait_weight":       0.6,
    },
    "tournament": {
        "min_quality":       0.50,
        "geo_filter":        "none",    # same event — location irrelevant
        "geo_relaxes_after": None,
        "geo_relaxed_to":    None,
        "wait_weight":       0.0,       # wait time is meaningless for seeding
    },
    "booked": {
        "min_quality":       0.40,
        "geo_filter":        "region",
        "geo_relaxes_after": None,      # scheduled match — no pressure to relax
        "geo_relaxed_to":    None,
        "wait_weight":       0.0,       # no live wait time for a planned match
    },
}

# Maps geo level names to the minimum score produced by compute_geo_score()
_GEO_LEVEL_MIN: dict = {
    "city":     1.0,
    "province": 0.7,
    "region":   0.4,
    "none":     0.0,   # accept anything
}

_MAX_RATING_GAP: dict = {
    "ranked": 500.0,
    "quick": 800.0,
    "club": 800.0,
    "friendly": 1200.0,
    "tournament": 900.0,
    "booked": 1000.0,
}

_MAX_AVG_RD: dict = {
    "ranked": 300.0,
    "quick": 450.0,
    "club": 450.0,
    "friendly": 500.0,
    "tournament": 450.0,
    "booked": 450.0,
}

_MAX_DOUBLES_ROLE_GAP: dict = {
    "ranked": 250.0,
    "quick": 500.0,
    "club": 500.0,
    "friendly": 700.0,
    "tournament": 450.0,
    "booked": 500.0,
}

_DOUBLES_ROLE_GAP_RELAX: dict = {
    "ranked": 0.0,
    "quick": 100.0,
    "club": 75.0,
    "friendly": 150.0,
    "tournament": 0.0,
    "booked": 0.0,
}

_model      = None
_sport_enc  = None
_format_enc = None
_model_info = None
_model_load_attempted = False





# MODEL LOADER
def load_model():
    """Load ML model and encoders into memory once at startup."""
    global _model, _sport_enc, _format_enc, _model_info, _model_load_attempted
    if _model_load_attempted:
        return
    _model_load_attempted = True
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



# HELPERS
def get_skill_category(rating: float) -> str:
    return get_skill_tier_slug(rating)


def compute_geo_score(
    city_a: Optional[str],     city_b: Optional[str],
    province_a: Optional[str], province_b: Optional[str],
    region_a: Optional[str],   region_b: Optional[str],
) -> float:
    if city_a and city_b and city_a == city_b:                 return 1.0
    if province_a and province_b and province_a == province_b: return 0.7
    if region_a and region_b and region_a == region_b:         return 0.4
    return 0.2


def average_geo_score(team_a: list[PlayerDict], team_b: list[PlayerDict]) -> float:
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


def team_avg(players: list[PlayerDict], field: str, default: float) -> float:
    """Average a stat field across a list of player dicts."""
    vals = [float(p.get(field, default)) for p in players]
    return float(np.mean(vals)) if vals else default


def _player_rating(player: PlayerDict, default: float = 1200.0) -> float:
    return float(player.get("rating", default))


def _effective_doubles_role_gap_cap(mode: str, wait_seconds: int) -> float:
    base_cap = float(_MAX_DOUBLES_ROLE_GAP.get(mode, _MAX_DOUBLES_ROLE_GAP["quick"]))
    relax_cap = float(_DOUBLES_ROLE_GAP_RELAX.get(mode, 0.0))
    if relax_cap <= 0.0:
        return base_cap
    relax_factor = float(np.clip(wait_seconds / 600.0, 0.0, 1.0))
    return base_cap + (relax_cap * relax_factor)


def _doubles_role_gaps(
    team_a: list[PlayerDict],
    team_b: list[PlayerDict],
    match_format: str,
) -> list[float] | None:
    if len(team_a) != 2 or len(team_b) != 2:
        return None

    if match_format == "mixed_doubles":
        def _player_for_gender(team: list[PlayerDict], gender: str) -> PlayerDict | None:
            return next((player for player in team if normalize_gender(player.get("gender")) == gender), None)

        male_a = _player_for_gender(team_a, "male")
        male_b = _player_for_gender(team_b, "male")
        female_a = _player_for_gender(team_a, "female")
        female_b = _player_for_gender(team_b, "female")
        if male_a is None or male_b is None or female_a is None or female_b is None:
            return None
        return [
            abs(_player_rating(male_a) - _player_rating(male_b)),
            abs(_player_rating(female_a) - _player_rating(female_b)),
        ]

    sorted_a = sorted(team_a, key=lambda player: _player_rating(player), reverse=True)
    sorted_b = sorted(team_b, key=lambda player: _player_rating(player), reverse=True)
    return [
        abs(_player_rating(sorted_a[0]) - _player_rating(sorted_b[0])),
        abs(_player_rating(sorted_a[1]) - _player_rating(sorted_b[1])),
    ]


def _passes_doubles_role_rules(
    team_a: list[PlayerDict],
    team_b: list[PlayerDict],
    match_format: str,
    mode: str,
    wait_seconds: int,
) -> bool:
    gaps = _doubles_role_gaps(team_a, team_b, match_format)
    if not gaps:
        return False
    max_gap = max(gaps)
    return max_gap <= _effective_doubles_role_gap_cap(mode, wait_seconds)


def _doubles_role_balance_adjustment(
    team_a: list[PlayerDict],
    team_b: list[PlayerDict],
    match_format: str,
    mode: str,
    wait_seconds: int,
) -> float:
    gaps = _doubles_role_gaps(team_a, team_b, match_format)
    if not gaps:
        return 0.0

    cap = _effective_doubles_role_gap_cap(mode, wait_seconds)
    avg_gap = float(np.mean(gaps))
    closeness = max(0.0, 1.0 - (avg_gap / cap))
    return round(0.06 * closeness, 4)


def _performance_signal(player: PlayerDict) -> tuple[float, float]:
    rating = float(player.get("performance_rating", PERFORMANCE_BASELINE))
    confidence = float(player.get("performance_confidence", 0.0))
    reliable = bool(player.get("performance_reliable", False))
    confidence_factor = float(np.clip(confidence / 100.0, 0.0, 1.0))

    if not reliable and confidence_factor < 0.35:
        return PERFORMANCE_BASELINE, 0.0

    weighted_rating = PERFORMANCE_BASELINE + ((rating - PERFORMANCE_BASELINE) * confidence_factor)
    return weighted_rating, confidence_factor


def _team_avg_performance(players: list[PlayerDict]) -> tuple[float, float]:
    if not players:
        return PERFORMANCE_BASELINE, 0.0

    signals = [_performance_signal(player) for player in players]
    ratings = [signal[0] for signal in signals]
    confidences = [signal[1] for signal in signals]
    return float(np.mean(ratings)), float(np.mean(confidences))


def _doubles_performance_balance_adjustment(
    team_a: list[PlayerDict],
    team_b: list[PlayerDict],
) -> float:
    avg_a, conf_a = _team_avg_performance(team_a)
    avg_b, conf_b = _team_avg_performance(team_b)
    confidence = min(conf_a, conf_b)
    if confidence <= 0.0:
        return 0.0

    diff = abs(avg_a - avg_b)
    closeness = max(0.0, 1.0 - (diff / 25.0))
    reward = 0.04 * confidence * closeness
    penalty = 0.18 * confidence * (1.0 - closeness)
    return round(reward - penalty, 4)


def normalize_gender(value: Optional[str]) -> Optional[str]:
    """Return the supported gender key used by mixed doubles rules."""
    gender = str(value or "").strip().lower()
    if gender in ("male", "m"):
        return "male"
    if gender in ("female", "f"):
        return "female"
    return None


def is_mixed_doubles_team(players: list) -> bool:
    """Mixed doubles team must be exactly one male and one female."""
    genders = [normalize_gender(p.get("gender")) for p in players]
    if any(g is None for g in genders):
        return False
    return len(genders) == 2 and sorted(g for g in genders if g is not None) == ["female", "male"]


def is_mixed_doubles_pool_viable(players: list) -> bool:
    """A mixed doubles pool can contain at most two male and two female players."""
    genders = [normalize_gender(p.get("gender")) for p in players]
    if any(gender is None for gender in genders):
        return False
    return genders.count("male") <= 2 and genders.count("female") <= 2


# ══════════════════════════════════════════════════════════
# CORE ML PREDICTION (shared by singles + doubles)
# ══════════════════════════════════════════════════════════
def _passes_hard_match_rules(
    rating_a: float,
    rd_a: float,
    rating_b: float,
    rd_b: float,
    mode: str,
) -> bool:
    rating_gap = abs(float(rating_a) - float(rating_b))
    avg_rd = (float(rd_a) + float(rd_b)) / 2
    return (
        rating_gap <= _MAX_RATING_GAP.get(mode, _MAX_RATING_GAP["quick"])
        and avg_rd <= _MAX_AVG_RD.get(mode, _MAX_AVG_RD["quick"])
    )


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
    mode: str = "quick",
) -> Optional[dict]:
    """
    1v1: Find the best opponent from a list of queue candidates.

    player and each candidate dict must have:
        rating, rating_deviation, win_rate, activeness_score,
        current_streak, city_code, province_code, region_code,
        player_id, h2h_count (optional), queue_wait_seconds (optional)

    mode controls geo filtering, quality threshold, and wait weighting.
    Supported modes: quick | ranked | friendly | club | tournament | booked

    Returns best candidate dict (with _ml_score attached), or None.
    """
    if not candidates:
        return None

    cfg         = MATCH_MODE_CONFIG.get(mode, MATCH_MODE_CONFIG["quick"])
    base_min    = min_score if min_score != 0.45 else cfg["min_quality"]
    wait_weight = float(cfg["wait_weight"])

    best_candidate, best_score = None, -1.0

    for candidate in candidates:
        try:
            effective_wait = max(wait_seconds, int(candidate.get("queue_wait_seconds", 60)))
            weighted_wait  = int(effective_wait * wait_weight)

            # ── Mode-aware geo pre-filter ──────────────────────────────────────
            geo_filter   = cfg["geo_filter"]
            relax_after  = cfg["geo_relaxes_after"]
            relaxed_to   = cfg.get("geo_relaxed_to")

            # Determine which geo level applies right now
            if geo_filter == "none":
                min_geo = 0.0   # no geo restriction
            else:
                base_min_geo = _GEO_LEVEL_MIN.get(geo_filter, 0.4)
                if relax_after is not None and effective_wait >= relax_after and relaxed_to:
                    min_geo = _GEO_LEVEL_MIN.get(relaxed_to, 0.0)
                else:
                    min_geo = base_min_geo

            if min_geo > 0.0:
                geo = compute_geo_score(
                    player.get("city_code"),     candidate.get("city_code"),
                    player.get("province_code"), candidate.get("province_code"),
                    player.get("region_code"),   candidate.get("region_code"),
                )
                if geo < min_geo:
                    logger.debug(
                        f"[1v1/{mode}] Skipping {candidate.get('player_id')}: "
                        f"geo={geo:.1f} < required {min_geo:.1f} (wait={effective_wait}s)"
                    )
                    continue
            # ──────────────────────────────────────────────────────────────────

            player_rating = float(player.get("rating", 1200))
            player_rd = float(player.get("rating_deviation", 200))
            candidate_rating = float(candidate.get("rating", 1200))
            candidate_rd = float(candidate.get("rating_deviation", 200))
            if not _passes_hard_match_rules(player_rating, player_rd, candidate_rating, candidate_rd, mode):
                logger.debug(
                    f"[1v1/{mode}] Skipping {candidate.get('player_id')}: "
                    f"rating_gap={abs(player_rating - candidate_rating):.0f}, "
                    f"avg_rd={(player_rd + candidate_rd) / 2:.0f}"
                )
                continue

            score = score_candidate(
                rating_a     = player_rating,
                rd_a         = player_rd,
                win_rate_a   = float(player.get("win_rate", 0.5)),
                activeness_a = float(player.get("activeness_score", 0.5)),
                streak_a     = int(player.get("current_streak", 0)),
                city_a       = player.get("city_code"),
                province_a   = player.get("province_code"),
                region_a     = player.get("region_code"),

                rating_b     = candidate_rating,
                rd_b         = candidate_rd,
                win_rate_b   = float(candidate.get("win_rate", 0.5)),
                activeness_b = float(candidate.get("activeness_score", 0.5)),
                streak_b     = int(candidate.get("current_streak", 0)),
                city_b       = candidate.get("city_code"),
                province_b   = candidate.get("province_code"),
                region_b     = candidate.get("region_code"),

                sport        = sport,
                match_format = match_format,
                wait_seconds = weighted_wait,
                h2h_count    = int(candidate.get("h2h_count", 0)),
            )

            candidate["_ml_score"] = score

            if score > best_score:
                best_score, best_candidate = score, candidate

        except Exception as e:
            logger.warning(f"Error scoring candidate {candidate.get('player_id')}: {e}")
            continue

    # For quick/friendly modes: threshold relaxes slightly the longer players wait.
    # For ranked mode: threshold never drops below a hard floor.
    if mode == "ranked":
        effective_min = base_min   # no relaxation for ranked
    else:
        wait_bonus    = min(wait_seconds / 600.0, 0.15)
        effective_min = max(base_min - wait_bonus, 0.20)

    if best_score >= effective_min:
        logger.info(f"[1v1/{mode}] Match found: score={best_score:.3f} threshold={effective_min:.3f}")
        return best_candidate

    logger.info(f"[1v1/{mode}] No match. Best={best_score:.3f} < threshold={effective_min:.3f}")
    return None


# ══════════════════════════════════════════════════════════
# 2 VS 2 — DOUBLES MATCHMAKING
# ══════════════════════════════════════════════════════════
def _score_doubles_split(
    team_a: list[PlayerDict],
    team_b: list[PlayerDict],
    sport: str,
    match_format: str,
    wait_seconds: int,
    mode: str = "quick",
) -> float:
    """
    Score one specific team split by averaging each team's stats
    and running them through score_candidate.
    """
    if match_format == "mixed_doubles" and (
        not is_mixed_doubles_team(team_a) or not is_mixed_doubles_team(team_b)
    ):
        return 0.0

    rating_a = team_avg(team_a, "rating", 1200)
    rd_a = team_avg(team_a, "rating_deviation", 200)
    rating_b = team_avg(team_b, "rating", 1200)
    rd_b = team_avg(team_b, "rating_deviation", 200)
    if not _passes_hard_match_rules(rating_a, rd_a, rating_b, rd_b, mode):
        return 0.0
    if not _passes_doubles_role_rules(team_a, team_b, match_format, mode, wait_seconds):
        return 0.0

    base_score = score_candidate(
        # Team A averages
        rating_a     = rating_a,
        rd_a         = rd_a,
        win_rate_a   = team_avg(team_a, "win_rate", 0.5),
        activeness_a = team_avg(team_a, "activeness_score", 0.5),
        streak_a     = int(team_avg(team_a, "current_streak", 0)),
        city_a       = team_a[0].get("city_code"),
        province_a   = team_a[0].get("province_code"),
        region_a     = team_a[0].get("region_code"),

        # Team B averages
        rating_b     = rating_b,
        rd_b         = rd_b,
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
    performance_adjustment = _doubles_performance_balance_adjustment(team_a, team_b)
    role_adjustment = _doubles_role_balance_adjustment(team_a, team_b, match_format, mode, wait_seconds)
    return round(float(np.clip(base_score + performance_adjustment + role_adjustment, 0.0, 1.0)), 4)


def run_matchmaking(
    four_players: list,
    sport: str,
    match_format: str,
    wait_seconds: int = 0,
    mode: str = "quick",
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
    if match_format == "mixed_doubles" and not is_mixed_doubles_pool_viable(four_players):
        logger.info("[2v2/mixed_doubles] Pool is not gender-viable for mixed doubles.")
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
        score = _score_doubles_split(team_a, team_b, sport, match_format, wait_seconds, mode)

        logger.info(
            f"[2v2] Split: {[p.get('player_id', '?') for p in team_a]} "
            f"vs {[p.get('player_id', '?') for p in team_b]} "
            f"→ score={score:.3f}"
        )

        if score > best_score:
            best_score = score
            best_split = (team_a, team_b)

    if best_split is None or best_score <= 0.0:
        logger.error("[2v2] No valid split found.")
        return None

    team_a, team_b = best_split
    avg_a = team_avg(team_a, "rating", 1200)
    avg_b = team_avg(team_b, "rating", 1200)
    perf_a, perf_conf_a = _team_avg_performance(team_a)
    perf_b, perf_conf_b = _team_avg_performance(team_b)

    logger.info(
        f"[2v2] Best split chosen: score={best_score:.3f} "
        f"TeamA_avg={avg_a:.0f} TeamB_avg={avg_b:.0f} "
        f"diff={abs(avg_a - avg_b):.0f} "
        f"perf_diff={abs(perf_a - perf_b):.1f}"
    )

    return {
        "team_a":            team_a,
        "team_b":            team_b,
        "score":             best_score,
        "team_a_avg_rating": round(avg_a, 1),
        "team_b_avg_rating": round(avg_b, 1),
        "rating_diff":       round(abs(avg_a - avg_b), 1),
        "team_a_avg_performance": round(perf_a, 1),
        "team_b_avg_performance": round(perf_b, 1),
        "performance_diff": round(abs(perf_a - perf_b), 1),
        "performance_confidence": round(min(perf_conf_a, perf_conf_b) * 100.0, 1),
    }


# ══════════════════════════════════════════════════════════
# DOUBLES ENTRY GATE — skill check before slot assignment
# ══════════════════════════════════════════════════════════
def score_doubles_entry(
    incoming: dict,
    lobby_players: list,
    sport: str,
    match_format: str,
    lobby_wait_seconds: int = 0,
    mode: str = "quick",
) -> float:
    """
    Score how well an incoming player fits an existing doubles lobby.
    Compares the incoming player's stats against the average of current lobby players.
    Returns a quality score 0.0–1.0 using the same ML model as singles.
    """
    if not lobby_players:
        return 1.0
    cfg          = MATCH_MODE_CONFIG.get(mode, MATCH_MODE_CONFIG["quick"])
    weighted_wait = int(lobby_wait_seconds * float(cfg["wait_weight"]))
    if len(lobby_players) == 3:
        best_split = run_matchmaking(
            [*lobby_players, incoming],
            sport=sport,
            match_format=match_format,
            wait_seconds=weighted_wait,
            mode=mode,
        )
        return float(best_split["score"]) if best_split else 0.0

    incoming_rating = float(incoming.get("rating", 1200))
    incoming_rd = float(incoming.get("rating_deviation", 200))
    lobby_rating = team_avg(lobby_players, "rating", 1200)
    lobby_rd = team_avg(lobby_players, "rating_deviation", 200)
    if not _passes_hard_match_rules(incoming_rating, incoming_rd, lobby_rating, lobby_rd, mode):
        return 0.0
    return score_candidate(
        rating_a     = incoming_rating,
        rd_a         = incoming_rd,
        win_rate_a   = float(incoming.get("win_rate", 0.5)),
        activeness_a = float(incoming.get("activeness_score", 0.5)),
        streak_a     = int(incoming.get("current_streak", 0)),
        city_a       = incoming.get("city_code"),
        province_a   = incoming.get("province_code"),
        region_a     = incoming.get("region_code"),

        rating_b     = lobby_rating,
        rd_b         = lobby_rd,
        win_rate_b   = team_avg(lobby_players, "win_rate", 0.5),
        activeness_b = team_avg(lobby_players, "activeness_score", 0.5),
        streak_b     = int(team_avg(lobby_players, "current_streak", 0)),
        city_b       = lobby_players[0].get("city_code"),
        province_b   = lobby_players[0].get("province_code"),
        region_b     = lobby_players[0].get("region_code"),

        sport        = sport,
        match_format = match_format,
        wait_seconds = weighted_wait,
    )


def can_join_doubles_lobby(
    incoming: dict,
    lobby_players: list,
    sport: str,
    match_format: str,
    lobby_wait_seconds: int = 0,
    mode: str = "quick",
) -> bool:
    """
    Return True if the incoming player's skill is compatible with the existing lobby.
    Applies the same mode-aware quality threshold (with wait-time relaxation) as singles.
    """
    if not lobby_players:
        return True
    score = score_doubles_entry(
        incoming, lobby_players, sport, match_format, lobby_wait_seconds, mode
    )
    cfg      = MATCH_MODE_CONFIG.get(mode, MATCH_MODE_CONFIG["quick"])
    base_min = cfg["min_quality"]
    if mode == "ranked":
        effective_min = base_min          # ranked never relaxes
    else:
        wait_bonus    = min(lobby_wait_seconds / 600.0, 0.15)
        effective_min = max(base_min - wait_bonus, 0.20)
    logger.debug(
        f"[doubles_entry/{mode}] score={score:.3f} threshold={effective_min:.3f} "
        f"incoming_rating={incoming.get('rating', '?')} "
        f"lobby_avg={team_avg(lobby_players, 'rating', 1200):.0f}"
    )
    return score >= effective_min


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

