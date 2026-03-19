import os
import sys
import io
import json
import numpy as np
import pandas as pd

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

np.random.seed(42)  # reproducible results

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(BASE_DIR, "processed")
os.makedirs(OUT_DIR, exist_ok=True)

# ── LOAD REAL DATA CALIBRATION STATS ─────────────────────
score_stats_path = os.path.join(OUT_DIR, "score_pattern_stats.json")
with open(score_stats_path, "r") as f:
    SCORE_STATS = json.load(f)

print("ISMS SYNTHETIC DATASET GENERATOR")
print("=" * 50)
print("Calibrated from real badminton/pickleball/tennis data")
print("Output: " + OUT_DIR)

# SPORT CONFIGURATIONS (from real data)

SPORT_CONFIG = {
    "pickleball": {
        "points_to_win": 11,
        "win_by_two": True,
        "max_points": 21,
        "sets": 3,
        "scoring": "rally",
        # Calibrated from real pickleball game.csv
        "avg_winner_score": 11.0,
        "avg_loser_score": 7.5,
        "score_std": 2.5,
    },
    "badminton": {
        "points_to_win": 21,
        "win_by_two": True,
        "max_points": 30,
        "sets": 3,
        "scoring": "rally",
        # Calibrated from real BWF data: avg winner=21.1, avg loser=14.7
        "avg_winner_score": 21.1,
        "avg_loser_score": 14.7,
        "score_std": 3.2,
    },
    "table_tennis": {
        "points_to_win": 11,
        "win_by_two": True,
        "max_points": None,
        "sets": 5,
        "scoring": "rally",
        # Similar to pickleball structure
        "avg_winner_score": 11.0,
        "avg_loser_score": 7.0,
        "score_std": 2.8,
    },
    "lawn_tennis": {
        "sets": 3,
        "scoring": "games",
        # Calibrated from real ATP data
        "avg_winner_score": 13.5,
        "avg_loser_score": 8.2,
        "score_std": 3.5,
    },
}

# ══════════════════════════════════════════════════════════
# PLAYER GENERATOR
# Skill categories calibrated from real rating distributions
# ══════════════════════════════════════════════════════════
SKILL_CATEGORIES = [
    # name,        weight, rating_mean, rating_std, rd_mean, rd_std
    ("beginner",      0.30,   1050,       120,        280,    40),
    ("intermediate",  0.45,   1350,       150,        200,    35),
    ("advanced",      0.20,   1700,       130,        150,    30),
    ("expert",        0.05,   2050,       120,        100,    25),
]

def generate_player(player_id):
    # Pick skill category
    weights = [c[1] for c in SKILL_CATEGORIES]
    cat_idx = np.random.choice(len(SKILL_CATEGORIES), p=weights)
    cat_name, _, rating_mean, rating_std, rd_mean, rd_std = SKILL_CATEGORIES[cat_idx]

    # Generate rating
    rating = np.clip(np.random.normal(rating_mean, rating_std), 800, 2400)

    # Rating deviation (uncertainty) — higher for less active players
    rd = np.clip(np.random.normal(rd_mean, rd_std), 50, 350)

    # Win rate correlates with rating
    base_winrate = (rating - 800) / (2400 - 800)  # 0.0 to 1.0
    win_rate = np.clip(base_winrate + np.random.normal(0, 0.08), 0.05, 0.95)

    # Activity
    activeness = np.clip(np.random.beta(2, 3), 0.01, 1.0)

    # Matches played
    matches_played = max(5, int(np.random.exponential(50 * activeness)) + 5)

    # Streak (-10 to +10)
    streak = int(np.clip(np.random.normal(0, 3), -10, 10))

    # Days since last match
    days_inactive = int(np.random.exponential(14 / (activeness + 0.1)))

    # Gender (for mixed doubles pairing)
    gender = "M" if np.random.random() < 0.5 else "F"

    return {
        "player_id": "P" + str(player_id),
        "skill_category": cat_name,
        "rating": round(rating, 1),
        "rating_deviation": round(rd, 1),
        "win_rate": round(win_rate, 4),
        "activeness_score": round(activeness, 4),
        "matches_played": matches_played,
        "current_streak": streak,
        "days_since_last_match": days_inactive,
        "gender": gender,
    }


# ══════════════════════════════════════════════════════════
# MATCH SIMULATOR
# ══════════════════════════════════════════════════════════
def win_probability(rating_a, rating_b):
    """ELO-style win probability for player A"""
    return 1.0 / (1.0 + 10 ** ((rating_b - rating_a) / 400.0))


def simulate_set_score(winner_is_a, sport, win_prob):
    """Simulate a realistic set score based on sport config and win probability"""
    cfg = SPORT_CONFIG[sport]
    avg_w = cfg["avg_winner_score"]
    avg_l = cfg["avg_loser_score"]
    std = cfg["score_std"]

    # Winner score — near target
    score_winner = int(np.clip(np.random.normal(avg_w, std * 0.3), avg_w * 0.85, avg_w * 1.15))

    # Loser score — varies more based on how dominant the winner is
    # Closer win_prob = tighter score
    closeness_factor = 1.0 - abs(win_prob - 0.5) * 2  # 0 = dominant, 1 = even
    avg_loser_adjusted = avg_l + (closeness_factor * (avg_w - avg_l) * 0.4)
    score_loser = int(np.clip(np.random.normal(avg_loser_adjusted, std), 0, score_winner - 1))

    if winner_is_a:
        return score_winner, score_loser
    else:
        return score_loser, score_winner


def simulate_match(player_a, player_b, sport, match_format):
    """Simulate a full match between two players/teams"""
    cfg = SPORT_CONFIG[sport]
    max_sets = cfg["sets"]
    sets_to_win = (max_sets // 2) + 1  # 2 for best-of-3, 3 for best-of-5

    # Win probability with some noise
    base_prob = win_probability(player_a["rating"], player_b["rating"])
    noise = np.random.normal(0, 0.08)
    win_prob_a = np.clip(base_prob + noise, 0.05, 0.95)

    sets_a = 0
    sets_b = 0
    set_scores = []
    total_score_a = 0
    total_score_b = 0

    while sets_a < sets_to_win and sets_b < sets_to_win:
        # Determine set winner
        a_wins_set = np.random.random() < win_prob_a
        score_a, score_b = simulate_set_score(a_wins_set, sport, win_prob_a)

        if a_wins_set:
            sets_a += 1
        else:
            sets_b += 1

        set_scores.append((score_a, score_b))
        total_score_a += score_a
        total_score_b += score_b

    nb_sets = sets_a + sets_b
    player_a_won = sets_a > sets_b

    # Upset detection
    upset = (player_a_won and player_a["rating"] < player_b["rating"]) or \
            (not player_a_won and player_a["rating"] > player_b["rating"])

    winner = player_a if player_a_won else player_b
    loser = player_b if player_a_won else player_a

    score_w = total_score_a if player_a_won else total_score_b
    score_l = total_score_b if player_a_won else total_score_a

    return {
        "player_a_won": player_a_won,
        "nb_sets": nb_sets,
        "score_w": score_w,
        "score_l": score_l,
        "upset": int(upset),
        "set_scores": set_scores,
        "winner_rating": winner["rating"],
        "loser_rating": loser["rating"],
    }


# ══════════════════════════════════════════════════════════
# MATCH QUALITY SCORE (label for ML model)
# ══════════════════════════════════════════════════════════
def compute_match_quality(score_w, score_l, nb_sets, rating_diff, upset, max_sets):
    # Factor 1: Score closeness (40%)
    total = score_w + score_l
    closeness = (1.0 - abs(score_w - score_l) / total) if total > 0 else 0.5

    # Factor 2: Sets played (20%)
    sets_factor = nb_sets / max_sets

    # Factor 3: Rating proximity (40%)
    rating_factor = max(0.0, 1.0 - rating_diff / 600.0)

    # Upset bonus (+5%)
    upset_bonus = 0.05 if upset else 0.0

    quality = (closeness * 0.40) + (sets_factor * 0.20) + (rating_factor * 0.40) + upset_bonus
    return round(min(max(quality, 0.0), 1.0), 4)


# ══════════════════════════════════════════════════════════
# MATCHMAKING FEATURE VECTOR
# (what the ML model will be trained on)
# ══════════════════════════════════════════════════════════
def compute_match_features(pa, pb, wait_seconds=60):
    """Compute the 9 features the matchmaking ML model will use"""
    rating_diff = abs(pa["rating"] - pb["rating"])
    avg_rd = (pa["rating_deviation"] + pb["rating_deviation"]) / 2
    winrate_diff = abs(pa["win_rate"] - pb["win_rate"])
    activeness_diff = abs(pa["activeness_score"] - pb["activeness_score"])
    streak_diff = abs(pa["current_streak"] - pb["current_streak"])

    # Geographic score (simulated: 0=different region, 1=same area)
    geo_score = np.random.choice([0.2, 0.5, 0.8, 1.0], p=[0.1, 0.2, 0.3, 0.4])

    # Head to head (simulated)
    h2h_matches = np.random.choice([0, 1, 2, 3, 5], p=[0.5, 0.2, 0.15, 0.1, 0.05])

    # Same skill category
    same_skill = int(pa["skill_category"] == pb["skill_category"])

    return {
        "rating_diff": round(rating_diff, 1),
        "avg_rating_deviation": round(avg_rd, 1),
        "winrate_diff": round(winrate_diff, 4),
        "activeness_diff": round(activeness_diff, 4),
        "streak_diff": streak_diff,
        "geo_score": geo_score,
        "h2h_matches": h2h_matches,
        "same_skill_category": same_skill,
        "wait_time_seconds": wait_seconds,
    }


# ══════════════════════════════════════════════════════════
# DOUBLES TEAM FORMATION
# ══════════════════════════════════════════════════════════
def form_doubles_team(p1, p2):
    """Average two players into a team representation"""
    return {
        "player_id": p1["player_id"] + "+" + p2["player_id"],
        "skill_category": p1["skill_category"],
        "rating": round((p1["rating"] * 0.6 + p2["rating"] * 0.4), 1),
        "rating_deviation": round((p1["rating_deviation"] + p2["rating_deviation"]) / 2, 1),
        "win_rate": round((p1["win_rate"] + p2["win_rate"]) / 2, 4),
        "activeness_score": round((p1["activeness_score"] + p2["activeness_score"]) / 2, 4),
        "matches_played": (p1["matches_played"] + p2["matches_played"]) // 2,
        "current_streak": (p1["current_streak"] + p2["current_streak"]) // 2,
        "days_since_last_match": max(p1["days_since_last_match"], p2["days_since_last_match"]),
        "gender": p1["gender"],
    }


# ══════════════════════════════════════════════════════════
# MAIN GENERATION LOOP
# ══════════════════════════════════════════════════════════
TOTAL_PLAYERS = 5000
MATCHES_PER_COMBO = 2500  # per sport+format combination

SPORTS = ["pickleball", "badminton", "table_tennis", "lawn_tennis"]
FORMATS = {
    "singles": ["pickleball", "badminton", "table_tennis", "lawn_tennis"],
    "doubles": ["pickleball", "badminton", "table_tennis"],
    "mixed_doubles": ["pickleball", "badminton"],
}

print("\nGenerating " + str(TOTAL_PLAYERS) + " synthetic players...")
players = [generate_player(i) for i in range(1, TOTAL_PLAYERS + 1)]
players_df = pd.DataFrame(players)

# Split players by gender for mixed doubles
male_players = [p for p in players if p["gender"] == "M"]
female_players = [p for p in players if p["gender"] == "F"]

print("  Male players:   " + str(len(male_players)))
print("  Female players: " + str(len(female_players)))
print("  Rating distribution:")
print("    mean: " + str(round(players_df["rating"].mean(), 1)))
print("    std:  " + str(round(players_df["rating"].std(), 1)))
print("    min:  " + str(round(players_df["rating"].min(), 1)))
print("    max:  " + str(round(players_df["rating"].max(), 1)))

# Save players
players_path = os.path.join(OUT_DIR, "synthetic_players.csv")
players_df.to_csv(players_path, index=False)
print("  Saved: " + players_path)

# ── Generate matches ──────────────────────────────────────
print("\nGenerating synthetic matches...")
all_matches = []
all_ml_features = []

match_id = 1

for fmt, sports_list in FORMATS.items():
    for sport in sports_list:
        print("  " + sport + " / " + fmt + " ...")
        count = 0

        for _ in range(MATCHES_PER_COMBO):
            try:
                if fmt == "singles":
                    # Pick 2 random players by index to satisfy numpy's ArrayLike requirement
                    pa, pb = (players[i] for i in np.random.choice(len(players), size=2, replace=False))
                    team_a = pa
                    team_b = pb

                elif fmt == "doubles":
                    # Pick 4 random players, form 2 teams
                    four = [players[i] for i in np.random.choice(len(players), size=4, replace=False)]
                    # Balance teams: best+worst vs 2nd+3rd
                    sorted_four = sorted(four, key=lambda x: x["rating"], reverse=True)
                    team_a = form_doubles_team(sorted_four[0], sorted_four[3])
                    team_b = form_doubles_team(sorted_four[1], sorted_four[2])

                elif fmt == "mixed_doubles":
                    # Pick 2 male + 2 female
                    if len(male_players) < 2 or len(female_players) < 2:
                        continue
                    m1, m2 = (male_players[i] for i in np.random.choice(len(male_players), size=2, replace=False))
                    f1, f2 = (female_players[i] for i in np.random.choice(len(female_players), size=2, replace=False))
                    # Random pairing
                    if np.random.random() < 0.5:
                        team_a = form_doubles_team(m1, f1)
                        team_b = form_doubles_team(m2, f2)
                    else:
                        team_a = form_doubles_team(m1, f2)
                        team_b = form_doubles_team(m2, f1)

                else:
                    continue  # unknown format — skip

                # Simulate match
                result = simulate_match(team_a, team_b, sport, fmt)

                # Compute features
                wait_seconds = int(np.random.exponential(120))
                features = compute_match_features(team_a, team_b, wait_seconds)

                # Compute match quality label
                rating_diff = abs(team_a["rating"] - team_b["rating"])
                quality = compute_match_quality(
                    result["score_w"], result["score_l"],
                    result["nb_sets"], rating_diff,
                    result["upset"],
                    SPORT_CONFIG[sport]["sets"]
                )

                # Match record
                all_matches.append({
                    "match_id": "SM" + str(match_id),
                    "sport": sport,
                    "format": fmt,
                    "team_a_rating": team_a["rating"],
                    "team_b_rating": team_b["rating"],
                    "nb_sets": result["nb_sets"],
                    "score_w": result["score_w"],
                    "score_l": result["score_l"],
                    "upset": result["upset"],
                    "match_quality_score": quality,
                    "source": "synthetic"
                })

                # ML feature record (for model training)
                ml_row: dict[str, str | float | int] = {
                    "match_id": "SM" + str(match_id),
                    "sport": sport,
                    "format": fmt,
                }
                ml_row.update(features)
                ml_row["match_quality_score"] = quality
                all_ml_features.append(ml_row)

                match_id += 1
                count += 1

            except Exception as e:
                continue

        print("    Generated: " + str(count) + " matches")

# ── Save outputs ──────────────────────────────────────────
print("\n" + "="*50)
print("Saving outputs...")

synthetic_df = pd.DataFrame(all_matches)
ml_df = pd.DataFrame(all_ml_features)

# Save synthetic match data
synthetic_path = os.path.join(OUT_DIR, "synthetic_match_data.csv")
synthetic_df.to_csv(synthetic_path, index=False)
print("  Synthetic matches: " + synthetic_path)
print("  Total rows: " + str(len(synthetic_df)))

# Save ML training features
ml_path = os.path.join(OUT_DIR, "ml_training_data.csv")
ml_df.to_csv(ml_path, index=False)
print("  ML training data: " + ml_path)
print("  Total rows: " + str(len(ml_df)))

# ── Summary stats ─────────────────────────────────────────
print("\nSynthetic dataset summary:")
print("  Rows by sport:")
for sport, cnt in synthetic_df["sport"].value_counts().items():
    print("    " + str(sport) + ": " + str(cnt))

print("  Rows by format:")
for fmt, cnt in synthetic_df["format"].value_counts().items():
    print("    " + str(fmt) + ": " + str(cnt))

print("  Match quality distribution:")
print("    mean: " + str(round(synthetic_df["match_quality_score"].mean(), 3)))
print("    std:  " + str(round(synthetic_df["match_quality_score"].std(), 3)))
print("    min:  " + str(round(synthetic_df["match_quality_score"].min(), 3)))
print("    max:  " + str(round(synthetic_df["match_quality_score"].max(), 3)))

print("  Upset rate: " + str(round(synthetic_df["upset"].mean() * 100, 1)) + "%")

print("\n" + "="*50)
print("GENERATION COMPLETE!")
print("Files saved:")
print("  - synthetic_players.csv    (" + str(len(players_df)) + " players)")
print("  - synthetic_match_data.csv (" + str(len(synthetic_df)) + " matches)")
print("  - ml_training_data.csv     (" + str(len(ml_df)) + " training rows)")
print("\nNext step: Run train_model.py")
