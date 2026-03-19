# dataset/process_datasets.py
# Run: python process_datasets.py
# Output: processed/unified_training_data.csv

import os
import sys
import io
import pandas as pd
import numpy as np
import glob
import json

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE_DIR, "raw")
OUT_DIR = os.path.join(BASE_DIR, "processed")
os.makedirs(OUT_DIR, exist_ok=True)

# ── HELPER: compute match quality score ───────────────────
def compute_match_quality(score_w, score_l, nb_sets, rating_diff):
    """
    Compute match quality 0.0 - 1.0 based on:
    - Score closeness (how tight was the final game score)
    - Number of sets played (more sets = more competitive)
    - Rating difference (closer ratings = better quality)
    """
    # Factor 1: Score closeness (40%)
    total_points = score_w + score_l
    if total_points > 0:
        closeness = 1.0 - abs(score_w - score_l) / total_points
    else:
        closeness = 0.5

    # Factor 2: Sets played (20%) — 3 sets is max for badminton/pickleball
    sets_factor = min(nb_sets / 3.0, 1.0)

    # Factor 3: Rating proximity (40%) — closer ratings = better match
    if rating_diff is not None and not np.isnan(rating_diff):
        # rating_diff is normalized 0-1 where 0 = identical, 1 = very different
        rating_factor = max(0.0, 1.0 - (abs(rating_diff) / 500.0))
    else:
        rating_factor = 0.5  # unknown = neutral

    quality = (closeness * 0.4) + (sets_factor * 0.2) + (rating_factor * 0.4)
    return round(min(max(quality, 0.0), 1.0), 4)


# ── HELPER: parse score string "21-15" → (21, 15) ─────────
def parse_score(score_str):
    try:
        if pd.isna(score_str):
            return None, None
        parts = str(score_str).strip().split("-")
        if len(parts) == 2:
            return int(parts[0]), int(parts[1])
    except:
        pass
    return None, None


# ══════════════════════════════════════════════════════════
# 1. PROCESS TENNIS ATP DATA
# ══════════════════════════════════════════════════════════
def process_tennis_atp():
    print("\n" + "="*50)
    print("Processing Tennis ATP data...")

    tennis_dir = os.path.join(RAW_DIR, "tennis_atp-master")
    # Only use recent years (2000-2024) for relevant rating distributions
    pattern = os.path.join(tennis_dir, "atp_matches_2*.csv")
    files = sorted(glob.glob(pattern))

    all_rows = []
    for f in files:
        try:
            df = pd.read_csv(f, low_memory=False)
            # Only keep rows with valid rankings
            df = df.dropna(subset=["winner_rank", "loser_rank", "score"])
            all_rows.append(df)
        except Exception as e:
            print("  Skip " + os.path.basename(f) + ": " + str(e))

    if not all_rows:
        print("  No tennis ATP files found!")
        return pd.DataFrame()

    tennis = pd.concat(all_rows, ignore_index=True)
    print("  Loaded " + str(len(tennis)) + " tennis matches")

    # Parse score → extract sets info
    # Tennis score format: "6-3 7-5" or "6-3 4-6 7-5"
    def parse_tennis_score(score_str):
        try:
            sets = str(score_str).strip().split()
            nb_sets = len(sets)
            # Last set score
            last_set = sets[-1]
            w, l = last_set.split("-")
            # Handle tiebreak notation like "7-6(3)"
            w = int(str(w).split("(")[0])
            l = int(str(l).split("(")[0])
            total_games_w = 0
            total_games_l = 0
            for s in sets:
                parts = s.split("-")
                if len(parts) == 2:
                    gw = int(str(parts[0]).split("(")[0])
                    gl = int(str(parts[1]).split("(")[0])
                    total_games_w += gw
                    total_games_l += gl
            return nb_sets, total_games_w, total_games_l
        except:
            return 3, 12, 8  # default

    results = []
    for _, row in tennis.iterrows():
        nb_sets, score_w, score_l = parse_tennis_score(row["score"])
        winner_rank = float(row["winner_rank"])
        loser_rank = float(row["loser_rank"])
        rating_diff = abs(winner_rank - loser_rank)

        # Convert rank to approximate rating (rank 1 = ~2400, rank 500 = ~1200)
        winner_rating = max(2400 - (winner_rank * 2.4), 800)
        loser_rating = max(2400 - (loser_rank * 2.4), 800)
        avg_rating = (winner_rating + loser_rating) / 2
        rating_diff_pts = abs(winner_rating - loser_rating)

        upset = 1 if winner_rank > loser_rank else 0
        quality = compute_match_quality(score_w, score_l, nb_sets, rating_diff_pts)

        results.append({
            "sport": "lawn_tennis",
            "format": "singles",
            "player1_rating": winner_rating,
            "player2_rating": loser_rating,
            "avg_rating": avg_rating,
            "rating_diff": rating_diff_pts,
            "nb_sets": nb_sets,
            "score_w": score_w,
            "score_l": score_l,
            "upset": upset,
            "match_quality_score": quality,
            "source": "atp"
        })

    df_out = pd.DataFrame(results)
    print("  Processed " + str(len(df_out)) + " tennis rows")
    return df_out


# ══════════════════════════════════════════════════════════
# 2. PROCESS BADMINTON DATA (ms, ws, md, wd, xd)
# ══════════════════════════════════════════════════════════
def process_badminton():
    print("\n" + "="*50)
    print("Processing Badminton data...")

    files_config = [
        ("ms.csv", "singles", "MS"),
        ("ws.csv", "singles", "WS"),
        ("md.csv", "doubles", "MD"),
        ("wd.csv", "doubles", "WD"),
        ("xd.csv", "mixed_doubles", "XD"),
    ]

    all_results = []

    for filename, fmt, discipline in files_config:
        filepath = os.path.join(RAW_DIR, filename)
        if not os.path.exists(filepath):
            print("  Missing: " + filename)
            continue

        df = pd.read_csv(filepath, low_memory=False)
        print("  " + filename + ": " + str(len(df)) + " rows")

        for _, row in df.iterrows():
            try:
                winner = int(row["winner"]) if not pd.isna(row["winner"]) else 1
                nb_sets = int(row["nb_sets"]) if not pd.isna(row["nb_sets"]) else 2

                # Get final set score
                game_scores = ["game_1_score", "game_2_score", "game_3_score"]
                total_w = 0
                total_l = 0
                for gs in game_scores:
                    if gs in df.columns and not pd.isna(row[gs]):
                        w, l = parse_score(row[gs])
                        if w and l:
                            if winner == 1:
                                total_w += w
                                total_l += l
                            else:
                                total_w += l
                                total_l += w

                # Get total points if available
                if "team_one_total_points" in df.columns:
                    t1_pts = row.get("team_one_total_points", 0) or 0
                    t2_pts = row.get("team_two_total_points", 0) or 0
                    score_w = max(t1_pts, t2_pts)
                    score_l = min(t1_pts, t2_pts)
                else:
                    score_w = total_w if total_w > 0 else 42
                    score_l = total_l if total_l > 0 else 30

                quality = compute_match_quality(score_w, score_l, nb_sets, None)

                all_results.append({
                    "sport": "badminton",
                    "format": fmt,
                    "discipline": discipline,
                    "player1_rating": None,
                    "player2_rating": None,
                    "avg_rating": None,
                    "rating_diff": None,
                    "nb_sets": nb_sets,
                    "score_w": score_w,
                    "score_l": score_l,
                    "upset": 0,
                    "match_quality_score": quality,
                    "source": "bwf_kaggle"
                })
            except Exception as e:
                continue

    df_out = pd.DataFrame(all_results)
    print("  Total badminton rows: " + str(len(df_out)))
    return df_out


# ══════════════════════════════════════════════════════════
# 3. PROCESS PICKLEBALL DATA
# ══════════════════════════════════════════════════════════
def process_pickleball():
    print("\n" + "="*50)
    print("Processing Pickleball data...")

    # Load player DUPR ratings
    player_file = os.path.join(RAW_DIR, "player.csv")
    team_file = os.path.join(RAW_DIR, "team.csv")
    game_file = os.path.join(RAW_DIR, "game.csv")

    try:
        players = pd.read_csv(player_file)
        teams = pd.read_csv(team_file)
        games = pd.read_csv(game_file)
    except Exception as e:
        print("  Error loading pickleball files: " + str(e))
        return pd.DataFrame()

    print("  Players: " + str(len(players)))
    print("  Teams: " + str(len(teams)))
    print("  Games: " + str(len(games)))

    # Build player rating lookup
    player_ratings = {}
    for _, p in players.iterrows():
        pid = p["player_id"]
        dupr = p.get("doubles_dupr", None)
        if dupr and not pd.isna(dupr):
            # Convert DUPR (2.0-8.0) to our rating scale (800-2400)
            rating = 800 + (float(dupr) - 2.0) / 6.0 * 1600
            player_ratings[pid] = rating

    # Build team → players lookup
    team_players = {}
    for _, t in teams.iterrows():
        tid = t["team_id"]
        pid = t["player_id"]
        if tid not in team_players:
            team_players[tid] = []
        team_players[tid].append(pid)

    # Compute team average rating
    def get_team_rating(team_id):
        players_in_team = team_players.get(team_id, [])
        ratings = [player_ratings[p] for p in players_in_team if p in player_ratings]
        if ratings:
            return np.mean(ratings)
        return None

    # Skill level to approximate rating
    skill_to_rating = {
        "2.5": 900, "3.0": 1050, "3.5": 1200,
        "4.0": 1400, "4.5": 1600, "5.0": 1800,
        "Pro": 2200
    }

    results = []
    for _, g in games.iterrows():
        try:
            w_team = g["w_team_id"]
            l_team = g["l_team_id"]
            score_w = int(g["score_w"])
            score_l = int(g["score_l"])
            skill = str(g.get("skill_lvl", "3.5"))

            # Get ratings
            r1 = get_team_rating(w_team)
            r2 = get_team_rating(l_team)

            if r1 is None:
                r1 = skill_to_rating.get(skill, 1200)
            if r2 is None:
                r2 = skill_to_rating.get(skill, 1200)

            rating_diff = abs(r1 - r2)
            avg_rating = (r1 + r2) / 2
            upset = 1 if r1 < r2 else 0

            quality = compute_match_quality(score_w, score_l, 1, rating_diff)

            results.append({
                "sport": "pickleball",
                "format": "doubles",
                "player1_rating": r1,
                "player2_rating": r2,
                "avg_rating": avg_rating,
                "rating_diff": rating_diff,
                "nb_sets": 1,
                "score_w": score_w,
                "score_l": score_l,
                "upset": upset,
                "match_quality_score": quality,
                "source": "pklmart"
            })
        except Exception as e:
            continue

    df_out = pd.DataFrame(results)
    print("  Processed " + str(len(df_out)) + " pickleball rows")
    return df_out


# ══════════════════════════════════════════════════════════
# 4. PROCESS TABLE TENNIS (ITTF RANKINGS)
# ══════════════════════════════════════════════════════════
def process_table_tennis():
    print("\n" + "="*50)
    print("Processing Table Tennis (ITTF) data...")

    rankings_file = os.path.join(RAW_DIR, "ittf_rankings.csv")
    rankings_w_file = os.path.join(RAW_DIR, "ittf_rankings_women.csv")

    try:
        rankings_m = pd.read_csv(rankings_file, low_memory=False)
        rankings_w = pd.read_csv(rankings_w_file, low_memory=False)
    except Exception as e:
        print("  Error: " + str(e))
        return pd.DataFrame()

    print("  Men rankings: " + str(len(rankings_m)) + " rows")
    print("  Women rankings: " + str(len(rankings_w)) + " rows")

    # Extract rating distributions from rankings
    # Points → normalized to our 800-2400 scale
    all_points_m = rankings_m["Points"].dropna().to_numpy(dtype=float)
    all_points_w = rankings_w["Points"].dropna().to_numpy(dtype=float)

    print("  Men points range: " + str(int(all_points_m.min())) + " - " + str(int(all_points_m.max())))
    print("  Women points range: " + str(int(all_points_w.min())) + " - " + str(int(all_points_w.max())))

    # Compute percentile-based rating distribution
    # We use this to calibrate our synthetic generator
    percentiles = [10, 25, 50, 75, 90, 95, 99]
    print("\n  Men ITTF Points Percentiles:")
    for p in percentiles:
        val = np.percentile(all_points_m, p)
        print("    P" + str(p) + ": " + str(int(val)))

    print("\n  Women ITTF Points Percentiles:")
    for p in percentiles:
        val = np.percentile(all_points_w, p)
        print("    P" + str(p) + ": " + str(int(val)))

    # Save distribution stats for synthetic generator calibration
    stats = {
        "table_tennis_men": {
            "mean": float(np.mean(all_points_m)),
            "std": float(np.std(all_points_m)),
            "min": float(np.min(all_points_m)),
            "max": float(np.max(all_points_m)),
            "percentiles": {str(p): float(np.percentile(all_points_m, p)) for p in percentiles}
        },
        "table_tennis_women": {
            "mean": float(np.mean(all_points_w)),
            "std": float(np.std(all_points_w)),
            "min": float(np.min(all_points_w)),
            "max": float(np.max(all_points_w)),
            "percentiles": {str(p): float(np.percentile(all_points_w, p)) for p in percentiles}
        }
    }

    stats_path = os.path.join(OUT_DIR, "ittf_distribution_stats.json")
    with open(stats_path, "w") as f:
        json.dump(stats, f, indent=2)
    print("\n  Saved distribution stats to: " + stats_path)

    # ITTF has no match results — we use it only for rating calibration
    # Return empty DataFrame (no match rows to add)
    return pd.DataFrame()


# ══════════════════════════════════════════════════════════
# 5. EXTRACT SCORE PATTERNS FROM BADMINTON (for calibration)
# ══════════════════════════════════════════════════════════
def extract_score_patterns():
    print("\n" + "="*50)
    print("Extracting score patterns for synthetic generator calibration...")

    files = [
        ("ms.csv", "badminton_singles"),
        ("ws.csv", "badminton_singles"),
        ("md.csv", "badminton_doubles"),
    ]

    patterns = {}

    for filename, label in files:
        filepath = os.path.join(RAW_DIR, filename)
        if not os.path.exists(filepath):
            continue

        df = pd.read_csv(filepath, low_memory=False)
        winning_scores = []
        losing_scores = []

        for _, row in df.iterrows():
            for gs in ["game_1_score", "game_2_score", "game_3_score"]:
                if gs in df.columns and not pd.isna(row[gs]):
                    w, l = parse_score(row[gs])
                    if w and l:
                        winning_scores.append(max(w, l))
                        losing_scores.append(min(w, l))

        if winning_scores:
            patterns[label] = {
                "winner_score": {
                    "mean": float(np.mean(winning_scores)),
                    "std": float(np.std(winning_scores)),
                    "median": float(np.median(winning_scores))
                },
                "loser_score": {
                    "mean": float(np.mean(losing_scores)),
                    "std": float(np.std(losing_scores)),
                    "median": float(np.median(losing_scores))
                },
                "score_diff": {
                    "mean": float(np.mean([w - l for w, l in zip(winning_scores, losing_scores)])),
                    "std": float(np.std([w - l for w, l in zip(winning_scores, losing_scores)]))
                },
                "sample_count": len(winning_scores)
            }
            print("  " + label + ": avg winner=" + str(round(patterns[label]["winner_score"]["mean"], 1)) +
                  " avg loser=" + str(round(patterns[label]["loser_score"]["mean"], 1)) +
                  " avg diff=" + str(round(patterns[label]["score_diff"]["mean"], 1)))

    stats_path = os.path.join(OUT_DIR, "score_pattern_stats.json")
    with open(stats_path, "w") as f:
        json.dump(patterns, f, indent=2)
    print("  Saved score patterns to: " + stats_path)
    return patterns


# ══════════════════════════════════════════════════════════
# 6. EXTRACT PICKLEBALL RALLY PATTERNS
# ══════════════════════════════════════════════════════════
def extract_rally_patterns():
    print("\n" + "="*50)
    print("Extracting Pickleball rally patterns...")

    rally_file = os.path.join(RAW_DIR, "rally.csv")
    if not os.path.exists(rally_file):
        print("  rally.csv not found")
        return

    rally = pd.read_csv(rally_file, low_memory=False)
    print("  Rally rows: " + str(len(rally)))

    # Analyze ending types
    ending_counts = rally["ending_type"].value_counts()
    print("  Ending types:")
    for etype, cnt in ending_counts.items():
        pct = round(cnt / len(rally) * 100, 1)
        print("    " + str(etype) + ": " + str(cnt) + " (" + str(pct) + "%)")

    # Analyze rally lengths
    rally_lens = rally["rally_len"].dropna()
    print("\n  Rally length stats:")
    print("    mean: " + str(round(rally_lens.mean(), 1)))
    print("    median: " + str(round(rally_lens.median(), 1)))
    print("    std: " + str(round(rally_lens.std(), 1)))
    print("    max: " + str(int(rally_lens.max())))

    # Shot types used
    shot_counts = rally["ts_type"].value_counts()
    print("\n  Top shot types in rallies:")
    for stype, cnt in shot_counts.head(10).items():
        pct = round(cnt / len(rally) * 100, 1)
        print("    " + str(stype) + ": " + str(cnt) + " (" + str(pct) + "%)")

    # Save rally stats
    stats = {
        "ending_type_distribution": ending_counts.to_dict(),
        "rally_length": {
            "mean": float(rally_lens.mean()),
            "median": float(rally_lens.median()),
            "std": float(rally_lens.std()),
            "max": float(rally_lens.max())
        },
        "shot_type_distribution": shot_counts.head(15).to_dict()
    }

    stats_path = os.path.join(OUT_DIR, "rally_pattern_stats.json")
    with open(stats_path, "w") as f:
        json.dump(stats, f, indent=2)
    print("  Saved rally patterns to: " + stats_path)


# ══════════════════════════════════════════════════════════
# 7. COMBINE ALL AND SAVE
# ══════════════════════════════════════════════════════════
def combine_and_save(dfs):
    print("\n" + "="*50)
    print("Combining all datasets...")

    # Filter out empty dataframes
    valid_dfs = [df for df in dfs if df is not None and len(df) > 0]

    if not valid_dfs:
        print("  No data to combine!")
        return

    combined = pd.concat(valid_dfs, ignore_index=True)
    print("  Total rows before cleaning: " + str(len(combined)))

    # Basic cleaning
    combined = combined.dropna(subset=["match_quality_score"])
    combined = combined[combined["match_quality_score"] > 0]
    combined = combined[combined["match_quality_score"] <= 1.0]

    print("  Total rows after cleaning: " + str(len(combined)))

    # Show distribution by sport
    print("\n  Rows by sport:")
    for sport, cnt in combined["sport"].value_counts().items():
        print("    " + str(sport) + ": " + str(cnt))

    print("\n  Rows by format:")
    for fmt, cnt in combined["format"].value_counts().items():
        print("    " + str(fmt) + ": " + str(cnt))

    print("\n  Match quality score distribution:")
    print("    mean: " + str(round(combined["match_quality_score"].mean(), 3)))
    print("    std: " + str(round(combined["match_quality_score"].std(), 3)))
    print("    min: " + str(round(combined["match_quality_score"].min(), 3)))
    print("    max: " + str(round(combined["match_quality_score"].max(), 3)))

    # Save
    out_path = os.path.join(OUT_DIR, "real_match_data.csv")
    combined.to_csv(out_path, index=False)
    print("\n  Saved to: " + out_path)

    return combined


# ══════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════
print("ISMS DATA PROCESSING PIPELINE")
print("=" * 50)
print("Output directory: " + OUT_DIR)

# Process each sport
tennis_df = process_tennis_atp()
badminton_df = process_badminton()
pickleball_df = process_pickleball()
process_table_tennis()  # calibration only, no match rows

# Extract calibration patterns
extract_score_patterns()
extract_rally_patterns()

# Combine and save
combined = combine_and_save([tennis_df, badminton_df, pickleball_df])

print("\n" + "="*50)
print("PROCESSING COMPLETE!")
print("Files saved in: " + OUT_DIR)
print("  - real_match_data.csv         (unified match data)")
print("  - ittf_distribution_stats.json (table tennis calibration)")
print("  - score_pattern_stats.json     (score distribution calibration)")
print("  - rally_pattern_stats.json     (pickleball rally calibration)")
print("\nNext step: Run generate_synthetic_dataset.py")