# dataset/train_model.py
# Run: python train_model.py
# Output: models/matchmaking_model.pkl + models/model_info.json

import os
import sys
import io
import json
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
from sklearn.preprocessing import LabelEncoder
import pickle

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROCESSED_DIR = os.path.join(BASE_DIR, "processed")
MODELS_DIR = os.path.join(BASE_DIR, "models")
os.makedirs(MODELS_DIR, exist_ok=True)

print("ISMS MATCHMAKING MODEL TRAINER")
print("=" * 50)

# ══════════════════════════════════════════════════════════
# 1. LOAD DATA
# ══════════════════════════════════════════════════════════
print("\nLoading training data...")

# Load synthetic ML features
synthetic_path = os.path.join(PROCESSED_DIR, "ml_training_data.csv")
synthetic_df = pd.read_csv(synthetic_path)
print("  Synthetic rows: " + str(len(synthetic_df)))

# Load real match data for validation
real_path = os.path.join(PROCESSED_DIR, "real_match_data.csv")
real_df = pd.read_csv(real_path)
print("  Real rows: " + str(len(real_df)))

# ══════════════════════════════════════════════════════════
# 2. PREPARE FEATURES
# ══════════════════════════════════════════════════════════
print("\nPreparing features...")

# Features the model trains on
FEATURE_COLS = [
    "rating_diff",           # absolute rating difference
    "avg_rating_deviation",  # average uncertainty in ratings
    "winrate_diff",          # difference in win rates
    "activeness_diff",       # difference in activity levels
    "streak_diff",           # difference in current streaks
    "geo_score",             # geographic proximity 0-1
    "h2h_matches",           # head to head history count
    "same_skill_category",   # 1 if same skill tier
    "wait_time_seconds",     # how long players waited
]

TARGET_COL = "match_quality_score"

# Encode sport and format as numeric features
sport_encoder = LabelEncoder()
format_encoder = LabelEncoder()

synthetic_df["sport_encoded"] = np.asarray(sport_encoder.fit_transform(synthetic_df["sport"].to_numpy()), dtype=int)
synthetic_df["format_encoded"] = np.asarray(format_encoder.fit_transform(synthetic_df["format"].to_numpy()), dtype=int)



# Add encoded columns to features
FEATURE_COLS_FULL = FEATURE_COLS + ["sport_encoded", "format_encoded"]

print("  Features: " + str(FEATURE_COLS_FULL))
print("  Target: " + TARGET_COL)

# Check all features exist
missing = [c for c in FEATURE_COLS_FULL if c not in synthetic_df.columns]
if missing:
    print("  WARNING - Missing columns: " + str(missing))
else:
    print("  All features present")

# Drop rows with NaN in features or target
df_clean = synthetic_df[FEATURE_COLS_FULL + [TARGET_COL]].dropna()
print("  Rows after cleaning: " + str(len(df_clean)))

X = df_clean[FEATURE_COLS_FULL].to_numpy(dtype=float)
y = df_clean[TARGET_COL].to_numpy(dtype=float)

print("  X shape: " + str(X.shape))
print("  y range: " + str(round(y.min(), 3)) + " - " + str(round(y.max(), 3)))
print("  y mean:  " + str(round(y.mean(), 3)))

# ══════════════════════════════════════════════════════════
# 3. TRAIN / TEST SPLIT
# ══════════════════════════════════════════════════════════
print("\nSplitting data...")
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)
print("  Train size: " + str(len(X_train)))
print("  Test size:  " + str(len(X_test)))

# ══════════════════════════════════════════════════════════
# 4. TRAIN MODEL
# ══════════════════════════════════════════════════════════
print("\nTraining Gradient Boosting Regressor...")
print("  This may take 1-2 minutes...")

model = GradientBoostingRegressor(
    n_estimators=200,
    learning_rate=0.05,
    max_depth=5,
    min_samples_split=10,
    min_samples_leaf=5,
    subsample=0.8,
    random_state=42,
    verbose=0
)

model.fit(X_train, y_train)
print("  Training complete!")

# ══════════════════════════════════════════════════════════
# 5. EVALUATE MODEL
# ══════════════════════════════════════════════════════════
print("\nEvaluating model...")

# Test set predictions
y_pred = model.predict(X_test)
y_pred = np.clip(y_pred, 0.0, 1.0)

rmse = np.sqrt(mean_squared_error(y_test, y_pred))
mae = mean_absolute_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)

print("  Test Set Results:")
print("    RMSE: " + str(round(rmse, 4)))
print("    MAE:  " + str(round(mae, 4)))
print("    R2:   " + str(round(r2, 4)))

# Cross validation
print("\n  Running 5-fold cross validation...")
cv_scores = cross_val_score(model, X, y, cv=5, scoring="r2")
print("  CV R2 scores: " + str([round(s, 3) for s in cv_scores]))
print("  CV R2 mean:   " + str(round(cv_scores.mean(), 4)))
print("  CV R2 std:    " + str(round(cv_scores.std(), 4)))

# ══════════════════════════════════════════════════════════
# 6. FEATURE IMPORTANCES
# ══════════════════════════════════════════════════════════
print("\nFeature importances:")
importances = model.feature_importances_
feature_importance_pairs = sorted(
    zip(FEATURE_COLS_FULL, importances),
    key=lambda x: x[1],
    reverse=True
)
for feat, imp in feature_importance_pairs:
    bar = "#" * int(imp * 50)
    print("  " + feat.ljust(28) + str(round(imp * 100, 1)).rjust(5) + "% " + bar)

# ══════════════════════════════════════════════════════════
# 7. QUICK SANITY CHECK
# ══════════════════════════════════════════════════════════
print("\nSanity check - predicting known scenarios:")

def predict_quality(rating_diff, winrate_diff, activeness_diff,
                    streak_diff, geo_score, h2h, same_skill,
                    wait_sec, avg_rd, sport="badminton", fmt="singles"):
    sport_enc = list(sport_encoder.classes_).index(sport) if sport in sport_encoder.classes_ else 0
    fmt_enc = list(format_encoder.classes_).index(fmt) if fmt in format_encoder.classes_ else 0
    features = [[
        rating_diff, avg_rd, winrate_diff, activeness_diff,
        streak_diff, geo_score, h2h, same_skill, wait_sec,
        sport_enc, fmt_enc
    ]]
    pred = model.predict(features)[0]
    return round(np.clip(pred, 0.0, 1.0), 3)

# Scenario 1: Perfect match — same rating, same winrate, same area
s1 = predict_quality(0, 0.0, 0.0, 0, 1.0, 2, 1, 60, 150)
print("  Scenario 1 - Perfect match (same rating, same area): " + str(s1))

# Scenario 2: Good match — small rating diff
s2 = predict_quality(50, 0.05, 0.1, 1, 0.8, 1, 1, 90, 180)
print("  Scenario 2 - Good match (50pt diff):                  " + str(s2))

# Scenario 3: Average match — moderate difference
s3 = predict_quality(200, 0.15, 0.2, 2, 0.5, 0, 0, 120, 220)
print("  Scenario 3 - Average match (200pt diff):              " + str(s3))

# Scenario 4: Poor match — large rating difference
s4 = predict_quality(600, 0.40, 0.5, 5, 0.2, 0, 0, 30, 280)
print("  Scenario 4 - Poor match (600pt diff):                 " + str(s4))

# Scenario 5: Long wait time boosted
s5 = predict_quality(300, 0.20, 0.3, 3, 0.5, 0, 0, 600, 250)
print("  Scenario 5 - Long wait (10 min), wider diff accepted: " + str(s5))

# Check ordering makes sense
print("\n  Expected order: s1 > s2 > s3 > s4")
print("  Actual order:   " + str(s1) + " > " + str(s2) + " > " + str(s3) + " > " + str(s4))
order_correct = s1 >= s2 >= s3 >= s4
print("  Order correct: " + str(order_correct))

# ══════════════════════════════════════════════════════════
# 8. SAVE MODEL
# ══════════════════════════════════════════════════════════
print("\nSaving model...")

model_path = os.path.join(MODELS_DIR, "matchmaking_model.pkl")
with open(model_path, "wb") as f:
    pickle.dump(model, f)
print("  Model saved: " + model_path)

# Save encoders
sport_enc_path = os.path.join(MODELS_DIR, "sport_encoder.pkl")
with open(sport_enc_path, "wb") as f:
    pickle.dump(sport_encoder, f)

format_enc_path = os.path.join(MODELS_DIR, "format_encoder.pkl")
with open(format_enc_path, "wb") as f:
    pickle.dump(format_encoder, f)

# Save model info (for backend to load metadata)
model_info = {
    "model_type": "GradientBoostingRegressor",
    "n_estimators": 200,
    "learning_rate": 0.05,
    "max_depth": 5,
    "features": FEATURE_COLS_FULL,
    "target": TARGET_COL,
    "training_rows": len(X_train),
    "test_rows": len(X_test),
    "performance": {
        "rmse": round(rmse, 4),
        "mae": round(mae, 4),
        "r2": round(r2, 4),
        "cv_r2_mean": round(cv_scores.mean(), 4),
        "cv_r2_std": round(cv_scores.std(), 4),
    },
    "feature_importances": {
        feat: round(float(imp), 4)
        for feat, imp in feature_importance_pairs
    },
    "sport_classes": list(sport_encoder.classes_),
    "format_classes": list(format_encoder.classes_),
    "sanity_check": {
        "perfect_match": s1,
        "good_match": s2,
        "average_match": s3,
        "poor_match": s4,
        "order_correct": bool(order_correct),
    },
    "trained_on": "synthetic_data_calibrated_from_real_atp_bwf_pklmart",
    "total_real_matches_used_for_calibration": 88993,
    "total_synthetic_matches_trained_on": len(X_train),
}

info_path = os.path.join(MODELS_DIR, "model_info.json")
with open(info_path, "w") as f:
    json.dump(model_info, f, indent=2)
print("  Model info saved: " + info_path)

print("\n" + "="*50)
print("TRAINING COMPLETE!")
print("  RMSE: " + str(round(rmse, 4)) + "  (lower is better, target < 0.10)")
print("  R2:   " + str(round(r2, 4)) + "   (higher is better, target > 0.85)")
print("\nFiles saved in: " + MODELS_DIR)
print("  - matchmaking_model.pkl  (trained model)")
print("  - sport_encoder.pkl      (sport label encoder)")
print("  - format_encoder.pkl     (format label encoder)")
print("  - model_info.json        (performance metrics)")
print("\nNext step: Copy models/ folder to backend/app/models/")
print("Then integrate with services/matchmaking.py")