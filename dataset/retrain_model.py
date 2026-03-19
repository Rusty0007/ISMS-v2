import os, pickle, json
import pandas as pd
import numpy as np
from supabase import create_client
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score
from sklearn.preprocessing import LabelEncoder
from dotenv import load_dotenv

load_dotenv("../backend/.env")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
assert SUPABASE_URL and SUPABASE_KEY, "Missing SUPABASE_URL or SUPABASE_SERVICE_KEY in .env"
MODELS_DIR   = os.path.join(os.path.dirname(__file__), "models")

print("Fetching real training data from Supabase...")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Fetch all unused training rows
response = supabase.table("matchmaking_training_data") \
    .select("*") \
    .eq("used_in_training", False) \
    .execute()

real_df = pd.DataFrame(response.data)
print("  New real rows: " + str(len(real_df)))

if len(real_df) < 500:
    print("  Not enough data yet (need 500+). Skipping retrain.")
    exit()

# Load existing synthetic data
synthetic_df = pd.read_csv(os.path.join(
    os.path.dirname(__file__), "processed", "ml_training_data.csv"
))
print("  Synthetic rows: " + str(len(synthetic_df)))

# Encode sport and format
sport_enc  = LabelEncoder()
format_enc = LabelEncoder()

# Combine real + synthetic
combined = pd.concat([synthetic_df, real_df], ignore_index=True)
combined["sport_encoded"]  = np.asarray(sport_enc.fit_transform(combined["sport"].to_numpy()), dtype=int)
combined["format_encoded"] = np.asarray(format_enc.fit_transform(combined["format"].to_numpy()), dtype=int)


FEATURES = [
    "rating_diff", "avg_rating_deviation", "winrate_diff",
    "activeness_diff", "streak_diff", "geo_score",
    "h2h_matches", "same_skill_category", "wait_time_seconds",
    "sport_encoded", "format_encoded"
]

df_clean = combined[FEATURES + ["match_quality_score"]].dropna()
X = df_clean[FEATURES].to_numpy(dtype=float)
y = df_clean["match_quality_score"].to_numpy(dtype=float)

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

print("Retraining model on " + str(len(X_train)) + " rows...")
model = GradientBoostingRegressor(
    n_estimators=200, learning_rate=0.05,
    max_depth=5, random_state=42
)
model.fit(X_train, y_train)

r2 = r2_score(y_test, model.predict(X_test))
print("  New R2: " + str(round(r2, 4)))

# Save new model (overwrites old one)
with open(os.path.join(MODELS_DIR, "matchmaking_model.pkl"), "wb") as f:
    pickle.dump(model, f)
with open(os.path.join(MODELS_DIR, "sport_encoder.pkl"), "wb") as f:
    pickle.dump(sport_enc, f)
with open(os.path.join(MODELS_DIR, "format_encoder.pkl"), "wb") as f:
    pickle.dump(format_enc, f)

# Mark rows as used
ids = [row["id"] for row in response.data]
supabase.table("matchmaking_training_data") \
    .update({"used_in_training": True}) \
    .in_("id", ids) \
    .execute()

print("Done! Model retrained with " + str(len(real_df)) + " real matches.")
print("Restart your FastAPI backend to load the new model.")
