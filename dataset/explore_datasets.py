import os
import sys
import io
import pandas as pd
import glob

# Fix Windows PowerShell encoding
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

# Points to dataset/raw/ folder
BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "raw")


def explore_csv(filepath, max_rows=3):
    try:
        df = pd.read_csv(filepath, encoding='utf-8', low_memory=False)
    except UnicodeDecodeError:
        df = pd.read_csv(filepath, encoding='latin-1', low_memory=False)
    except Exception as e:
        print("   Could not read: " + str(e))
        return None

    print("")
    print("   FILE: " + os.path.basename(filepath))
    print("   Rows: " + str(len(df)) + " | Columns: " + str(len(df.columns)))
    print("   Columns: " + str(list(df.columns)))
    print("   Sample:")
    print(df.head(max_rows).to_string())
    return df


def explore_folder(folder_name):
    folder_path = os.path.join(BASE_DIR, folder_name)

    if not os.path.exists(folder_path):
        print("   Folder not found: " + folder_path)
        return

    csv_files = glob.glob(os.path.join(folder_path, "**/*.csv"), recursive=True)

    print("")
    print("=" * 60)
    print("FOLDER: " + folder_name)
    print("   CSV files found: " + str(len(csv_files)))

    if not csv_files:
        print("   No CSV files found - listing files:")
        for f in os.listdir(folder_path)[:10]:
            print("   - " + f)
        return

    for csv_file in sorted(csv_files)[:3]:
        explore_csv(csv_file)

    if len(csv_files) > 3:
        print("   ... and " + str(len(csv_files) - 3) + " more CSV files")


# ── MAIN ──────────────────────────────────────────────────
print("ISMS DATASET EXPLORER")
print("Base directory: " + BASE_DIR)

# GitHub Folders
github_folders = [
    "tennis_atp-master",
    "tennis_MatchChartingProject-master",
    "badminton_data_analysis-main",
    "Elo-MMR-master",
]

for folder in github_folders:
    explore_folder(folder)

# Kaggle Loose CSV Files
print("")
print("=" * 60)
print("LOOSE CSV FILES (Kaggle Downloads)")

loose_csvs = glob.glob(os.path.join(BASE_DIR, "*.csv"))
print("   CSV files found: " + str(len(loose_csvs)))

for csv_file in sorted(loose_csvs):
    explore_csv(csv_file)

print("")
print("=" * 60)
print("Exploration complete!")