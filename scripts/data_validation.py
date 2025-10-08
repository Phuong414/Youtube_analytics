import pandas as pd
import os

# Load data
df = pd.read_csv('data/raw/videos.csv')

print("===== DATA OVERVIEW =====")
print(df.info())
print("\n===== NULL VALUES =====")
print(df.isnull().sum())
print("\n===== SAMPLE ROWS =====")
print(df.sample(5))

# Basic sanity checks
if df['video_id'].is_unique:
    print("\n[UNQ] All video IDs are unique!")
else:
    print("\n [!!!] Duplicate video IDs found!")

# Check numeric columns
num_cols = ['view_count', 'like_count', 'comment_count']
for col in num_cols:
    if (df[col] < 0).any():
        print(f"[!!!] Negative values found in {col}")
    else:
        print(f"[OK] {col} looks fine!")

# Check date parsing
df['published_at'] = pd.to_datetime(df['published_at'], errors='coerce')
if df['published_at'].isnull().sum() == 0:
    print("[OK] All published_at dates parsed correctly.")
else:
    print("[!!!] Some published_at could not be parsed.")

print("\nValidation completed.")