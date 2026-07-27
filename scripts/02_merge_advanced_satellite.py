import os
import pandas as pd

def main():
    base_file = "data/processed/01_daily_merged_clean.csv"
    gee_file = "data/raw/gee_advanced_satellite_test.csv"
    out_file = "data/processed/02_daily_merged_advanced.csv"

    print(f"Reading base dataset: {base_file}")
    df_base = pd.read_csv(base_file, parse_dates=['date'])
    
    if not os.path.exists(gee_file):
        print(f"Error: Advanced satellite data not found at {gee_file}.")
        print("Please ensure you have run the 01_crawl_satellite_gee.py script to completion.")
        return

    print(f"Reading advanced satellite data: {gee_file}")
    df_gee = pd.read_csv(gee_file, parse_dates=['date'])
    
    # Ensure types match
    df_base['location_id'] = df_base['location_id'].astype(str)
    df_gee['location_id'] = df_gee['location_id'].astype(str)

    # Merge
    print("Merging datasets on ['date', 'location_id']...")
    df_merged = pd.merge(df_base, df_gee, on=['date', 'location_id'], how='left')

    # Quick check on new columns
    new_cols = [c for c in df_gee.columns if c not in ['date', 'location_id']]
    print("\nMissing values for new satellite features:")
    for c in new_cols:
        missing = df_merged[c].isna().sum()
        pct = missing / len(df_merged) * 100
        print(f" - {c}: {missing} rows ({pct:.2f}%)")

    # Save
    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    df_merged.to_csv(out_file, index=False)
    print(f"\nSuccessfully saved new dataset to: {out_file}")
    print(f"Total rows: {len(df_merged)} | Total columns: {len(df_merged.columns)}")

if __name__ == "__main__":
    main()
