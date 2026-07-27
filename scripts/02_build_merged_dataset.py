"""
02_build_merged_dataset.py
==========================
Master data merge pipeline for PM2.5 Multimodal project.

Produces 3 output files in data/processed/:
  1) 01_daily_merged.csv         -- fully imputed, feature-engineered, ready for training
  2) 02_daily_merged_unimputed.csv -- same structure but satellite NaN preserved (for ablation)
  3) 00_raw_merged_all_sources.csv -- "raw merge" of ALL sources before any imputation/engineering

Workflow:
  Step 0  Load backbone  (PM2.5 ground + met + satellite)  from interim/
  Step 1  Merge ERA5 BLH, RH850, T-inversion       (04, 05, 06)
  Step 2  Merge CAMS AOD                             (07)
  Step 3  Merge MAIAC AOD                            (08)
  Step 4  Merge FIRMS fire                           (09)
  Step 5  Merge HCHO S5P                             (10)  <<< MISSING FROM OLD FILES
  Step 6  Merge spatial features                     (03)  <<< EXTRA COLS ADDED
  Step 7  Save 00_raw_merged_all_sources.csv (no imputation, no engineering)
  Step 8  Impute satellite NaN (temporal interpolation per station)
  Step 9  Feature engineering (lags, rolling, time encoding, split)
  Step 10 Save 01_daily_merged.csv (imputed) and 02_daily_merged_unimputed.csv

Reuse: Run this script any time raw sources are updated.
       python scripts/02_build_merged_dataset.py
"""
from __future__ import annotations
import json, sys, warnings
from pathlib import Path
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

ROOT         = Path(__file__).resolve().parent.parent
DATA_RAW     = ROOT / "data" / "raw"
DATA_INTERIM = ROOT / "data" / "interim"
DATA_PROC    = ROOT / "data" / "processed"
DATA_PROC.mkdir(parents=True, exist_ok=True)

TRAIN_FRAC, VAL_FRAC = 0.70, 0.15  # test = 1 - 0.70 - 0.15 = 0.15

# ─── Helpers ──────────────────────────────────────────────────────────────────
def log(msg: str):
    print(f"[merge] {msg}", flush=True)

def safe_merge(left: pd.DataFrame, right: pd.DataFrame,
               on: list[str], how="left", label="") -> pd.DataFrame:
    """Merge with diagnostics."""
    n_before = len(left)
    out = left.merge(right, on=on, how=how, suffixes=("", "_dup"))
    # Drop duplicate cols from merge
    dup_cols = [c for c in out.columns if c.endswith("_dup")]
    if dup_cols:
        out.drop(columns=dup_cols, inplace=True)
    n_after = len(out)
    new_cols = sorted(set(out.columns) - set(left.columns))
    log(f"  {label}: {n_before} -> {n_after} rows  (+{len(new_cols)} cols: {new_cols[:6]}{'...' if len(new_cols) > 6 else ''})")
    return out

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 0: Load backbone data from interim
# ═══════════════════════════════════════════════════════════════════════════════
log("Step 0: Loading backbone (PM2.5 ground + met + satellite)")

# PM2.5 + meteo merged
backbone_file = DATA_INTERIM / "openmeteo" / "all_stations_openaq_meteo_daily.csv"
if backbone_file.exists():
    df = pd.read_csv(backbone_file)
    log(f"  Loaded backbone from {backbone_file.relative_to(ROOT)}: {df.shape}")
else:
    # Fallback: load openaq + openmeteo separately
    openaq = pd.read_csv(DATA_INTERIM / "openaq" / "all_stations_daily.csv")
    openmeteo = pd.read_csv(DATA_INTERIM / "openmeteo" / "all_stations_daily.csv")
    df = openaq.merge(openmeteo, on=["location_id", "date"], how="inner",
                      suffixes=("", "_met"))
    dup_cols = [c for c in df.columns if c.endswith("_met")]
    df.drop(columns=dup_cols, inplace=True)
    log(f"  Built backbone from openaq + openmeteo: {df.shape}")

df["date"] = pd.to_datetime(df["date"])

# Rename pm25 column if needed (interim may use pm25_mean)
if "pm25" not in df.columns and "pm25_mean" in df.columns:
    df.rename(columns={"pm25_mean": "pm25"}, inplace=True)
if "pm25_count" not in df.columns and "pm25_hourly_count" in df.columns:
    df.rename(columns={"pm25_hourly_count": "pm25_count"}, inplace=True)
if "pm25_daily_std" not in df.columns and "pm25_std" in df.columns:
    df.rename(columns={"pm25_std": "pm25_daily_std"}, inplace=True)

# Drop rows without pm25
df = df.dropna(subset=["pm25"]).reset_index(drop=True)
log(f"  After dropna(pm25): {df.shape}")

# ── Merge satellite data ──
sat_file = DATA_INTERIM / "satellite" / "all_stations_satellite_clean.csv"
if sat_file.exists():
    sat = pd.read_csv(sat_file)
    sat["date"] = pd.to_datetime(sat["date"], utc=True).dt.tz_localize(None)
    # Ensure backbone date is also tz-naive
    if df["date"].dt.tz is not None:
        df["date"] = df["date"].dt.tz_localize(None)
    # Keep only satellite-specific columns + keys
    sat_only_cols = [c for c in sat.columns
                     if c not in df.columns or c in ["location_id", "date"]]
    df = safe_merge(df, sat[sat_only_cols], on=["location_id", "date"], label="satellite")

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 1-5: Merge external processed sources
# ═══════════════════════════════════════════════════════════════════════════════
merge_sources = {
    "04_era5_blh":    (DATA_PROC / "04_era5_blh_daily.csv",     ["location_id", "date"]),
    "05_era5_rh850":  (DATA_PROC / "05_era5_rh850_daily.csv",   ["location_id", "date"]),
    "06_era5_tinv":   (DATA_PROC / "06_era5_t_inversion_daily.csv", ["location_id", "date"]),
    "07_cams_aod":    (DATA_PROC / "07_cams_aod_daily.csv",     ["location_id", "date"]),
    "08_maiac_aod":   (DATA_PROC / "08_maiac_aod_daily.csv",    ["location_id", "date"]),
    "09_firms":       (DATA_PROC / "09_firms_daily.csv",        ["location_id", "date"]),
    "10_hcho_s5p":    (DATA_PROC / "10_hcho_s5p_daily.csv",     ["location_id", "date"]),
}

for label, (fpath, keys) in merge_sources.items():
    log(f"Step {label[:2]}: Merging {fpath.name}")
    if fpath.exists():
        src = pd.read_csv(fpath)
        src["date"] = pd.to_datetime(src["date"])
        # Drop 'month' if it will duplicate
        if "month" in src.columns and "month" not in keys:
            src.drop(columns=["month"], inplace=True, errors="ignore")
        # Only keep new columns + keys
        new_cols = [c for c in src.columns if c not in df.columns or c in keys]
        df = safe_merge(df, src[new_cols], on=keys, label=label)
    else:
        log(f"  WARNING: {fpath} not found, skipping")

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 6: Merge spatial features (station-level, not time-varying)
# ═══════════════════════════════════════════════════════════════════════════════
log("Step 06: Merging spatial features (station-level)")
spatial_file = DATA_PROC / "03_station_spatial_features.csv"
if spatial_file.exists():
    spatial = pd.read_csv(spatial_file)
    # Only keep cols not already in df
    new_spatial = [c for c in spatial.columns
                   if c not in df.columns or c == "location_id"]
    if len(new_spatial) > 1:  # at least location_id + 1 new col
        df = safe_merge(df, spatial[new_spatial], on=["location_id"], label="spatial")
    else:
        log("  All spatial features already present, skipping")

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 7: Save raw merged (all sources, no imputation, no feature engineering)
# ═══════════════════════════════════════════════════════════════════════════════
df = df.sort_values(["location_id", "date"]).reset_index(drop=True)

log(f"Step 07: Saving 00_raw_merged_all_sources.csv  shape={df.shape}")
out_raw = DATA_PROC / "00_raw_merged_all_sources.csv"
df.to_csv(out_raw, index=False)

# Also save the unimputed version (before any satellite imputation)
df_unimputed = df.copy()

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 8: Chronological split assignment
# ═══════════════════════════════════════════════════════════════════════════════
log("Step 08: Assigning chronological train/val/test split")

dates_unique = np.sort(df["date"].unique())
n_dates = len(dates_unique)
train_cutoff = dates_unique[int(n_dates * TRAIN_FRAC)]
val_cutoff   = dates_unique[int(n_dates * (TRAIN_FRAC + VAL_FRAC))]

df["split"] = "test"
df.loc[df["date"] <  train_cutoff, "split"] = "train"
df.loc[(df["date"] >= train_cutoff) & (df["date"] < val_cutoff), "split"] = "validation"

df_unimputed["split"] = df["split"].values.copy()

split_counts = df["split"].value_counts()
log(f"  train={split_counts.get('train',0)}  val={split_counts.get('validation',0)}  test={split_counts.get('test',0)}")

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 9: Satellite imputation (temporal interpolation per station)
# ═══════════════════════════════════════════════════════════════════════════════
log("Step 09: Imputing satellite NaN (temporal interpolation per station)")

SAT_IMPUTE_COLS = [c for c in df.columns if any(
    c.startswith(p) for p in ["no2_", "co_", "so2_", "aer_ai_", "ndvi_",
                               "hcho_", "s2_valid"])]
log(f"  Satellite cols to impute ({len(SAT_IMPUTE_COLS)}): {SAT_IMPUTE_COLS[:8]}...")

nan_before = df[SAT_IMPUTE_COLS].isna().sum().sum()

for c in SAT_IMPUTE_COLS:
    # Interpolate within each station, then fill remaining with station median
    df[c] = df.groupby("location_id")[c].transform(
        lambda s: s.interpolate(method="time" if s.index.dtype != "int64" else "linear",
                                limit_direction="both")
    )
    # Fill any remaining NaN with per-station median, then global median
    df[c] = df.groupby("location_id")[c].transform(
        lambda s: s.fillna(s.median())
    )
    df[c] = df[c].fillna(df[c].median())

nan_after = df[SAT_IMPUTE_COLS].isna().sum().sum()
log(f"  Satellite NaN: {nan_before} -> {nan_after}")

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 10: Feature Engineering
# ═══════════════════════════════════════════════════════════════════════════════
log("Step 10: Feature engineering (lags, rolling stats, time encoding)")

# PM2.5 interpolation flag
if "pm25_was_interpolated" not in df.columns:
    df["pm25_was_interpolated"] = 0
    df_unimputed["pm25_was_interpolated"] = 0

# Time encoding
df["day_of_week"] = df["date"].dt.dayofweek
df["month"]       = df["date"].dt.month
df["day_of_year"] = df["date"].dt.dayofyear
df["is_weekend"]  = (df["day_of_week"] >= 5).astype(int)
df["sin_doy"]     = np.sin(2 * np.pi * df["day_of_year"] / 365.25)
df["cos_doy"]     = np.cos(2 * np.pi * df["day_of_year"] / 365.25)
df["sin_month"]   = np.sin(2 * np.pi * df["month"] / 12)
df["cos_month"]   = np.cos(2 * np.pi * df["month"] / 12)

# PM2.5 lag features
for lag in [1, 2, 3, 7]:
    df[f"pm25_lag{lag}"] = df.groupby("location_id")["pm25"].transform(
        lambda s: s.shift(lag))

# Rolling statistics
df["pm25_delta"]   = df["pm25"] - df["pm25_lag1"]
df["pm25_roll3"]   = df.groupby("location_id")["pm25"].transform(
    lambda s: s.shift(1).rolling(3, min_periods=1).mean())
df["pm25_roll7"]   = df.groupby("location_id")["pm25"].transform(
    lambda s: s.shift(1).rolling(7, min_periods=1).mean())
df["pm25_roll7std"] = df.groupby("location_id")["pm25"].transform(
    lambda s: s.shift(1).rolling(7, min_periods=2).std())

log(f"  Final shape: {df.shape}")

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 11: Save output files
# ═══════════════════════════════════════════════════════════════════════════════

# ── 01_daily_merged.csv (fully imputed, feature-engineered) ──
out_imputed = DATA_PROC / "01_daily_merged.csv"
try:
    df.to_csv(out_imputed, index=False)
    log(f"Step 11: Saved {out_imputed.name}  shape={df.shape}  cols={len(df.columns)}")
except PermissionError:
    alt = DATA_PROC / "01_daily_merged_new.csv"
    df.to_csv(alt, index=False)
    log(f"Step 11: {out_imputed.name} is LOCKED. Saved to {alt.name} instead.")
    log(f"  -> Close the old file, then rename {alt.name} -> {out_imputed.name}")

# ── 02_daily_merged_unimputed.csv (satellite NaN preserved) ──
# Copy only the columns that existed before feature engineering
# to match the original unimputed format + add new source columns
out_unimputed = DATA_PROC / "02_daily_merged_unimputed.csv"
try:
    df_unimputed.to_csv(out_unimputed, index=False)
    log(f"Step 11: Saved {out_unimputed.name}  shape={df_unimputed.shape}  cols={len(df_unimputed.columns)}")
except PermissionError:
    alt = DATA_PROC / "02_daily_merged_unimputed_new.csv"
    df_unimputed.to_csv(alt, index=False)
    log(f"Step 11: {out_unimputed.name} is LOCKED. Saved to {alt.name} instead.")
    log(f"  -> Close the old file, then rename {alt.name} -> {out_unimputed.name}")

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 12: Preprocessing report
# ═══════════════════════════════════════════════════════════════════════════════
report = {
    "files_produced": [
        "00_raw_merged_all_sources.csv",
        "01_daily_merged.csv",
        "02_daily_merged_unimputed.csv"
    ],
    "backbone_source": str(backbone_file.relative_to(ROOT)) if backbone_file.exists() else "openaq+openmeteo",
    "external_sources_merged": list(merge_sources.keys()) + ["03_spatial"],
    "shape_raw": list(pd.read_csv(out_raw, nrows=0).shape),
    "shape_imputed": list(df.shape),
    "shape_unimputed": list(df_unimputed.shape),
    "split": {
        "train": int(split_counts.get("train", 0)),
        "validation": int(split_counts.get("validation", 0)),
        "test": int(split_counts.get("test", 0)),
    },
    "satellite_nan_before_impute": int(nan_before),
    "satellite_nan_after_impute": int(nan_after),
    "new_columns_vs_old": {
        "hcho": ["hcho_mean", "hcho_std", "hcho_min", "hcho_max", "hcho_median", "hcho_valid_pixels"],
        "spatial_extra": sorted(list(set(df.columns) - set(["location_id"])
                                     & set(pd.read_csv(spatial_file).columns) - set(pd.read_csv(DATA_PROC / "01_daily_merged.csv", nrows=0).columns))) if spatial_file.exists() else []
    }
}
report_file = DATA_PROC / "11_preprocessing_report.json"
with open(report_file, "w") as f:
    json.dump(report, f, indent=2, default=str)
log(f"Step 12: Saved preprocessing report to {report_file.name}")

# ── Summary ──
log("")
log("=" * 60)
log("MERGE COMPLETE")
log("=" * 60)
log(f"  00_raw_merged_all_sources.csv : ALL sources, no imputation")
log(f"  01_daily_merged.csv           : imputed + feature-engineered ({df.shape[1]} cols)")
log(f"  02_daily_merged_unimputed.csv : raw satellite NaN ({df_unimputed.shape[1]} cols)")
log("")
log(f"  NEW columns added:")
for c in sorted(set(df.columns)):
    if c.startswith("hcho"):
        log(f"    + {c}")
log(f"  Total feature columns: {df.shape[1]}")
log(f"  Total rows: {df.shape[0]}")
