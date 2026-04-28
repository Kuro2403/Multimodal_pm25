from __future__ import annotations

from pathlib import Path

import pandas as pd


INPUT_DIR = Path(r"notebooks/satellite_two_stations_output")
OUTPUT_DIR = Path(r"data/processed/satellite_pm25_daily")

TARGET = "pm25"
SPLIT_COL = "split"
TRAIN_FRAC = 0.70
VALIDATION_FRAC = 0.15


def _max_missing_run(series: pd.Series) -> int:
    max_run = 0
    run = 0
    for is_missing in series.isna():
        if is_missing:
            run += 1
            max_run = max(max_run, run)
        else:
            run = 0
    return max_run


def _load_station_files() -> pd.DataFrame:
    paths = sorted(
        path
        for path in INPUT_DIR.glob("*satellite_pm25_daily.csv")
        if path.name != "all_stations_satellite_pm25_daily.csv"
    )
    if not paths:
        raise FileNotFoundError(f"No station files found in {INPUT_DIR}")

    frames = []
    for path in paths:
        frame = pd.read_csv(path)
        frame["source_file"] = path.name
        frames.append(frame)

    df = pd.concat(frames, ignore_index=True)
    df["date"] = pd.to_datetime(df["date"], errors="coerce", format="mixed")
    if df["date"].isna().any():
        bad_rows = int(df["date"].isna().sum())
        raise ValueError(f"Found {bad_rows} rows with unparseable dates")
    return df.sort_values(["location_id", "date"]).reset_index(drop=True)


def _assign_chronological_splits(df: pd.DataFrame) -> pd.DataFrame:
    parts = []
    for _, group in df.groupby("location_id", sort=False):
        group = group.sort_values("date").copy()
        n_rows = len(group)
        train_end = int(n_rows * TRAIN_FRAC)
        validation_end = int(n_rows * (TRAIN_FRAC + VALIDATION_FRAC))

        group[SPLIT_COL] = "train"
        group.iloc[train_end:validation_end, group.columns.get_loc(SPLIT_COL)] = "validation"
        group.iloc[validation_end:, group.columns.get_loc(SPLIT_COL)] = "test"
        parts.append(group)

    return pd.concat(parts, ignore_index=True)


def _numeric_feature_columns(df: pd.DataFrame) -> list[str]:
    excluded = {
        "location_id",
        "latitude",
        "longitude",
        TARGET,
        "date",
        "location_name",
        "source_file",
        SPLIT_COL,
    }
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    return [col for col in numeric_cols if col not in excluded]


def _interpolate_group(
    group: pd.DataFrame,
    feature_cols: list[str],
    fallback_medians: pd.Series | None = None,
) -> pd.DataFrame:
    group = group.sort_values("date").copy()
    group = group.set_index("date", drop=False)

    for col in feature_cols:
        if group[col].isna().any():
            group[f"{col}_was_missing"] = group[col].isna().astype("int8")

    s5p_cols = [
        col
        for col in feature_cols
        if col.startswith(("no2_", "co_", "so2_")) and not col.endswith("_valid_pixels")
    ]
    s2_cols = [
        col
        for col in feature_cols
        if col.startswith(("ndvi_", "ndbi_", "ndwi_"))
    ]
    other_cols = [
        col
        for col in feature_cols
        if col not in set(s5p_cols + s2_cols) and not col.endswith("_valid_pixels")
    ]

    if s5p_cols:
        group[s5p_cols] = group[s5p_cols].interpolate(
            method="time",
            limit=7,
            limit_direction="both",
        )
    if s2_cols:
        group[s2_cols] = group[s2_cols].interpolate(
            method="time",
            limit=15,
            limit_direction="both",
        )
    if other_cols:
        group[other_cols] = group[other_cols].interpolate(
            method="time",
            limit=7,
            limit_direction="both",
        )

    # Pixel counts are coverage indicators; missing means no usable satellite pixels.
    valid_pixel_cols = [col for col in feature_cols if col.endswith("_valid_pixels")]
    if valid_pixel_cols:
        group[valid_pixel_cols] = group[valid_pixel_cols].fillna(0)

    # Daily std is NaN when only one hourly PM2.5 value is present.
    if "pm25_daily_std" in group.columns:
        group["pm25_daily_std_was_missing"] = group["pm25_daily_std"].isna().astype("int8")
        group["pm25_daily_std"] = group["pm25_daily_std"].fillna(0)

    remaining_numeric = group[feature_cols].select_dtypes(include="number").columns
    medians = (
        fallback_medians.reindex(remaining_numeric)
        if fallback_medians is not None
        else group[remaining_numeric].median(numeric_only=True)
    )
    group[remaining_numeric] = group[remaining_numeric].fillna(medians)

    return group.reset_index(drop=True)


def preprocess() -> tuple[pd.DataFrame, pd.DataFrame]:
    df = _load_station_files()
    df["date"] = pd.to_datetime(df["date"], format="mixed")
    df["day_of_week"] = df["date"].dt.dayofweek
    df["month"] = df["date"].dt.month
    df["day_of_year"] = df["date"].dt.dayofyear
    df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype("int8")

    if df[TARGET].isna().any():
        raise ValueError("Target pm25 contains missing values; do not impute the target.")

    df = _assign_chronological_splits(df)
    feature_cols = _numeric_feature_columns(df)

    before = []
    for location_id, group in df.groupby("location_id", sort=False):
        for col in feature_cols:
            before.append(
                {
                    "location_id": location_id,
                    "column": col,
                    "missing_before": int(group[col].isna().sum()),
                    "missing_rate_before": group[col].isna().mean(),
                    "max_missing_run_before": _max_missing_run(group[col]),
                }
            )

    processed_parts = []
    for _, station in df.groupby("location_id", sort=False):
        train = _interpolate_group(
            station.loc[station[SPLIT_COL] == "train"],
            feature_cols,
        )
        train_medians = train[feature_cols].median(numeric_only=True)
        processed_parts.append(train)

        for split in ["validation", "test"]:
            split_group = station.loc[station[SPLIT_COL] == split]
            if not split_group.empty:
                processed_parts.append(
                    _interpolate_group(
                        split_group,
                        feature_cols,
                        fallback_medians=train_medians,
                    )
                )

    processed = pd.concat(processed_parts, ignore_index=True)
    was_missing_cols = [col for col in processed.columns if col.endswith("_was_missing")]
    if was_missing_cols:
        processed[was_missing_cols] = processed[was_missing_cols].fillna(0).astype("int8")

    after = []
    for location_id, group in processed.groupby("location_id", sort=False):
        for col in feature_cols:
            after.append(
                {
                    "location_id": location_id,
                    "column": col,
                    "missing_after": int(group[col].isna().sum()),
                    "missing_rate_after": group[col].isna().mean(),
                }
            )

    report = pd.DataFrame(before).merge(
        pd.DataFrame(after),
        on=["location_id", "column"],
        how="left",
    )
    return processed, report


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    processed, report = preprocess()

    processed.to_csv(OUTPUT_DIR / "all_stations_preprocessed.csv", index=False)
    report.to_csv(OUTPUT_DIR / "missingness_report.csv", index=False)

    for split, group in processed.groupby(SPLIT_COL, sort=False):
        group.to_csv(OUTPUT_DIR / f"{split}.csv", index=False)

    for location_id, group in processed.groupby("location_id", sort=False):
        safe_name = str(location_id)
        group.to_csv(OUTPUT_DIR / f"station_{safe_name}_preprocessed.csv", index=False)

    print(f"Rows: {len(processed)}")
    print(f"Columns: {len(processed.columns)}")
    print("Split counts:")
    print(processed.groupby(["location_id", SPLIT_COL]).size().to_string())
    print(f"Output directory: {OUTPUT_DIR.as_posix()}")


if __name__ == "__main__":
    main()
