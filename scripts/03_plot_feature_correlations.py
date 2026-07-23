import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_FILE = ROOT / "data/processed/01_daily_merged_advanced_v3.csv"
OUT_DIR = ROOT / "outputs"
OUT_DIR.mkdir(parents=True, exist_ok=True)

def generate_correlation_analysis():
    print(f"Loading data from {DATA_FILE}...")
    df = pd.read_csv(DATA_FILE)
    df['date'] = pd.to_datetime(df['date'])
    
    rename_dict = {
        'wind_speed_10m_kmh_mean': 'wind_speed_mean_kmh',
        'boundary_layer_height_m_mean': 'blh_mean_m',
        'boundary_layer_height_m_max': 'blh_max_m',
        'boundary_layer_height_m_min': 'blh_min_m',
    }
    df = df.rename(columns=rename_dict)
    
    if "wind_speed_mean_kmh" in df.columns and "precipitation_mm" in df.columns:
        df["stagnation_index"] = 1.0 / (df["wind_speed_mean_kmh"] * df["precipitation_mm"] + 1.0)
    elif "wind_speed_mean_kmh" in df.columns and "precip_daily_mm" in df.columns:
        df["stagnation_index"] = 1.0 / (df["wind_speed_mean_kmh"] * df["precip_daily_mm"] + 1.0)
    else:
        df["stagnation_index"] = 0.0

    if "relative_humidity_pct_mean" in df.columns and "aod_550_mean" in df.columns:
        df["rh_aod_interaction"] = df["relative_humidity_pct_mean"] * df["aod_550_mean"]
    else:
        df["rh_aod_interaction"] = 0.0

    for lag in [1, 2, 3, 7]:
        df[f"pm25_lag{lag}"] = df.groupby("location_id")["pm25"].transform(lambda s: s.shift(lag))

    df["pm25_lag_decay"] = df["pm25_lag1"].fillna(0) * 0.5 + df["pm25_lag2"].fillna(0) * 0.25 + df["pm25_lag3"].fillna(0) * 0.125
    df["pm25_roll3_max"] = df.groupby("location_id")["pm25"].transform(lambda s: s.rolling(3, min_periods=1).max())
    df["pm25_roll7_median"] = df.groupby("location_id")["pm25"].transform(lambda s: s.rolling(7, min_periods=1).median())

    df["is_polluted"] = (df["pm25"] > 50).astype(int)
    df["polluted_group"] = df.groupby("location_id")["is_polluted"].transform(lambda s: (s == 0).cumsum())
    df["episode_length"] = df.groupby(["location_id", "polluted_group"])["is_polluted"].cumsum()
    df["episode_length_lag1"] = df.groupby("location_id")["episode_length"].transform(lambda s: s.shift(1)).fillna(0)

    pm25_pivot = df.pivot_table(index='date', columns='location_id', values='pm25')
    daily_spatial = df.groupby("date")["pm25"].transform("mean")
    df["pm25_spatial_mean"] = daily_spatial
    df["pm25_spatial_lag1"] = df.groupby("location_id")["pm25_spatial_mean"].transform(lambda s: s.shift(1))

    if "wind_speed_mean_kmh" in df.columns and "blh_mean_m" in df.columns:
        df["ventilation_coeff"] = df["wind_speed_mean_kmh"] * df["blh_mean_m"]

    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    exclude_cols = ['location_id', 'latitude', 'longitude', 'split', 'pm25_raw', 'pm25_spatial_mean', 'is_polluted', 'polluted_group', 'episode_length']
    feature_cols = [c for c in numeric_cols if c not in exclude_cols and c != 'pm25']

    corrs = df[feature_cols].apply(lambda x: x.corr(df['pm25'])).dropna()
    corrs_sorted = corrs.abs().sort_values(ascending=False)
    
    top_corrs = corrs.loc[corrs_sorted.index[:20]]

    sns.set_theme(style="whitegrid")
    fig, ax = plt.subplots(figsize=(10, 8), dpi=300)

    colors = ['#e74c3c' if val > 0 else '#3498db' for val in top_corrs.values]
    bars = ax.barh(top_corrs.index[::-1], top_corrs.values[::-1], color=colors[::-1], edgecolor='black', alpha=0.85, height=0.7)

    ax.set_title("Figure 3: Top Feature Correlations with Target PM2.5", fontsize=14, fontweight='bold', pad=15)
    ax.set_xlabel("Pearson Correlation Coefficient (r)", fontsize=12)
    ax.set_xlim(-0.5, 1.0)
    
    for bar in bars:
        width = bar.get_width()
        offset = 0.02 if width >= 0 else -0.06
        ax.text(width + offset, bar.get_y() + bar.get_height()/2, f"{width:.3f}", 
                va='center', ha='left' if width >= 0 else 'right', fontsize=9, fontweight='bold')

    plt.axvline(0, color='black', linewidth=1, linestyle='--')
    plt.tight_layout()

    out_png = OUT_DIR / "Figure3_Feature_Correlation.png"
    plt.savefig(out_png, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Saved correlation chart to {out_png}")

if __name__ == "__main__":
    generate_correlation_analysis()
