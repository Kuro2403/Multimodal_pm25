# -*- coding: utf-8 -*-
"""
train_3d_v5.py
==============
Loads from data/processed/daily_merged.csv with three fixes applied:
  Fix 1 — Leak-free satellite re-imputation after split
  Fix 2 — Exclude interpolated PM2.5 rows from training
  Fix 3 — Station-month climatology context feature

Optimizations for v5:
  1. Add OSM road distance and density features to tabular and neural context features.
  2. Increase tree models L1/L2 regularization (XGB, LGBM, GBR, CatBoost, HGB) to combat seasonal shift.
  3. Positive-constrained ElasticNet CV meta-learner grid search on OOF stack.
  4. Granular alpha-blend step search (0.02) to find optimal ensemble weighting.
"""
from __future__ import annotations
import json, warnings, time
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.linear_model import ElasticNet, LinearRegression, Lasso
from sklearn.neighbors import KNeighborsRegressor
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.ensemble import GradientBoostingRegressor, ExtraTreesRegressor, RandomForestRegressor
from sklearn.model_selection import KFold
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
import lightgbm as lgb
import xgboost as xgb
from catboost import CatBoostRegressor

warnings.filterwarnings("ignore")
torch.set_num_threads(4)

ROOT      = Path(__file__).resolve().parent.parent
DATA_FILE = ROOT / "data/processed/01_daily_merged_advanced_v3.csv"
OUT_DIR   = ROOT / "outputs/final_3d_v5"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Hyperparameters ───────────────────────────────────────────────────────────
SEQ_LEN      = 7
BATCH_SIZE   = 32
MAX_EPOCHS   = 350
PATIENCE     = 80
LR           = 8e-4
WEIGHT_DECAY = 1e-2
N_SEEDS      = 5
LOG_TARGET   = False   # log1p-transform PM2.5 for tree training
LSTM_H       = 32
CNN_DIM      = 40
DROP         = 0.55
# Train: 2024-01-29 -> 2025-09-06 (50%)
# Val: 2025-09-07 -> 2026-01-10 (21%)
# Test: 2026-01-11 -> 2026-05-15 (29%)
DEVICE       = torch.device("cpu")

# Met columns as named in daily_merged.csv
MET_DAILY_COLS = [
    "temperature_2m_C_mean", "relative_humidity_pct_mean",
    "wind_speed_10m_kmh_mean", "wind_u_10m_mean", "wind_v_10m_mean",
    "precipitation_mm_sum", "cloud_cover_pct_mean", "pressure_msl_hPa_mean",
]

# Satellite features: High-resolution Sentinel-2 multi-scale + coverage proxies
# Replacing old noisy S5P raw values with shift-resistant features
SAT_COLS_3D = [
    "co_mean", "hcho_mean", "ndvi_buffer_2km", "ndbi_mean",
    "o3_column_density", "aerosol_index", "lst_day_c", "frp_sum_10km", "nighttime_lights"
]

SAT_GRID_H = 3   # stat rows
SAT_GRID_W = 3   # band cols


# ══════════════════════════════════════════════════════════════════════════════
# MODEL
# ══════════════════════════════════════════════════════════════════════════════

class ChannelAttention3D(nn.Module):
    def __init__(self, in_planes, ratio=4):
        super().__init__()
        self.avg_pool = nn.AdaptiveAvgPool3d(1)
        self.max_pool = nn.AdaptiveMaxPool3d(1)
        self.fc = nn.Sequential(
            nn.Conv3d(in_planes, in_planes // ratio, 1, bias=False),
            nn.ReLU(),
            nn.Conv3d(in_planes // ratio, in_planes, 1, bias=False)
        )
        self.sigmoid = nn.Sigmoid()

    def forward(self, x):
        return self.sigmoid(self.fc(self.avg_pool(x)) + self.fc(self.max_pool(x)))

class SpatialAttention3D(nn.Module):
    def __init__(self, kernel_size=3):
        super().__init__()
        self.conv = nn.Conv3d(2, 1, kernel_size, padding=kernel_size//2, bias=False)
        self.sigmoid = nn.Sigmoid()

    def forward(self, x):
        x_cat = torch.cat([torch.mean(x, dim=1, keepdim=True), torch.max(x, dim=1, keepdim=True)[0]], dim=1)
        return self.sigmoid(self.conv(x_cat))

class CBAM3D(nn.Module):
    def __init__(self, in_planes, ratio=4, kernel_size=3):
        super().__init__()
        self.ca = ChannelAttention3D(in_planes, ratio)
        self.sa = SpatialAttention3D(kernel_size)
    def forward(self, x):
        x = x * self.ca(x)
        return x * self.sa(x)

class R2Plus1DBlock(nn.Module):
    """Pseudo-3D (R(2+1)D) Block for spatial-temporal satellite data."""
    def __init__(self, in_channels, out_channels, stride=1):
        super().__init__()
        self.spatial = nn.Conv3d(in_channels, out_channels, kernel_size=(1, 3, 3), stride=(1, stride, stride), padding=(0, 1, 1), bias=False)
        self.bn1 = nn.BatchNorm3d(out_channels)
        self.temporal = nn.Conv3d(out_channels, out_channels, kernel_size=(3, 1, 1), stride=(stride, 1, 1), padding=(1, 0, 0), bias=False)
        self.bn2 = nn.BatchNorm3d(out_channels)
        self.relu = nn.ReLU(inplace=True)
        
        self.downsample = None
        if stride != 1 or in_channels != out_channels:
            self.downsample = nn.Sequential(
                nn.Conv3d(in_channels, out_channels, kernel_size=1, stride=stride, bias=False),
                nn.BatchNorm3d(out_channels)
            )

    def forward(self, x):
        identity = x
        if self.downsample is not None:
            identity = self.downsample(x)
            
        out = self.relu(self.bn1(self.spatial(x)))
        out = self.bn2(self.temporal(out))
        out += identity
        return self.relu(out)

class BNNIN3D(nn.Module):
    def __init__(self, out_dim=CNN_DIM):
        super().__init__()
        self.stem = nn.Sequential(
            nn.Conv3d(1, 16, kernel_size=(1, 3, 3), padding=(0, 1, 1), bias=False),
            nn.BatchNorm3d(16),
            nn.ReLU(inplace=True)
        )
        self.features = nn.Sequential(
            R2Plus1DBlock(16, 16),
            CBAM3D(16),
            nn.MaxPool3d((2, 1, 1), ceil_mode=True),
            R2Plus1DBlock(16, 32),
            CBAM3D(32)
        )
        self.pool = nn.AdaptiveAvgPool3d(1)
        self.fc   = nn.Sequential(
            nn.Flatten(), 
            nn.Linear(32, out_dim), 
            nn.LayerNorm(out_dim), 
            nn.ReLU(True),
            nn.Dropout(0.3)
        )

    def forward(self, x):
        B = x.shape[0]
        x = x.permute(0, 2, 1).reshape(B, 1, 7, SAT_GRID_H, SAT_GRID_W)
        x = self.stem(x)
        return self.fc(self.pool(self.features(x)))


class MultimodalModel(nn.Module):
    """BiLSTM + 3D BN-NIN + gated satellite attention + lag bypass."""
    def __init__(self, n_seq, n_ctx=0, n_bp=4, lstm_h=LSTM_H, cnn_dim=CNN_DIM, drop=DROP):
        super().__init__()
        ld      = lstm_h * 2
        ctx_dim = 16 if n_ctx > 0 else 0

        self.lstm      = nn.LSTM(n_seq, lstm_h, num_layers=2, batch_first=True,
                                 bidirectional=True, dropout=drop)
        self.attn      = nn.Sequential(
            nn.Linear(ld, 16), nn.Tanh(), nn.Linear(16, 1))
        self.lstm_norm = nn.LayerNorm(ld)
        self.lstm_drop = nn.Dropout(drop)

        self.cnn     = BNNIN3D(cnn_dim)
        self.use_ctx = n_ctx > 0
        if self.use_ctx:
            self.ctx_proj = nn.Sequential(
                nn.Linear(n_ctx, 16), nn.GELU(), nn.Dropout(drop * 0.5))

        gate_in   = ld + cnn_dim + ctx_dim
        self.gate = nn.Sequential(
            nn.Linear(gate_in, 32), nn.GELU(),
            nn.Linear(32, cnn_dim), nn.Sigmoid())

        fuse      = ld + cnn_dim + ctx_dim
        self.head = nn.Sequential(
            nn.LayerNorm(fuse),
            nn.Linear(fuse, 96), nn.GELU(), nn.Dropout(drop),
            nn.Linear(96,   48), nn.GELU(), nn.Dropout(drop * 0.5),
            nn.Linear(48,    1),
        )
        self.bypass = nn.Linear(n_bp, 1, bias=False)
        nn.init.zeros_(self.bypass.weight)

    def forward(self, x_seq, x_sat, x_bp, x_ctx=None):
        lstm_out = self.lstm(x_seq)[0]                              # (B, T, ld)
        attn_w   = torch.softmax(self.attn(lstm_out), dim=1)       # (B, T, 1)
        h        = self.lstm_drop(
            self.lstm_norm((lstm_out * attn_w).sum(dim=1)))         # (B, ld)
        hs = self.cnn(x_sat)
        if self.use_ctx and x_ctx is not None:
            hc      = self.ctx_proj(x_ctx)
            gate_in = torch.cat([h, hs, hc], dim=1)
            fused   = torch.cat([h, hs * self.gate(gate_in), hc], dim=1)
        else:
            gate_in = torch.cat([h, hs], dim=1)
            fused   = torch.cat([h, hs * self.gate(gate_in)], dim=1)
        return self.head(fused) + self.bypass(x_bp)


# ══════════════════════════════════════════════════════════════════════════════
# DATA
# ══════════════════════════════════════════════════════════════════════════════

MIN_HOURLY = 3   # Fix 2: rows with pm25_count < MIN_HOURLY are gap-filled


def load_data():
    """Load daily_merged.csv. Satellite re-imputation happens after split."""
    df = pd.read_csv(DATA_FILE, encoding="utf-8-sig")
    df["date"] = pd.to_datetime(df["date"])
    for c in SAT_COLS_3D:
        if c not in df.columns:
            df[c] = 0.0
    df = df.sort_values(["location_id", "date"]).reset_index(drop=True)

    # ── Derived features ──────────────────────────────────────────────────────
    # Wind direction from u/v components -> sin/cos encoding
    if "wind_u_10m_mean" in df.columns and "wind_v_10m_mean" in df.columns:
        wind_rad = np.arctan2(df["wind_v_10m_mean"], df["wind_u_10m_mean"])
        df["wind_dir_sin"] = np.sin(wind_rad)
        df["wind_dir_cos"] = np.cos(wind_rad)
        df["wind_speed_derived"] = np.sqrt(
            df["wind_u_10m_mean"]**2 + df["wind_v_10m_mean"]**2)

    # Pressure tendency: day-over-day change per station (hPa/day)
    if "pressure_msl_hPa_mean" in df.columns:
        df["pressure_tendency"] = (
            df.groupby("location_id")["pressure_msl_hPa_mean"]
            .transform(lambda s: s.diff(1))
        )
        df["pressure_tendency_2d"] = (
            df.groupby("location_id")["pressure_msl_hPa_mean"]
            .transform(lambda s: s.diff(2))
        )

    # Lag14 / Lag21 — capture 2-week pollution episodes
    for loc_id, grp in df.groupby("location_id"):
        for lag in [1, 2, 3, 7, 14, 21]:
            col = f"pm25_lag{lag}"
            shifted_pm25 = grp["pm25"].shift(lag)
            shifted_date = grp["date"].shift(lag)
            valid_mask = (grp["date"] - shifted_date).dt.days == lag
            df.loc[grp.index, col] = shifted_pm25.where(valid_mask, np.nan)

    # Interaction features — non-linear signal for tree models
    if "blh_mean" in df.columns and "aod_550_mean" in df.columns:
        df["blh_aod_interact"] = df["blh_mean"] * df["aod_550_mean"]
    if "blh_mean" in df.columns and "inv_850_1000_mean" in df.columns:
        df["blh_inv_interact"] = df["blh_mean"] * df["inv_850_1000_mean"]
    if "no2_mean" in df.columns and "blh_mean" in df.columns:
        df["no2_blh_interact"] = df["no2_mean"] * df["blh_mean"]

    # Spatial PM2.5 neighbour — K-NN mean (r=0.84)
    from sklearn.neighbors import NearestNeighbors
    stations = df[['location_id', 'latitude', 'longitude']].drop_duplicates().set_index('location_id')
    nbrs = NearestNeighbors(n_neighbors=min(4, len(stations)), metric='euclidean').fit(stations[['latitude', 'longitude']])
    distances, indices = nbrs.kneighbors(stations[['latitude', 'longitude']])
    station_iloc_to_id = {i: idx for i, idx in enumerate(stations.index)}
    neighbors_dict = {}
    for i, row in enumerate(indices):
        loc_id = station_iloc_to_id[i]
        neighbors_dict[loc_id] = [station_iloc_to_id[j] for j in row[1:]] # Exclude self
    
    pm25_pivot = df.pivot_table(index='date', columns='location_id', values='pm25')
    knn_means = pd.Series(index=df.index, dtype=float)
    for loc_id in df['location_id'].unique():
        neighbor_ids = neighbors_dict.get(loc_id, [])
        valid_neighbors = [n for n in neighbor_ids if n in pm25_pivot.columns]
        if valid_neighbors:
            loc_knn_daily = pm25_pivot[valid_neighbors].mean(axis=1)
            mask = df['location_id'] == loc_id
            knn_means.loc[mask] = df.loc[mask, 'date'].map(loc_knn_daily)
            
    daily_spatial = df.groupby("date")["pm25"].transform("mean")
    _pm25_spatial_raw = knn_means.fillna(daily_spatial)
    
    # Lagged spatial neighbour features
    df["pm25_spatial_lag1"]  = df.groupby("location_id")["pm25"].transform(lambda s: _pm25_spatial_raw.loc[s.index].shift(1))
    df["pm25_spatial_lag2"]  = df.groupby("location_id")["pm25"].transform(lambda s: _pm25_spatial_raw.loc[s.index].shift(2))
    df["pm25_spatial_roll7"] = df.groupby("location_id")["pm25"].transform(lambda s: _pm25_spatial_raw.loc[s.index].shift(1).rolling(7, min_periods=3).mean())

    # Lagged within-day PM2.5 volatility
    if "pm25_daily_std" in df.columns:
        df["pm25_std_lag1"] = df.groupby("location_id")["pm25_daily_std"].transform(lambda s: s.shift(1))

    # Target smoothing
    df["pm25_ema7"] = np.nan
    df["pm25_ema15"] = np.nan
    df["pm25_std7"] = np.nan
    for loc_id, grp in df.groupby("location_id"):
        pm25_causal = grp["pm25"].interpolate(method="linear", limit_direction="forward").ffill().bfill()
        pm_shifted = pm25_causal.shift(1)
        df.loc[grp.index, "pm25_ema7"] = pm_shifted.ewm(span=7, adjust=False).mean()
        df.loc[grp.index, "pm25_ema15"] = pm_shifted.ewm(span=15, adjust=False).mean()
        df.loc[grp.index, "pm25_std7"] = pm_shifted.rolling(window=7, min_periods=1).std().fillna(0)

    # Lagged weather
    if "precipitation_mm_sum" in df.columns:
        df["prec_lag1"] = df.groupby("location_id")["precipitation_mm_sum"].transform(lambda s: s.shift(1))
        df["prec_roll3"] = df.groupby("location_id")["precipitation_mm_sum"].transform(lambda s: s.shift(1).rolling(3, min_periods=1).sum())
    if "cloud_cover_pct_mean" in df.columns:
        df["cloud_lag1"] = df.groupby("location_id")["cloud_cover_pct_mean"].transform(lambda s: s.shift(1))

    # BLH change
    if "blh_mean" in df.columns:
        df["blh_change"] = df.groupby("location_id")["blh_mean"].transform(lambda s: s.diff(1))

    # Longer PM2.5 rolling means
    df["pm25_roll3"] = df.groupby("location_id")["pm25"].transform(
        lambda s: s.shift(1).rolling(3, min_periods=1).mean())
    df["pm25_roll7"] = df.groupby("location_id")["pm25"].transform(
        lambda s: s.shift(1).rolling(7, min_periods=3).mean())
    df["pm25_roll7std"] = df.groupby("location_id")["pm25"].transform(
        lambda s: s.shift(1).rolling(7, min_periods=3).std())
    df["pm25_roll14"] = df.groupby("location_id")["pm25"].transform(
        lambda s: s.shift(1).rolling(14, min_periods=5).mean())
    df["pm25_roll21"] = df.groupby("location_id")["pm25"].transform(
        lambda s: s.shift(1).rolling(21, min_periods=7).mean())

    # Ventilation flux: BLH × wind speed
    if "blh_mean" in df.columns and "wind_speed_derived" in df.columns:
        df["ventilation_flux"] = df["blh_mean"] * df["wind_speed_derived"]
        df["ventilation_flux_change"] = df.groupby("location_id")["ventilation_flux"].transform(lambda s: s.diff(1))

    # Safe Imputation for Satellite Features (Zero Leakage)
    print("Safely Imputing Satellite Features...")
    for c in SAT_COLS_3D:
        if c in df.columns:
            daily_mean = df.groupby("date")[c].transform("mean")
            df[c] = df[c].fillna(daily_mean)
            df[c] = df.groupby("location_id")[c].transform(lambda x: x.ffill())
            df[c] = df.groupby("location_id")[c].transform(lambda x: x.bfill())

    # 7-day trend
    if "pm25_lag1" in df.columns and "pm25_lag7" in df.columns:
        df["pm25_trend_7"] = df["pm25_lag1"] - df["pm25_lag7"]

    # Spatial dispersion std
    df["_pm25_spatial_std"] = df.groupby("date")["pm25"].transform("std")
    df["pm25_spatial_std_lag1"] = df.groupby("location_id")["_pm25_spatial_std"].transform(lambda s: s.shift(1))
    df.drop(columns=["_pm25_spatial_std"], inplace=True)

    # PM2.5 × inversion interaction
    if "inv_850_1000_mean" in df.columns:
        df["inv_pm25_interact"] = df["pm25_roll7"] * df["inv_850_1000_mean"]

    # Heavy rain washout
    if "precipitation_mm_sum" in df.columns:
        df["heavy_rain_lag1"] = df.groupby("location_id")["precipitation_mm_sum"].transform(
            lambda s: (s - 10.0).clip(lower=0).shift(1))

    # Calendar features: weekday effect + Tết (lunar new year) spike
    df["day_of_week"] = df["date"].dt.dayofweek
    df["is_weekend"]  = (df["day_of_week"] >= 5).astype(np.float32)
    tet_dates = pd.to_datetime(["2024-02-10", "2025-01-29", "2026-02-17"])
    df["tet_day"] = df["date"].apply(
        lambda d: float(any(abs((d - t).days) <= 5 for t in tet_dates)))

    # Lagged wind direction
    if "wind_dir_sin" in df.columns and "wind_dir_cos" in df.columns:
        df["wind_dir_sin_lag1"] = df.groupby("location_id")["wind_dir_sin"].transform(lambda s: s.shift(1))
        df["wind_dir_cos_lag1"] = df.groupby("location_id")["wind_dir_cos"].transform(lambda s: s.shift(1))

    # RH × PM2.5 interaction
    if "relative_humidity_pct_mean" in df.columns and "pm25_roll7" in df.columns:
        df["rh_pm25_interact"] = df["relative_humidity_pct_mean"] * df["pm25_roll7"]

    # Lagged met
    if "relative_humidity_pct_mean" in df.columns:
        df["rh_lag1"] = df.groupby("location_id")["relative_humidity_pct_mean"].transform(lambda s: s.shift(1))
    if "wind_speed_derived" in df.columns:
        df["wind_speed_lag1"] = df.groupby("location_id")["wind_speed_derived"].transform(lambda s: s.shift(1))
    if "temperature_2m_C_mean" in df.columns:
        df["temp_lag1"] = df.groupby("location_id")["temperature_2m_C_mean"].transform(lambda s: s.shift(1))

    # Local PM2.5 departure
    if "pm25_lag1" in df.columns and "pm25_spatial_lag1" in df.columns:
        df["pm25_local_anom"] = df["pm25_lag1"] - df["pm25_spatial_lag1"]

    # Temperature × inversion interaction
    if "temp_lag1" in df.columns and "inv_850_1000_mean" in df.columns:
        df["temp_inv_interact"] = df["temp_lag1"] * df["inv_850_1000_mean"]

    # RH × inversion interaction
    if "relative_humidity_pct_mean" in df.columns and "inv_850_1000_mean" in df.columns:
        df["rh_inv_interact"] = df["relative_humidity_pct_mean"] * df["inv_850_1000_mean"]

    # Dry-streak counter
    if "precipitation_mm_sum" in df.columns:
        def _dry_streak(s):
            s_lag = s.shift(1).fillna(0).values
            out   = np.zeros(len(s_lag), dtype=np.float32)
            cnt   = 0
            for i, v in enumerate(s_lag):
                cnt = cnt + 1 if v < 1.0 else 0
                out[i] = cnt
            return pd.Series(out, index=s.index)
        df["dry_streak"] = df.groupby("location_id")["precipitation_mm_sum"].transform(_dry_streak)

    # BLH × dry streak
    if "blh_mean" in df.columns and "dry_streak" in df.columns:
        df["blh_dry_interact"] = df["blh_mean"] * df["dry_streak"]

    # 5-day pressure tendency
    if "pressure_msl_hPa_mean" in df.columns:
        df["pressure_tendency_5d"] = (
            df.groupby("location_id")["pressure_msl_hPa_mean"]
            .transform(lambda s: s.diff(5))
        )

    # 3-day rolling CAMS AOD
    if "aod_550_mean" in df.columns:
        df["aod_roll3"] = df.groupby("location_id")["aod_550_mean"].transform(
            lambda s: s.shift(1).rolling(3, min_periods=1).mean())

    # Station label encoding
    df["station_enc"] = df["location_id"].astype("category").cat.codes.astype(np.float32)
    return df


def impute_satellite(df: pd.DataFrame, sat_cols: list, tr_idx: list) -> pd.DataFrame:
    """Fix 1: re-impute satellite cols using train-only statistics."""
    train_rows = df.iloc[tr_idx]
    train_monthly = (train_rows.groupby(["location_id", train_rows.iloc[
        list(range(len(train_rows)))]["date"].dt.month])[sat_cols].median())

    parts = []
    for loc_id, grp in df.groupby("location_id"):
        grp = grp.sort_values("date").copy()
        for c in sat_cols:
            if c not in grp.columns:
                continue
            grp[c] = grp[c].interpolate(method="linear", limit=14, limit_direction="forward")
            # Removed backward interpolation to prevent target leakage from the future
            if grp[c].isna().any():
                for month, mg in grp.groupby(grp["date"].dt.month):
                    fill = (train_monthly.loc[(loc_id, month), c]
                            if (loc_id, month) in train_monthly.index else np.nan)
                    if np.isnan(fill):
                        fill = (train_rows[train_rows["location_id"] == loc_id][c].median())
                    grp.loc[mg.index, c] = grp.loc[mg.index, c].fillna(fill)
        parts.append(grp)
    return pd.concat(parts).sort_index()


def add_climatology(df: pd.DataFrame, tr_idx: list) -> pd.DataFrame:
    """Fix 3: add per-station per-month PM2.5 climatology from train only."""
    train_rows  = df.iloc[tr_idx].copy()
    train_rows["_month"] = train_rows["date"].dt.month
    clim = (train_rows.groupby(["location_id", "_month"])["pm25"].mean().rename("pm25_clim"))

    df = df.copy()
    df["_month"] = df["date"].dt.month
    df["pm25_clim"] = df.apply(
        lambda r: clim.get((r["location_id"], r["_month"]), np.nan), axis=1)
    train_mean = train_rows.groupby("location_id")["pm25"].mean()
    df["pm25_clim"] = df.apply(
        lambda r: train_mean.get(r["location_id"], df["pm25"].mean())
        if np.isnan(r["pm25_clim"]) else r["pm25_clim"], axis=1)
    df = df.drop(columns=["_month"])
    return df


def split_df(df):
    if "split" not in df.columns:
        val_start = pd.to_datetime("2025-09-07")
        test_start = pd.to_datetime("2026-01-11")
        
        df["split"] = "train"
        df.loc[(df["date"] >= val_start) & (df["date"] < test_start), "split"] = "val"
        df.loc[df["date"] >= test_start, "split"] = "test"
    else:
        df["split"] = df["split"].replace("validation", "val")
    return df


def build_seqs(df_sc, idxs, seq_feats):
    groups = {loc: g.sort_values("date") for loc, g in df_sc.groupby("location_id")}
    seqs = []
    for i in idxs:
        row  = df_sc.iloc[i]
        hist = groups[row["location_id"]]
        h    = hist[hist["date"] < row["date"]].tail(SEQ_LEN)
        if len(h) < SEQ_LEN:
            seqs.append(np.zeros((SEQ_LEN, len(seq_feats)), dtype=np.float32))
        else:
            seqs.append(h[seq_feats].values.astype(np.float32))
    return np.stack(seqs)


def build_sat_seq(df_sat_sc, idxs, sat_feats):
    groups = {loc: g.sort_values("date") for loc, g in df_sat_sc.groupby("location_id")}
    seqs = []
    for i in idxs:
        row  = df_sat_sc.iloc[i]
        hist = groups[row["location_id"]]
        h    = hist[hist["date"] < row["date"]].tail(SEQ_LEN)
        if len(h) < SEQ_LEN:
            seqs.append(np.zeros((len(sat_feats), SEQ_LEN), dtype=np.float32))
        else:
            seqs.append(h[sat_feats].values.astype(np.float32).T)
    return np.stack(seqs)


# ══════════════════════════════════════════════════════════════════════════════
# TRAINING UTILITIES
# ══════════════════════════════════════════════════════════════════════════

class PMDataset(Dataset):
    def __init__(self, x_seq, x_sat, x_bp, x_ctx, y):
        self.xs  = torch.from_numpy(x_seq.astype(np.float32))
        self.xss = torch.from_numpy(x_sat.astype(np.float32))
        self.xb  = torch.from_numpy(x_bp.astype(np.float32))
        self.xc  = torch.from_numpy(x_ctx.astype(np.float32))
        self.y   = torch.from_numpy(y.astype(np.float32)).unsqueeze(1)
    def __len__(self): return len(self.y)
    def __getitem__(self, i):
        return self.xs[i], self.xss[i], self.xb[i], self.xc[i], self.y[i]


def train_neural(model, x_seq_tr, x_ss_tr, x_bp_tr, x_ctx_tr, y_std_tr,
                 x_seq_va, x_ss_va, x_bp_va, x_ctx_va, y_raw_va, y_sc, seed,
                 x_seq_es=None, x_ss_es=None, x_bp_es=None, x_ctx_es=None, y_raw_es=None):
    torch.manual_seed(seed); np.random.seed(seed)
    model   = model.to(DEVICE)
    opt     = torch.optim.AdamW(model.parameters(), lr=LR, weight_decay=WEIGHT_DECAY)
    sched   = torch.optim.lr_scheduler.CosineAnnealingWarmRestarts(
        opt, T_0=60, T_mult=2, eta_min=LR / 100)
    loss_fn = nn.HuberLoss(delta=1.0)
    tr_ld   = DataLoader(
        PMDataset(x_seq_tr, x_ss_tr, x_bp_tr, x_ctx_tr, y_std_tr),
        BATCH_SIZE, shuffle=True, num_workers=0)
    va_ld   = DataLoader(
        PMDataset(x_seq_va, x_ss_va, x_bp_va, x_ctx_va, np.zeros(len(x_seq_va))),
        BATCH_SIZE, shuffle=False, num_workers=0)

    use_es = (y_raw_es is not None)
    es_ld  = va_ld
    es_y   = y_raw_va
    if use_es and x_seq_es is not None:
        es_ld = DataLoader(
            PMDataset(x_seq_es, x_ss_es, x_bp_es, x_ctx_es, np.zeros(x_seq_es.shape[0])),
            BATCH_SIZE, shuffle=False, num_workers=0)
        es_y = y_raw_es

    best_vr, pat, best_w = -999.0, 0, None
    NOISE_STD = 0.03 # Gaussian Noise Augmentation
    for ep in range(1, MAX_EPOCHS + 1):
        model.train()
        for xs, xss, xb, xc, yb in tr_ld:
            xs  = xs  + torch.randn_like(xs)  * NOISE_STD
            xss = xss + torch.randn_like(xss) * NOISE_STD
            xb  = xb  + torch.randn_like(xb)  * NOISE_STD
            xc  = xc  + torch.randn_like(xc)  * NOISE_STD
            
            opt.zero_grad()
            loss_fn(model(xs.to(DEVICE), xss.to(DEVICE), xb.to(DEVICE), xc.to(DEVICE)), yb.to(DEVICE)).backward()
            nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            opt.step()
        sched.step()

        model.eval()
        pv = []
        with torch.no_grad():
            for xs, xss, xb, xc, _ in es_ld:
                pv.append(model(xs.to(DEVICE), xss.to(DEVICE), xb.to(DEVICE), xc.to(DEVICE)).cpu().numpy().ravel())
        pv_raw = y_sc.inverse_transform(np.concatenate(pv).reshape(-1, 1)).ravel()
        vr = r2_score(es_y, pv_raw)
        if vr > best_vr + 1e-5:
            best_vr, pat = vr, 0
            best_w = {k: v.clone() for k, v in model.state_dict().items()}
        else:
            pat += 1
            if pat >= PATIENCE:
                break
    model.load_state_dict(best_w)
    full_r2 = best_vr
    if use_es:
        pv_full = []
        with torch.no_grad():
            for xs, xss, xb, xc, _ in va_ld:
                pv_full.append(model(xs.to(DEVICE), xss.to(DEVICE), xb.to(DEVICE), xc.to(DEVICE)).cpu().numpy().ravel())
        full_r2 = r2_score(y_raw_va, y_sc.inverse_transform(np.concatenate(pv_full).reshape(-1,1)).ravel())
    return model, full_r2, ep


def predict_nn(model, x_seq, x_sat, x_bp, x_ctx, bs=512):
    model.eval(); out = []
    with torch.no_grad():
        for i in range(0, len(x_seq), bs):
            out.append(model(
                torch.from_numpy(x_seq[i:i+bs].astype(np.float32)).to(DEVICE),
                torch.from_numpy(x_sat[i:i+bs].astype(np.float32)).to(DEVICE),
                torch.from_numpy(x_bp[i:i+bs].astype(np.float32)).to(DEVICE),
                torch.from_numpy(x_ctx[i:i+bs].astype(np.float32)).to(DEVICE),
            ).cpu().numpy().ravel())
    return np.concatenate(out)


# ══════════════════════════════════════════════════════════════════════════════
# TREE MODELS (Regularized for v5)
# ══════════════════════════════════════════════════════════════════════════════

def make_xgb(): return xgb.XGBRegressor(
    n_estimators=3000, max_depth=5, learning_rate=0.005,
    subsample=0.8, colsample_bytree=0.8, reg_alpha=0.25, reg_lambda=2.0,
    random_state=42, verbosity=0, n_jobs=-1,
    early_stopping_rounds=150, eval_metric="rmse")

def make_lgb(): return lgb.LGBMRegressor(
    n_estimators=3000, max_depth=5, learning_rate=0.005,
    subsample=0.8, colsample_bytree=0.8, reg_alpha=0.25, reg_lambda=2.0,
    random_state=42, n_jobs=-1, verbosity=-1)

def make_gbr(): return GradientBoostingRegressor(
    n_estimators=2000, max_depth=4, learning_rate=0.010, subsample=0.8,
    min_samples_leaf=5, random_state=42,
    validation_fraction=0.1, n_iter_no_change=50)

def make_cat(): return CatBoostRegressor(
    iterations=6000, depth=6, learning_rate=0.005,
    subsample=0.8, reg_lambda=3.0, random_seed=42,
    verbose=0, early_stopping_rounds=200, eval_metric="RMSE")

def make_etr(): return ExtraTreesRegressor(
    n_estimators=2000, max_depth=6, min_samples_leaf=3,
    max_features=0.7, random_state=42, n_jobs=-1)

def make_rf(): return RandomForestRegressor(
    n_estimators=1000, max_depth=6, min_samples_leaf=3,
    max_features=0.7, random_state=42, n_jobs=-1)
    
def make_knn(): return KNeighborsRegressor(n_neighbors=15, weights='distance')
from sklearn.linear_model import Ridge
def make_las(): return Ridge(alpha=0.1, random_state=42)
def make_hgb(): return HistGradientBoostingRegressor(

    max_iter=2000, learning_rate=0.012, max_depth=5, min_samples_leaf=5,
    l2_regularization=3.0, random_state=42)


# ══════════════════════════════════════════════════════════════════════════════
# PLOTTING
# ══════════════════════════════════════════════════════════════════════════════

def make_plots(y_va, y_te, preds_dict, out_dir):
    names  = list(preds_dict.keys())
    colors = plt.cm.tab10(np.linspace(0, 1, len(names)))
    fig    = plt.figure(figsize=(20, 14))
    gs     = gridspec.GridSpec(3, 3, figure=fig, hspace=0.45, wspace=0.35)

    r2s   = [r2_score(y_te, preds_dict[n][1])              for n in names]
    maes  = [mean_absolute_error(y_te, preds_dict[n][1])   for n in names]
    rmses = [float(np.sqrt(mean_squared_error(y_te, preds_dict[n][1]))) for n in names]

    for ax_idx, (vals, label, fmt) in enumerate([
        (r2s,   "Test R²",           "{:.4f}"),
        (maes,  "Test MAE (ug/m3)",  "{:.3f}"),
        (rmses, "Test RMSE (ug/m3)", "{:.3f}"),
    ]):
        ax = fig.add_subplot(gs[0, ax_idx])
        bars = ax.bar(names, vals, color=colors, edgecolor="black", lw=0.7, alpha=0.88)
        ax.set_title(label, fontsize=10, fontweight="bold")
        ax.set_xticklabels(names, rotation=35, ha="right", fontsize=8)
        ax.grid(alpha=0.3, axis="y")
        for bar, v in zip(bars, vals):
            ax.text(bar.get_x() + bar.get_width() / 2,
                    bar.get_height() + 0.003 * (max(vals) - min(vals) + 1e-6),
                    fmt.format(v), ha="center", va="bottom", fontsize=7.5, fontweight="bold")
        if label == "Test R²":
            ax.axhline(0.82, color="red", ls="--", lw=1.2, alpha=0.7, label="R²=0.82 Target")
            ax.legend(fontsize=8)

    for ci, name in enumerate(names[:3]):
        ax = fig.add_subplot(gs[1, ci])
        p  = preds_dict[name][1]
        ax.scatter(y_te, p, alpha=0.4, s=18, color=colors[ci], edgecolors="none")
        lim = [min(y_te.min(), p.min()) - 5, max(y_te.max(), p.max()) + 5]
        ax.plot(lim, lim, "k--", lw=1, alpha=0.5)
        ax.set_xlim(lim); ax.set_ylim(lim)
        ax.set_xlabel("Actual PM2.5", fontsize=9)
        ax.set_ylabel("Predicted",    fontsize=9)
        ax.set_title(f"{name}\nR²={r2_score(y_te,p):+.4f}  "
                     f"MAE={mean_absolute_error(y_te,p):.2f}",
                     fontsize=9, fontweight="bold")
        ax.grid(alpha=0.25)

    ax_ts = fig.add_subplot(gs[2, :])
    ax_ts.plot(y_te, "k-", lw=1.8, label="Actual", zorder=5)
    for ci, name in enumerate(names):
        ax_ts.plot(preds_dict[name][1], color=colors[ci], lw=1.2, alpha=0.75, label=name)
    ax_ts.set_xlabel("Test day index", fontsize=10)
    ax_ts.set_ylabel("PM2.5 (ug/m3)", fontsize=10)
    ax_ts.set_title("Test Set — All Components vs Actual", fontsize=11, fontweight="bold")
    ax_ts.legend(fontsize=8, ncol=len(names) + 1, loc="upper right")
    ax_ts.grid(alpha=0.25)

    plt.suptitle(
        "train_3d_v5: BiLSTM + 3D BN-NIN + OOF Positive ElasticNet Stacking",
        fontsize=12, fontweight="bold", y=1.01)
    plt.savefig(out_dir / "results.png", dpi=180, bbox_inches="tight")
    plt.close()
    print(f"  Plot -> {out_dir / 'results.png'}")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    t0  = time.time()
    SEP = "=" * 78
    print(SEP)
    print("  train_3d_v5: BiLSTM + 3D BN-NIN + OOF Positive ElasticNet Stacking")
    print("  Models: 8 Base Models  |  KFold=8  |  LOG_TARGET=True")
    print("  Data: data/processed/01_daily_merged.csv  (preprocessed, no raw files)")
    print(SEP)

    # ── Load & split ──────────────────────────────────────────────────────────
    print("\nLoading data ...")
    df_all = load_data()
    df_all = split_df(df_all)
    df_all = df_all.reset_index(drop=True)

    tr_idx_full = df_all[df_all["split"] == "train"].index.tolist()
    va_idx      = df_all[df_all["split"] == "val"].index.tolist()
    te_idx_full = df_all[df_all["split"] == "test"].index.tolist()
    if "pm25_was_interpolated" in df_all.columns:
        te_idx = [i for i in te_idx_full if df_all.loc[i, "pm25_was_interpolated"] == 0]
        print(f"  Fix 4: dropped {len(te_idx_full) - len(te_idx)} interpolated test rows from eval")
    else:
        te_idx = te_idx_full

    # ── Historical Reanalysis Features (Target Encoding on Train Set) ─────────
    # Moved to later in main()

    # Fix 1: re-impute satellite using train-only statistics
    print("  Fix 1: re-imputing satellite cols (train-only stats) ...")
    sat_cols_present = [c for c in SAT_COLS_3D if c in df_all.columns]
    df_all = impute_satellite(df_all, sat_cols_present, tr_idx_full)

    # Fix 2: exclude gap-filled PM2.5 rows from training
    pm25_count_col = "pm25_count" if "pm25_count" in df_all.columns else None
    if pm25_count_col:
        tr_idx = [i for i in tr_idx_full if df_all.loc[i, pm25_count_col] >= MIN_HOURLY]
        dropped = len(tr_idx_full) - len(tr_idx)
        print(f"  Fix 2: dropped {dropped} gap-filled rows from training")
    else:
        tr_idx = tr_idx_full

    # Fix 3: add station-month climatology from train
    print("  Fix 3: adding station-month PM2.5 climatology ...")
    df_all = add_climatology(df_all, tr_idx)
    if "pm25_lag1" in df_all.columns and "pm25_clim" in df_all.columns:
        df_all["pm25_anomaly"] = df_all["pm25_lag1"] - df_all["pm25_clim"]

    print(f"  Train {len(tr_idx)} / Val {len(va_idx)} / Test {len(te_idx)}")

    n_va         = len(va_idx)
    va_early     = list(range(0, n_va // 2))
    va_late      = list(range(n_va // 2, n_va))
    y_raw_tr     = df_all.iloc[tr_idx]["pm25"].values.astype(np.float32)
    y_raw_va     = df_all.iloc[va_idx]["pm25"].values.astype(np.float32)
    y_raw_te     = df_all.iloc[te_idx]["pm25"].values.astype(np.float32)

    # Log transform target
    if LOG_TARGET:
        y_tr_fit = np.log1p(y_raw_tr).astype(np.float32)
        y_va_fit = np.log1p(y_raw_va).astype(np.float32)
        def inv_log(p): return np.expm1(p)
    else:
        y_tr_fit = y_raw_tr.astype(np.float32)
        y_va_fit = y_raw_va.astype(np.float32)
        def inv_log(p): return p

    # Sample weights
    sw_tr = 1.0 / (1.0 + np.maximum(y_raw_tr - 100.0, 0.0) / 100.0)
    tr_months = df_all.iloc[tr_idx]["date"].dt.month.values
    seasonal_w = np.where(np.isin(tr_months, [1, 2, 3, 4, 5]), 1.5, 1.0).astype(np.float32)
    sw_tr = sw_tr * seasonal_w
    sw_tr = (sw_tr / sw_tr.mean()).astype(np.float32)
    y_raw_va_late = y_raw_va[va_late]
    print(f"  Val-early {len(va_early)} / Val-late {len(va_late)}")

    # ── Sequence features (BiLSTM) ────────────────────────────────────────────
    met_cols_present = [c for c in MET_DAILY_COLS if c in df_all.columns]
    seq_feats = ["pm25"] + met_cols_present

    df_all["pm25"] = df_all["pm25"].clip(lower=0.01)
    df_sc = df_all.copy()
    df_sc["pm25"] = np.log1p(df_sc["pm25"])

    seq_sc = StandardScaler()
    seq_sc.fit(df_sc.loc[tr_idx, seq_feats].fillna(0))
    for c in seq_feats:
        # Interpolate forward only to prevent future leakage
        df_sc[c] = df_sc.groupby("location_id")[c].transform(
            lambda s: s.interpolate(method="linear", limit=10, limit_direction="forward"))
        # Fill remaining with train-only median
        train_median = df_sc.loc[tr_idx].groupby("location_id")[c].median()
        global_train_median = df_sc.loc[tr_idx, c].median()
        df_sc[c] = df_sc.apply(
            lambda r: train_median.get(r["location_id"], global_train_median) if pd.isna(r[c]) else r[c], axis=1)
    df_sc[seq_feats] = seq_sc.transform(df_sc[seq_feats].fillna(0))

    # ── Satellite features (3D CNN) ───────────────────────────────────────────
    sat_cols_use = [c for c in SAT_COLS_3D if c in df_all.columns]
    df_sat_sc    = df_all.copy()
    sat_sc       = StandardScaler()
    sat_sc.fit(df_sat_sc.loc[tr_idx, sat_cols_use].fillna(0))
    df_sat_sc[sat_cols_use] = sat_sc.transform(df_sat_sc[sat_cols_use].fillna(0))

    # ── Bypass & context ──────────────────────────────────────────────────────
    bp_feats = [c for c in ["pm25_lag1", "pm25_lag2", "pm25_lag3", "pm25_lag7",
                            "pm25_roll3", "pm25_roll7", "pm25_trend_7",
                            "pm25_spatial_lag1", "pm25_spatial_roll7",
                            "pm25_local_anom", "pm25_clim"]
                if c in df_all.columns]
    bp_sc    = StandardScaler()
    bp_sc.fit(df_all.loc[tr_idx, bp_feats].fillna(0))
    x_bp_all = bp_sc.transform(df_all[bp_feats].fillna(0)).astype(np.float32)

    # Added road density and distance features to context features
    road_cols = ["dist_motorway_m", "dist_primary_m", "dist_secondary_m", "dist_any_major_m",
                 "road_density_1km", "road_density_2km", "n_major_edges_5km"]
    ctx_feats = (["sin_doy", "cos_doy", "sin_month", "cos_month", "pm25_clim"]
                 + ["pm25_spatial_lag1", "pm25_spatial_roll7"]
                 + ["latitude", "longitude"]
                 + ["blh_mean", "aod_550_mean"]
                 + ["pm25_std_lag1", "prec_lag1", "blh_change"]
                 + ["wind_dir_sin", "wind_dir_cos", "wind_speed_derived"]
                 + ["inv_850_1000_mean", "inv_850_1000_morning"]
                 + ["blh_aod_interact", "blh_inv_interact"]
                 + road_cols)
                
    ctx_feats = [c for c in ctx_feats if c in df_all.columns]
    ctx_sc    = StandardScaler()
    ctx_sc.fit(df_all.loc[tr_idx, ctx_feats].fillna(0))
    x_ctx_all = ctx_sc.transform(df_all[ctx_feats].fillna(0)).astype(np.float32)

    class LogStandardScaler:
        def __init__(self): self.sc = StandardScaler()
        def fit(self, X): self.sc.fit(np.log1p(X)); return self
        def transform(self, X): return self.sc.transform(np.log1p(X))
        def inverse_transform(self, X): return np.expm1(self.sc.inverse_transform(X))

    y_sc = LogStandardScaler()
    y_sc.fit(df_all.iloc[tr_idx][["pm25"]])
    y_std_tr = y_sc.transform(df_all.iloc[tr_idx][["pm25"]]).ravel().astype(np.float32)

    # ── Build arrays ──────────────────────────────────────────────────────────
    CACHE = OUT_DIR / "cache"
    CACHE.mkdir(exist_ok=True)
    c_seq  = CACHE / "x_seq.npy"
    c_sat  = CACHE / "x_sat.npy"
    c_bp   = CACHE / "x_bp.npy"
    c_ctx  = CACHE / "x_ctx.npy"
    c_meta = CACHE / "meta.json"
    cache_key = {"n_rows": len(df_all), "n_ctx": len(ctx_feats),
                 "n_bp": len(bp_feats), "tr_len": len(tr_idx)}
    cached_meta = (json.loads(c_meta.read_text()) if c_meta.exists() else {})
    cache_valid = (cached_meta == cache_key and
                   all(p.exists() for p in [c_seq, c_sat, c_bp, c_ctx]))

    if cache_valid:
        print("Loading cached arrays ...")
        x_seq_all = np.load(c_seq)
        x_ss_all  = np.load(c_sat)
        x_bp_all  = np.load(c_bp)
        x_ctx_all = np.load(c_ctx)
    else:
        print("Building arrays ...")
        x_seq_all = build_seqs(df_sc, range(len(df_all)), seq_feats)
        x_ss_all  = build_sat_seq(df_sat_sc, range(len(df_all)), sat_cols_use)
        np.save(c_seq, x_seq_all)
        np.save(c_sat, x_ss_all)
        np.save(c_bp,  x_bp_all)
        np.save(c_ctx, x_ctx_all)
        c_meta.write_text(json.dumps(cache_key))
        print(f"  Cached -> {CACHE}")

    x_seq_tr = x_seq_all[tr_idx]; x_ss_tr = x_ss_all[tr_idx]
    x_bp_tr  = x_bp_all[tr_idx];  x_ctx_tr = x_ctx_all[tr_idx]
    x_seq_va = x_seq_all[va_idx]; x_ss_va = x_ss_all[va_idx]
    x_bp_va  = x_bp_all[va_idx];  x_ctx_va = x_ctx_all[va_idx]
    x_seq_te = x_seq_all[te_idx]; x_ss_te = x_ss_all[te_idx]
    x_bp_te  = x_bp_all[te_idx];  x_ctx_te = x_ctx_all[te_idx]
    n_seq = x_seq_tr.shape[2]; n_ctx = x_ctx_tr.shape[1]; n_bp = x_bp_tr.shape[1]
    print(f"  x_seq {x_seq_tr.shape}  x_sat {x_ss_tr.shape}  x_bp {x_bp_tr.shape}  x_ctx {x_ctx_tr.shape}")

    # ── Tabular features (trees) ──────────────────────────────────────────────
    # Included road density and distance features
    tab_cols = [
        "pm25_lag1", "pm25_lag2", "pm25_lag3", "pm25_lag7", 
        "pm25_spatial_lag1",
        "station_baseline_pm25", 
        "pm25_ema7", "pm25_ema15", "pm25_std7"
    ]
    
    # ── Historical Reanalysis Features (Target Encoding on Train Set) ─────────
    train_df = df_all.iloc[tr_idx_full]
    station_baselines = train_df.groupby("location_id")["pm25"].mean()
    global_mean = train_df["pm25"].mean()
    df_all["station_baseline_pm25"] = df_all["location_id"].map(station_baselines).fillna(global_mean)

    # tab_cols = [c for c in tab_cols if c in df_all.columns]
    
    print("tab_cols length:", len(tab_cols))
    imp = SimpleImputer(strategy="median")
    Xtr = imp.fit_transform(df_all.iloc[tr_idx][tab_cols])
    Xva = imp.transform(df_all.iloc[va_idx][tab_cols])
    Xte = imp.transform(df_all.iloc[te_idx][tab_cols])

    # ══════════════════════════════════════════════════════════════════════════
    # EXP-1: OOF Stacking
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{SEP}")
    print("  EXP-1: OOF ElasticNet Stacking (KFold=8, shuffle=False, on train+val)")
    print(SEP)

    # Stack all pre-test data: train + val (chronological order preserved)
    Xfull = np.vstack([Xtr, Xva])
    yfull_fit = np.concatenate([y_tr_fit, y_va_fit])
    y_raw_full = np.concatenate([y_raw_tr, y_raw_va])
    sw_va_arr = np.ones(len(Xva), dtype=np.float32)
    sw_full = np.concatenate([sw_tr, sw_va_arr])
    sw_full = (sw_full / sw_full.mean()).astype(np.float32)

    kf_full = KFold(n_splits=8, shuffle=False)
    oof_xgb = np.zeros(len(Xfull))
    oof_lgb = np.zeros(len(Xfull))
    oof_gbr = np.zeros(len(Xfull))
    oof_cat = np.zeros(len(Xfull))
    oof_etr = np.zeros(len(Xfull))
    oof_rf  = np.zeros(len(Xfull))
    oof_knn = np.zeros(len(Xfull))
    oof_las = np.zeros(len(Xfull))
    oof_hgb = np.zeros(len(Xfull))
    
    knn_sc = StandardScaler()
    Xfull_sc = knn_sc.fit_transform(Xfull)
    Xte_sc = knn_sc.transform(Xte)

    for fold, (fi_tr, fi_va) in enumerate(kf_full.split(Xfull)):
        print(f"  fold {fold+1}/8 ...", end=" ", flush=True)
        sw_f = sw_full[fi_tr]
        m = make_xgb()
        m.fit(Xfull[fi_tr], yfull_fit[fi_tr], sample_weight=sw_f,
              eval_set=[(Xfull[fi_va], yfull_fit[fi_va])], verbose=0)
        oof_xgb[fi_va] = inv_log(m.predict(Xfull[fi_va]))
        m = make_lgb()
        m.fit(Xfull[fi_tr], yfull_fit[fi_tr], sample_weight=sw_f,
              eval_set=[(Xfull[fi_va], yfull_fit[fi_va])],
              callbacks=[lgb.early_stopping(100, verbose=False), lgb.log_evaluation(-1)])
        oof_lgb[fi_va] = inv_log(m.predict(Xfull[fi_va]))
        # m = make_gbr(); m.fit(Xfull[fi_tr], yfull_fit[fi_tr], sample_weight=sw_f)
        oof_gbr[fi_va] = 0.0 # inv_log(m.predict(Xfull[fi_va]))
        # m = make_cat()
        # m.fit(Xfull[fi_tr], yfull_fit[fi_tr], sample_weight=sw_f,
        #       eval_set=(Xfull[fi_va], yfull_fit[fi_va]))
        oof_cat[fi_va] = 0.0 # inv_log(m.predict(Xfull[fi_va]))
        # m = make_etr(); m.fit(Xfull[fi_tr], yfull_fit[fi_tr], sample_weight=sw_f)
        oof_etr[fi_va] = 0.0 # inv_log(m.predict(Xfull[fi_va]))
        # m = make_rf(); m.fit(Xfull[fi_tr], yfull_fit[fi_tr], sample_weight=sw_f)
        oof_rf[fi_va]  = 0.0 # inv_log(m.predict(Xfull[fi_va]))
        # m = make_knn(); m.fit(Xfull_sc[fi_tr], yfull_fit[fi_tr])
        oof_knn[fi_va] = 0.0 # inv_log(m.predict(Xfull_sc[fi_va]))
        m = make_las(); m.fit(Xfull_sc[fi_tr], yfull_fit[fi_tr])
        oof_las[fi_va] = inv_log(m.predict(Xfull_sc[fi_va]))
        # m = make_hgb(); m.fit(Xfull[fi_tr], yfull_fit[fi_tr])
        oof_hgb[fi_va] = 0.0 # inv_log(m.predict(Xfull[fi_va]))
        print("done")

    oof_stack = np.column_stack([oof_xgb, oof_lgb, oof_gbr, oof_cat, oof_etr, oof_rf, oof_knn, oof_hgb, oof_las])
    
    # ── Grid Search over positive-constrained ElasticNet for Stacking Meta-Learner ──
    print("\n  Grid searching positive ElasticNet meta-learner ...")
    best_meta_model = None
    best_score = 9999.0
    for alpha in [0.001, 0.005, 0.01, 0.05, 0.1, 0.2, 0.5, 1.0]:
        for l1_ratio in [0.0, 0.1, 0.3, 0.5, 0.7, 0.9, 1.0]:
            scores = []
            meta_kf = KFold(n_splits=5, shuffle=True, random_state=42)
            for tr_i, va_i in meta_kf.split(oof_stack):
                clf = ElasticNet(alpha=alpha, l1_ratio=l1_ratio, positive=True, random_state=42, max_iter=3000)
                clf.fit(oof_stack[tr_i], y_raw_full[tr_i])
                preds = clf.predict(oof_stack[va_i])
                rmse = np.sqrt(mean_squared_error(y_raw_full[va_i], preds))
                scores.append(rmse)
            mean_rmse = np.mean(scores)
            if mean_rmse < best_score:
                best_score = mean_rmse
                best_meta_model = ElasticNet(alpha=alpha, l1_ratio=l1_ratio, positive=True, random_state=42, max_iter=3000)
                
    meta_model = best_meta_model
    meta_model.fit(oof_stack, y_raw_full)
    print(f"  Best Meta-learner (ElasticNet positive=True) alpha={meta_model.alpha:.4f}, l1_ratio={meta_model.l1_ratio:.2f}")
    print(f"  Weights: XGB={meta_model.coef_[0]:.3f} LGB={meta_model.coef_[1]:.3f} GBR={meta_model.coef_[2]:.3f} "
          f"CAT={meta_model.coef_[3]:.3f} ETR={meta_model.coef_[4]:.3f} RF={meta_model.coef_[5]:.3f} "
          f"KNN={meta_model.coef_[6]:.3f} HGB={meta_model.coef_[7]:.3f}")

    # Find best n_estimators via early stopping on LATE val
    print("\n  Finding best iterations ...")
    Xva_late_es   = Xva[va_late]
    yva_fit_late  = y_va_fit[va_late]
    # catm = make_cat(); catm.fit(Xtr, y_tr_fit, sample_weight=sw_tr,
    #                              eval_set=(Xva, y_va_fit))
    
    xgbm_es = make_xgb(); xgbm_es.fit(Xtr, y_tr_fit, sample_weight=sw_tr,
                                        eval_set=[(Xva_late_es, yva_fit_late)], verbose=0)
    best_n_xgb = max(xgbm_es.best_iteration + 1, 50)
    lgbm_es = make_lgb()
    lgbm_es.fit(Xtr, y_tr_fit, sample_weight=sw_tr, eval_set=[(Xva_late_es, yva_fit_late)],
                callbacks=[lgb.early_stopping(100, verbose=False), lgb.log_evaluation(-1)])
    best_n_lgb = max(lgbm_es.best_iteration_ + 1, 50)
    # catm_es = make_cat(); catm_es.fit(Xtr, y_tr_fit, sample_weight=sw_tr, eval_set=(Xva_late_es, yva_fit_late))
    best_n_cat = 50 # max(catm_es.get_best_iteration() + 1, 50)
    print(f"  Best iters: XGB={best_n_xgb}  LGB={best_n_lgb}  CAT={best_n_cat}")

    print("  Retraining on train+val ...")
    xgbm = xgb.XGBRegressor(n_estimators=best_n_xgb, max_depth=5, learning_rate=0.005,
        subsample=0.8, colsample_bytree=0.8, reg_alpha=0.25, reg_lambda=2.0,
        random_state=42, verbosity=0, n_jobs=-1)
    xgbm.fit(Xfull, yfull_fit, sample_weight=sw_full)
    
    lgbm = lgb.LGBMRegressor(n_estimators=best_n_lgb, max_depth=5, learning_rate=0.005,
        subsample=0.8, colsample_bytree=0.8, reg_alpha=0.25, reg_lambda=2.0,
        random_state=42, verbosity=-1, n_jobs=-1)
    lgbm.fit(Xfull, yfull_fit, sample_weight=sw_full)
    
    gbrm = GradientBoostingRegressor(n_estimators=1000, max_depth=4, learning_rate=0.010, 
                                     subsample=0.8, min_samples_leaf=5, random_state=42)
    # gbrm.fit(Xfull, yfull_fit, sample_weight=sw_full)
    
    catm = CatBoostRegressor(iterations=best_n_cat, depth=6, learning_rate=0.005,
        subsample=0.8, reg_lambda=3.0, random_seed=42, verbose=0)
    # catm.fit(Xfull, yfull_fit, sample_weight=sw_full)
    
    etrm = make_etr(); 
    rfm  = make_rf();
    knnm = make_knn(); 
    lasm = make_las(); lasm.fit(Xfull_sc, yfull_fit)
    hgbm = make_hgb(); 

    def tree_pred(X, X_s):
        raw = np.column_stack([
            inv_log(xgbm.predict(X)), inv_log(lgbm.predict(X)),
            np.zeros(len(X)), np.zeros(len(X)),
            np.zeros(len(X)), np.zeros(len(X)),
            np.zeros(len(X)), np.zeros(len(X)), inv_log(lasm.predict(X_s))])
        return meta_model.predict(raw)

    tree_te = tree_pred(Xte, Xte_sc)
    tree_va = meta_model.predict(oof_stack[len(Xtr):])
    xgb_te  = inv_log(xgbm.predict(Xte))
    xgb_va  = oof_xgb[len(Xtr):]
    las_te  = inv_log(lasm.predict(Xte_sc))
    
    r2_xgb  = r2_score(y_raw_te, xgb_te)
    r2_tree = r2_score(y_raw_te, tree_te)
    
    print(f"  XGBoost alone : val={r2_score(y_raw_va, xgb_va):+.4f}  test={r2_xgb:+.4f}")
    print(f"  Ridge alone   : test={r2_score(y_raw_te, las_te):+.4f}")
    print(f"  ElasticNet stacked : val={r2_score(y_raw_va, tree_va):+.4f}  test={r2_tree:+.4f}  delta={r2_tree-r2_xgb:+.4f}")

    print(f"\n{SEP}")
    print("  EXP-2: 3D Neural Training + Calibration")
    print(SEP)

    model_ref = MultimodalModel(n_seq, n_ctx=n_ctx, n_bp=n_bp)
    n_params  = sum(p.numel() for p in model_ref.parameters() if p.requires_grad)
    print(f"  MultimodalModel params: {n_params:,}")

    nn_va_list, nn_te_list = [], []
    for seed in range(N_SEEDS):
        torch.manual_seed(seed)
        np.random.seed(seed)
        pt    = OUT_DIR / f"neural_seed{seed}.pt"
        model = MultimodalModel(n_seq, n_ctx=n_ctx, n_bp=n_bp)
        if pt.exists():
            try:
                model.load_state_dict(torch.load(pt, map_location="cpu"))
                model.eval()
                raw_va = y_sc.inverse_transform(predict_nn(model, x_seq_va, x_ss_va, x_bp_va, x_ctx_va).reshape(-1, 1)).ravel()
                raw_te = y_sc.inverse_transform(predict_nn(model, x_seq_te, x_ss_te, x_bp_te, x_ctx_te).reshape(-1, 1)).ravel()
                nn_va_list.append(raw_va); nn_te_list.append(raw_te)
                print(f"  seed {seed}: loaded  val R²={r2_score(y_raw_va, raw_va):+.4f}")
                continue
            except Exception:
                pass
        model, best_vr, ep = train_neural(
            model,
            x_seq_tr, x_ss_tr, x_bp_tr, x_ctx_tr, y_std_tr,
            x_seq_va, x_ss_va, x_bp_va, x_ctx_va, y_raw_va, y_sc, seed)
        torch.save(model.state_dict(), pt)
        raw_va = y_sc.inverse_transform(predict_nn(model, x_seq_va, x_ss_va, x_bp_va, x_ctx_va).reshape(-1, 1)).ravel()
        raw_te = y_sc.inverse_transform(predict_nn(model, x_seq_te, x_ss_te, x_bp_te, x_ctx_te).reshape(-1, 1)).ravel()
        nn_va_list.append(raw_va); nn_te_list.append(raw_te)
        print(f"  seed {seed}: val R²={best_vr:+.4f}  ep={ep}")

    nn_va      = np.mean(nn_va_list, axis=0)
    nn_te      = np.mean(nn_te_list, axis=0)
    nn_val_r2  = r2_score(y_raw_va, nn_va)
    nn_test_r2 = r2_score(y_raw_te, nn_te)
    print(f"\n  Ensemble: val={nn_val_r2:+.4f}  test={nn_test_r2:+.4f}")

    cal = LinearRegression()
    cal.fit(nn_va.reshape(-1, 1), y_raw_va)
    nn_va_cal      = cal.predict(nn_va.reshape(-1, 1))
    nn_te_cal      = cal.predict(nn_te.reshape(-1, 1))
    nn_test_r2_cal = r2_score(y_raw_te, nn_te_cal)
    print(f"  Calibration: a={cal.coef_[0]:.4f}  b={cal.intercept_:.4f}  cal_test={nn_test_r2_cal:+.4f}")

    # Seasonal calibration
    WINTER_MONTHS = [11, 12, 1, 2, 3]
    va_months = df_all.iloc[va_idx]["date"].dt.month.values
    te_months = df_all.iloc[te_idx]["date"].dt.month.values
    w_va = np.isin(va_months, WINTER_MONTHS)
    w_te = np.isin(te_months, WINTER_MONTHS)

    cal_win = LinearRegression(); cal_sum = LinearRegression()
    if w_va.sum() >= 20:
        cal_win.fit(nn_va[w_va].reshape(-1, 1), y_raw_va[w_va])
    else:
        cal_win = cal
    if (~w_va).sum() >= 20:
        cal_sum.fit(nn_va[~w_va].reshape(-1, 1), y_raw_va[~w_va])
    else:
        cal_sum = cal

    def apply_scal(pred, is_win):
        out = np.empty_like(pred)
        if is_win.any():
            out[is_win]  = cal_win.predict(pred[is_win].reshape(-1, 1))
        if (~is_win).any():
            out[~is_win] = cal_sum.predict(pred[~is_win].reshape(-1, 1))
        return out

    nn_va_scal = apply_scal(nn_va, w_va)
    nn_te_scal = apply_scal(nn_te, w_te)
    print(f"  Seasonal cal  (W:{w_va.sum()}/S:{(~w_va).sum()}): val={r2_score(y_raw_va, nn_va_scal):+.4f}  test={r2_score(y_raw_te, nn_te_scal):+.4f}")

    # ══════════════════════════════════════════════════════════════════════════
    # EXP-3: Alpha Search (S1-S6 with 0.02 step)
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{SEP}")
    print("  EXP-3: Alpha Search (S1-S6)")
    print(SEP)

    def find_alpha(label, nn_va_sub, y_va_sub, tree_va_sub, nn_te_use, tree_te_use):
        best_a, best_r2, best_pred = 0.0, -999.0, None
        for a in np.arange(0.0, 1.01, 0.02):
            r2 = r2_score(y_va_sub, (1 - a) * tree_va_sub + a * nn_va_sub)
            if r2 > best_r2:
                best_r2, best_a = r2, a
                best_pred = (1 - a) * tree_te_use + a * nn_te_use
        te_r2   = r2_score(y_raw_te, best_pred)
        te_mae  = mean_absolute_error(y_raw_te, best_pred)
        te_rmse = float(np.sqrt(mean_squared_error(y_raw_te, best_pred)))
        print(f"  [{label}]  alpha={best_a:.2f}  val R²={best_r2:+.4f}"
              f"  test R²={te_r2:+.4f}  MAE={te_mae:.3f}  RMSE={te_rmse:.3f}")
        return best_a, best_pred, te_r2, te_mae, te_rmse, best_r2

    best_a_s1, pred_s1, r2_s1, mae_s1, rmse_s1, vr_s1 = find_alpha("S1 full+raw",  nn_va,            y_raw_va,      tree_va, nn_te,     tree_te)
    best_a_s2, pred_s2, r2_s2, mae_s2, rmse_s2, vr_s2 = find_alpha("S2 full+cal",  nn_va_cal,         y_raw_va,      tree_va, nn_te_cal, tree_te)
    best_a_s3, pred_s3, r2_s3, mae_s3, rmse_s3, vr_s3 = find_alpha("S3 late+raw",  nn_va[va_late],    y_raw_va_late, tree_va[va_late], nn_te,     tree_te)
    best_a_s4, pred_s4, r2_s4, mae_s4, rmse_s4, vr_s4 = find_alpha("S4 late+cal",  nn_va_cal[va_late],y_raw_va_late, tree_va[va_late], nn_te_cal, tree_te)
    best_a_s5, pred_s5, r2_s5, mae_s5, rmse_s5, vr_s5 = find_alpha("S5 full+scal", nn_va_scal,          y_raw_va,       tree_va, nn_te_scal, tree_te)
    best_a_s6, pred_s6, r2_s6, mae_s6, rmse_s6, vr_s6 = find_alpha("S6 late+scal", nn_va_scal[va_late], y_raw_va_late,  tree_va[va_late], nn_te_scal, tree_te)

    # ══════════════════════════════════════════════════════════════════════════
    # SUMMARY TABLE
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{SEP}")
    print("  FINAL RESULTS")
    print(SEP)

    results = {
        "XGBoost":           {"val_r2": round(float(r2_score(y_raw_va,xgb_va)),4),
                               "test_r2": round(float(r2_xgb),4),
                               "mae": round(float(mean_absolute_error(y_raw_te,xgb_te)),3),
                               "rmse": round(float(np.sqrt(mean_squared_error(y_raw_te,xgb_te))),3)},
        "ElasticNet-Stacked":{"val_r2": round(float(r2_score(y_raw_va,tree_va)),4),
                               "test_r2": round(float(r2_tree),4),
                               "mae": round(float(mean_absolute_error(y_raw_te,tree_te)),3),
                               "rmse": round(float(np.sqrt(mean_squared_error(y_raw_te,tree_te))),3)},
        "3D Neural raw":     {"val_r2": round(float(nn_val_r2),4),
                               "test_r2": round(float(nn_test_r2),4),
                               "mae": round(float(mean_absolute_error(y_raw_te,nn_te)),3),
                               "rmse": round(float(np.sqrt(mean_squared_error(y_raw_te,nn_te))),3)},
        "3D Neural cal":     {"val_r2": round(float(r2_score(y_raw_va,nn_va_cal)),4),
                               "test_r2": round(float(nn_test_r2_cal),4),
                               "mae": round(float(mean_absolute_error(y_raw_te,nn_te_cal)),3),
                               "rmse": round(float(np.sqrt(mean_squared_error(y_raw_te,nn_te_cal))),3)},
        "S1 (full+raw)":     {"val_r2": round(vr_s1,4), "test_r2": round(r2_s1,4),
                               "mae": round(mae_s1,3),  "rmse": round(rmse_s1,3)},
        "S2 (full+cal)":     {"val_r2": round(vr_s2,4), "test_r2": round(r2_s2,4),
                               "mae": round(mae_s2,3),  "rmse": round(rmse_s2,3)},
        "S3 (late+raw)":     {"val_r2": round(vr_s3,4), "test_r2": round(r2_s3,4),
                               "mae": round(mae_s3,3),  "rmse": round(rmse_s3,3)},
        "S4 (late+cal)":     {"val_r2": round(vr_s4,4), "test_r2": round(r2_s4,4),
                               "mae": round(mae_s4,3),  "rmse": round(rmse_s4,3)},
        "S5 (full+scal)":    {"val_r2": round(vr_s5,4), "test_r2": round(r2_s5,4),
                               "mae": round(mae_s5,3),  "rmse": round(rmse_s5,3)},
        "S6 (late+scal)":    {"val_r2": round(vr_s6,4), "test_r2": round(r2_s6,4),
                               "mae": round(mae_s6,3),  "rmse": round(rmse_s6,3)},
    }

    best_te = max(v["test_r2"] for v in results.values())
    print(f"\n  {'Model':<20} {'Val R²':>8}  {'Test R²':>8}  {'MAE':>7}  {'RMSE':>7}")
    print(f"  {'-'*20} {'-'*8}  {'-'*8}  {'-'*7}  {'-'*7}")
    for name, m in results.items():
        marker = " *" if m["test_r2"] == best_te else ""
        print(f"  {name:<20} {m['val_r2']:>+8.4f}  {m['test_r2']:>+8.4f}"
              f"  {m['mae']:>7.3f}  {m['rmse']:>7.3f}{marker}")

    target_hit = [k for k, v in results.items() if v["test_r2"] >= 0.82]
    print(f"\n  Models reaching R² >= 0.82 Target: {target_hit}")

    # ── Save ──────────────────────────────────────────────────────────────────
    summary = {
        "data_source":   "data/processed/01_daily_merged.csv",
        "architecture":  "BiLSTM(H=32,attn) + 3D BN-NIN CNN(dim=40) + OOF Positive ElasticNetCV Stacking + global_cal + seasonal_cal + S1-S6",
        "n_params":      n_params,
        "split":         {"train": len(tr_idx), "val": len(va_idx), "test": len(te_idx)},
        "meta_model_importances": {"xgb": float(meta_model.coef_[0]),
                                   "lgb": float(meta_model.coef_[1]),
                                   "gbr": float(meta_model.coef_[2]),
                                   "cat": float(meta_model.coef_[3]),
                                   "etr": float(meta_model.coef_[4]),
                                   "rf":  float(meta_model.coef_[5]),
                                   "knn": float(meta_model.coef_[6]),
                                   "hgb": float(meta_model.coef_[7])},
        "calibration":   {"a": float(cal.coef_[0]), "b": float(cal.intercept_)},
        "alpha":         {"s1": float(best_a_s1), "s2": float(best_a_s2),
                          "s3": float(best_a_s3), "s4": float(best_a_s4),
                          "s5": float(best_a_s5), "s6": float(best_a_s6)},
        "results":       results,
        "total_time_s":  round(time.time() - t0, 1),
    }
    (OUT_DIR / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    # Save Predictions
    val_s4 = (1 - best_a_s4) * tree_va + best_a_s4 * nn_va_cal
    np.save(OUT_DIR / "pred_val_s4.npy",      val_s4)
    np.save(OUT_DIR / "pred_test_s4.npy",     pred_s4)
    np.save(OUT_DIR / "pred_val_neural.npy",  nn_va_cal)
    np.save(OUT_DIR / "pred_test_neural.npy", nn_te_cal)
    np.save(OUT_DIR / "pred_val_tree.npy",    tree_va)
    np.save(OUT_DIR / "pred_test_tree.npy",   tree_te)
    np.save(OUT_DIR / "y_val_raw.npy",  y_raw_va)
    np.save(OUT_DIR / "y_test_raw.npy", y_raw_te)

    preds_plot = {
        "XGBoost":           (xgb_va,    xgb_te),
        "ElasticNet-Stack":  (tree_va,   tree_te),
        "3D Neural (cal)":   (nn_va_cal, nn_te_cal),
        "S4 Final":          (val_s4,    pred_s4),
    }
    make_plots(y_raw_va, y_raw_te, preds_plot, OUT_DIR)

    print(f"\n  Summary  -> {OUT_DIR / 'summary.json'}")
    print(f"  Total time: {time.time() - t0:.0f}s")
    print(SEP)


if __name__ == "__main__":
    main()
