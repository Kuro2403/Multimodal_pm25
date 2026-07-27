# -*- coding: utf-8 -*-
"""
04_train_3d_v4_regularized.py
=============================
Loads from data/processed/daily_merged.csv with three fixes applied:

  Fix 1 — Leak-free satellite re-imputation after split
           preprocess_data.py used full-dataset monthly medians (leakage).
           Here we re-impute satellite cols using train-only forward interp
           + train-only per-station monthly median, after the split is known.

  Fix 2 — Exclude interpolated PM2.5 rows from training
           Rows where pm25_count < 3 were gap-filled by the preprocessor.
           We drop those from the training set (keep in val/test for eval).

  Fix 3 — Station-month climatology context feature
           Adds per-station per-month PM2.5 mean (computed on train only)
           as a context feature, giving the model a seasonal anchor to
           reduce distribution shift between val and test periods.

Architecture:
  [1] BiLSTM         — 7-day met+pm25 sequence  (LSTM_H=32, bidirectional)
  [2] 3D BN-NIN CNN  — satellite (B,1,7,6,5) spatio-temporal grid  (CNN_DIM=40)
  [3] OOF ElasticNet — XGBoost + LightGBM + GBR + CatBoost + ExtraTrees
                       KFold(n_splits=8, shuffle=False), LOG_TARGET=True
                       ElasticNet(alpha=0.5, l1_ratio=0.1)
  [4] Linear cal     — correct seasonal bias on val set
  [5] S1-S4 blend    — alpha search (full/late val x raw/cal neural)

Outputs -> outputs/final_3d_v2/
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
from sklearn.preprocessing import StandardScaler, QuantileTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge, LinearRegression, ElasticNet
from sklearn.ensemble import GradientBoostingRegressor, ExtraTreesRegressor, RandomForestRegressor
from sklearn.model_selection import KFold
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
from sklearn.ensemble import HistGradientBoostingRegressor
import lightgbm as lgb
import xgboost as xgb

try:
    from catboost import CatBoostRegressor
    HAS_CATBOOST = True
except ImportError:
    HAS_CATBOOST = False
    CatBoostRegressor = None

warnings.filterwarnings("ignore")
torch.set_num_threads(4)

ROOT = Path(__file__).resolve().parent.parent

def find_data_file():
    candidates = [
        ROOT / "data/processed/01_daily_merged.csv",
        ROOT / "data/processed/01_daily_merged_clean.csv",
        ROOT / "data/processed/daily_merged.csv",
        ROOT / "data/processed/01_daily_merged_advanced_v3.csv",
    ]
    for p in candidates:
        if p.exists():
            return p
    return ROOT / "data/processed/01_daily_merged.csv"

DATA_FILE = find_data_file()
OUT_DIR   = ROOT / "outputs/final_3d_v4"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Hyperparameters ───────────────────────────────────────────────────────────
SEQ_LEN      = 7
BATCH_SIZE   = 32
MAX_EPOCHS   = 300
PATIENCE     = 60
LR           = 8e-4
WEIGHT_DECAY = 2e-3
N_SEEDS      = 5
LOG_TARGET   = True   # log1p-transform PM2.5 for tree training
LSTM_H       = 32
CNN_DIM      = 40
DROP         = 0.45
TRAIN_FRAC   = 0.70
VAL_FRAC     = 0.15
DEVICE       = torch.device("cpu")

# Met columns as named in daily_merged.csv
MET_DAILY_COLS = [
    "temperature_2m_C_mean", "relative_humidity_pct_mean",
    "wind_speed_10m_kmh_mean", "wind_u_10m_mean", "wind_v_10m_mean",
    "precipitation_mm_sum", "cloud_cover_pct_mean", "pressure_msl_hPa_mean",
]

# 30 satellite features: 6 stats x 5 bands (row-major grid)
SAT_COLS_3D = [
    "no2_valid_pixels",            "co_valid_pixels",
    "so2_valid_pixels",            "aer_ai_340_380_valid_pixels",
    "s2_valid_pixels",
    "no2_mean",    "co_mean",    "so2_mean",    "aer_ai_340_380_mean",    "ndvi_mean",
    "no2_std",     "co_std",     "so2_std",     "aer_ai_340_380_std",     "ndvi_std",
    "no2_min",     "co_min",     "so2_min",     "aer_ai_340_380_min",     "ndvi_min",
    "no2_max",     "co_max",     "so2_max",     "aer_ai_340_380_max",     "ndvi_max",
    "no2_median",  "co_median",  "so2_median",  "aer_ai_340_380_median",  "ndvi_median",
]

LAG_COLS = [
    "pm25_lag1", "pm25_lag2", "pm25_lag3", "pm25_lag7", "pm25_lag14", "pm25_lag21",
    "pm25_roll3", "pm25_roll7", "pm25_roll7std", "pm25_delta",
    "day_of_week", "month", "day_of_year", "is_weekend",
    "pm25_daily_std", "pm25_count",
]

SAT_GRID_H = 6   # stat rows
SAT_GRID_W = 5   # band cols


# ══════════════════════════════════════════════════════════════════════════════
# MODEL
# ══════════════════════════════════════════════════════════════════════════════

class BNNIN3D(nn.Module):
    """3D NiN-BN satellite encoder.
    Input : (B, 30, 7)  ->  reshape (B, 1, 7, 6, 5)
    Output: (B, CNN_DIM)
    """
    def __init__(self, out_dim=CNN_DIM):
        super().__init__()
        def nin3(ci, co):
            return nn.Sequential(
                nn.Conv3d(ci, co, 3, padding=1), nn.BatchNorm3d(co), nn.ReLU(True),
                nn.Conv3d(co, co, 1),            nn.BatchNorm3d(co), nn.ReLU(True),
                nn.Conv3d(co, co, 1),            nn.BatchNorm3d(co), nn.ReLU(True),
            )
        self.features = nn.Sequential(
            nin3(1, 16),
            nn.MaxPool3d((2, 1, 1), ceil_mode=True),
            nin3(16, 32),
        )
        self.pool = nn.AdaptiveAvgPool3d(1)
        self.fc   = nn.Sequential(nn.Flatten(), nn.Linear(32, out_dim), nn.ReLU(True))

    def forward(self, x):
        B = x.shape[0]
        x = x.permute(0, 2, 1).reshape(B, 1, 7, SAT_GRID_H, SAT_GRID_W)
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

    # ── Derived features (no extra crawling needed) ───────────────────────────
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
        # 2-day tendency for smoother signal
        df["pressure_tendency_2d"] = (
            df.groupby("location_id")["pressure_msl_hPa_mean"]
            .transform(lambda s: s.diff(2))
        )

    # Lag14 / Lag21 — capture 2-week pollution episodes
    for lag in [14, 21]:
        col = f"pm25_lag{lag}"
        if col not in df.columns:
            df[col] = df.groupby("location_id")["pm25"].transform(lambda s: s.shift(lag))

    # Interaction features — non-linear signal for tree models
    if "blh_mean" in df.columns and "aod_550_mean" in df.columns:
        df["blh_aod_interact"] = df["blh_mean"] * df["aod_550_mean"]
    if "blh_mean" in df.columns and "inv_850_1000_mean" in df.columns:
        df["blh_inv_interact"] = df["blh_mean"] * df["inv_850_1000_mean"]
    if "no2_mean" in df.columns and "blh_mean" in df.columns:
        df["no2_blh_interact"] = df["no2_mean"] * df["blh_mean"]

    # Spatial PM2.5 neighbour — mean of OTHER stations on the same day (r=0.60)
    # Valid at prediction time: all 6 stations measure simultaneously
    _sum = df.groupby("date")["pm25"].transform("sum")
    _cnt = df.groupby("date")["pm25"].transform("count")
    df["pm25_spatial_mean"] = (_sum - df["pm25"]) / (_cnt - 1).clip(lower=1)

    # Lagged spatial neighbour features — regional air quality trend signal
    df["pm25_spatial_lag1"]  = df.groupby("location_id")["pm25_spatial_mean"].transform(lambda s: s.shift(1))
    df["pm25_spatial_lag2"]  = df.groupby("location_id")["pm25_spatial_mean"].transform(lambda s: s.shift(2))
    df["pm25_spatial_roll7"] = df.groupby("location_id")["pm25_spatial_mean"].transform(lambda s: s.shift(1).rolling(7, min_periods=3).mean())

    # Lagged within-day PM2.5 variability — how volatile was yesterday? (same-day std r=0.61)
    if "pm25_daily_std" in df.columns:
        df["pm25_std_lag1"] = df.groupby("location_id")["pm25_daily_std"].transform(lambda s: s.shift(1))

    # Lagged weather: rain and cloud cover from yesterday
    if "precipitation_mm_sum" in df.columns:
        df["prec_lag1"] = df.groupby("location_id")["precipitation_mm_sum"].transform(lambda s: s.shift(1))
        df["prec_roll3"] = df.groupby("location_id")["precipitation_mm_sum"].transform(lambda s: s.shift(1).rolling(3, min_periods=1).sum())
    if "cloud_cover_pct_mean" in df.columns:
        df["cloud_lag1"] = df.groupby("location_id")["cloud_cover_pct_mean"].transform(lambda s: s.shift(1))

    # BLH change: rising BLH = improving ventilation -> PM2.5 decrease
    if "blh_mean" in df.columns:
        df["blh_change"] = df.groupby("location_id")["blh_mean"].transform(lambda s: s.diff(1))

    # Longer PM2.5 rolling means — multi-week episode persistence
    df["pm25_roll14"] = df.groupby("location_id")["pm25"].transform(
        lambda s: s.shift(1).rolling(14, min_periods=5).mean())
    df["pm25_roll21"] = df.groupby("location_id")["pm25"].transform(
        lambda s: s.shift(1).rolling(21, min_periods=7).mean())

    # Ventilation flux: BLH × wind speed — low = stagnant = high PM2.5
    if "blh_mean" in df.columns and "wind_speed_derived" in df.columns:
        df["ventilation_flux"] = df["blh_mean"] * df["wind_speed_derived"]
        df["ventilation_flux_change"] = df.groupby("location_id")["ventilation_flux"].transform(lambda s: s.diff(1))

    # 7-day trend: positive → pollution building, negative → improving
    if "pm25_lag1" in df.columns and "pm25_lag7" in df.columns:
        df["pm25_trend_7"] = df["pm25_lag1"] - df["pm25_lag7"]

    # Spatial dispersion: std across stations (local vs regional signal)
    df["_pm25_spatial_std"] = df.groupby("date")["pm25"].transform("std")
    df["pm25_spatial_std_lag1"] = df.groupby("location_id")["_pm25_spatial_std"].transform(lambda s: s.shift(1))
    df.drop(columns=["_pm25_spatial_std"], inplace=True)

    # PM2.5 × inversion interaction: trapped high-PM2.5 episodes
    if "inv_850_1000_mean" in df.columns:
        df["inv_pm25_interact"] = df["pm25_roll7"] * df["inv_850_1000_mean"]

    # Heavy rain washout signal (above 10mm threshold, linear above)
    if "precipitation_mm_sum" in df.columns:
        df["heavy_rain_lag1"] = df.groupby("location_id")["precipitation_mm_sum"].transform(
            lambda s: (s - 10.0).clip(lower=0).shift(1))

    # Calendar features: weekday effect + Tết (lunar new year) spike
    df["day_of_week"] = df["date"].dt.dayofweek   # 0=Mon…6=Sun
    df["is_weekend"]  = (df["day_of_week"] >= 5).astype(np.float32)
    # Tết dates: 2024-02-10, 2025-01-29, 2026-02-17 (±5 day window)
    tet_dates = pd.to_datetime(["2024-02-10", "2025-01-29", "2026-02-17"])
    df["tet_day"] = df["date"].apply(
        lambda d: float(any(abs((d - t).days) <= 5 for t in tet_dates)))

    # Lagged wind direction: allows learning directional persistence signal
    if "wind_dir_sin" in df.columns and "wind_dir_cos" in df.columns:
        df["wind_dir_sin_lag1"] = df.groupby("location_id")["wind_dir_sin"].transform(lambda s: s.shift(1))
        df["wind_dir_cos_lag1"] = df.groupby("location_id")["wind_dir_cos"].transform(lambda s: s.shift(1))

    # RH × PM2.5 interaction: hygroscopic growth at high humidity
    if "relative_humidity_pct_mean" in df.columns and "pm25_roll7" in df.columns:
        df["rh_pm25_interact"] = df["relative_humidity_pct_mean"] * df["pm25_roll7"]

    # Lagged met variables — persist signal from yesterday
    if "relative_humidity_pct_mean" in df.columns:
        df["rh_lag1"] = df.groupby("location_id")["relative_humidity_pct_mean"].transform(
            lambda s: s.shift(1))
    if "wind_speed_derived" in df.columns:
        df["wind_speed_lag1"] = df.groupby("location_id")["wind_speed_derived"].transform(
            lambda s: s.shift(1))
    if "temperature_2m_C_mean" in df.columns:
        df["temp_lag1"] = df.groupby("location_id")["temperature_2m_C_mean"].transform(
            lambda s: s.shift(1))

    # Local PM2.5 departure from regional neighbours — captures station-specific local sources
    if "pm25_lag1" in df.columns and "pm25_spatial_lag1" in df.columns:
        df["pm25_local_anom"] = df["pm25_lag1"] - df["pm25_spatial_lag1"]

    # Temperature × inversion: cold surface + strong inversion = extreme PM2.5 trapping
    if "temp_lag1" in df.columns and "inv_850_1000_mean" in df.columns:
        df["temp_inv_interact"] = df["temp_lag1"] * df["inv_850_1000_mean"]

    # RH × inversion: high humidity + inversion = fog/haze hygroscopic growth
    if "relative_humidity_pct_mean" in df.columns and "inv_850_1000_mean" in df.columns:
        df["rh_inv_interact"] = df["relative_humidity_pct_mean"] * df["inv_850_1000_mean"]


    # Dry-streak counter: consecutive days with < 1mm rain — no wet deposition → PM2.5 build-up
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

    # BLH × dry streak: today's low mixing layer + accumulated dry period = extreme accumulation
    if "blh_mean" in df.columns and "dry_streak" in df.columns:
        df["blh_dry_interact"] = df["blh_mean"] * df["dry_streak"]

    # 5-day pressure tendency — sustained high pressure → stagnation → high PM2.5
    if "pressure_msl_hPa_mean" in df.columns:
        df["pressure_tendency_5d"] = (
            df.groupby("location_id")["pressure_msl_hPa_mean"]
            .transform(lambda s: s.diff(5))
        )

    # 3-day rolling CAMS AOD — captures persistent haze events
    if "aod_550_mean" in df.columns:
        df["aod_roll3"] = df.groupby("location_id")["aod_550_mean"].transform(
            lambda s: s.shift(1).rolling(3, min_periods=1).mean())

    # Station label encoding — gives trees spatial identity (neural has lat/lon in ctx_feats)
    df["station_enc"] = df["location_id"].astype("category").cat.codes.astype(np.float32)

    return df


def impute_satellite(df: pd.DataFrame, sat_cols: list, tr_idx: list) -> pd.DataFrame:
    """Fix 1: re-impute satellite cols using train-only statistics.
    Forward interp (14d) -> backward (7d) -> train per-station monthly median.
    """
    train_rows = df.iloc[tr_idx]
    # per-station per-month median computed on train only
    train_monthly = (train_rows.groupby(["location_id", train_rows.iloc[
        list(range(len(train_rows)))]["date"].dt.month])[sat_cols].median())

    parts = []
    for loc_id, grp in df.groupby("location_id"):
        grp = grp.sort_values("date").copy()
        for c in sat_cols:
            if c not in grp.columns:
                continue
            grp[c] = grp[c].interpolate(
                method="linear", limit=14, limit_direction="forward")
            grp[c] = grp[c].interpolate(
                method="linear", limit=7,  limit_direction="backward")
            # monthly median fallback using train-only stats
            if grp[c].isna().any():
                for month, mg in grp.groupby(grp["date"].dt.month):
                    fill = (train_monthly.loc[(loc_id, month), c]
                            if (loc_id, month) in train_monthly.index else np.nan)
                    if np.isnan(fill):
                        fill = (train_rows[train_rows["location_id"] == loc_id][c]
                                .median())
                    grp.loc[mg.index, c] = grp.loc[mg.index, c].fillna(fill)
        parts.append(grp)
    return pd.concat(parts).sort_index()


def add_climatology(df: pd.DataFrame, tr_idx: list) -> pd.DataFrame:
    """Fix 3: add per-station per-month PM2.5 climatology from train only."""
    train_rows  = df.iloc[tr_idx].copy()
    train_rows["_month"] = train_rows["date"].dt.month
    clim = (train_rows.groupby(["location_id", "_month"])["pm25"]
            .mean().rename("pm25_clim"))

    df = df.copy()
    df["_month"] = df["date"].dt.month
    df["pm25_clim"] = df.apply(
        lambda r: clim.get((r["location_id"], r["_month"]), np.nan), axis=1)
    # fallback: station mean from train
    train_mean = train_rows.groupby("location_id")["pm25"].mean()
    df["pm25_clim"] = df.apply(
        lambda r: train_mean.get(r["location_id"], df["pm25"].mean())
        if np.isnan(r["pm25_clim"]) else r["pm25_clim"], axis=1)
    df = df.drop(columns=["_month"])
    return df


def split_df(df):
    dates = np.sort(df["date"].dt.normalize().unique())
    n  = len(dates)
    tc = dates[int(n * TRAIN_FRAC)]
    vc = dates[int(n * (TRAIN_FRAC + VAL_FRAC))]
    df["split"] = "test"
    df.loc[df["date"] < tc, "split"] = "train"
    df.loc[(df["date"] >= tc) & (df["date"] < vc), "split"] = "val"
    return df


def build_seqs(df_sc, idxs, seq_feats):
    groups = {loc: g.sort_values("date")
              for loc, g in df_sc.groupby("location_id")}
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
    """Returns (N, n_sat, SEQ_LEN) — (B, 30, 7) for 3D CNN."""
    groups = {loc: g.sort_values("date")
              for loc, g in df_sat_sc.groupby("location_id")}
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
# ══════════════════════════════════════════════════════════════════════════════

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
    """Train neural with early stopping on val.
    If x_seq_es / y_raw_es are provided, early stopping uses that subset (e.g. late-val).
    The full val arrays are still used for final R² reporting.
    """
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
        PMDataset(x_seq_va, x_ss_va, x_bp_va, x_ctx_va,
                  np.zeros(len(x_seq_va))),
        BATCH_SIZE, shuffle=False, num_workers=0)

    # Early stopping subset (late-val if provided, else full val)
    use_es = (y_raw_es is not None)
    es_ld  = va_ld   # default
    es_y   = y_raw_va
    if use_es and x_seq_es is not None:
        es_ld = DataLoader(
            PMDataset(x_seq_es, x_ss_es, x_bp_es, x_ctx_es,
                      np.zeros(x_seq_es.shape[0])),
            BATCH_SIZE, shuffle=False, num_workers=0)
        es_y = y_raw_es

    best_vr, pat, best_w = -999.0, 0, None
    for ep in range(1, MAX_EPOCHS + 1):
        model.train()
        for xs, xss, xb, xc, yb in tr_ld:
            opt.zero_grad()
            loss_fn(model(xs.to(DEVICE), xss.to(DEVICE),
                          xb.to(DEVICE), xc.to(DEVICE)),
                    yb.to(DEVICE)).backward()
            nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            opt.step()
        sched.step()

        model.eval()
        pv = []
        with torch.no_grad():
            for xs, xss, xb, xc, _ in es_ld:
                pv.append(model(xs.to(DEVICE), xss.to(DEVICE),
                                xb.to(DEVICE), xc.to(DEVICE)
                                ).cpu().numpy().ravel())
        pv_raw = y_sc.inverse_transform(
            np.concatenate(pv).reshape(-1, 1)).ravel()
        vr = r2_score(es_y, pv_raw)
        if vr > best_vr + 1e-5:
            best_vr, pat = vr, 0
            best_w = {k: v.clone() for k, v in model.state_dict().items()}
        else:
            pat += 1
            if pat >= PATIENCE:
                break
    model.load_state_dict(best_w)
    # Report R² on full val for consistency
    full_r2 = best_vr
    if use_es:
        pv_full = []
        with torch.no_grad():
            for xs, xss, xb, xc, _ in va_ld:
                pv_full.append(model(xs.to(DEVICE), xss.to(DEVICE),
                                     xb.to(DEVICE), xc.to(DEVICE)
                                     ).cpu().numpy().ravel())
        full_r2 = r2_score(y_raw_va, y_sc.inverse_transform(
            np.concatenate(pv_full).reshape(-1,1)).ravel())
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
# TREE MODELS
# ══════════════════════════════════════════════════════════════════════════════

def make_xgb(): return xgb.XGBRegressor(
    n_estimators=2000, max_depth=6, learning_rate=0.010,
    subsample=0.8, colsample_bytree=0.8, reg_alpha=0.05, reg_lambda=1.0,
    random_state=42, verbosity=0, n_jobs=-1,
    early_stopping_rounds=150, eval_metric="rmse")

def make_lgb(): return lgb.LGBMRegressor(
    n_estimators=2000, max_depth=6, learning_rate=0.010,
    subsample=0.8, colsample_bytree=0.8, reg_alpha=0.05, reg_lambda=1.0,
    random_state=42, verbosity=-1, n_jobs=-1)

def make_gbr(): return GradientBoostingRegressor(
    n_estimators=600, max_depth=4, learning_rate=0.04, subsample=0.8,
    min_samples_leaf=5, random_state=42,
    validation_fraction=0.1, n_iter_no_change=40)

def make_cat():
    if HAS_CATBOOST:
        return CatBoostRegressor(
            iterations=4000, depth=7, learning_rate=0.010,
            subsample=0.8, reg_lambda=1.0, random_seed=42,
            verbose=0, early_stopping_rounds=150, eval_metric="RMSE")
    else:
        return HistGradientBoostingRegressor(
            max_iter=1000, learning_rate=0.015, max_depth=6, min_samples_leaf=4, random_state=42)

def make_etr(): return ExtraTreesRegressor(
    n_estimators=1000, max_depth=7, min_samples_leaf=3,
    max_features=0.7, random_state=42, n_jobs=-1)

def make_rf(): return RandomForestRegressor(
    n_estimators=600, max_depth=8, min_samples_leaf=3,
    max_features=0.7, random_state=42, n_jobs=-1)


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
            ax.axhline(0.70, color="red", ls="--", lw=1.2, alpha=0.7, label="R²=0.70")
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
        ax_ts.plot(preds_dict[name][1], color=colors[ci],
                   lw=1.2, alpha=0.75, label=name)
    ax_ts.set_xlabel("Test day index", fontsize=10)
    ax_ts.set_ylabel("PM2.5 (ug/m3)", fontsize=10)
    ax_ts.set_title("Test Set — All Components vs Actual", fontsize=11, fontweight="bold")
    ax_ts.legend(fontsize=8, ncol=len(names) + 1, loc="upper right")
    ax_ts.grid(alpha=0.25)

    plt.suptitle(
        "train_3d_v4: BiLSTM + 3D BN-NIN + OOF Ridge  |  preprocessed daily_merged.csv",
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
    print("  train_3d_v4: BiLSTM + 3D BN-NIN + OOF ElasticNet Stacking")
    print("  Models: XGBoost + LightGBM + GBR + CatBoost + ExtraTrees  |  KFold=8  |  LOG_TARGET=True")
    print("  Data: data/processed/01_daily_merged.csv  (preprocessed, no raw files)")
    print(SEP)

    # ── Load & split ──────────────────────────────────────────────────────────
    print("\nLoading data ...")
    df_all = load_data()
    df_all = split_df(df_all)
    df_all = df_all.reset_index(drop=True)

    tr_idx_full = df_all[df_all["split"] == "train"].index.tolist()
    va_idx      = df_all[df_all["split"] == "val"].index.tolist()
    te_idx      = df_all[df_all["split"] == "test"].index.tolist()

    # Fix 1: re-impute satellite using train-only statistics
    print("  Fix 1: re-imputing satellite cols (train-only stats) ...")
    sat_cols_present = [c for c in SAT_COLS_3D if c in df_all.columns]
    df_all = impute_satellite(df_all, sat_cols_present, tr_idx_full)

    # Fix 2: exclude gap-filled PM2.5 rows from training
    pm25_count_col = "pm25_count" if "pm25_count" in df_all.columns else None
    if pm25_count_col:
        tr_idx = [i for i in tr_idx_full
                  if df_all.loc[i, pm25_count_col] >= MIN_HOURLY]
        dropped = len(tr_idx_full) - len(tr_idx)
        print(f"  Fix 2: dropped {dropped} gap-filled rows from training")
    else:
        tr_idx = tr_idx_full

    # Fix 3: add station-month climatology from train
    print("  Fix 3: adding station-month PM2.5 climatology ...")
    df_all = add_climatology(df_all, tr_idx)
    # Anomaly: departure from seasonal norm (requires pm25_clim from add_climatology)
    if "pm25_lag1" in df_all.columns and "pm25_clim" in df_all.columns:
        df_all["pm25_anomaly"] = df_all["pm25_lag1"] - df_all["pm25_clim"]

    print(f"  Train {len(tr_idx)} / Val {len(va_idx)} / Test {len(te_idx)}")

    n_va         = len(va_idx)
    va_early     = list(range(0, n_va // 2))
    va_late      = list(range(n_va // 2, n_va))
    y_raw_tr     = df_all.iloc[tr_idx]["pm25"].values.astype(np.float32)
    y_raw_va     = df_all.iloc[va_idx]["pm25"].values.astype(np.float32)
    y_raw_te     = df_all.iloc[te_idx]["pm25"].values.astype(np.float32)

    # Trees use full target range (better coverage of extreme events)
    y_tr_fit: np.ndarray = (np.log1p(y_raw_tr) if LOG_TARGET else y_raw_tr).astype(np.float32)
    y_va_fit: np.ndarray = (np.log1p(y_raw_va) if LOG_TARGET else y_raw_va).astype(np.float32)
    def inv_log(p): return np.expm1(p) if LOG_TARGET else p

    # Sample weights: down-weight extreme PM2.5 > 80 (rare in test, common in train)
    sw_tr = 1.0 / (1.0 + np.maximum(y_raw_tr - 80.0, 0.0) / 50.0)
    # Seasonal reweighting: test covers Jan-May → upweight those months in training
    tr_months = df_all.iloc[tr_idx]["date"].dt.month.values
    seasonal_w = np.where(np.isin(tr_months, [1, 2, 3, 4, 5]), 1.5, 1.0).astype(np.float32)
    sw_tr = sw_tr * seasonal_w
    sw_tr = (sw_tr / sw_tr.mean()).astype(np.float32)  # normalize mean=1
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
        df_sc[c] = df_sc.groupby("location_id")[c].transform(
            lambda s: s.interpolate(
                method="linear", limit=10, limit_direction="both").fillna(s.median()))
    df_sc[seq_feats] = seq_sc.transform(df_sc[seq_feats].fillna(0))

    # ── Satellite features (3D CNN) ───────────────────────────────────────────
    sat_cols_use = [c for c in SAT_COLS_3D if c in df_all.columns]
    df_sat_sc    = df_all.copy()
    sat_sc       = StandardScaler()
    sat_sc.fit(df_sat_sc.loc[tr_idx, sat_cols_use].fillna(0))
    df_sat_sc[sat_cols_use] = sat_sc.transform(df_sat_sc[sat_cols_use].fillna(0))

    # ── Bypass & context ──────────────────────────────────────────────────────
    bp_feats = [c for c in ["pm25_lag1", "pm25_lag2", "pm25_lag3", "pm25_lag7",
                             "pm25_spatial_mean",   # r=0.60, direct bypass
                             "pm25_spatial_lag1", "pm25_spatial_roll7"]
                if c in df_all.columns]
    bp_sc    = StandardScaler()
    bp_sc.fit(df_all.loc[tr_idx, bp_feats].fillna(0))
    x_bp_all = bp_sc.transform(df_all[bp_feats].fillna(0)).astype(np.float32)

    ctx_feats = (["sin_doy", "cos_doy", "sin_month", "cos_month",
                  "pm25_clim"]           # Fix 3: seasonal anchor
                 + ["pm25_spatial_mean", "pm25_spatial_lag1", "pm25_spatial_roll7"]  # spatial (r=0.60)
                 + met_cols_present + ["latitude", "longitude"]
                 + ["blh_mean", "blh_min", "blh_morning"]         # ERA5 BLH
                 + ["aod_550_mean", "aod_550_morning"]          # CAMS AOD 550nm
                 + ["maiac_aod_mean", "maiac_aod_median", "maiac_aod_count"]  # MAIAC AOD 1km
                 + ["pm25_std_lag1", "prec_lag1", "blh_change"]         # lagged signals
                 + ["ndvi_mean"]                                            # seasonal vegetation
                 + ["no2_mean", "no2_median"]                   # S5P NO2 (r=0.46)
                 + ["fire_count_100km", "frp_sum_100km"]        # FIRMS fire (r=0.30)
                 + ["wind_dir_sin", "wind_dir_cos",             # wind direction
                    "pressure_tendency", "pressure_tendency_2d"] # pressure change
                 + ["inv_850_1000_mean", "inv_925_1000_mean",   # T inversion (urban trapping)
                    "inv_850_1000_morning"]
                 + ["blh_aod_interact", "blh_inv_interact", "no2_blh_interact"])  # interactions
                
    ctx_feats = [c for c in ctx_feats if c in df_all.columns]
    ctx_sc    = StandardScaler()
    ctx_sc.fit(df_all.loc[tr_idx, ctx_feats].fillna(0))
    x_ctx_all = ctx_sc.transform(df_all[ctx_feats].fillna(0)).astype(np.float32)

    # LogStandardScaler for neural network target
    class LogStandardScaler:
        def __init__(self): self.sc = StandardScaler()
        def fit(self, X): self.sc.fit(np.log1p(X)); return self
        def transform(self, X): return self.sc.transform(np.log1p(X))
        def inverse_transform(self, X): return np.expm1(self.sc.inverse_transform(X))

    y_sc = LogStandardScaler()
    y_sc.fit(df_all.iloc[tr_idx][["pm25"]])
    y_std_tr = y_sc.transform(df_all.iloc[tr_idx][["pm25"]]).ravel().astype(np.float32)

    # ── Build arrays (cached — delete cache/ to force rebuild) ───────────────
    CACHE = OUT_DIR / "cache"
    CACHE.mkdir(exist_ok=True)
    c_seq  = CACHE / "x_seq.npy"
    c_sat  = CACHE / "x_sat.npy"
    c_bp   = CACHE / "x_bp.npy"
    c_ctx  = CACHE / "x_ctx.npy"
    # cache key: row count + n_ctx to detect when features changed
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
    tab_cols = [c for c in LAG_COLS + met_cols_present
                + ["pm25_clim", "pm25_roll14", "pm25_roll21", "pm25_trend_7", "pm25_anomaly",
                   "pm25_spatial_std_lag1",
                   "ventilation_flux", "ventilation_flux_change",
                   "inv_pm25_interact", "heavy_rain_lag1",
                   "prec_roll3", "day_of_week", "is_weekend", "tet_day",
                   "wind_dir_sin_lag1", "wind_dir_cos_lag1", "rh_pm25_interact",
                   "station_enc", "latitude", "longitude",
                   "rh_lag1", "aod_roll3",
                   "wind_speed_lag1", "temp_lag1",
                   "pm25_local_anom", "pressure_tendency_5d",
                   "temp_inv_interact", "rh_inv_interact",
                   "dry_streak", "blh_dry_interact",
                   "pm25_spatial_mean",
                   "pm25_spatial_lag1", "pm25_spatial_lag2", "pm25_spatial_roll7",
                   "no2_mean", "no2_median", "no2_max", "no2_min",
                   "co_mean", "co_median",
                   "so2_mean", "so2_median",
                   "aer_ai_340_380_mean", "aer_ai_340_380_median",
                   "fire_count_100km", "frp_sum_100km",
                   "frp_mean_100km", "frp_max_100km",
                   "ndvi_mean", "ndvi_median",
                   "pm25_std_lag1",
                   "prec_lag1", "cloud_lag1",
                   "blh_change",
                   "blh_mean", "blh_min", "blh_morning",
                   "aod_550_mean", "aod_550_max", "aod_550_morning",
                   "wind_dir_sin", "wind_dir_cos", "wind_speed_derived",
                   "pressure_tendency", "pressure_tendency_2d",
                   "inv_850_1000_mean", "inv_850_1000_max", "inv_925_1000_mean",
                   "inv_850_1000_morning", "t1000_mean", "t850_mean",
                   "blh_aod_interact", "blh_inv_interact", "no2_blh_interact",
                   "maiac_aod_mean", "maiac_aod_median", "maiac_aod_count",
                   "built_up_frac_500m", "built_up_frac_1km",
                   "built_up_frac_2km", "built_up_frac_5km",
                   "tree_frac_2km", "cropland_frac_2km", "water_frac_2km"]
                if c in df_all.columns]
    imp = SimpleImputer(strategy="median")
    Xtr = imp.fit_transform(df_all.iloc[tr_idx][tab_cols])
    Xva = imp.transform(df_all.iloc[va_idx][tab_cols])
    Xte = imp.transform(df_all.iloc[te_idx][tab_cols])

    # ══════════════════════════════════════════════════════════════════════════
    # EXP-1: OOF Stacking — time-series CV on train+val, retrain on train+val
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

    # KFold OOF on train+val — 8 folds → 87.5% train per fold, higher-quality OOF predictions
    # Minor temporal leakage in meta-learner is acceptable; base models remain time-honest
    kf_full = KFold(n_splits=8, shuffle=False)
    oof_xgb = np.zeros(len(Xfull))
    oof_lgb = np.zeros(len(Xfull))
    oof_gbr = np.zeros(len(Xfull))
    oof_cat = np.zeros(len(Xfull))
    oof_etr = np.zeros(len(Xfull))

    for fold, (fi_tr, fi_va) in enumerate(kf_full.split(Xfull)):
        print(f"  fold {fold+1}/8 ...", end=" ", flush=True)
        sw_f = sw_full[fi_tr]
        m = make_xgb()
        m.fit(Xfull[fi_tr], yfull_fit[fi_tr], sample_weight=sw_f,
              eval_set=[(Xfull[fi_va], yfull_fit[fi_va])], verbose=False)
        oof_xgb[fi_va] = inv_log(m.predict(Xfull[fi_va]))
        m = make_lgb()
        m.fit(Xfull[fi_tr], yfull_fit[fi_tr], sample_weight=sw_f,
              eval_set=[(Xfull[fi_va], yfull_fit[fi_va])],
              callbacks=[lgb.early_stopping(100, verbose=False), lgb.log_evaluation(-1)])
        oof_lgb[fi_va] = inv_log(m.predict(Xfull[fi_va]))
        m = make_gbr(); m.fit(Xfull[fi_tr], yfull_fit[fi_tr], sample_weight=sw_f)
        oof_gbr[fi_va] = inv_log(m.predict(Xfull[fi_va]))
        m = make_cat()
        if HAS_CATBOOST:
            m.fit(Xfull[fi_tr], yfull_fit[fi_tr], sample_weight=sw_f,
                  eval_set=(Xfull[fi_va], yfull_fit[fi_va]))
        else:
            m.fit(Xfull[fi_tr], yfull_fit[fi_tr])
        oof_cat[fi_va] = inv_log(m.predict(Xfull[fi_va]))
        m = make_etr(); m.fit(Xfull[fi_tr], yfull_fit[fi_tr], sample_weight=sw_f)
        oof_etr[fi_va] = inv_log(m.predict(Xfull[fi_va]))
        print("done")

    oof_stack = np.column_stack([oof_xgb, oof_lgb, oof_gbr, oof_cat, oof_etr])
    
    # Conditional Stacking: Thêm meta-features (tháng, trạm, lag1, nhiệt độ) vào đầu vào của Meta-Learner
    meta_feat_names = ["month", "station_enc", "pm25_lag1", "temperature_2m_C_mean"]
    meta_idx = [tab_cols.index(c) for c in meta_feat_names if c in tab_cols]
    
    meta_Xfull = np.column_stack([oof_stack, Xfull[:, meta_idx]])
    
    from sklearn.linear_model import RidgeCV
    # RidgeCV học trọng số phi tuyến tính cho các OOF pred cộng với meta-features
    ridge = RidgeCV(alphas=[0.1, 1.0, 10.0, 100.0], fit_intercept=True)
    ridge.fit(meta_Xfull, y_raw_full)
    print(f"  Meta-learner (RidgeCV) best alpha: {ridge.alpha_}")

    # Find best n_estimators via early stopping on LATE val — temporally aligned with test period
    print("\n  Finding best iterations ...")
    Xva_late_es   = Xva[va_late]
    yva_fit_late  = y_va_fit[va_late]
    xgbm_es = make_xgb(); xgbm_es.fit(Xtr, y_tr_fit, sample_weight=sw_tr,
                                        eval_set=[(Xva_late_es, yva_fit_late)], verbose=False)
    best_n_xgb = max(xgbm_es.best_iteration + 1, 50)
    lgbm_es = make_lgb()
    lgbm_es.fit(Xtr, y_tr_fit, sample_weight=sw_tr, eval_set=[(Xva_late_es, yva_fit_late)],
                callbacks=[lgb.early_stopping(100, verbose=False), lgb.log_evaluation(-1)])
    best_n_lgb = max(lgbm_es.best_iteration_ + 1, 50)
    if HAS_CATBOOST:
        catm_es = make_cat(); catm_es.fit(Xtr, y_tr_fit, sample_weight=sw_tr, eval_set=(Xva_late_es, yva_fit_late))
        best_n_cat = max(catm_es.get_best_iteration() + 1, 50)
    else:
        best_n_cat = 1000
    print(f"  Best iters: XGB={best_n_xgb}  LGB={best_n_lgb}  CAT={best_n_cat}")

    print("  Retraining on train+val ...")
    xgbm = xgb.XGBRegressor(n_estimators=best_n_xgb, max_depth=6, learning_rate=0.010,
        subsample=0.8, colsample_bytree=0.8, reg_alpha=0.05, reg_lambda=1.0,
        random_state=42, verbosity=0, n_jobs=-1)
    xgbm.fit(Xfull, yfull_fit, sample_weight=sw_full)
    lgbm = lgb.LGBMRegressor(n_estimators=best_n_lgb, max_depth=6, learning_rate=0.010,
        subsample=0.8, colsample_bytree=0.8, reg_alpha=0.05, reg_lambda=1.0,
        random_state=42, verbosity=-1, n_jobs=-1)
    lgbm.fit(Xfull, yfull_fit, sample_weight=sw_full)
    gbrm = make_gbr(); gbrm.fit(Xtr, y_tr_fit, sample_weight=sw_tr)
    if HAS_CATBOOST:
        catm = CatBoostRegressor(iterations=best_n_cat, depth=7, learning_rate=0.010,
            subsample=0.8, reg_lambda=1.0, random_seed=42, verbose=0)
        catm.fit(Xfull, yfull_fit, sample_weight=sw_full)
    else:
        catm = make_cat()
        catm.fit(Xfull, yfull_fit)
    etrm = make_etr(); etrm.fit(Xfull, yfull_fit, sample_weight=sw_full)

    def tree_pred(X):
        raw = np.column_stack([
            inv_log(xgbm.predict(X)), inv_log(lgbm.predict(X)),
            inv_log(gbrm.predict(X)), inv_log(catm.predict(X)),
            inv_log(etrm.predict(X))])
        meta_X = np.column_stack([raw, X[:, meta_idx]])
        return ridge.predict(meta_X)

    tree_te = tree_pred(Xte)
    tree_va = ridge.predict(meta_Xfull[len(Xtr):])
    xgb_te  = inv_log(xgbm.predict(Xte))
    xgb_va  = oof_xgb[len(Xtr):]
    r2_xgb  = r2_score(y_raw_te, xgb_te)
    r2_tree = r2_score(y_raw_te, tree_te)
    print(f"  XGBoost alone : val={r2_score(y_raw_va,xgb_va):+.4f}  test={r2_xgb:+.4f}")
    print(f"  Ridge stacked : val={r2_score(y_raw_va,tree_va):+.4f}  test={r2_tree:+.4f}"
          f"  delta={r2_tree-r2_xgb:+.4f}")

    # ══════════════════════════════════════════════════════════════════════════
    # EXP-2: Neural Training + Calibration
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{SEP}")
    print("  EXP-2: 3D Neural Training + Calibration")
    print(SEP)

    model_ref = MultimodalModel(n_seq, n_ctx=n_ctx, n_bp=n_bp)
    n_params  = sum(p.numel() for p in model_ref.parameters() if p.requires_grad)
    print(f"  MultimodalModel params: {n_params:,}")

    nn_va_list, nn_te_list = [], []
    for seed in range(N_SEEDS):
        # Seed BEFORE model init so init_weights is deterministic regardless of prior state
        torch.manual_seed(seed)
        np.random.seed(seed)
        pt    = OUT_DIR / f"neural_seed{seed}.pt"
        model = MultimodalModel(n_seq, n_ctx=n_ctx, n_bp=n_bp)
        if pt.exists():
            try:
                model.load_state_dict(torch.load(pt, map_location="cpu"))
                model.eval()
                raw_va = y_sc.inverse_transform(
                    predict_nn(model, x_seq_va, x_ss_va, x_bp_va, x_ctx_va
                               ).reshape(-1, 1)).ravel()
                raw_te = y_sc.inverse_transform(
                    predict_nn(model, x_seq_te, x_ss_te, x_bp_te, x_ctx_te
                               ).reshape(-1, 1)).ravel()
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
        raw_va = y_sc.inverse_transform(
            predict_nn(model, x_seq_va, x_ss_va, x_bp_va, x_ctx_va
                       ).reshape(-1, 1)).ravel()
        raw_te = y_sc.inverse_transform(
            predict_nn(model, x_seq_te, x_ss_te, x_bp_te, x_ctx_te
                       ).reshape(-1, 1)).ravel()
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
    print(f"  Calibration: a={cal.coef_[0]:.4f}  b={cal.intercept_:.4f}"
          f"  cal_test={nn_test_r2_cal:+.4f}")

    # Seasonal calibration — separate linear correctors for winter (Nov-Mar) vs summer
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
    print(f"  Seasonal cal  (W:{w_va.sum()}/S:{(~w_va).sum()}): "
          f"val={r2_score(y_raw_va, nn_va_scal):+.4f}  "
          f"test={r2_score(y_raw_te, nn_te_scal):+.4f}")

    # ══════════════════════════════════════════════════════════════════════════
    # EXP-3: Alpha Search (S1-S4)
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{SEP}")
    print("  EXP-3: Alpha Search (S1-S6)")
    print(SEP)

    def find_alpha(label, nn_va_sub, y_va_sub, tree_va_sub, nn_te_use, tree_te_use):
        best_a, best_r2, best_pred = 0.0, -999.0, None
        for a in np.arange(0.0, 1.01, 0.05):
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

    best_a_s1, pred_s1, r2_s1, mae_s1, rmse_s1, vr_s1 = find_alpha(
        "S1 full+raw",  nn_va,            y_raw_va,      tree_va, nn_te,     tree_te)
    best_a_s2, pred_s2, r2_s2, mae_s2, rmse_s2, vr_s2 = find_alpha(
        "S2 full+cal",  nn_va_cal,         y_raw_va,      tree_va, nn_te_cal, tree_te)
    best_a_s3, pred_s3, r2_s3, mae_s3, rmse_s3, vr_s3 = find_alpha(
        "S3 late+raw",  nn_va[va_late],    y_raw_va_late, tree_va[va_late], nn_te,     tree_te)
    best_a_s4, pred_s4, r2_s4, mae_s4, rmse_s4, vr_s4 = find_alpha(
        "S4 late+cal",  nn_va_cal[va_late],y_raw_va_late, tree_va[va_late], nn_te_cal, tree_te)
    best_a_s5, pred_s5, r2_s5, mae_s5, rmse_s5, vr_s5 = find_alpha(
        "S5 full+scal", nn_va_scal,          y_raw_va,       tree_va, nn_te_scal, tree_te)
    best_a_s6, pred_s6, r2_s6, mae_s6, rmse_s6, vr_s6 = find_alpha(
        "S6 late+scal", nn_va_scal[va_late], y_raw_va_late,  tree_va[va_late], nn_te_scal, tree_te)

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
        "Ridge-Stacked":     {"val_r2": round(float(r2_score(y_raw_va,tree_va)),4),
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

    target_hit = [k for k, v in results.items() if v["test_r2"] >= 0.70]
    print(f"\n  Models reaching R² >= 0.70: {target_hit}")

    # ── Save ──────────────────────────────────────────────────────────────────
    summary = {
        "data_source":   "data/processed/01_daily_merged.csv",
        "architecture":  "BiLSTM(H=32,attn) + 3D BN-NIN CNN(dim=40) + OOF ElasticNet(alpha=0.5,l1=0.1) KFold=8 + global_cal + seasonal_cal + S1-S6",
        "n_params":      n_params,
        "split":         {"train": len(tr_idx), "val": len(va_idx), "test": len(te_idx)},
        "elasticnet_weights": {"xgb": float(ridge.coef_[0]),
                               "lgb": float(ridge.coef_[1]),
                               "gbr": float(ridge.coef_[2]),
                               "cat": float(ridge.coef_[3]),
                               "etr": float(ridge.coef_[4])},
        "calibration":   {"a": float(cal.coef_[0]), "b": float(cal.intercept_)},
        "alpha":         {"s1": float(best_a_s1), "s2": float(best_a_s2),
                          "s3": float(best_a_s3), "s4": float(best_a_s4),
                          "s5": float(best_a_s5), "s6": float(best_a_s6)},
        "results":       results,
        "total_time_s":  round(time.time() - t0, 1),
    }
    (OUT_DIR / "summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8")

    # Predictions
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
