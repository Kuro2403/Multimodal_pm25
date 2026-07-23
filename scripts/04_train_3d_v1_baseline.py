# -*- coding: utf-8 -*-
"""
train_3d_final.py — Best Multimodal Architecture with 3D CNN Satellite Branch
==============================================================================
Replaces the 1D BN-NIN CNN with a 3D BN-NIN CNN that processes satellite data
as a (7-day × 6-stat × 5-band) spatio-temporal volume (30 features total).

Architecture:
  [1] BiLSTM (7-day met+pm25 sequence)
  [2] 3D BN-NIN CNN  (B, 1, 7, 6, 5) — temporal × stat-grid × band-grid
  [3] OOF Ridge-Stacked trees  (XGBoost + LightGBM + GradBoost)
  [4] Linear neural calibration (correct winter→spring seasonal bias)
  [5] Val-late alpha blend      (final ensemble weight selection)

3D satellite grid layout:
         NO2   CO   SO2   AerAI  NDVI
  vpix    ·     ·     ·     ·     ·
  mean    ·     ·     ·     ·     ·
  std     ·     ·     ·     ·     ·
  min     ·     ·     ·     ·     ·
  max     ·     ·     ·     ·     ·
  median  ·     ·     ·     ·     ·

Outputs → outputs/final_3d/
  neural_3d_seed*.pt    — saved model weights
  summary.json          — all metrics
  final_3d_results.png  — comparison plots
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
from sklearn.linear_model import Ridge, LinearRegression
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import KFold
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
import lightgbm as lgb
import xgboost as xgb

warnings.filterwarnings("ignore")
torch.set_num_threads(4)

ROOT     = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data/raw/DataAOD/Hanoi"
OUT_DIR  = ROOT / "outputs/final_3d"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Hyperparameters (same as best v2 / train_target_r2) ──────────────────────
SEQ_LEN      = 7
BATCH_SIZE   = 32
MAX_EPOCHS   = 300
PATIENCE     = 40
LR           = 8e-4
WEIGHT_DECAY = 1e-3
N_SEEDS      = 5
LSTM_H       = 48
CNN_DIM      = 64
DROP         = 0.45
TRAIN_FRAC   = 0.70
VAL_FRAC     = 0.15
DEVICE       = torch.device("cpu")

SEQ_MET_COLS = [
    "temperature_2m_C", "relative_humidity_pct",
    "wind_speed_10m_kmh", "wind_u_10m", "wind_v_10m",
    "precipitation_mm", "cloud_cover_pct", "pressure_msl_hPa",
]
# 30 satellite features arranged as 6-stat × 5-band grid (row-major)
# rows: valid_pixels / mean / std / min / max / median
# cols: NO2 / CO / SO2 / AerAI / NDVI
SAT_COLS_3D = [
    "no2_valid_pixels",          "co_valid_pixels",          "so2_valid_pixels",
    "aer_ai_340_380_valid_pixels", "s2_valid_pixels",
    "no2_mean",                  "co_mean",                  "so2_mean",
    "aer_ai_340_380_mean",       "ndvi_mean",
    "no2_std",                   "co_std",                   "so2_std",
    "aer_ai_340_380_std",        "ndvi_std",
    "no2_min",                   "co_min",                   "so2_min",
    "aer_ai_340_380_min",        "ndvi_min",
    "no2_max",                   "co_max",                   "so2_max",
    "aer_ai_340_380_max",        "ndvi_max",
    "no2_median",                "co_median",                "so2_median",
    "aer_ai_340_380_median",     "ndvi_median",
]
LAG_COLS = [
    "pm25_lag1","pm25_lag2","pm25_lag3","pm25_lag7",
    "pm25_roll3","pm25_roll7","pm25_roll7std","pm25_delta",
    "day_of_week","month","day_of_year","is_weekend",
    "pm25_daily_std","pm25_hour_count",
]
SAT_GRID_H = 6   # stat rows
SAT_GRID_W = 5   # band cols


# ══════════════════════════════════════════════════════════════════════════════
# 3D BN-NIN CNN  (satellite encoder)
# ══════════════════════════════════════════════════════════════════════════════

class BNNIN3D(nn.Module):
    """3D Network-in-Network with BatchNorm.

    Input  : (B, 30, T=7) — 30 satellite features × 7 days
    Reshape: (B, 1, 7, 6, 5) — temporal depth × stat-H × band-W
    Output : (B, CNN_DIM=64)
    """
    def __init__(self, in_days=SEQ_LEN, H=SAT_GRID_H, W=SAT_GRID_W,
                 out_dim=CNN_DIM):
        super().__init__()
        self.H = H; self.W = W

        def nin3(ci, co):
            return nn.Sequential(
                nn.Conv3d(ci, co, 3, padding=1), nn.BatchNorm3d(co), nn.ReLU(True),
                nn.Conv3d(co, co, 1),            nn.BatchNorm3d(co), nn.ReLU(True),
                nn.Conv3d(co, co, 1),            nn.BatchNorm3d(co), nn.ReLU(True),
            )

        # Pool temporal only (days/2) — spatial 6×5 is already small
        self.features = nn.Sequential(
            nin3(1, 16),
            nn.MaxPool3d((2, 1, 1), ceil_mode=True),
            nin3(16, 32),
        )
        self.pool = nn.AdaptiveAvgPool3d(1)
        self.fc   = nn.Sequential(
            nn.Flatten(),
            nn.Linear(32, out_dim),
            nn.ReLU(True),
        )

    def forward(self, x):
        # x: (B, 30, 7)
        B = x.shape[0]
        x = x.permute(0, 2, 1)                    # (B, 7, 30)
        x = x.reshape(B, 1, 7, self.H, self.W)    # (B, 1, 7, 6, 5)
        return self.fc(self.pool(self.features(x)))


# ══════════════════════════════════════════════════════════════════════════════
# MULTIMODAL MODEL
# ══════════════════════════════════════════════════════════════════════════════

class FinalMultimodal3D(nn.Module):
    """BiLSTM + 3D BN-NIN CNN + gated fusion + lag-bypass shortcut.

    Inputs:
      x_seq : (B, SEQ_LEN, n_seq)   met + pm25 sequence
      x_sat : (B, 30, SEQ_LEN)      satellite 6×5 grid over 7 days
      x_bp  : (B, 4)                lag1/2/3/7 bypass
      x_ctx : (B, n_ctx)            current-day context
    """
    def __init__(self, n_seq, n_ctx=0, lstm_h=LSTM_H, cnn_dim=CNN_DIM, drop=DROP):
        super().__init__()
        ld = lstm_h * 2

        self.lstm      = nn.LSTM(n_seq, lstm_h, num_layers=2, batch_first=True,
                                 bidirectional=True, dropout=drop)
        self.lstm_norm = nn.LayerNorm(ld)
        self.lstm_drop = nn.Dropout(drop)

        self.cnn = BNNIN3D(out_dim=cnn_dim)

        self.use_ctx = n_ctx > 0
        ctx_dim = 16 if n_ctx > 0 else 0
        if self.use_ctx:
            self.ctx_proj = nn.Sequential(
                nn.Linear(n_ctx, 16), nn.GELU(), nn.Dropout(drop * 0.5))

        gate_in = ld + cnn_dim + ctx_dim
        self.gate = nn.Sequential(
            nn.Linear(gate_in, 32), nn.GELU(),
            nn.Linear(32, cnn_dim), nn.Sigmoid())

        fuse = ld + cnn_dim + ctx_dim
        self.head = nn.Sequential(
            nn.LayerNorm(fuse),
            nn.Linear(fuse, 96),  nn.GELU(), nn.Dropout(drop),
            nn.Linear(96,   48),  nn.GELU(), nn.Dropout(drop * 0.5),
            nn.Linear(48,    1),
        )
        self.bypass = nn.Linear(4, 1, bias=False)
        nn.init.zeros_(self.bypass.weight)

    def forward(self, x_seq, x_sat, x_bp, x_ctx=None):
        out, _ = self.lstm(x_seq)
        h  = self.lstm_drop(self.lstm_norm(out[:, -1, :]))
        hs = self.cnn(x_sat)

        if self.use_ctx and x_ctx is not None:
            hc = self.ctx_proj(x_ctx)
            gate_in = torch.cat([h, hs, hc], dim=1)
            fused   = torch.cat([h, hs * self.gate(gate_in), hc], dim=1)
        else:
            gate_in = torch.cat([h, hs], dim=1)
            fused   = torch.cat([h, hs * self.gate(gate_in)], dim=1)

        return self.head(fused) + self.bypass(x_bp)


# ══════════════════════════════════════════════════════════════════════════════
# DATA PIPELINE
# ══════════════════════════════════════════════════════════════════════════════

def load_daily():
    df_sat = pd.read_csv(DATA_DIR / "all_stations_satellite_hourly.csv",
                         encoding="utf-8-sig")
    df_met = pd.read_csv(DATA_DIR / "all_stations_openmeteo_hourly.csv",
                         encoding="utf-8-sig")
    for df in [df_sat, df_met]:
        df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True, errors="coerce")
    df_sat = df_sat.dropna(subset=["datetime_utc"]).rename(columns={"PM2.5 µg/m³": "pm25"})
    df_met = df_met.dropna(subset=["datetime_utc"])
    met_avail = [c for c in SEQ_MET_COLS if c in df_met.columns]
    df = df_sat.merge(df_met[["location_id", "datetime_utc"] + met_avail],
                      on=["location_id", "datetime_utc"], how="left")
    df["date"] = df["datetime_utc"].dt.strftime("%Y-%m-%d")

    sat_want  = list(dict.fromkeys(SAT_COLS_3D))
    sat_avail = [c for c in sat_want if c in df.columns]

    def fv(s): v = s.dropna(); return v.iloc[0] if len(v) else np.nan
    agg = {"pm25": ("pm25", "mean"), "pm25_daily_std": ("pm25", "std"),
           "pm25_hour_count": ("pm25", "count")}
    for c in sat_avail:
        agg[c] = (c, fv)
    met_map = {}
    for c in met_avail:
        k = f"{c}_sum" if c == "precipitation_mm" else f"{c}_mean"
        agg[k] = (c, "sum" if c == "precipitation_mm" else "mean")
        met_map[c] = k
    df = (df.groupby(["location_id", "location_name", "latitude", "longitude", "date"])
            .agg(**agg).reset_index())
    df["date"] = pd.to_datetime(df["date"])
    df = (df[(df["pm25"] > 0) & (df["pm25"] <= 500)]
          .sort_values(["location_id", "date"]).reset_index(drop=True))

    parts = []
    for _, g in df.groupby("location_id"):
        g = g.sort_values("date").copy()
        for lag in [1, 2, 3, 7]:
            g[f"pm25_lag{lag}"] = g["pm25"].shift(lag)
        g["pm25_roll3"]    = g["pm25"].shift(1).rolling(3).mean()
        g["pm25_roll7"]    = g["pm25"].shift(1).rolling(7).mean()
        g["pm25_roll7std"] = g["pm25"].shift(1).rolling(7).std()
        g["pm25_delta"]    = g["pm25"].shift(1) - g["pm25"].shift(2)
        parts.append(g)
    df = pd.concat(parts, ignore_index=True)
    df["day_of_week"] = df["date"].dt.dayofweek
    df["month"]       = df["date"].dt.month
    df["day_of_year"] = df["date"].dt.dayofyear
    df["is_weekend"]  = df["day_of_week"].isin([5, 6]).astype(int)
    df = df.dropna(subset=["pm25_lag7"]).reset_index(drop=True)

    # Satellite imputation deferred to after split (done in main() on train-only stats)
    # zero-pad any missing 3D columns
    for c in SAT_COLS_3D:
        if c not in df.columns:
            df[c] = 0.0

    doy = df["date"].dt.dayofyear
    df["sin_doy"]   = np.sin(2 * np.pi * doy / 365.25)
    df["cos_doy"]   = np.cos(2 * np.pi * doy / 365.25)
    df["sin_month"] = np.sin(2 * np.pi * df["date"].dt.month / 12)
    df["cos_month"] = np.cos(2 * np.pi * df["date"].dt.month / 12)
    return df, met_map


def split_df(df):
    dates = np.sort(df["date"].dt.normalize().unique()); n = len(dates)
    tc = dates[int(n * TRAIN_FRAC)]; vc = dates[int(n * (TRAIN_FRAC + VAL_FRAC))]
    df["split"] = "test"
    df.loc[df["date"] < tc, "split"] = "train"
    df.loc[(df["date"] >= tc) & (df["date"] < vc), "split"] = "val"
    return df


def impute_satellite(df: pd.DataFrame, sat_cols: list, tr_idx: list) -> pd.DataFrame:
    """Impute satellite gaps using train-only forward interpolation + train median fallback.
    Fixes leakage from limit_direction='both' and global median in load_daily().
    """
    # Compute per-station, per-column train medians
    train_rows = df.iloc[tr_idx]
    train_medians = (train_rows.groupby("location_id")[sat_cols]
                     .median().to_dict(orient="index"))

    parts = []
    for loc_id, grp in df.groupby("location_id"):
        grp = grp.sort_values("date").copy()
        med = train_medians.get(loc_id, {})
        for c in sat_cols:
            if c not in grp.columns:
                continue
            # forward-only interpolation (no backward fill into future)
            grp[c] = grp[c].interpolate(
                method="linear", limit=10, limit_direction="forward")
            # fallback: train median only (not global median)
            fallback = med.get(c, grp[c].median())
            grp[c] = grp[c].fillna(fallback)
        parts.append(grp)

    return pd.concat(parts).sort_index()


def build_seqs(df_sc, idxs, seq_feats):
    groups = {loc: g.sort_values("date") for loc, g in df_sc.groupby("location_id")}
    seqs = []
    for i in idxs:
        row = df_sc.iloc[i]; hist = groups[row["location_id"]]
        h = hist[hist["date"] < row["date"]].tail(SEQ_LEN)
        if len(h) < SEQ_LEN:
            seqs.append(np.zeros((SEQ_LEN, len(seq_feats)), dtype=np.float32))
        else:
            seqs.append(h[seq_feats].values.astype(np.float32))
    return np.stack(seqs)


def build_sat_seq(df_sat_sc, idxs, sat_feats):
    """Returns (N, n_sat, SEQ_LEN) — (B, 30, 7) for 3D CNN."""
    groups = {loc: g.sort_values("date") for loc, g in df_sat_sc.groupby("location_id")}
    seqs = []
    for i in idxs:
        row = df_sat_sc.iloc[i]; hist = groups[row["location_id"]]
        h = hist[hist["date"] < row["date"]].tail(SEQ_LEN)
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
    def __getitem__(self, i): return self.xs[i], self.xss[i], self.xb[i], self.xc[i], self.y[i]


def train_neural(model, x_seq_tr, x_ss_tr, x_bp_tr, x_ctx_tr, y_std_tr,
                 x_seq_va, x_ss_va, x_bp_va, x_ctx_va, y_raw_va, y_sc, seed):
    torch.manual_seed(seed); np.random.seed(seed)
    model = model.to(DEVICE)
    opt   = torch.optim.AdamW(model.parameters(), lr=LR, weight_decay=WEIGHT_DECAY)
    sched = torch.optim.lr_scheduler.CosineAnnealingWarmRestarts(
        opt, T_0=60, T_mult=2, eta_min=LR / 100)
    loss_fn = nn.HuberLoss(delta=1.0)
    tr_ld = DataLoader(
        PMDataset(x_seq_tr, x_ss_tr, x_bp_tr, x_ctx_tr, y_std_tr),
        BATCH_SIZE, shuffle=True, num_workers=0)
    va_ld = DataLoader(
        PMDataset(x_seq_va, x_ss_va, x_bp_va, x_ctx_va, np.zeros(len(x_seq_va))),
        BATCH_SIZE, shuffle=False, num_workers=0)
    best_vr, pat, best_w = -999.0, 0, None
    for ep in range(1, MAX_EPOCHS + 1):
        model.train()
        for xs, xss, xb, xc, yb in tr_ld:
            opt.zero_grad()
            loss_fn(model(xs.to(DEVICE), xss.to(DEVICE),
                          xb.to(DEVICE), xc.to(DEVICE)), yb.to(DEVICE)).backward()
            nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            opt.step()
        sched.step()
        model.eval(); pv = []
        with torch.no_grad():
            for xs, xss, xb, xc, _ in va_ld:
                pv.append(model(xs.to(DEVICE), xss.to(DEVICE),
                                xb.to(DEVICE), xc.to(DEVICE)).cpu().numpy().ravel())
        pv_raw = y_sc.inverse_transform(
            np.concatenate(pv).reshape(-1, 1)).ravel()
        vr = r2_score(y_raw_va, pv_raw)
        if vr > best_vr + 1e-5:
            best_vr = vr; pat = 0
            best_w = {k: v.clone() for k, v in model.state_dict().items()}
        else:
            pat += 1
            if pat >= PATIENCE: break
    model.load_state_dict(best_w)
    return model, best_vr, ep


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
    n_estimators=1200, max_depth=5, learning_rate=0.015,
    subsample=0.8, colsample_bytree=0.8, reg_alpha=0.05, reg_lambda=1.0,
    random_state=42, verbosity=0, n_jobs=-1,
    early_stopping_rounds=100, eval_metric="rmse")

def make_lgb(): return lgb.LGBMRegressor(
    n_estimators=1200, max_depth=5, learning_rate=0.015,
    subsample=0.8, colsample_bytree=0.8, reg_alpha=0.05, reg_lambda=1.0,
    random_state=42, verbosity=-1, n_jobs=-1)

def make_gbr(): return GradientBoostingRegressor(
    n_estimators=500, max_depth=4, learning_rate=0.05, subsample=0.8,
    min_samples_leaf=5, random_state=42,
    validation_fraction=0.1, n_iter_no_change=30)


# ══════════════════════════════════════════════════════════════════════════════
# PLOTTING
# ══════════════════════════════════════════════════════════════════════════════

def make_plots(y_raw_va, y_raw_te, preds_dict, out_dir):
    """preds_dict: {name: (val_pred, test_pred)}"""
    names  = list(preds_dict.keys())
    colors = plt.cm.tab10(np.linspace(0, 1, len(names)))
    fig = plt.figure(figsize=(20, 14))
    gs  = gridspec.GridSpec(3, 3, figure=fig, hspace=0.45, wspace=0.35)

    ax_r2   = fig.add_subplot(gs[0, 0])
    ax_mae  = fig.add_subplot(gs[0, 1])
    ax_rmse = fig.add_subplot(gs[0, 2])
    r2s   = [r2_score(y_raw_te, preds_dict[n][1]) for n in names]
    maes  = [mean_absolute_error(y_raw_te, preds_dict[n][1]) for n in names]
    rmses = [float(np.sqrt(mean_squared_error(y_raw_te, preds_dict[n][1]))) for n in names]
    for ax, vals, label, fmt in [
        (ax_r2,  r2s,   "Test R²",          "{:.4f}"),
        (ax_mae, maes,  "Test MAE (µg/m³)", "{:.3f}"),
        (ax_rmse,rmses, "Test RMSE (µg/m³)","{:.3f}"),
    ]:
        bars = ax.bar(names, vals, color=colors, edgecolor="black", lw=0.7, alpha=0.88)
        ax.set_title(label, fontsize=10, fontweight="bold")
        ax.set_xticklabels(names, rotation=35, ha="right", fontsize=8)
        ax.grid(alpha=0.3, axis="y")
        for bar, v in zip(bars, vals):
            ax.text(bar.get_x() + bar.get_width() / 2,
                    bar.get_height() + 0.003 * (max(vals) - min(vals) + 1e-6),
                    fmt.format(v), ha="center", va="bottom",
                    fontsize=7.5, fontweight="bold")
        if label == "Test R²":
            ax.axhline(0.70, color="red", ls="--", lw=1.2, alpha=0.7, label="0.70 target")
            ax.legend(fontsize=8)

    for ci, name in enumerate(names[:3]):
        ax = fig.add_subplot(gs[1, ci])
        p  = preds_dict[name][1]
        ax.scatter(y_raw_te, p, alpha=0.4, s=18, color=colors[ci], edgecolors="none")
        lim = [min(y_raw_te.min(), p.min()) - 5, max(y_raw_te.max(), p.max()) + 5]
        ax.plot(lim, lim, "k--", lw=1, alpha=0.5)
        ax.set_xlim(lim); ax.set_ylim(lim)
        ax.set_xlabel("Actual PM2.5", fontsize=9); ax.set_ylabel("Predicted", fontsize=9)
        ax.set_title(f"{name}\nR²={r2_score(y_raw_te,p):+.4f}  "
                     f"MAE={mean_absolute_error(y_raw_te,p):.2f}",
                     fontsize=9, fontweight="bold")
        ax.grid(alpha=0.25)

    ax_ts = fig.add_subplot(gs[2, :])
    ax_ts.plot(range(len(y_raw_te)), y_raw_te, "k-", lw=1.8, label="Actual", zorder=5)
    for ci, name in enumerate(names):
        ax_ts.plot(range(len(y_raw_te)), preds_dict[name][1],
                   color=colors[ci], lw=1.2, alpha=0.75, label=name)
    ax_ts.set_xlabel("Test day index", fontsize=10, fontweight="bold")
    ax_ts.set_ylabel("PM2.5 (µg/m³)", fontsize=10, fontweight="bold")
    ax_ts.set_title("Test Set Time Series — Final 3D Multimodal vs Components",
                    fontsize=11, fontweight="bold")
    ax_ts.legend(fontsize=8, ncol=len(names) + 1, loc="upper right")
    ax_ts.grid(alpha=0.25)

    plt.suptitle(
        "Final Multimodal (3D CNN): BiLSTM + 3D BN-NIN + OOF-Stacked Trees\n"
        "6-Station Hanoi  |  70/15/15 Chronological Split  |  "
        "val-late alpha + linear neural calibration",
        fontsize=12, fontweight="bold", y=1.01)
    plt.savefig(out_dir / "final_3d_results.png", dpi=180, bbox_inches="tight")
    plt.close()
    print(f"  Plot → {out_dir / 'final_3d_results.png'}")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    t0 = time.time()
    SEP = "=" * 78
    print(SEP)
    print("  Final 3D Multimodal: BiLSTM + 3D BN-NIN + OOF Trees")
    print("  Satellite input: (B, 1, 7-days, 6-stats, 5-bands)")
    print(SEP)

    # ── Load & split ──────────────────────────────────────────────────────────
    print("\nLoading data ...")
    df, met_map = load_daily()
    df = split_df(df)
    df_all = df.reset_index(drop=True)
    met_daily_cols = list(met_map.values())

    tr_idx = df_all[df_all["split"] == "train"].index.tolist()
    va_idx = df_all[df_all["split"] == "val"].index.tolist()
    te_idx = df_all[df_all["split"] == "test"].index.tolist()
    print(f"  Train {len(tr_idx)} / Val {len(va_idx)} / Test {len(te_idx)}")

    # Fix satellite leakage: impute using train-only forward interpolation + train median
    sat_cols_present = [c for c in SAT_COLS_3D if c in df_all.columns]
    df_all = impute_satellite(df_all, sat_cols_present, tr_idx)
    print(f"  Satellite imputation (leak-free): {len(sat_cols_present)} cols")

    n_va = len(va_idx)
    va_early = list(range(0, n_va // 2))
    va_late  = list(range(n_va // 2, n_va))
    y_raw_tr = df_all.iloc[tr_idx]["pm25"].values.astype(np.float32)
    y_raw_va = df_all.iloc[va_idx]["pm25"].values.astype(np.float32)
    y_raw_te = df_all.iloc[te_idx]["pm25"].values.astype(np.float32)
    y_raw_va_late = y_raw_va[va_late]
    print(f"  Val-early {len(va_early)} / Val-late {len(va_late)}")

    # ── Sequence features (BiLSTM) ────────────────────────────────────────────
    seq_feats = ["pm25"] + [c for c in met_daily_cols if c in df_all.columns]
    df_all["pm25"] = df_all["pm25"].clip(lower=0.01)
    df_sc = df_all.copy(); df_sc["pm25"] = np.log1p(df_sc["pm25"])
    seq_sc = StandardScaler(); seq_sc.fit(df_sc.loc[tr_idx, seq_feats].fillna(0))
    for c in seq_feats:
        df_sc[c] = df_sc.groupby("location_id")[c].transform(
            lambda s: s.interpolate(method="linear", limit=10,
                                    limit_direction="both").fillna(s.median()))
    df_sc[seq_feats] = seq_sc.transform(df_sc[seq_feats].fillna(0))

    # ── 3D satellite features ─────────────────────────────────────────────────
    sat_sc = StandardScaler()
    df_sat_sc = df_all.copy()
    sat_sc.fit(df_sat_sc.loc[tr_idx, SAT_COLS_3D].fillna(0))
    df_sat_sc[SAT_COLS_3D] = sat_sc.transform(df_sat_sc[SAT_COLS_3D].fillna(0))

    # ── Bypass & context ──────────────────────────────────────────────────────
    bp_feats = [c for c in ["pm25_lag1","pm25_lag2","pm25_lag3","pm25_lag7"]
                if c in df_all.columns]
    bp_sc = StandardScaler(); bp_sc.fit(df_all.loc[tr_idx, bp_feats].fillna(0))
    x_bp_all = bp_sc.transform(df_all[bp_feats].fillna(0)).astype(np.float32)

    ctx_feats = (["sin_doy","cos_doy","sin_month","cos_month"]
                 + [c for c in met_daily_cols if c in df_all.columns]
                 + ["latitude","longitude"])
    ctx_feats = [c for c in ctx_feats if c in df_all.columns]
    ctx_sc = StandardScaler(); ctx_sc.fit(df_all.loc[tr_idx, ctx_feats].fillna(0))
    x_ctx_all = ctx_sc.transform(df_all[ctx_feats].fillna(0)).astype(np.float32)

    y_sc = StandardScaler(); y_sc.fit(df_all.loc[tr_idx, ["pm25"]])
    y_std_tr = y_sc.transform(df_all.iloc[tr_idx][["pm25"]]).ravel().astype(np.float32)

    # ── Build arrays (cached as .npy for fast re-runs) ───────────────────────
    CACHE_DIR = OUT_DIR / "cache"
    CACHE_DIR.mkdir(exist_ok=True)
    _cache_seq = CACHE_DIR / "x_seq_all.npy"
    _cache_sat = CACHE_DIR / "x_sat_all.npy"   # 3D satellite tensor
    _cache_bp  = CACHE_DIR / "x_bp_all.npy"
    _cache_ctx = CACHE_DIR / "x_ctx_all.npy"
    _cache_y   = CACHE_DIR / "y_raw_all.npy"

    if all(p.exists() for p in [_cache_seq, _cache_sat, _cache_bp, _cache_ctx, _cache_y]):
        print("Loading cached arrays ...")
        x_seq_all = np.load(_cache_seq)
        x_ss_all  = np.load(_cache_sat)
        x_bp_all  = np.load(_cache_bp)
        x_ctx_all = np.load(_cache_ctx)
        print(f"  x_seq {x_seq_all.shape}  x_sat {x_ss_all.shape}  (from cache)")
    else:
        print("Building sequence arrays ...")
        x_seq_all = build_seqs(df_sc, range(len(df_all)), seq_feats)
        x_ss_all  = build_sat_seq(df_sat_sc, range(len(df_all)), SAT_COLS_3D)
        np.save(_cache_seq, x_seq_all)
        np.save(_cache_sat, x_ss_all)
        np.save(_cache_bp,  x_bp_all)
        np.save(_cache_ctx, x_ctx_all)
        np.save(_cache_y,   df_all["pm25"].values.astype(np.float32))
        print(f"  x_seq {x_seq_all.shape}  x_sat {x_ss_all.shape}  x_ctx {x_ctx_all.shape}")
        print(f"  Cached -> {CACHE_DIR}")

    x_seq_tr=x_seq_all[tr_idx]; x_ss_tr=x_ss_all[tr_idx]
    x_bp_tr=x_bp_all[tr_idx];   x_ctx_tr=x_ctx_all[tr_idx]
    x_seq_va=x_seq_all[va_idx]; x_ss_va=x_ss_all[va_idx]
    x_bp_va=x_bp_all[va_idx];   x_ctx_va=x_ctx_all[va_idx]
    x_seq_te=x_seq_all[te_idx]; x_ss_te=x_ss_all[te_idx]
    x_bp_te=x_bp_all[te_idx];   x_ctx_te=x_ctx_all[te_idx]
    n_seq = x_seq_tr.shape[2]; n_ctx = x_ctx_tr.shape[1]
    print(f"  x_seq {x_seq_tr.shape}  x_sat {x_ss_tr.shape}  x_ctx {x_ctx_tr.shape}")

    # ── Tabular features (trees) ──────────────────────────────────────────────
    tab_cols = [c for c in LAG_COLS + met_daily_cols if c in df_all.columns]
    imp = SimpleImputer(strategy="median")
    Xtr = imp.fit_transform(df_all.iloc[tr_idx][tab_cols])
    Xva = imp.transform(df_all.iloc[va_idx][tab_cols])
    Xte = imp.transform(df_all.iloc[te_idx][tab_cols])

    # ══════════════════════════════════════════════════════════════════════════
    # EXP-1: OOF STACKING
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{SEP}")
    print("  EXP-1: OOF Stacking (5-fold → Ridge meta-learner)")
    print(SEP)

    kf = KFold(n_splits=5, shuffle=False)
    oof_xgb = np.zeros(len(tr_idx))
    oof_lgb = np.zeros(len(tr_idx))
    oof_gbr = np.zeros(len(tr_idx))
    for fold, (fi_tr, fi_va) in enumerate(kf.split(Xtr)):
        print(f"  fold {fold+1}/5 ...", end=" ", flush=True)
        m = make_xgb()
        m.fit(Xtr[fi_tr], y_raw_tr[fi_tr],
              eval_set=[(Xtr[fi_va], y_raw_tr[fi_va])], verbose=False)
        oof_xgb[fi_va] = m.predict(Xtr[fi_va])
        m = make_lgb()
        m.fit(Xtr[fi_tr], y_raw_tr[fi_tr],
              eval_set=[(Xtr[fi_va], y_raw_tr[fi_va])],
              callbacks=[lgb.early_stopping(100, verbose=False), lgb.log_evaluation(-1)])
        oof_lgb[fi_va] = m.predict(Xtr[fi_va])
        m = make_gbr(); m.fit(Xtr[fi_tr], y_raw_tr[fi_tr])
        oof_gbr[fi_va] = m.predict(Xtr[fi_va])
        print("done")

    ridge = Ridge(alpha=1.0, positive=True, fit_intercept=True)
    ridge.fit(np.column_stack([oof_xgb, oof_lgb, oof_gbr]), y_raw_tr)
    print(f"  Ridge weights: XGB={ridge.coef_[0]:.3f}  LGB={ridge.coef_[1]:.3f}"
          f"  GBR={ridge.coef_[2]:.3f}  intercept={ridge.intercept_:.2f}")

    print("\n  Retraining on full training set ...")
    xgbm = make_xgb(); xgbm.fit(Xtr, y_raw_tr, eval_set=[(Xva, y_raw_va)], verbose=False)
    lgbm = make_lgb()
    lgbm.fit(Xtr, y_raw_tr, eval_set=[(Xva, y_raw_va)],
             callbacks=[lgb.early_stopping(100, verbose=False), lgb.log_evaluation(-1)])
    gbrm = make_gbr(); gbrm.fit(Xtr, y_raw_tr)

    def tree_pred(X):
        return ridge.predict(np.column_stack(
            [xgbm.predict(X), lgbm.predict(X), gbrm.predict(X)]))

    tree_va = tree_pred(Xva); tree_te = tree_pred(Xte)
    xgb_va  = xgbm.predict(Xva); xgb_te = xgbm.predict(Xte)
    r2_tree_va = r2_score(y_raw_va, tree_va)
    r2_tree_te = r2_score(y_raw_te, tree_te)
    r2_xgb_te  = r2_score(y_raw_te, xgb_te)
    print(f"  XGBoost alone:  val={r2_score(y_raw_va,xgb_va):+.4f}  test={r2_xgb_te:+.4f}")
    print(f"  Ridge stacked:  val={r2_tree_va:+.4f}  test={r2_tree_te:+.4f}"
          f"  Δtest={r2_tree_te - r2_xgb_te:+.4f}")

    # ══════════════════════════════════════════════════════════════════════════
    # EXP-2: 3D NEURAL TRAINING + VAL CALIBRATION
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{SEP}")
    print("  EXP-2: 3D Neural training + val-based linear calibration")
    print(SEP)

    model_ref = FinalMultimodal3D(n_seq, n_ctx=n_ctx)
    n_params = sum(p.numel() for p in model_ref.parameters() if p.requires_grad)
    print(f"  FinalMultimodal3D params: {n_params:,}")

    nn_va_list, nn_te_list = [], []
    for seed in range(N_SEEDS):
        pt = OUT_DIR / f"neural_3d_seed{seed}.pt"
        model = FinalMultimodal3D(n_seq, n_ctx=n_ctx)
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
                print(f"  seed {seed}: loaded from disk  "
                      f"val R²={r2_score(y_raw_va, raw_va):+.4f}")
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

    nn_va = np.mean(nn_va_list, axis=0)
    nn_te = np.mean(nn_te_list, axis=0)
    nn_val_r2  = r2_score(y_raw_va, nn_va)
    nn_test_r2 = r2_score(y_raw_te, nn_te)
    print(f"\n  3D Neural ensemble: val={nn_val_r2:+.4f}  test={nn_test_r2:+.4f}")

    # Linear calibration
    cal = LinearRegression()
    cal.fit(nn_va.reshape(-1, 1), y_raw_va)
    nn_va_cal = cal.predict(nn_va.reshape(-1, 1))
    nn_te_cal = cal.predict(nn_te.reshape(-1, 1))
    nn_test_r2_cal = r2_score(y_raw_te, nn_te_cal)
    print(f"  After calibration: a={cal.coef_[0]:.3f}  b={cal.intercept_:.2f}"
          f"  Δtest={nn_test_r2_cal - nn_test_r2:+.4f}  cal_test={nn_test_r2_cal:+.4f}")

    # ══════════════════════════════════════════════════════════════════════════
    # EXP-3: VAL-LATE ALPHA SELECTION
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{SEP}")
    print("  EXP-3: Val-late alpha selection")
    print(SEP)

    def find_alpha(label, nn_va_sub, y_va_sub, tree_va_sub, nn_te_use, tree_te_use):
        best_a, best_r2, best_pred = 0.0, -999.0, None
        for a in np.arange(0.0, 1.01, 0.05):
            r2 = r2_score(y_va_sub, (1 - a) * tree_va_sub + a * nn_va_sub)
            if r2 > best_r2:
                best_r2, best_a = r2, a
                best_pred = (1 - a) * tree_te_use + a * nn_te_use
        te_r2  = r2_score(y_raw_te, best_pred)
        te_mae = mean_absolute_error(y_raw_te, best_pred)
        te_rmse= float(np.sqrt(mean_squared_error(y_raw_te, best_pred)))
        print(f"  [{label}]  alpha={best_a:.2f}  val R²={best_r2:+.4f}"
              f"  test R²={te_r2:+.4f}  MAE={te_mae:.3f}  RMSE={te_rmse:.3f}")
        return best_a, best_pred, te_r2, te_mae, te_rmse, best_r2

    # S1: full-val + raw neural
    best_a_s1, pred_s1, r2_s1, mae_s1, rmse_s1, val_r2_s1 = find_alpha(
        "S1 full-val + raw",
        nn_va,     y_raw_va,      tree_va,
        nn_te,     tree_te)
    # S2: full-val + calibrated neural
    best_a_s2, pred_s2, r2_s2, mae_s2, rmse_s2, val_r2_s2 = find_alpha(
        "S2 full-val + cal",
        nn_va_cal, y_raw_va,      tree_va,
        nn_te_cal, tree_te)
    # S3: val-late + raw neural
    best_a_s3, pred_s3, r2_s3, mae_s3, rmse_s3, val_r2_s3 = find_alpha(
        "S3 late-val + raw",
        nn_va[va_late],     y_raw_va_late, tree_va[va_late],
        nn_te,              tree_te)
    # S4: val-late + calibrated neural  (BEST EXPECTED)
    best_a_s4, pred_s4, r2_s4, mae_s4, rmse_s4, val_r2_s4 = find_alpha(
        "S4 late-val + cal",
        nn_va_cal[va_late], y_raw_va_late, tree_va[va_late],
        nn_te_cal,          tree_te)

    # ══════════════════════════════════════════════════════════════════════════
    # SUMMARY
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{SEP}")
    print("  FINAL RESULTS")
    print(SEP)

    final_results = {
        "XGBoost (base)":        {"val_r2": round(float(r2_score(y_raw_va,xgb_va)),4),
                                   "test_r2": round(float(r2_xgb_te),4),
                                   "mae": round(float(mean_absolute_error(y_raw_te,xgb_te)),3)},
        "Ridge-Stacked":         {"val_r2": round(float(r2_tree_va),4),
                                   "test_r2": round(float(r2_tree_te),4),
                                   "mae": round(float(mean_absolute_error(y_raw_te,tree_te)),3)},
        "3D Neural raw":         {"val_r2": round(float(nn_val_r2),4),
                                   "test_r2": round(float(nn_test_r2),4),
                                   "mae": round(float(mean_absolute_error(y_raw_te,nn_te)),3)},
        "3D Neural cal":         {"val_r2": round(float(r2_score(y_raw_va,nn_va_cal)),4),
                                   "test_r2": round(float(nn_test_r2_cal),4),
                                   "mae": round(float(mean_absolute_error(y_raw_te,nn_te_cal)),3)},
        "FINAL S1 (full+raw)":   {"val_r2": round(val_r2_s1,4), "test_r2": round(r2_s1,4),
                                   "mae": round(mae_s1,3), "rmse": round(rmse_s1,3)},
        "FINAL S2 (full+cal)":   {"val_r2": round(val_r2_s2,4), "test_r2": round(r2_s2,4),
                                   "mae": round(mae_s2,3), "rmse": round(rmse_s2,3)},
        "FINAL S3 (late+raw)":   {"val_r2": round(val_r2_s3,4), "test_r2": round(r2_s3,4),
                                   "mae": round(mae_s3,3), "rmse": round(rmse_s3,3)},
        "FINAL S4 (late+cal)":   {"val_r2": round(val_r2_s4,4), "test_r2": round(r2_s4,4),
                                   "mae": round(mae_s4,3), "rmse": round(rmse_s4,3)},
    }

    print(f"\n  {'Model':<28} {'Val R²':>8}  {'Test R²':>8}  {'MAE':>7}  {'RMSE':>7}")
    print(f"  {'-'*28} {'-'*8}  {'-'*8}  {'-'*7}  {'-'*7}")
    best_te = max(v["test_r2"] for v in final_results.values())
    for name, m in final_results.items():
        vr     = f"{m['val_r2']:+.4f}" if "val_r2" in m else "      —"
        rmse_s = f"{m['rmse']:.3f}"    if "rmse"   in m else "      —"
        marker = " ★" if m["test_r2"] == best_te else ""
        print(f"  {name:<28} {vr:>8}  {m['test_r2']:>+8.4f}  {m['mae']:>7.3f}  {rmse_s:>7}{marker}")

    target_hit = [k for k, v in final_results.items() if v["test_r2"] >= 0.70]
    print(f"\n  Models reaching R² ≥ 0.70: {target_hit}")

    # Compare to 1D baseline
    r2_1d_baseline = 0.7037  # from compare_cnn_dimensions.py
    print(f"\n  3D FINAL S4 vs 1D baseline: "
          f"{r2_s4 - r2_1d_baseline:+.4f}  "
          f"({'improvement' if r2_s4 > r2_1d_baseline else 'regression'})")

    summary = {
        "architecture": "BiLSTM + 3D BN-NIN (7×6×5) + OOF-Ridge + neural-cal + val-late-alpha",
        "satellite_input": "(B, 30, 7) → (B, 1, 7, 6, 5)",
        "n_params_neural": n_params,
        "ridge_weights": {"xgb": float(ridge.coef_[0]),
                          "lgb": float(ridge.coef_[1]),
                          "gbr": float(ridge.coef_[2])},
        "calibration": {"a": float(cal.coef_[0]), "b": float(cal.intercept_)},
        "alpha_s4": float(best_a_s4),
        "final_results": final_results,
        "target_0.70_reached": target_hit,
        "vs_1d_baseline": round(float(r2_s4 - r2_1d_baseline), 4),
        "total_time_s": round(time.time() - t0, 1),
    }
    (OUT_DIR / "summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8")
    print(f"\n  Summary → {OUT_DIR / 'summary.json'}")

    # Plots
    preds = {
        "XGBoost":       (xgb_va,    xgb_te),
        "Ridge-Stack":   (tree_va,   tree_te),
        "3D Neural cal": (nn_va_cal, nn_te_cal),
        "FINAL S4":      (None,      pred_s4),
    }
    # fill None val preds
    preds["FINAL S4"] = (
        (1 - best_a_s4) * tree_va + best_a_s4 * nn_va_cal,
        pred_s4)
    make_plots(y_raw_va, y_raw_te, preds, OUT_DIR)

    # Save predictions
    np.save(OUT_DIR / "pred_val_s4.npy",    preds["FINAL S4"][0])
    np.save(OUT_DIR / "pred_test_s4.npy",   pred_s4)
    np.save(OUT_DIR / "pred_val_neural.npy",  nn_va_cal)
    np.save(OUT_DIR / "pred_test_neural.npy", nn_te_cal)
    np.save(OUT_DIR / "pred_val_tree.npy",  tree_va)
    np.save(OUT_DIR / "pred_test_tree.npy", tree_te)
    np.save(OUT_DIR / "y_val_raw.npy",  y_raw_va)
    np.save(OUT_DIR / "y_test_raw.npy", y_raw_te)
    print(f"  Predictions saved -> {OUT_DIR}")

    print(f"\n  Total time: {time.time() - t0:.0f}s")
    print(SEP)


if __name__ == "__main__":
    main()
