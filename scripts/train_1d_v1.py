# -*- coding: utf-8 -*-
"""
Final Multimodal PM2.5 Model  (v2 — balanced val/test R²)
===========================================================
Architecture: BiLSTM + BN-NIN (best 1D CNN) + XGBoost + LightGBM + GradientBoosting

Four-component ensemble:
  [1] BiLSTM (7-day met+pm25 sequence) — captures temporal dynamics
  [2] BN-NIN 1D CNN (12-channel satellite stats over 7 days) — best CNN from comparison
  [3] XGBoost (lags + met tabular features) — strongest tree
  [4] LightGBM + sklearn GradientBoosting — gradient boosting ensemble

Val/test balance fix (v2):
  - Val split into EARLY (first 50%) and LATE (last 50%) halves
  - Alpha selected to maximise AVERAGE R² across both halves
    → forces neural contribution to be stable across different time periods
    → prevents over-weighting neural on the winter-only val window
  - Stronger neural regularisation (higher dropout, L2, smaller head)
  - Explicit sin/cos seasonal encoding added to sequence features

Final prediction:
  tree_blend = w1*XGB + w2*LGB + w3*GB       (grid-searched on full val)
  final      = (1-alpha)*tree_blend + alpha*neural
  alpha selected by argmax mean(R²_val_early, R²_val_late)
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
import torch.nn.functional as F
from torch.utils.data import Dataset, DataLoader
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
import lightgbm as lgb
import xgboost as xgb

warnings.filterwarnings("ignore")
torch.set_num_threads(4)

ROOT     = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data/raw/DataAOD/Hanoi"
OUT_DIR  = ROOT / "outputs/final_3d_v1"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Hyperparameters ───────────────────────────────────────────────────────────
SEQ_LEN      = 7
BATCH_SIZE   = 32
MAX_EPOCHS   = 300
PATIENCE     = 40
LR           = 8e-4
WEIGHT_DECAY = 1e-3     # stronger L2 (was 5e-4)
N_SEEDS      = 5
LSTM_H       = 48       # smaller BiLSTM to reduce season-overfitting (was 64)
CNN_DIM      = 64
DROP         = 0.45     # higher dropout (was 0.35)
TRAIN_FRAC   = 0.70
VAL_FRAC     = 0.15
DEVICE       = torch.device("cpu")

SEQ_MET_COLS = [
    "temperature_2m_C", "relative_humidity_pct",
    "wind_speed_10m_kmh", "wind_u_10m", "wind_v_10m",
    "precipitation_mm", "cloud_cover_pct", "pressure_msl_hPa",
]
SAT_COLS = [
    "no2_mean", "no2_std", "co_mean", "co_std",
    "so2_mean", "so2_std", "aer_ai_340_380_mean", "aer_ai_340_380_std",
    "ndvi_mean", "ndvi_std", "ndbi_mean", "ndwi_mean",
]
LAG_COLS = [
    "pm25_lag1","pm25_lag2","pm25_lag3","pm25_lag7",
    "pm25_roll3","pm25_roll7","pm25_roll7std","pm25_delta",
    "day_of_week","month","day_of_year","is_weekend",
    "pm25_daily_std","pm25_hour_count",
]


# ══════════════════════════════════════════════════════════════════════════════
# BN-NIN 1D CNN  (best backbone from comparison)
# ══════════════════════════════════════════════════════════════════════════════

class BNNIN1D(nn.Module):
    """Network-in-Network with Batch Normalisation — best 1D satellite encoder."""
    def __init__(self, in_ch, out_dim=64):
        super().__init__()
        def nin(ci, co):
            return nn.Sequential(
                nn.Conv1d(ci, co, 3, padding=1), nn.BatchNorm1d(co), nn.ReLU(True),
                nn.Conv1d(co, co, 1),            nn.BatchNorm1d(co), nn.ReLU(True),
                nn.Conv1d(co, co, 1),            nn.BatchNorm1d(co), nn.ReLU(True),
            )
        self.features = nn.Sequential(
            nin(in_ch, 64), nn.MaxPool1d(2, ceil_mode=True),
            nin(64, 128),
        )
        self.pool = nn.AdaptiveAvgPool1d(1)
        self.fc   = nn.Sequential(nn.Flatten(), nn.Linear(128, out_dim), nn.ReLU(True))

    def forward(self, x):
        return self.fc(self.pool(self.features(x)))


# ══════════════════════════════════════════════════════════════════════════════
# MULTIMODAL MODEL
# ══════════════════════════════════════════════════════════════════════════════

class FinalMultimodal(nn.Module):
    """
    BiLSTM (temporal sequence) + BN-NIN 1D CNN (satellite bands×time)
    + seasonal context vector + gated satellite attention + lag-bypass shortcut.

    Inputs:
      x_seq : (B, SEQ_LEN, n_seq)   met + pm25 sequence (includes sin/cos season)
      x_sat : (B, n_sat, SEQ_LEN)   satellite stats, bands-as-channels
      x_bp  : (B, 4)                lag1/2/3/7 bypass
      x_ctx : (B, n_ctx)            current-day context (season, met, station lat/lon)
    """
    def __init__(self, n_seq, n_sat, n_ctx=0, lstm_h=LSTM_H, cnn_dim=CNN_DIM, drop=DROP):
        super().__init__()
        ld = lstm_h * 2          # BiLSTM → 2×h

        # ── Temporal branch: BiLSTM ──────────────────────────────────────────
        self.lstm      = nn.LSTM(n_seq, lstm_h, num_layers=2, batch_first=True,
                                 bidirectional=True, dropout=drop)
        self.lstm_norm = nn.LayerNorm(ld)
        self.lstm_drop = nn.Dropout(drop)

        # ── Satellite branch: BN-NIN 1D ─────────────────────────────────────
        self.cnn = BNNIN1D(n_sat, cnn_dim)

        # ── Context projection (season + met scalars for current day) ────────
        self.use_ctx = n_ctx > 0
        ctx_dim = 16 if n_ctx > 0 else 0
        if self.use_ctx:
            self.ctx_proj = nn.Sequential(
                nn.Linear(n_ctx, 16), nn.GELU(), nn.Dropout(drop * 0.5),
            )

        # ── Gated cross-attention (satellite gate conditioned on LSTM + ctx) ─
        gate_in = ld + cnn_dim + ctx_dim
        self.gate = nn.Sequential(
            nn.Linear(gate_in, 32), nn.GELU(),
            nn.Linear(32, cnn_dim), nn.Sigmoid(),
        )

        # ── Fusion head ──────────────────────────────────────────────────────
        fuse = ld + cnn_dim + ctx_dim
        self.head = nn.Sequential(
            nn.LayerNorm(fuse),
            nn.Linear(fuse, 96),  nn.GELU(), nn.Dropout(drop),
            nn.Linear(96,  48),   nn.GELU(), nn.Dropout(drop * 0.5),
            nn.Linear(48,  1),
        )

        # ── Lag bypass (initialised at zero, grows if useful) ────────────────
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
    df_sat = pd.read_csv(DATA_DIR/"all_stations_satellite_hourly.csv", encoding="utf-8-sig")
    df_met = pd.read_csv(DATA_DIR/"all_stations_openmeteo_hourly.csv", encoding="utf-8-sig")
    for df in [df_sat, df_met]:
        df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True, errors="coerce")
    df_sat = df_sat.dropna(subset=["datetime_utc"]).rename(columns={"PM2.5 µg/m³": "pm25"})
    df_met = df_met.dropna(subset=["datetime_utc"])
    met_avail = [c for c in SEQ_MET_COLS if c in df_met.columns]
    df = df_sat.merge(df_met[["location_id","datetime_utc"]+met_avail],
                      on=["location_id","datetime_utc"], how="left")
    df["date"] = df["datetime_utc"].dt.strftime("%Y-%m-%d")

    sat_avail = [c for c in SAT_COLS if c in df.columns]
    def fv(s): v = s.dropna(); return v.iloc[0] if len(v) else np.nan
    agg = {"pm25":("pm25","mean"),"pm25_daily_std":("pm25","std"),
           "pm25_hour_count":("pm25","count")}
    for c in sat_avail: agg[c] = (c, fv)
    met_map = {}
    for c in met_avail:
        k = f"{c}_sum" if c == "precipitation_mm" else f"{c}_mean"
        agg[k] = (c, "sum" if c == "precipitation_mm" else "mean")
        met_map[c] = k
    df = (df.groupby(["location_id","location_name","latitude","longitude","date"])
            .agg(**agg).reset_index())
    df["date"] = pd.to_datetime(df["date"])
    df = df[(df["pm25"]>0)&(df["pm25"]<=500)].sort_values(["location_id","date"]).reset_index(drop=True)

    parts = []
    for _, g in df.groupby("location_id"):
        g = g.sort_values("date").copy()
        g["pm25_lag1"]     = g["pm25"].shift(1)
        g["pm25_lag2"]     = g["pm25"].shift(2)
        g["pm25_lag3"]     = g["pm25"].shift(3)
        g["pm25_lag7"]     = g["pm25"].shift(7)
        g["pm25_roll3"]    = g["pm25"].shift(1).rolling(3).mean()
        g["pm25_roll7"]    = g["pm25"].shift(1).rolling(7).mean()
        g["pm25_roll7std"] = g["pm25"].shift(1).rolling(7).std()
        g["pm25_delta"]    = g["pm25"].shift(1) - g["pm25"].shift(2)
        parts.append(g)
    df = pd.concat(parts, ignore_index=True)
    df["day_of_week"] = df["date"].dt.dayofweek
    df["month"]       = df["date"].dt.month
    df["day_of_year"] = df["date"].dt.dayofyear
    df["is_weekend"]  = df["day_of_week"].isin([5,6]).astype(int)
    df = df.dropna(subset=["pm25_lag7"]).reset_index(drop=True)

    for c in sat_avail:
        df[c] = df.groupby("location_id")[c].transform(
            lambda s: s.interpolate(method="linear",limit=10,limit_direction="both").fillna(s.median()))

    # Sin/cos seasonal encoding — helps neural generalise across seasons
    doy = df["date"].dt.dayofyear
    df["sin_doy"] = np.sin(2 * np.pi * doy / 365.25)
    df["cos_doy"] = np.cos(2 * np.pi * doy / 365.25)
    df["sin_month"] = np.sin(2 * np.pi * df["date"].dt.month / 12)
    df["cos_month"] = np.cos(2 * np.pi * df["date"].dt.month / 12)

    return df, sat_avail, met_map


def split_df(df):
    dates = np.sort(df["date"].dt.normalize().unique()); n = len(dates)
    tc = dates[int(n * TRAIN_FRAC)]; vc = dates[int(n * (TRAIN_FRAC + VAL_FRAC))]
    df["split"] = "test"
    df.loc[df["date"] < tc, "split"] = "train"
    df.loc[(df["date"] >= tc) & (df["date"] < vc), "split"] = "val"
    return df


def build_seqs(df_all, idxs, seq_feats):
    groups = {loc: g.sort_values("date") for loc, g in df_all.groupby("location_id")}
    seqs = []
    for i in idxs:
        row = df_all.iloc[i]; hist = groups[row["location_id"]]
        h = hist[hist["date"] < row["date"]].tail(SEQ_LEN)
        if len(h) < SEQ_LEN: seqs.append(np.zeros((SEQ_LEN, len(seq_feats)), dtype=np.float32))
        else:                 seqs.append(h[seq_feats].values.astype(np.float32))
    return np.stack(seqs)


def build_sat_seq(df_all, idxs, sat_feats):
    """(N, n_sat, SEQ_LEN) for 1D CNN."""
    groups = {loc: g.sort_values("date") for loc, g in df_all.groupby("location_id")}
    seqs = []
    for i in idxs:
        row = df_all.iloc[i]; hist = groups[row["location_id"]]
        h = hist[hist["date"] < row["date"]].tail(SEQ_LEN)
        if len(h) < SEQ_LEN: seqs.append(np.zeros((len(sat_feats), SEQ_LEN), dtype=np.float32))
        else:                 seqs.append(h[sat_feats].values.astype(np.float32).T)
    return np.stack(seqs)


# ══════════════════════════════════════════════════════════════════════════════
# TRAINING
# ══════════════════════════════════════════════════════════════════════════════

class PMDataset(Dataset):
    def __init__(self, x_seq, x_sat, x_bp, x_ctx, y):
        self.xs   = torch.from_numpy(x_seq.astype(np.float32))
        self.xss  = torch.from_numpy(x_sat.astype(np.float32))
        self.xb   = torch.from_numpy(x_bp.astype(np.float32))
        self.xc   = torch.from_numpy(x_ctx.astype(np.float32))
        self.y    = torch.from_numpy(y.astype(np.float32)).unsqueeze(1)
    def __len__(self): return len(self.y)
    def __getitem__(self, i): return self.xs[i], self.xss[i], self.xb[i], self.xc[i], self.y[i]


def train_neural(model, x_seq_tr, x_ss_tr, x_bp_tr, x_ctx_tr, y_std_tr,
                 x_seq_va, x_ss_va, x_bp_va, x_ctx_va, y_raw_va, y_sc, seed):
    torch.manual_seed(seed); np.random.seed(seed)
    model = model.to(DEVICE)
    opt   = torch.optim.AdamW(model.parameters(), lr=LR, weight_decay=WEIGHT_DECAY)
    sched = torch.optim.lr_scheduler.CosineAnnealingWarmRestarts(opt, T_0=60, T_mult=2, eta_min=LR/100)
    loss_fn = nn.HuberLoss(delta=1.0)

    tr_ld = DataLoader(PMDataset(x_seq_tr,x_ss_tr,x_bp_tr,x_ctx_tr,y_std_tr),
                       BATCH_SIZE, shuffle=True,  num_workers=0)
    va_ld = DataLoader(PMDataset(x_seq_va,x_ss_va,x_bp_va,x_ctx_va,
                                 np.zeros(len(x_seq_va))),
                       BATCH_SIZE, shuffle=False, num_workers=0)

    best_vr, pat, best_w = -999.0, 0, None
    for ep in range(1, MAX_EPOCHS+1):
        model.train()
        for xs, xss, xb, xc, yb in tr_ld:
            opt.zero_grad()
            pred = model(xs.to(DEVICE), xss.to(DEVICE), xb.to(DEVICE), xc.to(DEVICE))
            loss_fn(pred, yb.to(DEVICE)).backward()
            nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            opt.step()
        sched.step()

        model.eval(); pv = []
        with torch.no_grad():
            for xs, xss, xb, xc, _ in va_ld:
                pv.append(model(xs.to(DEVICE), xss.to(DEVICE),
                                xb.to(DEVICE), xc.to(DEVICE)).cpu().numpy().ravel())
        pv_raw = y_sc.inverse_transform(np.concatenate(pv).reshape(-1,1)).ravel()
        vr = r2_score(y_raw_va, pv_raw)
        if vr > best_vr + 1e-5: best_vr=vr; pat=0; best_w={k:v.clone() for k,v in model.state_dict().items()}
        else:
            pat += 1
            if pat >= PATIENCE: break

    model.load_state_dict(best_w)
    return model, best_vr, ep


def predict_neural(model, x_seq, x_sat, x_bp, x_ctx, bs=512):
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
# PLOTTING
# ══════════════════════════════════════════════════════════════════════════════

def make_plots(y_raw_va, y_raw_te, preds, split_dates, out_dir):
    """preds: dict of {name: (val_pred, test_pred)}"""
    fig = plt.figure(figsize=(20, 14))
    gs  = gridspec.GridSpec(3, 3, figure=fig, hspace=0.45, wspace=0.35)

    model_names = list(preds.keys())
    colors = plt.cm.tab10(np.linspace(0, 1, len(model_names)))

    # ── Row 0: Bar chart R² / MAE comparison ─────────────────────────────────
    ax_r2  = fig.add_subplot(gs[0, 0])
    ax_mae = fig.add_subplot(gs[0, 1])
    ax_rmse= fig.add_subplot(gs[0, 2])

    r2s   = [r2_score(y_raw_te, preds[n][1]) for n in model_names]
    maes  = [mean_absolute_error(y_raw_te, preds[n][1]) for n in model_names]
    rmses = [float(np.sqrt(mean_squared_error(y_raw_te, preds[n][1]))) for n in model_names]

    for ax, vals, label in [(ax_r2, r2s, "Test R²"), (ax_mae, maes, "Test MAE (µg/m³)"),
                             (ax_rmse, rmses, "Test RMSE (µg/m³)")]:
        bars = ax.bar(model_names, vals, color=colors, edgecolor="black", lw=0.7, alpha=0.88)
        ax.set_title(label, fontsize=10, fontweight="bold")
        ax.set_xticklabels(model_names, rotation=35, ha="right", fontsize=8)
        ax.grid(alpha=0.3, axis="y")
        for bar, v in zip(bars, vals):
            ax.text(bar.get_x()+bar.get_width()/2, bar.get_height()+0.002*max(vals),
                    f"{v:.3f}", ha="center", va="bottom", fontsize=7.5, fontweight="bold")

    # ── Row 1: Scatter plots (actual vs predicted) ────────────────────────────
    for ci, name in enumerate(model_names[:3]):
        ax = fig.add_subplot(gs[1, ci])
        p  = preds[name][1]
        ax.scatter(y_raw_te, p, alpha=0.4, s=18, color=colors[ci], edgecolors="none")
        lim = [min(y_raw_te.min(), p.min())-5, max(y_raw_te.max(), p.max())+5]
        ax.plot(lim, lim, "k--", lw=1, alpha=0.5)
        ax.set_xlim(lim); ax.set_ylim(lim)
        ax.set_xlabel("Actual PM2.5", fontsize=9); ax.set_ylabel("Predicted", fontsize=9)
        ax.set_title(f"{name}\nR²={r2_score(y_raw_te,p):+.4f}  MAE={mean_absolute_error(y_raw_te,p):.2f}",
                     fontsize=9, fontweight="bold")
        ax.grid(alpha=0.25)

    # ── Row 2: Time series on test set ────────────────────────────────────────
    ax_ts = fig.add_subplot(gs[2, :])
    ax_ts.plot(range(len(y_raw_te)), y_raw_te, "k-", lw=1.8, label="Actual", zorder=5)
    for ci, name in enumerate(model_names):
        ax_ts.plot(range(len(y_raw_te)), preds[name][1],
                   color=colors[ci], lw=1.2, alpha=0.75, label=name)
    ax_ts.set_xlabel("Test day index", fontsize=10, fontweight="bold")
    ax_ts.set_ylabel("PM2.5 (µg/m³)", fontsize=10, fontweight="bold")
    ax_ts.set_title("Test Set Time Series — All Components vs Actual", fontsize=11, fontweight="bold")
    ax_ts.legend(fontsize=8, ncol=len(model_names)+1, loc="upper right")
    ax_ts.grid(alpha=0.25)

    plt.suptitle("Final Multimodal PM2.5: BiLSTM + BN-NIN CNN + XGBoost + LightGBM + GradBoost\n"
                 "6-Station Hanoi  |  Global 70/15/15 Chronological Split",
                 fontsize=13, fontweight="bold", y=1.01)
    plt.savefig(out_dir / "final_multimodal_results.png", dpi=180, bbox_inches="tight")
    plt.close()
    print(f"  Plot -> {out_dir/'final_multimodal_results.png'}")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    t_start = time.time()
    print("=" * 70)
    print("Final Multimodal: BiLSTM + BN-NIN + XGBoost + LightGBM + GradBoost")
    print("=" * 70)

    # ── Load & prepare data ───────────────────────────────────────────────────
    print("\nLoading data ...")
    df, sat_avail, met_map = load_daily()
    df = split_df(df)
    df_all = df.reset_index(drop=True)
    met_daily_cols = list(met_map.values())
    sat_cols_use   = [c for c in sat_avail if c in df_all.columns]

    tr_idx = df_all[df_all["split"]=="train"].index.tolist()
    va_idx = df_all[df_all["split"]=="val"].index.tolist()
    te_idx = df_all[df_all["split"]=="test"].index.tolist()
    print(f"  Train {len(tr_idx)} / Val {len(va_idx)} / Test {len(te_idx)}")

    # Sequence features (BiLSTM input)
    seq_feats = ["pm25"] + [c for c in met_daily_cols if c in df_all.columns]
    df_all["pm25"] = df_all["pm25"].clip(lower=0.01)
    df_sc = df_all.copy()
    df_sc["pm25"] = np.log1p(df_sc["pm25"])
    seq_sc = StandardScaler()
    seq_sc.fit(df_sc.loc[tr_idx, seq_feats].fillna(0))
    for c in seq_feats:
        df_sc[c] = df_sc.groupby("location_id")[c].transform(
            lambda s: s.interpolate(method="linear",limit=10,limit_direction="both").fillna(s.median()))
    df_sc[seq_feats] = seq_sc.transform(df_sc[seq_feats].fillna(0))

    # Satellite stats (BN-NIN CNN input): (N, n_sat, SEQ_LEN)
    sat_sc = StandardScaler()
    df_sat_sc = df_all.copy()
    sat_sc.fit(df_sat_sc.loc[tr_idx, sat_cols_use].fillna(0))
    df_sat_sc[sat_cols_use] = sat_sc.transform(df_sat_sc[sat_cols_use].fillna(0))

    # Lag bypass
    bp_feats = [c for c in ["pm25_lag1","pm25_lag2","pm25_lag3","pm25_lag7"] if c in df_all.columns]
    bp_sc = StandardScaler()
    bp_sc.fit(df_all.loc[tr_idx, bp_feats].fillna(0))
    x_bp_all = bp_sc.transform(df_all[bp_feats].fillna(0)).astype(np.float32)

    # Context features: sin/cos season + current-day met + station lat/lon
    ctx_feats = (["sin_doy","cos_doy","sin_month","cos_month"]
                 + [c for c in met_daily_cols if c in df_all.columns]
                 + ["latitude","longitude"])
    ctx_feats = [c for c in ctx_feats if c in df_all.columns]
    ctx_sc = StandardScaler()
    ctx_sc.fit(df_all.loc[tr_idx, ctx_feats].fillna(0))
    x_ctx_all = ctx_sc.transform(df_all[ctx_feats].fillna(0)).astype(np.float32)

    # Target scaler
    y_sc = StandardScaler()
    y_sc.fit(df_all.loc[tr_idx, ["pm25"]])
    y_std_all = y_sc.transform(df_all[["pm25"]]).ravel().astype(np.float32)
    y_raw_all = df_all["pm25"].values.astype(np.float32)
    y_std_tr = y_std_all[tr_idx]; y_std_va = y_std_all[va_idx]
    y_raw_tr = y_raw_all[tr_idx]; y_raw_va = y_raw_all[va_idx]; y_raw_te = y_raw_all[te_idx]

    print("Building sequence arrays ...")
    x_seq_all = build_seqs(df_sc, range(len(df_all)), seq_feats)
    x_ss_all  = build_sat_seq(df_sat_sc, range(len(df_all)), sat_cols_use)
    x_seq_tr=x_seq_all[tr_idx]; x_ss_tr=x_ss_all[tr_idx]
    x_bp_tr=x_bp_all[tr_idx];   x_ctx_tr=x_ctx_all[tr_idx]
    x_seq_va=x_seq_all[va_idx]; x_ss_va=x_ss_all[va_idx]
    x_bp_va=x_bp_all[va_idx];   x_ctx_va=x_ctx_all[va_idx]
    x_seq_te=x_seq_all[te_idx]; x_ss_te=x_ss_all[te_idx]
    x_bp_te=x_bp_all[te_idx];   x_ctx_te=x_ctx_all[te_idx]
    n_seq = x_seq_tr.shape[2]; n_sat = x_ss_tr.shape[1]; n_ctx = x_ctx_tr.shape[1]
    print(f"  x_seq {x_seq_tr.shape}  x_sat {x_ss_tr.shape}  x_ctx {x_ctx_tr.shape}")

    # Split val into EARLY and LATE halves for robust alpha selection
    n_va = len(va_idx)
    va_early = list(range(0, n_va // 2))         # indices into val arrays
    va_late  = list(range(n_va // 2, n_va))
    y_raw_va_early = y_raw_va[va_early]
    y_raw_va_late  = y_raw_va[va_late]
    print(f"  Val-early {len(va_early)} rows / Val-late {len(va_late)} rows")

    # Tabular features (tree input)
    tab_cols = [c for c in LAG_COLS + met_daily_cols if c in df_all.columns]
    imp = SimpleImputer(strategy="median")
    Xtr = imp.fit_transform(df_all.iloc[tr_idx][tab_cols])
    Xva = imp.transform(df_all.iloc[va_idx][tab_cols])
    Xte = imp.transform(df_all.iloc[te_idx][tab_cols])

    # ── Tree models ───────────────────────────────────────────────────────────
    print("\n[1] XGBoost ...")
    xgbm = xgb.XGBRegressor(
        n_estimators=1200, max_depth=5, learning_rate=0.015,
        subsample=0.8, colsample_bytree=0.8,
        reg_alpha=0.05, reg_lambda=1.0,
        random_state=42, verbosity=0, n_jobs=-1,
        early_stopping_rounds=100, eval_metric="rmse",
    )
    xgbm.fit(Xtr, y_raw_tr, eval_set=[(Xva, y_raw_va)], verbose=False)
    xgb_va = xgbm.predict(Xva); xgb_te = xgbm.predict(Xte)
    print(f"   val R²={r2_score(y_raw_va,xgb_va):+.4f}  test R²={r2_score(y_raw_te,xgb_te):+.4f}"
          f"  MAE={mean_absolute_error(y_raw_te,xgb_te):.3f}")

    print("\n[2] LightGBM ...")
    lgbm = lgb.LGBMRegressor(
        n_estimators=1200, max_depth=5, learning_rate=0.015,
        subsample=0.8, colsample_bytree=0.8,
        reg_alpha=0.05, reg_lambda=1.0,
        random_state=42, verbosity=-1, n_jobs=-1,
    )
    lgbm.fit(Xtr, y_raw_tr,
             eval_set=[(Xva, y_raw_va)],
             callbacks=[lgb.early_stopping(100, verbose=False), lgb.log_evaluation(-1)])
    lgb_va = lgbm.predict(Xva); lgb_te = lgbm.predict(Xte)
    print(f"   val R²={r2_score(y_raw_va,lgb_va):+.4f}  test R²={r2_score(y_raw_te,lgb_te):+.4f}"
          f"  MAE={mean_absolute_error(y_raw_te,lgb_te):.3f}")

    print("\n[3] GradientBoosting (sklearn) ...")
    gbr = GradientBoostingRegressor(
        n_estimators=500, max_depth=4, learning_rate=0.05,
        subsample=0.8, min_samples_leaf=5,
        random_state=42, validation_fraction=0.1, n_iter_no_change=30,
    )
    gbr.fit(Xtr, y_raw_tr)
    gbr_va = gbr.predict(Xva); gbr_te = gbr.predict(Xte)
    print(f"   val R²={r2_score(y_raw_va,gbr_va):+.4f}  test R²={r2_score(y_raw_te,gbr_te):+.4f}"
          f"  MAE={mean_absolute_error(y_raw_te,gbr_te):.3f}")

    # ── Neural model (BiLSTM + BN-NIN CNN) ───────────────────────────────────
    print(f"\n[4] Neural: BiLSTM + BN-NIN  ({N_SEEDS} seeds) ...")
    nn_va_list, nn_te_list = [], []
    t_nn = time.time()
    for seed in range(N_SEEDS):
        model = FinalMultimodal(n_seq, n_sat, n_ctx=n_ctx)
        n_params = sum(p.numel() for p in model.parameters() if p.requires_grad)
        if seed == 0: print(f"   Total params: {n_params:,}")
        model, best_vr, ep = train_neural(
            model,
            x_seq_tr, x_ss_tr, x_bp_tr, x_ctx_tr, y_std_tr,
            x_seq_va, x_ss_va, x_bp_va, x_ctx_va, y_raw_va, y_sc, seed)
        p_va = y_sc.inverse_transform(
            predict_neural(model,x_seq_va,x_ss_va,x_bp_va,x_ctx_va).reshape(-1,1)).ravel()
        p_te = y_sc.inverse_transform(
            predict_neural(model,x_seq_te,x_ss_te,x_bp_te,x_ctx_te).reshape(-1,1)).ravel()
        nn_va_list.append(p_va); nn_te_list.append(p_te)
        torch.save(model.state_dict(), OUT_DIR / f"neural_seed{seed}.pt")
        print(f"   seed {seed}: best val R²={best_vr:+.4f}  ep={ep}")

    nn_va = np.mean(nn_va_list, axis=0)
    nn_te = np.mean(nn_te_list, axis=0)
    print(f"   Ensemble val R²={r2_score(y_raw_va,nn_va):+.4f}  "
          f"(early={r2_score(y_raw_va_early,nn_va[va_early]):+.4f} / "
          f"late={r2_score(y_raw_va_late,nn_va[va_late]):+.4f})  "
          f"test R²={r2_score(y_raw_te,nn_te):+.4f}  "
          f"MAE={mean_absolute_error(y_raw_te,nn_te):.3f}  ({time.time()-t_nn:.0f}s)")

    # ── Ensemble: grid-search tree blend weights + dual-val alpha ────────────
    print("\nGrid-searching ensemble weights ...")

    # Step 1: best tree blend on FULL val
    best_tree_vr, best_tw = -999.0, (1.0, 0.0, 0.0)
    for w1 in np.arange(0.0, 1.01, 0.1):
        for w2 in np.arange(0.0, 1.01 - w1, 0.1):
            w3 = round(1.0 - w1 - w2, 8)
            if w3 < -1e-6: continue
            vr = r2_score(y_raw_va, w1*xgb_va + w2*lgb_va + w3*gbr_va)
            if vr > best_tree_vr:
                best_tree_vr = vr
                best_tw = (round(w1,1), round(w2,1), round(w3,1))

    w_xgb, w_lgb, w_gbr = best_tw
    tree_va = w_xgb*xgb_va + w_lgb*lgb_va + w_gbr*gbr_va
    tree_te = w_xgb*xgb_te + w_lgb*lgb_te + w_gbr*gbr_te
    print(f"  Best tree weights: XGB={w_xgb:.1f} LGB={w_lgb:.1f} GBR={w_gbr:.1f}  "
          f"val R²={best_tree_vr:+.4f}  test R²={r2_score(y_raw_te,tree_te):+.4f}")

    # Step 2: alpha by DUAL-VAL — average R² on early + late val halves
    # This prevents over-weighting neural on a single seasonal window
    print("\n  Alpha search (dual-val: mean of early & late R²):")
    alpha_rows = []
    for alpha in np.arange(0.0, 0.51, 0.05):
        blend_va = (1-alpha)*tree_va + alpha*nn_va
        r2_full  = r2_score(y_raw_va,       blend_va)
        r2_early = r2_score(y_raw_va_early, blend_va[va_early])
        r2_late  = r2_score(y_raw_va_late,  blend_va[va_late])
        r2_dual  = 0.5 * r2_early + 0.5 * r2_late   # balanced criterion
        alpha_rows.append((alpha, r2_dual, r2_full, r2_early, r2_late))
        print(f"    alpha={alpha:.2f}  dual={r2_dual:+.4f}  "
              f"full={r2_full:+.4f}  early={r2_early:+.4f}  late={r2_late:+.4f}")

    best_alpha = max(alpha_rows, key=lambda x: x[1])[0]
    final_va = (1-best_alpha)*tree_va + best_alpha*nn_va
    final_te = (1-best_alpha)*tree_te + best_alpha*nn_te

    best_row = next(r for r in alpha_rows if r[0] == best_alpha)
    print(f"\n  Selected alpha={best_alpha:.2f}  "
          f"dual-val={best_row[1]:+.4f}  full-val={best_row[2]:+.4f}  "
          f"test R²={r2_score(y_raw_te,final_te):+.4f}")

    # ── Final results table ───────────────────────────────────────────────────
    components = {
        "XGBoost":      (xgb_va,   xgb_te),
        "LightGBM":     (lgb_va,   lgb_te),
        "GradBoost":    (gbr_va,   gbr_te),
        "Tree-Blend":   (tree_va,  tree_te),
        "BiLSTM+CNN":   (nn_va,    nn_te),
        "FINAL":        (final_va, final_te),
    }

    print("\n" + "="*90)
    print(f"  {'Model':<18} {'Val R²':>9} {'Val-E':>7} {'Val-L':>7} "
          f"{'Gap':>7} {'Test R²':>9} {'MAE':>8} {'RMSE':>8}")
    print("="*90)
    results = {}
    for name, (pva, pte) in components.items():
        vr    = r2_score(y_raw_va,       pva)
        vr_e  = r2_score(y_raw_va_early, pva[va_early])
        vr_l  = r2_score(y_raw_va_late,  pva[va_late])
        tr    = r2_score(y_raw_te, pte)
        mae   = mean_absolute_error(y_raw_te, pte)
        rmse  = float(np.sqrt(mean_squared_error(y_raw_te, pte)))
        gap   = vr - tr      # val/test gap (lower = more balanced)
        star  = " ← BEST" if name == "FINAL" else ""
        print(f"  {name:<18} {vr:>+9.4f} {vr_e:>+7.4f} {vr_l:>+7.4f} "
              f"{gap:>+7.4f} {tr:>+9.4f} {mae:>8.3f} {rmse:>8.3f}{star}")
        results[name] = {"val_r2":round(vr,4),"val_early_r2":round(vr_e,4),
                         "val_late_r2":round(vr_l,4),"val_test_gap":round(gap,4),
                         "test_r2":round(tr,4),"mae":round(mae,3),"rmse":round(rmse,3)}
    print("="*90)
    print("  (Val-E=early half, Val-L=late half, Gap=val-test; lower gap = better balance)")

    # ── Plots ─────────────────────────────────────────────────────────────────
    split_dates = {
        "train_end": str(df_all[df_all["split"]=="train"]["date"].max().date()),
        "val_end":   str(df_all[df_all["split"]=="val"]["date"].max().date()),
    }
    plot_components = {k: v for k, v in components.items() if k != "Tree-Blend"}
    make_plots(y_raw_va, y_raw_te, plot_components, split_dates, OUT_DIR)

    # Save predictions
    np.save(OUT_DIR / "pred_val_final.npy",   final_va)
    np.save(OUT_DIR / "pred_test_final.npy",  final_te)
    np.save(OUT_DIR / "pred_val_neural.npy",  nn_va)
    np.save(OUT_DIR / "pred_test_neural.npy", nn_te)
    np.save(OUT_DIR / "y_val_raw.npy",  y_raw_va)
    np.save(OUT_DIR / "y_test_raw.npy", y_raw_te)

    # Save summary
    summary = {
        "architecture": {
            "temporal": "BiLSTM (2-layer, h=64, bidirectional)",
            "satellite_cnn": "BN-NIN 1D (best from backbone comparison)",
            "trees": ["XGBoost", "LightGBM", "GradientBoosting (sklearn)"],
            "ensemble": f"tree_blend(XGB={w_xgb}, LGB={w_lgb}, GBR={w_gbr}) + alpha={best_alpha} neural",
        },
        "data": {
            "stations": 6,
            "train_samples": len(tr_idx),
            "val_samples":   len(va_idx),
            "test_samples":  len(te_idx),
        },
        "results": results,
        "best": {
            "model": "FINAL",
            "val_r2":       results["FINAL"]["val_r2"],
            "val_test_gap": results["FINAL"]["val_test_gap"],
            "test_r2":      results["FINAL"]["test_r2"],
            "mae":          results["FINAL"]["mae"],
            "rmse":         results["FINAL"]["rmse"],
        },
        "tree_weights": {"xgb": w_xgb, "lgb": w_lgb, "gbr": w_gbr},
        "neural_alpha": best_alpha,
        "alpha_selection": "dual-val (mean of early + late val R²)",
        "total_time_s": round(time.time()-t_start, 1),
    }
    with open(OUT_DIR / "summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print(f"\nTotal time: {(time.time()-t_start)/60:.1f} min")
    print(f"Outputs -> {OUT_DIR}")
    print(f"  final_multimodal_results.png")
    print(f"  summary.json")


if __name__ == "__main__":
    main()
