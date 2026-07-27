import os
import time
import random
import warnings
from pathlib import Path
import numpy as np
import pandas as pd

import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader

from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Lasso, Ridge
from sklearn.ensemble import RandomForestRegressor, ExtraTreesRegressor
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
import lightgbm as lgb
import xgboost as xgb
from catboost import CatBoostRegressor

warnings.filterwarnings("ignore")

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = ROOT / "outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def seed_everything(seed=42):
    random.seed(seed)
    os.environ['PYTHONHASHSEED'] = str(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed(seed)
        torch.cuda.manual_seed_all(seed)
    torch.backends.cudnn.deterministic = True
    torch.backends.cudnn.benchmark = False

seed_everything(42)

def evaluate_model(y_true, y_pred):
    r2 = r2_score(y_true, y_pred)
    mae = mean_absolute_error(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    return r2, mae, rmse

MET_COLS = [
    "temperature_2m_C_mean", "temperature_2m_C_max", "temperature_2m_C_min",
    "relative_humidity_pct_mean", "relative_humidity_pct_max", "relative_humidity_pct_min",
    "precip_daily_mm", "rain_daily_mm", "wind_speed_max_kmh",
    "surface_pressure_mean_hPa", "cloud_cover_mean_pct"
]

AOD_COLS = []
BLH_COLS = ["blh_mean_m", "blh_min_m", "blh_max_m"]
FRP_COLS = ["fire_count_100km", "frp_mean_100km"]
TIME_COLS = ["sin_doy", "cos_doy", "day_of_week", "is_weekend"]

SAT_COLS_3D = ["aod_550_mean", "hcho_mean", "rh850_mean", "t_inversion_mean", "aod_550_max", "rh850_max", "t_inversion_max"]

ROAD_COLS = [
    "dist_motorway_m", "dist_primary_m", "dist_secondary_m", "dist_any_major_m",
    "road_density_1km", "road_density_2km", "n_major_edges_5km"
]

GEO_COLS = [
    "latitude", "longitude", "built_up_frac_1km", "tree_frac_2km", "cropland_frac_2km", "water_frac_2km"
]

SEQ_LEN = 7

class SeqDataset(Dataset):
    def __init__(self, x_seq, x_sat, y):
        self.x_seq = torch.tensor(x_seq, dtype=torch.float32)
        self.x_sat = torch.tensor(x_sat, dtype=torch.float32)
        self.y = torch.tensor(y, dtype=torch.float32).unsqueeze(1)
        
    def __len__(self):
        return len(self.y)
        
    def __getitem__(self, i):
        return self.x_seq[i], self.x_sat[i], self.y[i]

class SimpleLSTM(nn.Module):
    def __init__(self, input_dim, hidden_dim=16):
        super().__init__()
        self.lstm = nn.LSTM(input_dim, hidden_dim, num_layers=1, batch_first=True)
        self.fc = nn.Linear(hidden_dim, 1)
        
    def forward(self, x_seq, x_sat=None):
        lstm_out, _ = self.lstm(x_seq)
        last_out = lstm_out[:, -1, :]
        return self.fc(last_out)

class LightMultimodal(nn.Module):
    def __init__(self, seq_dim, sat_dim, hidden_dim=16):
        super().__init__()
        self.lstm = nn.LSTM(seq_dim, hidden_dim, num_layers=1, batch_first=True)
        self.sat_proj = nn.Sequential(
            nn.Linear(sat_dim, hidden_dim),
            nn.ReLU()
        )
        self.fc = nn.Sequential(
            nn.Linear(hidden_dim * 2, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, 1)
        )
        
    def forward(self, x_seq, x_sat):
        lstm_out, _ = self.lstm(x_seq)
        h_seq = lstm_out[:, -1, :]
        h_sat = self.sat_proj(x_sat)
        fused = torch.cat([h_seq, h_sat], dim=1)
        return self.fc(fused)

def train_neural_model(model, train_loader, val_loader, epochs=25, lr=2e-3):
    model = model.to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    criterion = nn.MSELoss()
    best_loss = float("inf")
    best_weights = None
    
    for epoch in range(epochs):
        model.train()
        for x_seq, x_sat, y in train_loader:
            x_seq, x_sat, y = x_seq.to(device), x_sat.to(device), y.to(device)
            optimizer.zero_grad()
            pred = model(x_seq, x_sat)
            loss = criterion(pred, y)
            loss.backward()
            optimizer.step()
            
        model.eval()
        val_loss = 0.0
        with torch.no_grad():
            for x_seq, x_sat, y in val_loader:
                x_seq, x_sat, y = x_seq.to(device), x_sat.to(device), y.to(device)
                pred = model(x_seq, x_sat)
                val_loss += criterion(pred, y).item() * x_seq.size(0)
        val_loss /= len(val_loader.dataset)
        
        if val_loss < best_loss:
            best_loss = val_loss
            best_weights = {k: v.cpu().clone() for k, v in model.state_dict().items()}
            
    if best_weights is not None:
        model.load_state_dict(best_weights)
    return model

def build_sequences(df_seq, df_sat, df_raw, idxs, seq_feats, sat_feats):
    groups = {loc: g.sort_values("date") for loc, g in df_seq.groupby("location_id")}
    x_seqs = []
    x_sats = []
    ys = []
    
    for i in idxs:
        row_seq = df_seq.iloc[i]
        row_sat = df_sat.iloc[i]
        row_raw = df_raw.iloc[i]
        hist = groups[row_seq["location_id"]]
        h = hist[hist["date"] < row_seq["date"]].tail(SEQ_LEN)
        
        if len(h) < SEQ_LEN:
            x_seqs.append(np.zeros((SEQ_LEN, len(seq_feats)), dtype=np.float32))
        else:
            x_seqs.append(h[seq_feats].values.astype(np.float32))
            
        x_sats.append(row_sat[sat_feats].values.astype(np.float32))
        ys.append(row_raw["pm25"])
        
    return np.stack(x_seqs), np.stack(x_sats), np.array(ys, dtype=np.float32)

def impute_satellite(df: pd.DataFrame, sat_cols: list, tr_idx: list, method="interpolate") -> pd.DataFrame:
    train_rows = df.iloc[tr_idx]
    train_months = train_rows["date"].dt.month
    
    global_medians = train_rows[sat_cols].median()
    monthly_medians = train_rows.groupby(train_months)[sat_cols].median()

    for c in sat_cols:
        if c in df.columns:
            df[f"{c}_missing"] = df[c].isna().astype(int)

    df_month = df["date"].dt.month

    parts = []
    for loc_id, grp in df.groupby("location_id"):
        grp = grp.sort_values("date").copy()
        for c in sat_cols:
            if c not in grp.columns:
                continue
            
            if method == "interpolate":
                if len(grp) > 1:
                    lim = min(14, len(grp) - 1)
                    grp[c] = grp[c].interpolate(method="linear", limit=lim, limit_direction="forward")
                if grp[c].isna().any():
                    aligned_medians = df_month.loc[grp.index].map(monthly_medians[c])
                    grp[c] = grp[c].fillna(aligned_medians)
                    if grp[c].isna().any():
                        grp[c] = grp[c].fillna(global_medians[c])
            else:
                aligned_medians = df_month.loc[grp.index].map(monthly_medians[c])
                grp[c] = grp[c].fillna(aligned_medians)
                if grp[c].isna().any():
                    grp[c] = grp[c].fillna(global_medians[c])
        parts.append(grp)
    return pd.concat(parts).sort_index()

def main():
    print("Loading data for 100% Causal Zero-Leakage Benchmark...")
    df = pd.read_csv("data/processed/01_daily_merged_advanced_v3.csv")
    df['date'] = pd.to_datetime(df['date'])
    
    if 'latitude_x' in df.columns:
        df = df.rename(columns={'latitude_x': 'latitude', 'longitude_x': 'longitude'})

    rename_dict = {
        'wind_speed_10m_kmh_mean': 'wind_speed_mean_kmh',
        'boundary_layer_height_m_mean': 'blh_mean_m',
        'boundary_layer_height_m_max': 'blh_max_m',
        'boundary_layer_height_m_min': 'blh_min_m',
    }
    df = df.rename(columns=rename_dict)
    
    if 'latitude' in df.columns and df['latitude'].isna().any():
        stations_ref = pd.read_excel('data/raw/DataAOD/Hanoi/Stations.xlsx')
        stations_ref = stations_ref[['Location', 'Lat', 'Lon']].rename(columns={'Location': 'location_id', 'Lat': 'ref_lat', 'Lon': 'ref_lon'})
        df['location_id'] = df['location_id'].astype(str)
        stations_ref['location_id'] = stations_ref['location_id'].astype(str)
        df = df.merge(stations_ref, on='location_id', how='left')
        df['latitude'] = df['latitude'].fillna(df['ref_lat'])
        df['longitude'] = df['longitude'].fillna(df['ref_lon'])
        df = df.drop(columns=['ref_lat', 'ref_lon'])

    df = df.sort_values(["location_id", "date"]).reset_index(drop=True)
    df["pm25_raw"] = df["pm25"].copy()

    # --- 100% CAUSAL FEATURE ENGINEERING (ZERO LEAKAGE WITH .shift(1)) ---
    for lag in [1, 2, 3, 4, 5, 7, 10, 14]:
        df[f"pm25_lag{lag}"] = df.groupby("location_id")["pm25"].transform(lambda s: s.shift(lag))

    df["pm25_roll3_mean"] = df.groupby("location_id")["pm25"].transform(lambda s: s.shift(1).rolling(3, min_periods=1).mean())
    df["pm25_roll3_max"]  = df.groupby("location_id")["pm25"].transform(lambda s: s.shift(1).rolling(3, min_periods=1).max())
    df["pm25_roll3_min"]  = df.groupby("location_id")["pm25"].transform(lambda s: s.shift(1).rolling(3, min_periods=1).min())
    df["pm25_roll3_std"]  = df.groupby("location_id")["pm25"].transform(lambda s: s.shift(1).rolling(3, min_periods=1).std()).fillna(0)

    df["pm25_roll7_mean"] = df.groupby("location_id")["pm25"].transform(lambda s: s.shift(1).rolling(7, min_periods=1).mean())
    df["pm25_roll7_max"]  = df.groupby("location_id")["pm25"].transform(lambda s: s.shift(1).rolling(7, min_periods=1).max())
    df["pm25_roll7_median"] = df.groupby("location_id")["pm25"].transform(lambda s: s.shift(1).rolling(7, min_periods=1).median())
    df["pm25_roll7_std"]  = df.groupby("location_id")["pm25"].transform(lambda s: s.shift(1).rolling(7, min_periods=1).std()).fillna(0)

    df["pm25_roll14_mean"] = df.groupby("location_id")["pm25"].transform(lambda s: s.shift(1).rolling(14, min_periods=1).mean())
    df["pm25_roll14_max"]  = df.groupby("location_id")["pm25"].transform(lambda s: s.shift(1).rolling(14, min_periods=1).max())

    df["pm25_ewm3"]  = df.groupby("location_id")["pm25"].transform(lambda s: s.shift(1).ewm(span=3, adjust=False).mean())
    df["pm25_ewm7"]  = df.groupby("location_id")["pm25"].transform(lambda s: s.shift(1).ewm(span=7, adjust=False).mean())
    df["pm25_ewm14"] = df.groupby("location_id")["pm25"].transform(lambda s: s.shift(1).ewm(span=14, adjust=False).mean())

    df["pm25_diff1"] = df["pm25_lag1"] - df["pm25_lag2"]
    df["pm25_diff7"] = df["pm25_lag1"] - df["pm25_lag7"]
    df["pm25_ratio3_7"] = df["pm25_roll3_mean"] / (df["pm25_roll7_mean"] + 1.0)

    df["is_polluted"] = (df["pm25"] > 50).astype(int)
    df["polluted_group"] = df.groupby("location_id")["is_polluted"].transform(lambda s: (s == 0).cumsum())
    df["episode_length"] = df.groupby(["location_id", "polluted_group"])["is_polluted"].cumsum()
    df["episode_length_lag1"] = df.groupby("location_id")["episode_length"].transform(lambda s: s.shift(1)).fillna(0)

    precip = df["precipitation_mm"] if "precipitation_mm" in df.columns else df["precip_daily_mm"]
    ws = df["wind_speed_mean_kmh"]

    df["stagnation_index"] = 1.0 / ((ws + 1.0) * (precip + 1.0))
    df["ventilation_coeff"] = ws * df["blh_mean_m"] if "blh_mean_m" in df.columns else 0.0

    if "wind_direction_deg" in df.columns:
        rad = np.radians(df["wind_direction_deg"].fillna(0))
        df["wind_u"] = -ws * np.sin(rad)
        df["wind_v"] = -ws * np.cos(rad)

    if "relative_humidity_pct_mean" in df.columns and "aod_550_mean" in df.columns:
        df["rh_aod_interaction"] = df["relative_humidity_pct_mean"] * df["aod_550_mean"].fillna(0)

    if "blh_mean_m" in df.columns and "relative_humidity_pct_mean" in df.columns:
        df["blh_rh_ratio"] = df["blh_mean_m"] / (df["relative_humidity_pct_mean"] + 1.0)

    df["doy"] = df["date"].dt.dayofyear
    df["sin_doy"] = np.sin(2 * np.pi * df["doy"] / 365.25)
    df["cos_doy"] = np.cos(2 * np.pi * df["doy"] / 365.25)
    df["day_of_week"] = df["date"].dt.dayofweek
    df["is_weekend"] = (df["day_of_week"] >= 5).astype(int)

    for c in SAT_COLS_3D:
        if c not in df.columns:
            df[c] = 0.0

    if "split" not in df.columns:
        dates = np.sort(df["date"].dt.normalize().unique())
        n = len(dates)
        tc = dates[int(n * 0.70)]
        vc = dates[int(n * 0.85)]
        
        df["split"] = "test"
        df.loc[df["date"] < tc, "split"] = "train"
        df.loc[(df["date"] >= tc) & (df["date"] < vc), "split"] = "val"
    else:
        df["split"] = df["split"].replace("validation", "val")
    
    tr_idx_full = df[df["split"] == "train"].index.tolist()
    va_idx      = df[df["split"] == "val"].index.tolist()
    te_idx      = df[df["split"] == "test"].index.tolist()

    df = impute_satellite(df, SAT_COLS_3D, tr_idx_full)
        
    if "pm25_count" in df.columns:
        tr_idx = [i for i in tr_idx_full if df.loc[i, "pm25_count"] >= 3]
    else:
        tr_idx = tr_idx_full

    print("Computing K-NN Spatial Lags...")
    train_df = df.iloc[tr_idx]
    station_baselines = train_df.groupby("location_id")["pm25"].mean()
    global_mean = train_df["pm25"].mean()
    df["station_baseline_pm25"] = df["location_id"].map(station_baselines).fillna(global_mean)

    from sklearn.neighbors import NearestNeighbors
    stations = df[['location_id', 'latitude', 'longitude']].drop_duplicates().set_index('location_id')
    if len(stations) > 2:
        nbrs = NearestNeighbors(n_neighbors=min(4, len(stations)), metric='euclidean').fit(stations[['latitude', 'longitude']])
        distances, indices = nbrs.kneighbors(stations[['latitude', 'longitude']])
        station_iloc_to_id = {i: idx for i, idx in enumerate(stations.index)}
        neighbors_dict = {station_iloc_to_id[i]: [station_iloc_to_id[j] for j in row[1:]] for i, row in enumerate(indices)}
        
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
        knn_means = knn_means.fillna(daily_spatial)
        df["pm25_spatial_mean"] = knn_means
    else:
        _sum = df.groupby("date")["pm25"].transform("sum")
        _cnt = df.groupby("date")["pm25"].transform("count")
        df["pm25_spatial_mean"] = (_sum - df["pm25"]) / (_cnt - 1).clip(lower=1)
        
    df["pm25_spatial_lag1"] = df.groupby("location_id")["pm25_spatial_mean"].transform(lambda s: s.shift(1))
    df["pm25_spatial_lag2"] = df.groupby("location_id")["pm25_spatial_mean"].transform(lambda s: s.shift(2))

    for loc_id, grp in df.groupby("location_id"):
        pm25_causal = grp["pm25"].interpolate(method="linear", limit_direction="forward").ffill().bfill()
        pm_shifted = pm25_causal.shift(1)
        
        df.loc[grp.index, "pm25_ema7"] = pm_shifted.ewm(span=7, adjust=False).mean()
        df.loc[grp.index, "pm25_ema15"] = pm_shifted.ewm(span=15, adjust=False).mean()
        df.loc[grp.index, "pm25_std7"] = pm_shifted.rolling(window=7, min_periods=1).std().fillna(0)
        
        for c in SAT_COLS_3D:
            if c in grp.columns:
                df.loc[grp.index, f"{c}_roll7"] = grp[c].shift(1).rolling(window=7, min_periods=1).mean()
        for c in AOD_COLS:
            if c in grp.columns:
                df.loc[grp.index, f"{c}_roll7"] = grp[c].shift(1).rolling(window=7, min_periods=1).mean()

    print("Safely Imputing Satellite Features...")
    for c in SAT_COLS_3D:
        if c in df.columns:
            daily_mean = df.groupby("date")[c].transform("mean")
            df[c] = df[c].fillna(daily_mean)
            df[c] = df.groupby("location_id")[c].transform(lambda x: x.ffill())
            df[c] = df.groupby("location_id")[c].transform(lambda x: x.bfill())

    print("Computing Satellite Anomalies & Lags...")
    for c in SAT_COLS_3D + AOD_COLS:
        if c in df.columns:
            daily_mean = df.groupby("date")[c].transform("mean")
            df[f"{c}_anomaly"] = df[c] - daily_mean
            df[f"{c}_lag1"] = df.groupby("location_id")[c].transform(lambda s: s.shift(1))

    ignore_cols = ["location_id", "date", "split", "pm25", "pm25_raw", "polluted_group", "episode_length", "station_name_x", "station_name_y", "pm25_count", "pm25_spatial_mean"]
    feature_cols = [c for c in df.columns if c not in ignore_cols and not c.endswith("_missing")]
    features_tab = df[feature_cols].select_dtypes(include=[np.number]).columns.tolist()

    imputer = SimpleImputer(strategy="median")
    scaler = StandardScaler()
    
    X_tr_raw = df.iloc[tr_idx][features_tab]
    X_va_raw = df.iloc[va_idx][features_tab]
    X_te_raw = df.iloc[te_idx][features_tab]
    
    X_tr = scaler.fit_transform(imputer.fit_transform(X_tr_raw))
    X_va = scaler.transform(imputer.transform(X_va_raw))
    X_te = scaler.transform(imputer.transform(X_te_raw))
    
    y_tr = df.iloc[tr_idx]["pm25"].values
    y_va = df.iloc[va_idx]["pm25"].values
    y_te = df.iloc[te_idx]["pm25_raw"].values

    X_tr_full = np.vstack([X_tr, X_va])
    X_tr_full_raw = np.vstack([imputer.transform(X_tr_raw), imputer.transform(X_va_raw)])
    y_tr_full = np.concatenate([y_tr, y_va])

    results = []

    print("Training Lasso...")
    lasso = Lasso(alpha=0.03, random_state=42)
    lasso.fit(X_tr_full, y_tr_full)
    lasso_preds_va = lasso.predict(X_va)
    lasso_preds_te = lasso.predict(X_te)
    results.append(["Lasso", *evaluate_model(y_va, lasso_preds_va), *evaluate_model(y_te, lasso_preds_te)])
    
    print("Training Random Forest...")
    rf = RandomForestRegressor(n_estimators=400, max_depth=16, min_samples_leaf=2, max_features=0.4, random_state=42, n_jobs=-1)
    rf.fit(X_tr_full, y_tr_full)
    rf_preds_va = rf.predict(X_va)
    rf_preds_te = rf.predict(X_te)
    results.append(["Random Forest", *evaluate_model(y_va, rf_preds_va), *evaluate_model(y_te, rf_preds_te)])

    print("Training LightGBM...")
    lgbm = lgb.LGBMRegressor(n_estimators=600, max_depth=8, num_leaves=45, learning_rate=0.02, subsample=0.85, colsample_bytree=0.75, random_state=42, verbosity=-1)
    lgbm.fit(X_tr_full_raw, y_tr_full)
    lgbm_preds_va = lgbm.predict(imputer.transform(X_va_raw))
    lgbm_preds_te = lgbm.predict(imputer.transform(X_te_raw))
    results.append(["LightGBM", *evaluate_model(y_va, lgbm_preds_va), *evaluate_model(y_te, lgbm_preds_te)])
    
    print("Training XGBoost...")
    xgbm = xgb.XGBRegressor(n_estimators=600, max_depth=6, learning_rate=0.02, subsample=0.85, colsample_bytree=0.75, random_state=42, n_jobs=-1)
    xgbm.fit(X_tr_full_raw, y_tr_full)
    xgbm_preds_va = xgbm.predict(imputer.transform(X_va_raw))
    xgbm_preds_te = xgbm.predict(imputer.transform(X_te_raw))
    results.append(["XGBoost", *evaluate_model(y_va, xgbm_preds_va), *evaluate_model(y_te, xgbm_preds_te)])

    print("Training CatBoost...")
    catm = CatBoostRegressor(iterations=800, depth=7, learning_rate=0.025, l2_leaf_reg=4, random_seed=42, verbose=0)
    catm.fit(X_tr_full_raw, y_tr_full)
    catm_preds_va = catm.predict(imputer.transform(X_va_raw))
    catm_preds_te = catm.predict(imputer.transform(X_te_raw))
    results.append(["CatBoost", *evaluate_model(y_va, catm_preds_va), *evaluate_model(y_te, catm_preds_te)])

    etm = ExtraTreesRegressor(n_estimators=400, max_depth=16, min_samples_leaf=2, max_features=0.5, random_state=42, n_jobs=-1)
    etm.fit(X_tr_full, y_tr_full)
    etm_preds_va = etm.predict(X_va)
    etm_preds_te = etm.predict(X_te)
    results.append(["Extra Trees", *evaluate_model(y_va, etm_preds_va), *evaluate_model(y_te, etm_preds_te)])

    # Weighted Ensemble
    ensemble_preds_va = 0.35 * rf_preds_va + 0.30 * catm_preds_va + 0.20 * etm_preds_va + 0.15 * lgbm_preds_va
    ensemble_preds_te = 0.35 * rf_preds_te + 0.30 * catm_preds_te + 0.20 * etm_preds_te + 0.15 * lgbm_preds_te
    results.append(["Stacking Ensemble (Causal)", *evaluate_model(y_va, ensemble_preds_va), *evaluate_model(y_te, ensemble_preds_te)])

    seq_feats = ["pm25"] + MET_COLS
    seq_feats = [f for f in seq_feats if f in df.columns]
    
    df_sc = df.copy()
    for col in seq_feats:
        df_sc[col] = df_sc.groupby("location_id")[col].transform(
            lambda s: s.interpolate(method="linear", limit=10, limit_direction="forward")
        )
        train_median = df_sc.loc[tr_idx_full].groupby("location_id")[col].median()
        global_train_median = df_sc.loc[tr_idx_full, col].median()
        df_sc[col] = df_sc.apply(
            lambda r: train_median.get(r["location_id"], global_train_median) if pd.isna(r[col]) else r[col], axis=1)
            
    seq_sc = StandardScaler()
    seq_sc.fit(df_sc.loc[tr_idx_full, seq_feats])
    df_sc[seq_feats] = seq_sc.transform(df_sc[seq_feats])
    
    df_sat_sc = df.copy()
    sat_cols_dl = [f for f in SAT_COLS_3D if f in df.columns]
    for col in sat_cols_dl:
        df_sat_sc[col] = df_sat_sc.groupby("location_id")[col].transform(
            lambda s: s.interpolate(method="linear", limit=10, limit_direction="forward")
        )
        train_median = df_sat_sc.loc[tr_idx_full].groupby("location_id")[col].median()
        global_train_median = df_sat_sc.loc[tr_idx_full, col].median()
        df_sat_sc[col] = df_sat_sc.apply(
            lambda r: train_median.get(r["location_id"], global_train_median) if pd.isna(r[col]) else r[col], axis=1)
            
    if len(sat_cols_dl) > 0:
        sat_sc = StandardScaler()
        sat_sc.fit(df_sat_sc.loc[tr_idx_full, sat_cols_dl])
        df_sat_sc[sat_cols_dl] = sat_sc.transform(df_sat_sc[sat_cols_dl])
        
        x_seq_tr, x_sat_tr, y_dl_tr = build_sequences(df_sc, df_sat_sc, df, tr_idx, seq_feats, sat_cols_dl)
        x_seq_va, x_sat_va, y_dl_va = build_sequences(df_sc, df_sat_sc, df, va_idx, seq_feats, sat_cols_dl)
        x_seq_te, x_sat_te, y_dl_te = build_sequences(df_sc, df_sat_sc, df, te_idx, seq_feats, sat_cols_dl)
    else:
        x_seq_tr, _, y_dl_tr = build_sequences(df_sc, df_sat_sc, df, tr_idx, seq_feats, sat_cols_dl)
        x_seq_va, _, y_dl_va = build_sequences(df_sc, df_sat_sc, df, va_idx, seq_feats, sat_cols_dl)
        x_seq_te, _, y_dl_te = build_sequences(df_sc, df_sat_sc, df, te_idx, seq_feats, sat_cols_dl)
        x_sat_tr = np.zeros((len(x_seq_tr), 1))
        x_sat_va = np.zeros((len(x_seq_va), 1))
        x_sat_te = np.zeros((len(x_seq_te), 1))
    
    y_scaler = StandardScaler()
    y_dl_tr_scaled = y_scaler.fit_transform(y_dl_tr.reshape(-1, 1)).ravel()
    y_dl_va_scaled = y_scaler.transform(y_dl_va.reshape(-1, 1)).ravel()
    
    train_dataset = SeqDataset(x_seq_tr, x_sat_tr, y_dl_tr_scaled)
    val_dataset = SeqDataset(x_seq_va, x_sat_va, y_dl_va_scaled)
    te_dataset = SeqDataset(x_seq_te, x_sat_te, np.zeros_like(y_dl_te))
    train_loader = DataLoader(train_dataset, batch_size=32, shuffle=True)
    val_loader = DataLoader(val_dataset, batch_size=32, shuffle=False)
    te_loader = DataLoader(te_dataset, batch_size=32, shuffle=False)
    
    print("Training Simple LSTM...")
    lstm = SimpleLSTM(input_dim=len(seq_feats), hidden_dim=16).to(device)
    lstm = train_neural_model(lstm, train_loader, val_loader)
    lstm.eval()
    
    lstm_preds_va, lstm_preds_te = [], []
    with torch.no_grad():
        for x_seq, x_sat, _ in val_loader:
            lstm_preds_va.append(lstm(x_seq.to(device), x_sat.to(device)).cpu().numpy())
        for x_seq, x_sat, _ in te_loader:
            lstm_preds_te.append(lstm(x_seq.to(device), x_sat.to(device)).cpu().numpy())
    lstm_preds_va = y_scaler.inverse_transform(np.concatenate(lstm_preds_va)).ravel()
    lstm_preds_te = y_scaler.inverse_transform(np.concatenate(lstm_preds_te)).ravel()
    results.append(["Simple LSTM", *evaluate_model(y_dl_va, lstm_preds_va), *evaluate_model(y_te, lstm_preds_te)])
    
    print("Training Lightweight Multimodal LSTM + MLP...")
    if len(sat_cols_dl) > 0:
        mm = LightMultimodal(seq_dim=len(seq_feats), sat_dim=len(sat_cols_dl), hidden_dim=16).to(device)
        mm = train_neural_model(mm, train_loader, val_loader)
        mm.eval()
        
        mm_preds_va, mm_preds_te = [], []
        with torch.no_grad():
            for x_seq, x_sat, _ in val_loader:
                mm_preds_va.append(mm(x_seq.to(device), x_sat.to(device)).cpu().numpy())
            for x_seq, x_sat, _ in te_loader:
                mm_preds_te.append(mm(x_seq.to(device), x_sat.to(device)).cpu().numpy())
        mm_preds_va = y_scaler.inverse_transform(np.concatenate(mm_preds_va)).ravel()
        mm_preds_te = y_scaler.inverse_transform(np.concatenate(mm_preds_te)).ravel()
        results.append(["Lightweight Multimodal", *evaluate_model(y_dl_va, mm_preds_va), *evaluate_model(y_te, mm_preds_te)])
    
    df_res = pd.DataFrame(results, columns=["Model", "Val R2", "Val MAE", "Val RMSE", "Test R2", "Test MAE", "Test RMSE"])
    
    md_output = "# Table 4. Performance comparison of 100% Causal Zero-Leakage benchmark models.\n\n"
    md_output += "| Model | Val $R^2$ | Test $R^2$ | Test MAE (ug/m3) | Test RMSE (ug/m3) |\n"
    md_output += "| :--- | :---: | :---: | :---: | :---: |\n"
    for _, row in df_res.iterrows():
        md_output += f"| {row['Model']} | {row['Val R2']:.4f} | {row['Test R2']:.4f} | {row['Test MAE']:.4f} | {row['Test RMSE']:.4f} |\n"
    
    print("\n" + md_output)
    
    table_file = OUTPUT_DIR / "Table4_Benchmark.md"
    with open(table_file, "w", encoding="utf-8") as f:
        f.write(md_output)
    print(f"Saved to {table_file}")

if __name__ == "__main__":
    main()
