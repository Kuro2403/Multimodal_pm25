import pandas as pd
import numpy as np
import warnings
import os
import random

from sklearn.model_selection import KFold
from sklearn.metrics import r2_score
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.neighbors import NearestNeighbors
from sklearn.linear_model import Ridge, Lasso
from sklearn.ensemble import StackingRegressor, RandomForestRegressor

import xgboost as xgb
import lightgbm as lgb

warnings.filterwarnings("ignore")

def seed_everything(seed=42):
    random.seed(seed)
    os.environ['PYTHONHASHSEED'] = str(seed)
    np.random.seed(seed)

def prepare_data():
    print("Loading data...")
    df = pd.read_csv("data/processed/daily_merged_advanced_v3.csv")
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

    # --- TOP 5 NEW FEATURES ---
    if "wind_speed_mean_kmh" in df.columns and "precipitation_mm" in df.columns:
        df["stagnation_index"] = 1.0 / (df["wind_speed_mean_kmh"] * df["precipitation_mm"] + 1.0)
    else:
        df["stagnation_index"] = 0.0
    
    if "relative_humidity_pct_mean" in df.columns and "aod_550_mean" in df.columns:
        df["rh_aod_interaction"] = df["relative_humidity_pct_mean"] * df["aod_550_mean"]
    else:
        df["rh_aod_interaction"] = 0.0

    for lag in [1, 2, 3]:
        df[f"pm25_lag{lag}"] = df.groupby("location_id")["pm25"].transform(lambda s: s.shift(lag))

    df["pm25_lag_decay"] = df["pm25_lag1"].fillna(0) * 0.5 + df["pm25_lag2"].fillna(0) * 0.25 + df["pm25_lag3"].fillna(0) * 0.125
    df["pm25_roll3_max"] = df.groupby("location_id")["pm25"].transform(lambda s: s.rolling(3, min_periods=1).max())
    df["pm25_roll7_median"] = df.groupby("location_id")["pm25"].transform(lambda s: s.rolling(7, min_periods=1).median())
    
    df["is_polluted"] = (df["pm25"] > 50).astype(int)
    df["polluted_group"] = df.groupby("location_id")["is_polluted"].transform(lambda s: (s == 0).cumsum())
    df["episode_length"] = df.groupby(["location_id", "polluted_group"])["is_polluted"].cumsum()
    df["episode_length_lag1"] = df.groupby("location_id")["episode_length"].transform(lambda s: s.shift(1)).fillna(0)

    # Missing flags
    SAT_COLS_3D = ["aod_550_mean", "hcho_mean", "rh850_mean", "t_inversion_mean", "aod_550_max", "rh850_max", "t_inversion_max"]
    for c in SAT_COLS_3D:
        if c not in df.columns:
            df[c] = 0.0
        df[f"{c}_missing"] = df[c].isna().astype(int)
    
    # 3. Data Split
    test_dates = df['date'].dt.year >= 2026
    val_dates = (df['date'].dt.year == 2022) & (df['date'].dt.month >= 7)
    train_dates = ~(test_dates | val_dates)
    
    tr_idx = np.where(train_dates)[0]
    te_idx = np.where(test_dates)[0]
    
    # K-NN Spatial Lags
    train_df = df.iloc[tr_idx]
    station_baselines = train_df.groupby("location_id")["pm25"].mean()
    global_mean = train_df["pm25"].mean()
    df["station_baseline_pm25"] = df["location_id"].map(station_baselines).fillna(global_mean)

    stations = df[['location_id', 'latitude', 'longitude']].drop_duplicates().set_index('location_id')
    if len(stations) > 2:
        nbrs = NearestNeighbors(n_neighbors=min(3, len(stations)), metric='euclidean').fit(stations[['latitude', 'longitude']])
        distances, indices = nbrs.kneighbors(stations[['latitude', 'longitude']])
        station_iloc_to_id = {i: idx for i, idx in enumerate(stations.index)}
        neighbors_dict = {}
        for i, row in enumerate(indices):
            loc_id = station_iloc_to_id[i]
            neighbors_dict[loc_id] = [station_iloc_to_id[j] for j in row[1:]]
            
        pm25_pivot = df.pivot_table(index='date', columns='location_id', values='pm25')
        def get_spatial_lag(row):
            loc = row['location_id']
            d = row['date']
            neighbors = neighbors_dict.get(loc, [])
            if not neighbors:
                return np.nan
            vals = []
            for n in neighbors:
                try:
                    vals.append(pm25_pivot.at[d, n])
                except KeyError:
                    pass
            return np.nanmean(vals) if vals else np.nan

        df["pm25_spatial_mean"] = df.apply(get_spatial_lag, axis=1)
        df["pm25_spatial_lag1"] = df.groupby("location_id")["pm25_spatial_mean"].shift(1)
        
        tr_sp = df.iloc[tr_idx]["pm25_spatial_lag1"]
        sp_mean = tr_sp.mean()
        df["pm25_spatial_lag1"] = df["pm25_spatial_lag1"].fillna(sp_mean)
    else:
        df["pm25_spatial_lag1"] = 0.0

    # Satellite Imputation & Lags
    sat_cols = SAT_COLS_3D
    train_rows = df.iloc[tr_idx]
    train_months = train_rows["date"].dt.month
    global_medians = train_rows[sat_cols].median()
    monthly_medians = train_rows.groupby(train_months)[sat_cols].median()

    parts = []
    for loc_id, grp in df.groupby("location_id"):
        grp = grp.sort_values("date").copy()
        for c in sat_cols:
            if c not in grp.columns:
                continue
            grp[f"{c}_lag1"] = grp[c].shift(1)
            grp[f"{c}_roll3"] = grp[c].rolling(3, min_periods=1).mean()
            grp[f"{c}_anom"] = grp[c] - grp["date"].dt.month.map(monthly_medians[c]).fillna(global_medians[c])
            if grp[c].isna().any():
                grp[c] = grp[c].fillna(global_medians[c])
        parts.append(grp)
    
    df = pd.concat(parts).sort_index()

    # Create target features_tab
    MET_COLS = ['temperature_2m_C_mean', 'temperature_2m_C_max', 'temperature_2m_C_min', 'relative_humidity_pct_mean', 'relative_humidity_pct_min', 'relative_humidity_pct_max', 'precipitation_mm', 'surface_pressure_hPa', 'wind_speed_mean_kmh', 'wind_speed_10m_kmh_max', 'wind_direction_deg']
    AOD_COLS = ["aod_mean"]
    BLH_COLS = ["blh_mean_m", "blh_min_m", "blh_max_m"]
    FRP_COLS = []
    TIME_COLS = ["day_of_year", "month", "day_of_week", "is_weekend"]
    PHYSICS_COLS = ["ventilation_coeff"]
    GEO_COLS = ["latitude", "longitude", "tree_frac_1km", "shrubland_frac_1km", "grassland_frac_1km", "cropland_frac_1km", "built_up_frac_1km", "bare_frac_1km", "water_frac_1km", "ndvi_buffer_1km"]
    ROAD_COLS = ["dist_motorway_m", "dist_primary_m", "dist_secondary_m", "dist_any_major_m", "road_density_1km"]
    
    ADVANCED_LAGS = ["pm25_lag1", "pm25_lag2", "pm25_lag3", "pm25_lag7", "pm25_spatial_lag1", "station_baseline_pm25"]
    SAT_ROLLING = [f"{c}_roll3" for c in SAT_COLS_3D]
    SAT_ANOMALIES = [f"{c}_anom" for c in SAT_COLS_3D]
    SAT_LAGS = [f"{c}_lag1" for c in SAT_COLS_3D]
    MISSING_FLAGS = [f"{c}_missing" for c in SAT_COLS_3D]

    # Additional standard features
    df['day_of_year'] = df['date'].dt.dayofyear
    df['month'] = df['date'].dt.month
    df['day_of_week'] = df['date'].dt.dayofweek
    df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
    
    if "wind_speed_mean_kmh" in df.columns and "blh_mean_m" in df.columns:
        df["ventilation_coeff"] = df["wind_speed_mean_kmh"] * df["blh_mean_m"]
    else:
        df["ventilation_coeff"] = 0.0

    features_tab = (
        MET_COLS + AOD_COLS + BLH_COLS + FRP_COLS + TIME_COLS + PHYSICS_COLS +
        ADVANCED_LAGS + GEO_COLS + ROAD_COLS + SAT_COLS_3D + SAT_ROLLING + SAT_ANOMALIES + SAT_LAGS +
        MISSING_FLAGS + ["stagnation_index", "rh_aod_interaction", "pm25_lag_decay", "pm25_roll3_max", "pm25_roll7_median", "episode_length_lag1"]
    )
    
    features_tab = [f for f in features_tab if f in df.columns]
    features_tab = list(dict.fromkeys(features_tab)) # Deduplicate

    print(f"Total features: {len(features_tab)}")

    imputer = SimpleImputer(strategy="median")
    scaler = StandardScaler()
    
    X_tr_raw = df.iloc[tr_idx][features_tab]
    X_te_raw = df.iloc[te_idx][features_tab]
    
    y_tr = df.iloc[tr_idx]["pm25"].values
    y_te = df.iloc[te_idx]["pm25"].values
    
    X_tr = scaler.fit_transform(imputer.fit_transform(X_tr_raw))
    X_te = scaler.transform(imputer.transform(X_te_raw))
    
    return X_tr, X_te, y_tr, y_te

if __name__ == "__main__":
    seed_everything(42)
    X_tr, X_te, y_tr, y_te = prepare_data()
    
    print("\\n--- Training Base Models ---")
    

    estimators = [
        ('rf', RandomForestRegressor(n_estimators=100, max_depth=8, random_state=42, n_jobs=-1)),
        ('xgb', xgb.XGBRegressor(n_estimators=400, max_depth=8, learning_rate=0.05, subsample=0.8, colsample_bytree=0.8, random_state=42, n_jobs=-1)),
        ('lgb', lgb.LGBMRegressor(n_estimators=400, max_depth=8, learning_rate=0.05, subsample=0.8, colsample_bytree=0.8, random_state=42, n_jobs=-1, verbosity=-1)),
        ('lasso', Lasso(alpha=0.1, random_state=42))
    ]
    
    for name, model in estimators:
        model.fit(X_tr, y_tr)
        preds = model.predict(X_te)
        print(f"{name.upper()} Test R2: {r2_score(y_te, preds):.4f}")
    
    print("\\n--- Training Stacking Ensemble ---")
    stack_reg = StackingRegressor(
        estimators=estimators,
        final_estimator=Ridge(alpha=1.0),
        cv=5,
        n_jobs=-1
    )
    
    stack_reg.fit(X_tr, y_tr)
    stack_preds = stack_reg.predict(X_te)
    final_r2 = r2_score(y_te, stack_preds)
    
    print(f"\\nFinal Stacked Ensemble Test R2: {final_r2:.4f}")
    
    if final_r2 >= 0.90:
        print("🎉 GOAL ACHIEVED! R2 >= 0.90! 🎉")
    else:
        print(f"Almost there... R2 is {final_r2:.4f}")
