import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import folium
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
STATIONS_FILE = ROOT / "data/raw/DataAOD/Hanoi/Stations.xlsx"
TRAIN_FILE = ROOT / "data/processed/daily_merged_advanced_v3.csv"
OUT_DIR = ROOT / "outputs"
OUT_DIR.mkdir(parents=True, exist_ok=True)

def create_maps():
    print(f"Loading stations from {STATIONS_FILE}...")
    try:
        df = pd.read_excel(STATIONS_FILE)
    except Exception as e:
        print(f"Failed to load file: {e}")
        return

    if not all(col in df.columns for col in ['Lat', 'Lon', 'Location']):
        print("Required columns (Lat, Lon, Location) not found in Excel file.")
        return

    print("Filtering to match exactly the stations in the training dataset...")
    try:
        train_df = pd.read_csv(TRAIN_FILE, usecols=['location_id'])
        valid_stations = set(train_df['location_id'].astype(str).str.strip())
        df['Location'] = df['Location'].astype(str).str.strip()
        df = df[df['Location'].isin(valid_stations)].reset_index(drop=True)
        print(f"Plotting {len(df)} stations after filtering.")
    except Exception as e:
        print(f"Warning: Could not filter stations based on train file: {e}")

    center_lat = df['Lat'].mean()
    center_lon = df['Lon'].mean()

    # --- 1. Interactive Folium Map (HTML) ---
    print("Generating Interactive HTML Map...")
    m = folium.Map(location=[center_lat, center_lon], zoom_start=11, tiles='CartoDB positron')
    
    for idx, row in df.iterrows():
        folium.CircleMarker(
            location=[row['Lat'], row['Lon']],
            radius=7,
            popup=f"Station: {row['Location']}",
            tooltip=f"<b>{row['Location']}</b>",
            color='#2980b9',
            fill=True,
            fill_color='#3498db',
            fill_opacity=0.8,
            weight=2
        ).add_to(m)
        
    html_path = OUT_DIR / 'Figure1_Stations_Map.html'
    m.save(str(html_path))
    print(f"Saved interactive map to {html_path}")

    # --- 2. Static Publication-Ready Map (PNG) ---
    print("Generating Static PNG Map...")
    sns.set_theme(style="whitegrid")
    fig, ax = plt.subplots(figsize=(10, 8), dpi=300)
    
    # Plot points
    ax.scatter(df['Lon'], df['Lat'], c='#3498db', s=100, alpha=0.8, edgecolors='black', linewidth=1.2, label='Monitoring Stations')
    
    ax.set_title(f"Figure 1: Geographical Distribution of {len(df)} PM2.5 Monitoring Stations in Hanoi", fontsize=14, fontweight='bold', pad=15)
    ax.set_xlabel("Longitude", fontsize=12)
    ax.set_ylabel("Latitude", fontsize=12)
    ax.legend(loc='lower right')
    
    ax.grid(True, linestyle='--', alpha=0.6)
    
    png_path = OUT_DIR / 'Figure1_Stations_Map.png'
    plt.tight_layout()
    plt.savefig(png_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Saved static map to {png_path}")

if __name__ == "__main__":
    create_maps()
