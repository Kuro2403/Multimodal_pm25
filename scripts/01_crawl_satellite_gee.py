import os
import time
import pandas as pd
import ee
from datetime import datetime, timedelta

GEE_PROJECT = 'silken-gadget-419107'

# Initialize Earth Engine
try:
    ee.Initialize(project=GEE_PROJECT)
except Exception as e:
    ee.Authenticate()
    ee.Initialize(project=GEE_PROJECT)

def get_station_data(metadata_path):
    df = pd.read_csv(metadata_path)
    stations = []
    for _, row in df.iterrows():
        if pd.notna(row.get('latitude')) and pd.notna(row.get('longitude')):
            stations.append({
                'location_id': row['location_id'],
                'lat': row['latitude'],
                'lon': row['longitude']
            })
    return stations

def get_daily_feature(collection_id, band_name, point, date_str, reducer=ee.Reducer.mean(), scale=1000, buffer_m=2000):
    start_date = ee.Date(date_str)
    end_date = start_date.advance(1, 'day')
    
    collection = ee.ImageCollection(collection_id)\
        .filterBounds(point)\
        .filterDate(start_date, end_date)
    
    def compute_val(col):
        img = col.mean()
        region = point.buffer(buffer_m)
        dict_val = img.reduceRegion(
            reducer=reducer,
            geometry=region,
            scale=scale,
            maxPixels=1e9
        )
        return dict_val.get(band_name)
    
    try:
        val = collection.size().gt(0).getInfo()
        if val:
            res = compute_val(collection).getInfo()
            return res
        else:
            return None
    except Exception as e:
        print(f"Error fetching {band_name}: {e}")
        return None

def main():
    print("Starting GEE Advanced Satellite Crawler...")
    metadata_path = 'data/raw/metadata/STATION_SUMMARY.csv'
    if not os.path.exists(metadata_path):
        print(f"File not found: {metadata_path}")
        return
        
    stations = get_station_data(metadata_path)
    print(f"Found {len(stations)} stations.")
    
    start_date = datetime(2024, 1, 29)
    end_date = datetime(2026, 5, 15)
    date_list = [start_date + timedelta(days=x) for x in range((end_date-start_date).days)]
    
    # # We will test on just the first 3 days to see if the script works correctly
    # print("Testing crawler on a small 3-day subset first...")
    # date_list = date_list[:3]
    
    results = []
    
    for date in date_list:
        date_str = date.strftime('%Y-%m-%d')
        print(f"Processing {date_str}...")
        
        for st in stations:
            point = ee.Geometry.Point([st['lon'], st['lat']])
            
            # 1. Ozone (S5P)
            o3_val = get_daily_feature('COPERNICUS/S5P/OFFL/L3_O3', 'O3_column_number_density', point, date_str, scale=1113)
            
            # 2. Aerosol Index (S5P)
            ai_val = get_daily_feature('COPERNICUS/S5P/OFFL/L3_AER_AI', 'absorbing_aerosol_index', point, date_str, scale=1113)
            
            # 3. LST Day (MOD11A1)
            lst_val = get_daily_feature('MODIS/061/MOD11A1', 'LST_Day_1km', point, date_str, scale=1000)
            if lst_val is not None:
                lst_val = lst_val * 0.02 - 273.15 # Convert to Celsius
                
            # 4. Fire Radiative Power (MOD14A1) - 10km buffer
            frp_val = get_daily_feature('MODIS/061/MOD14A1', 'MaxFRP', point, date_str, reducer=ee.Reducer.sum(), scale=1000, buffer_m=10000)
            
            # 5. Nighttime Lights (VIIRS DNB Monthly) - We take the month's image
            start_month = ee.Date.fromYMD(date.year, date.month, 1)
            end_month = start_month.advance(1, 'month')
            viirs_col = ee.ImageCollection('NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG').filterBounds(point).filterDate(start_month, end_month)
            try:
                if viirs_col.size().gt(0).getInfo():
                    ntl_val = viirs_col.mean().reduceRegion(reducer=ee.Reducer.mean(), geometry=point.buffer(2000), scale=463).get('avg_rad').getInfo()
                else:
                    ntl_val = None
            except:
                ntl_val = None
            
            results.append({
                'date': date_str,
                'location_id': st['location_id'],
                'o3_column_density': o3_val,
                'aerosol_index': ai_val,
                'lst_day_c': lst_val,
                'frp_sum_10km': frp_val,
                'nighttime_lights': ntl_val
            })
            time.sleep(0.5)
            
    df_res = pd.DataFrame(results)
    out_dir = 'data/raw'
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, 'gee_advanced_satellite_test.csv')
    df_res.to_csv(out_file, index=False)
    print(f"Saved test results to {out_file}")
    print(df_res.head(10))

if __name__ == "__main__":
    main()
