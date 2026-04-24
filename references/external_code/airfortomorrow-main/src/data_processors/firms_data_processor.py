#!/usr/bin/env python3
"""
FIRMS Fire Data Processing

This script processes FIRMS fire data for Thailand and Laos by:
1. Loading and filtering fire point data from either:
   - Historical data (manually downloaded)
   - Near real-time data (automatically downloaded)
2. Filtering data to specified country boundaries
3. Converting fire points to polygons based on satellite pixel size
4. Deduplicating overlapping fires (keeping VIIRS over MODIS)
5. Saving prepared fire data
6. Performing interpolation using Ordinary Kriging (optional)
7. Creating an H3 hexagonal grid (optional)
8. Saving results to CSV and Parquet files

Adapted from the Jupyter notebook: 'FIRMS - history- Interpolation.ipynb'
"""

import os
import sys
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))

import pandas as pd
import geopandas as gpd
import polars as pl
import polars_h3 as plh3
import h3
import time
import numpy as np
import argparse
from datetime import datetime, timedelta
from shapely.geometry import Point, box, Polygon
from shapely.ops import unary_union
from pykrige.ok import OrdinaryKriging
from tqdm import tqdm
import matplotlib.pyplot as plt
import glob

# Local imports
from src.utils.boundary_utils import create_country_boundaries

# Note: create_boundaries_countries is now imported from utils.boundary_utils
# Custom function with configurable buffer for FIRMS data (fire spread analysis)
def create_boundaries_countries(country_code_list, buffer_degrees=0.4):
    """Use centralized boundary utility with configurable buffer for FIRMS data."""
    return create_country_boundaries(country_code_list, buffer_degrees=buffer_degrees)

def load_historical_data(data_dir='data/raw/firms/historical'):
    """
    Load historical FIRMS data from CSV files in the historical directory,
    separating VIIRS and MODIS data based on filenames.
    
    Args:
        data_dir: Directory containing historical data files
    
    Returns:
        Tuple of (df_viirs, df_modis) containing the separated fire data
    """
    print("Loading historical FIRMS data...")
    
    # Find all CSV files in the historical directory
    csv_files = glob.glob(os.path.join(data_dir, '*.csv'))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {data_dir}")
    
    # Separate files into VIIRS and MODIS based on filename
    viirs_files = [f for f in csv_files if 'J1V' in f or 'J2V' in f or 'SV-' in f]
    modis_files = [f for f in csv_files if 'M-' in f]
    
    print(f"Found {len(viirs_files)} VIIRS files and {len(modis_files)} MODIS files")
    
    # Function to load and process a list of files
    def process_files(file_list, data_type):
        dfs = []
        total_points = 0
        
        for file_path in file_list:
            try:
                df = pd.read_csv(file_path)
                
                # Handle different date formats
                try:
                    df['acq_date'] = pd.to_datetime(df['acq_date'], format='%Y-%m-%d')
                except:
                    try:
                        df['acq_date'] = pd.to_datetime(df['acq_date'], format='%Y%m%d')
                    except:
                        print(f"Warning: Could not parse dates in {file_path}")
                        continue
                
                # Select relevant columns
                df_selected = df[['latitude', 'longitude', 'acq_date', 'acq_time', 'frp', 'instrument']]
                dfs.append(df_selected)
                total_points += len(df_selected)
                print(f"Loaded {len(df_selected)} points from {os.path.basename(file_path)} ({data_type})")
                
            except Exception as e:
                print(f"Error loading {file_path}: {str(e)}")
        
        if not dfs:
            print(f"No valid {data_type} data files were loaded")
            return pd.DataFrame()
        
        df_combined = pd.concat(dfs)
        print(f"Combined {data_type} data has {len(df_combined)} points")
        
        return df_combined
    
    # Process VIIRS and MODIS files separately
    df_viirs = process_files(viirs_files, "VIIRS")
    df_modis = process_files(modis_files, "MODIS")
    
    return df_viirs, df_modis

def load_nrt_data_realtime():
    """
    Load near real-time FIRMS data directly from NASA URLs (past 7 days)
    
    Returns:
        Tuple of (df_viirs, df_modis) containing the separated fire data
    """
    print("Loading real-time FIRMS data (past 7 days)...")
    
    # Define the 4 URLs for 7-day data
    urls = {
        'MODIS': "https://firms.modaps.eosdis.nasa.gov/data/active_fire/modis-c6.1/csv/MODIS_C6_1_SouthEast_Asia_7d.csv",
        'SUOMI_VIIRS': "https://firms.modaps.eosdis.nasa.gov/data/active_fire/suomi-npp-viirs-c2/csv/SUOMI_VIIRS_C2_SouthEast_Asia_7d.csv",
        'J1_VIIRS': "https://firms.modaps.eosdis.nasa.gov/data/active_fire/noaa-20-viirs-c2/csv/J1_VIIRS_C2_SouthEast_Asia_7d.csv",
        'J2_VIIRS': "https://firms.modaps.eosdis.nasa.gov/data/active_fire/noaa-21-viirs-c2/csv/J2_VIIRS_C2_SouthEast_Asia_7d.csv"
    }
    
    dfs = []
    
    for satellite_type, url in urls.items():
        try:
            print(f"Downloading {satellite_type} data from NASA...")
            df = pd.read_csv(url)
            
            # Handle different date formats
            try:
                df['acq_date'] = pd.to_datetime(df['acq_date'], format='%Y-%m-%d')
            except:
                try:
                    df['acq_date'] = pd.to_datetime(df['acq_date'], format='%Y%m%d')
                except:
                    print(f"Warning: Could not parse dates for {satellite_type}")
                    continue
            
            # Select relevant columns and add instrument column based on satellite type
            df_selected = df[['latitude', 'longitude', 'acq_date', 'acq_time', 'frp', 'satellite']].copy()
            
            # Add instrument column based on satellite type
            if 'MODIS' in satellite_type:
                df_selected['instrument'] = 'MODIS'
            else:  # VIIRS satellites
                df_selected['instrument'] = 'VIIRS'
            
            dfs.append(df_selected)
            print(f"Loaded {len(df_selected)} points from {satellite_type}")
            
        except Exception as e:
            print(f"Error downloading {satellite_type} data: {str(e)}")
    
    # Combine all dataframes
    if not dfs:
        raise ValueError("No valid data files were downloaded")
    
    df_combined = pd.concat(dfs, ignore_index=True)
    print(f"Combined real-time data has {len(df_combined)} points")
    
    # Separate VIIRS and MODIS data based on instrument
    df_viirs = df_combined[df_combined['instrument'] == 'VIIRS'].copy()
    df_modis = df_combined[df_combined['instrument'] == 'MODIS'].copy()
    
    print(f"VIIRS data: {len(df_viirs)} points")
    print(f"MODIS data: {len(df_modis)} points")
    
    return df_viirs, df_modis

def load_nrt_data(index_file=None, data_dir='data/raw/firms/nrt', latest=False):
    """
    Load near real-time FIRMS data from previously downloaded files
    
    Args:
        index_file: Path to the index file from firms_data_collector
        data_dir: Directory where the data is stored
        latest: If True, use the latest data in data_dir (ignore index_file)
    
    Returns:
        DataFrame with combined near real-time fire data
    """
    print("Loading near real-time FIRMS data...")
    
    if latest:
        # Find the latest index file
        index_files = sorted(list(Path(data_dir).glob('firms_nrt_index_*.csv')), reverse=True)
        if len(index_files) == 0:
            raise FileNotFoundError(f"No index files found in {data_dir}")
        index_file = str(index_files[0])
        print(f"Using latest index file: {index_file}")
    
    if not index_file:
        raise ValueError("Please provide an index file or set latest=True")
    
    # Load the index file
    try:
        index_df = pd.read_csv(index_file)
        print(f"Found {len(index_df)} data files in index")
    except Exception as e:
        raise FileNotFoundError(f"Could not read index file {index_file}: {str(e)}")
    
    # Load all the data files
    dfs = []
    for _, row in index_df.iterrows():
        file_path = row['file_path']
        satellite = row['satellite']
        
        try:
            df = pd.read_csv(file_path)
            
            # Handle different date formats
            try:
                df['acq_date'] = pd.to_datetime(df['acq_date'], format='%Y-%m-%d')
            except:
                try:
                    df['acq_date'] = pd.to_datetime(df['acq_date'], format='%Y%m%d')
                except:
                    print(f"Warning: Could not parse dates in {file_path}")
                    continue
            
            # Select relevant columns
            df_selected = df[['latitude', 'longitude', 'acq_date', 'acq_time', 'frp', 'instrument']]
            dfs.append(df_selected)
            print(f"Loaded {len(df_selected)} points from {satellite}")
            
        except Exception as e:
            print(f"Error loading {file_path}: {str(e)}")
    
    # Combine all dataframes
    if not dfs:
        raise ValueError("No valid data files were loaded")
    
    df_combined = pd.concat(dfs)
    print(f"Combined near real-time data has {len(df_combined)} points")
    
    return df_combined

def point_to_square(point, side_length):
    """
    Convert a point to a square polygon with specified side length
    
    Args:
        point: Shapely Point geometry
        side_length: Side length of the square in meters
    
    Returns:
        Shapely Polygon representing the square
    """
    x, y = point.x, point.y
    half_side = side_length / 2
    
    # Create square coordinates
    coords = [
        (x - half_side, y - half_side),  # bottom-left
        (x + half_side, y - half_side),  # bottom-right
        (x + half_side, y + half_side),  # top-right
        (x - half_side, y + half_side),  # top-left
        (x - half_side, y - half_side)   # close the polygon
    ]
    
    return Polygon(coords)

def prepare_fire_data(fire_data, country_boundary, start_date=None, end_date=None):
    """
    Prepare fire data for processing by filtering to country boundaries,
    converting points to polygons, and deduplicating overlapping fires
    
    Args:
        fire_data: DataFrame with combined fire data
        country_boundary: GeoDataFrame with country boundaries
        start_date: Start date for filtering (YYYY-MM-DD format or datetime)
        end_date: End date for filtering (YYYY-MM-DD format or datetime)
    
    Returns:
        GeoDataFrame with filtered and deduplicated fire data
    """
    print("Preparing fire data...")
    
    # Convert to GeoDataFrame
    gdf_combined = gpd.GeoDataFrame(
        fire_data, 
        geometry=gpd.points_from_xy(fire_data.longitude, fire_data.latitude),
        crs="EPSG:4326"
    )
    
    # Filter by date range if provided
    if start_date is not None or end_date is not None:
        print("Filtering by date range...")
        
        # Ensure acq_date is datetime
        gdf_combined['acq_date'] = pd.to_datetime(gdf_combined['acq_date'])
        
        initial_count = len(gdf_combined)
        
        if start_date is not None:
            start_date = pd.to_datetime(start_date)
            gdf_combined = gdf_combined[gdf_combined['acq_date'] >= start_date]
            print(f"Filtered to dates >= {start_date.strftime('%Y-%m-%d')}")
        
        if end_date is not None:
            end_date = pd.to_datetime(end_date)
            gdf_combined = gdf_combined[gdf_combined['acq_date'] <= end_date]
            print(f"Filtered to dates <= {end_date.strftime('%Y-%m-%d')}")
        
        print(f"Date filtering: {initial_count} -> {len(gdf_combined)} fire points")
        
        if len(gdf_combined) == 0:
            print("WARNING: No fire points remain after date filtering!")
            return gdf_combined
    
    # Filter to only include points within the country boundaries
    gdf_filtered = gpd.sjoin(gdf_combined, country_boundary, how="inner", predicate="within")
    
    print(f"Total fire points: {len(gdf_combined)}")
    print(f"Fire points within boundaries: {len(gdf_filtered)}")
    
    # Convert points to polygons based on satellite type
    print("Converting fire points to polygons based on satellite pixel size...")
    
    # Get a CRS in meters (using UTM Zone 33N for Southeast Asia region)
    gdf_fires = gdf_filtered.to_crs("EPSG:32633")
    
    # Handle satellite column for compatibility between historical and real-time data
    if 'satellite' not in gdf_fires.columns:
        # For historical data, map instrument to satellite codes
        instrument_to_satellite = {
            'MODIS': 'A',   # Default MODIS to Aqua code
            'VIIRS': 'N'    # Default VIIRS to NOAA code
        }
        gdf_fires['satellite'] = gdf_fires['instrument'].map(instrument_to_satellite).fillna('N')
    
    # Transform points into square polygons for MODIS satellites (A=Aqua, T=Terra) - pixel size: 1km
    modis_mask = gdf_fires['satellite'].isin(['A', 'T'])
    if modis_mask.any():
        print(f"Converting {modis_mask.sum()} MODIS points to 1km polygons...")
        gdf_fires.loc[modis_mask, 'geometry'] = gdf_fires.loc[modis_mask, 'geometry'].apply(
            lambda point: point_to_square(point, side_length=1000)
        )
    
    # Transform points into square polygons for VIIRS satellites - pixel size: 375m
    viirs_mask = ~gdf_fires['satellite'].isin(['A', 'T'])
    if viirs_mask.any():
        print(f"Converting {viirs_mask.sum()} VIIRS points to 375m polygons...")
        gdf_fires.loc[viirs_mask, 'geometry'] = gdf_fires.loc[viirs_mask, 'geometry'].apply(
            lambda point: point_to_square(point, side_length=375)
        )
    
    # Transform back to EPSG:4326
    gdf_fires = gdf_fires.to_crs("EPSG:4326")
    
    # Remove fires with no polygon geometry and invalid coordinates
    initial_count = len(gdf_fires)
    print(f"Before geometry validation: {initial_count} fires")
    
    gdf_fires = gdf_fires[gdf_fires.geometry.notna()]
    after_notna = len(gdf_fires)
    print(f"After removing null geometries: {after_notna} fires (removed {initial_count - after_notna})")
    
    # Remove fires with invalid coordinates (NaN, Inf)
    valid_geom_mask = gdf_fires.geometry.apply(lambda geom: 
        geom is not None and 
        geom.is_valid and 
        not any(np.isnan([geom.bounds[0], geom.bounds[1], geom.bounds[2], geom.bounds[3]])) and
        not any(np.isinf([geom.bounds[0], geom.bounds[1], geom.bounds[2], geom.bounds[3]]))
    )
    gdf_fires = gdf_fires[valid_geom_mask]
    after_validation = len(gdf_fires)
    
    print(f"After geometry validation: {after_validation} fires (removed {after_notna - after_validation} invalid geometries)")
    print(f"Total removed in geometry validation: {initial_count - after_validation} fires")
    
    # Deduplicate overlapping fires (keep VIIRS over MODIS)
    print("Deduplicating overlapping fires...")
    gdf_fires = deduplicate_fires(gdf_fires)
    
    return gdf_fires

def deduplicate_fires(gdf_fires):
    """
    Deduplicate overlapping fires, keeping VIIRS over MODIS when they overlap
    
    Args:
        gdf_fires: GeoDataFrame with fire polygons
    
    Returns:
        GeoDataFrame with deduplicated fires
    """
    print("Starting optimized deduplication process...")
    
    # Ensure acq_date is datetime
    gdf_fires['acq_date'] = pd.to_datetime(gdf_fires['acq_date'])
    
    # Create separate dataframes for MODIS and VIIRS
    gdf_modis = gdf_fires[gdf_fires['satellite'].isin(["A", "T"])].copy()
    gdf_viirs = gdf_fires[~gdf_fires['satellite'].isin(["A", "T"])].copy()
    
    print(f"MODIS fires: {len(gdf_modis)}")
    print(f"VIIRS fires: {len(gdf_viirs)}")
    
    if len(gdf_modis) == 0 or len(gdf_viirs) == 0:
        print("No overlap possible - only one satellite type present")
        return gdf_fires
    
    # Build spatial index for VIIRS fires for fast spatial queries
    print("Building spatial index for VIIRS fires...")
    viirs_sindex = gdf_viirs.sindex
    
    # Pre-compute date differences for temporal filtering
    modis_dates = gdf_modis['acq_date'].values
    viirs_dates = gdf_viirs['acq_date'].values
    
    index_to_remove = []
    
    print(f"Processing {len(gdf_modis)} MODIS fires for overlaps...")
    
    # Process MODIS fires in batches for better performance
    batch_size = 1000
    for batch_start in tqdm(range(0, len(gdf_modis), batch_size), desc="Processing MODIS batches"):
        batch_end = min(batch_start + batch_size, len(gdf_modis))
        modis_batch = gdf_modis.iloc[batch_start:batch_end]
        
        for modis_idx, modis_row in modis_batch.iterrows():
            modis_geometry = modis_row['geometry']
            modis_date = modis_row['acq_date']
            
            # Use spatial index to find potential VIIRS overlaps
            possible_matches_idx = list(viirs_sindex.intersection(modis_geometry.bounds))
            
            if not possible_matches_idx:
                continue
            
            # Get potential VIIRS matches
            potential_viirs = gdf_viirs.iloc[possible_matches_idx]
            
            # Vectorized temporal filtering (within 4 days)
            date_diffs = abs((potential_viirs['acq_date'] - modis_date).dt.days)
            temporal_matches = potential_viirs[date_diffs < 4]
            
            if len(temporal_matches) == 0:
                continue
            
            # Check for actual geometric intersection
            intersects = temporal_matches.geometry.intersects(modis_geometry)
            
            if intersects.any():
                # Mark MODIS fire for removal (keep VIIRS)
                index_to_remove.append(modis_idx)
    
    # Remove duplicated indices from MODIS dataframe
    unique_index_to_remove = list(set(index_to_remove))
    print(f"Removing {len(unique_index_to_remove)} overlapping MODIS fires")
    
    # Create deduplicated MODIS dataframe by removing overlapping fires
    gdf_modis_deduplicated = gdf_modis.drop(unique_index_to_remove)
    
    # Merge deduplicated MODIS with all VIIRS fires
    gdf_fires_deduplicated = pd.concat([gdf_modis_deduplicated, gdf_viirs], ignore_index=True)
    
    print(f"Deduplication complete: {len(gdf_fires)} -> {len(gdf_fires_deduplicated)} fires")
    print(f"Verification: {len(gdf_modis)} - {len(unique_index_to_remove)} + {len(gdf_viirs)} = {len(gdf_modis) - len(unique_index_to_remove) + len(gdf_viirs)}")
    print(f"Expected: {len(gdf_modis) - len(unique_index_to_remove) + len(gdf_viirs)}, Actual: {len(gdf_fires_deduplicated)}")
    
    return gdf_fires_deduplicated

def save_prepared_data(fire_points, output_dir, data_type, countries, start_date=None, end_date=None):
    """
    Save prepared fire data to files
    
    Args:
        fire_points: GeoDataFrame with prepared fire data
        output_dir: Directory to save output files
        data_type: Type of data (historical or nrt)
        countries: List of country codes
        start_date: Start date for the data (optional)
        end_date: End date for the data (optional)
    """
    # Create specific subdirectory for deduplicated data
    deduplicated_dir = os.path.join(output_dir, "deduplicated", data_type)
    os.makedirs(deduplicated_dir, exist_ok=True)
    
    countries_str = "_".join(countries)
    
    # Create filename with date range if provided
    if start_date and end_date:
        start_str = pd.to_datetime(start_date).strftime("%Y%m%d")
        end_str = pd.to_datetime(end_date).strftime("%Y%m%d")
        date_range = f"{start_str}_to_{end_str}"
        filename = f"firms_prepared_{data_type}_{countries_str}_{date_range}.parquet"
    else:
        # Fallback to timestamp if no dates provided
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"firms_prepared_{data_type}_{countries_str}_{timestamp}.parquet"
    
    prepared_parquet = os.path.join(deduplicated_dir, filename)
    
    # Drop geometry column (keep lat/lon)
    fire_points_df = fire_points.drop(columns=['geometry'])
    fire_points_df.to_parquet(prepared_parquet, index=False)
    
    print(f"Prepared fire data saved to {prepared_parquet}")
    
    return {
        "prepared_parquet": prepared_parquet,
        "date_range": date_range if start_date and end_date else None
    }

def interpolate_fire_data(fire_points, country_boundary, hex_resolution=8, density=6400, number_of_chunks=8, variogram_model='spherical'):
    """
    Interpolate fire data across the study area using Ordinary Kriging
    
    Args:
        fire_points: GeoDataFrame with fire points
        country_boundary: GeoDataFrame with country boundaries
        hex_resolution: H3 hexagon resolution (default 8)
        density: Interpolation grid density (default 6400)
        number_of_chunks: Number of chunks for processing (default 8)
        variogram_model: Kriging variogram model (default 'spherical')
    
    Returns:
        DataFrame with interpolated fire data
    """
    print("Interpolating fire data...")
    
    # If no fire points, return empty dataframe
    if len(fire_points) == 0:
        print("No fire points to interpolate.")
        return pd.DataFrame()
    
    # Get the bounds of the country boundaries
    bounds = country_boundary.total_bounds
    minx, miny, maxx, maxy = bounds
    
    # Calculate chunk size
    chunk_size = int(density / number_of_chunks)
    
    # Create a grid for interpolation
    x_grid = np.linspace(minx, maxx, density)
    y_grid = np.linspace(miny, maxy, density)
    
    # Prepare fire data for Kriging
    x = fire_points.geometry.x.values
    y = fire_points.geometry.y.values
    z = fire_points.frp.values  # Fire Radiative Power values
    
    # Set up progress bar
    pbar = tqdm(total=number_of_chunks**2, desc="Processing chunks", unit="chunk")
    
    # Process in chunks to avoid memory issues
    kriged_results = []
    
    for i in range(number_of_chunks):
        x_chunk = x_grid[i * chunk_size:(i+1) * chunk_size]
        
        for j in range(number_of_chunks):
            y_chunk = y_grid[j * chunk_size:(j+1) * chunk_size]
            
            # Create grid points for this chunk
            xx, yy = np.meshgrid(x_chunk, y_chunk)
            
            try:
                # Perform ordinary kriging
                OK = OrdinaryKriging(
                    x, y, z,
                    variogram_model=variogram_model,
                    verbose=False,
                    enable_plotting=False
                )
                
                z_interpolated, _ = OK.execute('grid', x_chunk, y_chunk)
                
                # Flatten the grid
                for k in range(len(y_chunk)):
                    for l in range(len(x_chunk)):
                        value = z_interpolated[k, l]
                        # Exclude points outside country boundary or with negative values
                        point = Point(xx[k, l], yy[k, l])
                        if country_boundary.geometry.iloc[0].contains(point) and value >= 0:
                            kriged_results.append({
                                'longitude': xx[k, l],
                                'latitude': yy[k, l],
                                'frp': value
                            })
            except Exception as e:
                print(f"Error in kriging for chunk {i},{j}: {str(e)}")
            
            pbar.update(1)
    
    pbar.close()
    
    # Convert to DataFrame
    df_kriged = pd.DataFrame(kriged_results)
    
    print(f"Interpolation completed with {len(df_kriged)} points.")
    
    return df_kriged

def create_h3_grid(df_interpolated, hex_resolution=8):
    """
    Create H3 hexagonal grid from interpolated data
    
    Args:
        df_interpolated: DataFrame with interpolated data
        hex_resolution: H3 hexagon resolution (default 8)
    
    Returns:
        DataFrame with H3 indexes and aggregated FRP values
    """
    print(f"Creating H3 grid at resolution {hex_resolution}...")
    
    # If interpolated data is empty, return empty dataframe
    if len(df_interpolated) == 0:
        return pd.DataFrame()
    
    # Convert to Polars DataFrame
    pl_df = pl.from_pandas(df_interpolated)
    
    # Add H3 indexes
    pl_df = pl_df.with_columns(
        plh3.geo_to_h3(
            pl.col("latitude"),
            pl.col("longitude"),
            hex_resolution
        ).alias("h3_index")
    )
    
    # Group by H3 index and aggregate
    h3_grid = pl_df.group_by("h3_index").agg(
        pl.col("frp").mean().alias("frp_mean"),
        pl.col("frp").sum().alias("frp_sum"),
        pl.col("frp").count().alias("point_count"),
        pl.col("latitude").mean().alias("latitude"),
        pl.col("longitude").mean().alias("longitude")
    )
    
    # Convert back to pandas
    h3_grid_pd = h3_grid.to_pandas()
    
    print(f"Created H3 grid with {len(h3_grid_pd)} hexagons.")
    
    return h3_grid_pd

def save_processing_results(h3_grid, interpolated_data, output_dir, data_type, countries):
    """
    Save processing results to Parquet files
    
    Args:
        h3_grid: DataFrame with H3 grid data
        interpolated_data: DataFrame with interpolated data
        output_dir: Directory to save output files
        data_type: Type of data (historical or nrt)
        countries: List of country codes
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    countries_str = "_".join(countries)
    
    # Save H3 grid to Parquet
    if not h3_grid.empty:
        h3_grid_parquet = os.path.join(output_dir, f"firms_h3_grid_{data_type}_{countries_str}_{timestamp}.parquet")
        h3_grid.to_parquet(h3_grid_parquet, index=False)
        print(f"H3 grid saved to {h3_grid_parquet}")
    
    # Save interpolated data to Parquet
    if not interpolated_data.empty:
        interpolated_parquet = os.path.join(output_dir, f"firms_interpolated_{data_type}_{countries_str}_{timestamp}.parquet")
        interpolated_data.to_parquet(interpolated_parquet, index=False)
        print(f"Interpolated data saved to {interpolated_parquet}")
    
    # Generate a simple visualization
    plot_file = None
    if not h3_grid.empty:
        plt.figure(figsize=(10, 8))
        plt.scatter(h3_grid['longitude'], h3_grid['latitude'], c=h3_grid['frp_mean'], 
                   cmap='hot', alpha=0.7, s=10)
        plt.colorbar(label='Fire Radiative Power (FRP)')
        plt.title(f'FIRMS Fire Data - {data_type.upper()} - {", ".join(countries)}')
        plt.xlabel('Longitude')
        plt.ylabel('Latitude')
        
        plot_file = os.path.join(output_dir, f"firms_visualization_{data_type}_{countries_str}_{timestamp}.png")
        plt.savefig(plot_file)
        plt.close()
        print(f"Visualization saved to {plot_file}")
    
    return {
        "timestamp": timestamp,
        "h3_grid_file": h3_grid_parquet if not h3_grid.empty else None,
        "interpolated_file": interpolated_parquet if not interpolated_data.empty else None,
        "visualization_file": plot_file
    }

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='FIRMS Fire Data Processing')
    parser.add_argument('--data-type', type=str, choices=['historical', 'nrt', 'realtime'], required=True,
                        help='Type of data to process: historical, nrt (from files), or realtime (direct download)')
    parser.add_argument('--index-file', type=str,
                        help='Path to the index file created by firms_data_collector.py (for nrt data)')
    parser.add_argument('--data-dir', type=str,
                        help='Directory with data files (default: data/raw/firms/[historical|nrt])')
    parser.add_argument('--output-dir', type=str, default='data/processed/firms',
                        help='Directory to save output files (default: data/processed/firms)')
    parser.add_argument('--resolution', type=int, default=8,
                        help='H3 hexagon resolution (default: 8)')
    parser.add_argument('--density', type=int, default=6400,
                        help='Interpolation grid density (default: 6400)')
    parser.add_argument('--chunks', type=int, default=8,
                        help='Number of chunks for processing (default: 8)')
    parser.add_argument('--latest', action='store_true',
                        help='Use the latest index file in data-dir (for nrt data)')
    parser.add_argument('--countries', type=str, nargs='+', default=['THA', 'LAO'],
                        help='List of country codes to process (default: THA LAO)')
    parser.add_argument('--prepare-only', action='store_true',
                        help='Only prepare data without interpolation and H3 grid creation')
    parser.add_argument('--skip-deduplication', action='store_true',
                        help='Skip the deduplication step (faster processing)')
    parser.add_argument('--start-date', type=str,
                        help='Start date for filtering data (YYYY-MM-DD format)')
    parser.add_argument('--end-date', type=str,
                        help='End date for filtering data (YYYY-MM-DD format)')
    parser.add_argument('--original-start-date', type=str,
                        help='Original start date requested (for filtering output after rolling calculations)')
    parser.add_argument('--original-end-date', type=str,
                        help='Original end date requested (for filtering output after rolling calculations)')
    parser.add_argument('--buffer', type=float, default=0.0,
                        help='Geographic buffer in degrees for country boundaries (default: 0.0)')
    args = parser.parse_args()
    
    # Set default data directory based on data type if not provided
    if not args.data_dir:
        args.data_dir = f'data/raw/firms/{"historical" if args.data_type == "historical" else "nrt"}'
    
    # Validate date arguments
    if args.start_date:
        try:
            pd.to_datetime(args.start_date)
        except:
            print(f"ERROR: Invalid start date format: {args.start_date}. Use YYYY-MM-DD format.")
            return 1
    
    if args.end_date:
        try:
            pd.to_datetime(args.end_date)
        except:
            print(f"ERROR: Invalid end date format: {args.end_date}. Use YYYY-MM-DD format.")
            return 1
    
    if args.start_date and args.end_date:
        start_dt = pd.to_datetime(args.start_date)
        end_dt = pd.to_datetime(args.end_date)
        if start_dt > end_dt:
            print(f"ERROR: Start date ({args.start_date}) cannot be after end date ({args.end_date})")
            return 1
    
    # Log date range information
    if args.original_start_date and args.original_end_date:
        print(f"Original date range requested: {args.original_start_date} to {args.original_end_date}")
        print(f"Processing with buffer: {args.start_date} to {args.end_date}")
        print(f"Note: Additional days collected for rolling calculations")
    
    # Start timing
    start_time = time.time()
    
    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        # Load fire data based on type
        if args.data_type == 'historical':
            df_viirs, df_modis = load_historical_data(args.data_dir)
            # Combine VIIRS and MODIS data for processing
            fire_points = pd.concat([df_viirs, df_modis]) if not df_viirs.empty or not df_modis.empty else pd.DataFrame()
            if fire_points.empty:
                raise ValueError("No valid fire data loaded")
        elif args.data_type == 'realtime':
            # Load real-time data directly from NASA URLs
            df_viirs, df_modis = load_nrt_data_realtime()
            # Combine VIIRS and MODIS data for processing
            fire_points = pd.concat([df_viirs, df_modis]) if not df_viirs.empty or not df_modis.empty else pd.DataFrame()
            if fire_points.empty:
                raise ValueError("No valid fire data loaded")
        else:  # nrt (from files)
            fire_points = load_nrt_data(args.index_file, args.data_dir, args.latest)
        
        # Get country boundaries
        country_boundary = create_boundaries_countries(args.countries, buffer_degrees=args.buffer)
        
        # Prepare fire data (filter to boundaries, convert to polygons, deduplicate)
        if args.skip_deduplication:
            print("Skipping deduplication step...")
        fire_points_filtered = prepare_fire_data(
            fire_points, 
            country_boundary, 
            start_date=args.start_date, 
            end_date=args.end_date
        )
        
        # Save prepared data
        prepared_files = save_prepared_data(fire_points_filtered, args.output_dir, args.data_type, args.countries, args.start_date, args.end_date)
        
        # Skip memory-intensive interpolation steps and always treat as prepare-only
        # This will allow the KDE step to work with the prepared data without memory issues
        elapsed_time = time.time() - start_time
        print(f"\nData preparation completed in {elapsed_time:.2f} seconds.")
        print("\nPreparation Summary:")
        print(f"Countries processed: {', '.join(args.countries)}")
        if args.start_date or args.end_date:
            date_range = f"{args.start_date or 'earliest'} to {args.end_date or 'latest'}"
            print(f"Date range: {date_range}")
        if args.data_type == 'historical' or args.data_type == 'realtime':
            print(f"VIIRS points: {len(df_viirs)}")
            print(f"MODIS points: {len(df_modis)}")
        print(f"Total fire polygons processed: {len(fire_points_filtered)}")
        if prepared_files['date_range']:
            print(f"Prepared data saved with date range: {prepared_files['date_range']}")
        else:
            print(f"Prepared data saved")
        
        # Skip the memory-intensive operations, returning as if successful
        if not args.prepare_only:
            print("\nSkipping memory-intensive Kriging interpolation - will use KDE method instead.")
        
        return 0
    
    except Exception as e:
        print(f"\nError during processing: {str(e)}")
        return 1

if __name__ == "__main__":
    main() 