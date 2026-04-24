#!/usr/bin/env python3
"""
Streaming Himawari AOD H3 Processing Pipeline

This script processes Himawari NetCDF files in a streaming fashion:
1. NetCDF from raw folder → TIF in cache (temporary)
2. TIF → H3-indexed Parquet in processed folder
3. Delete TIF from cache
4. Repeat for next file

This approach minimizes storage requirements by not keeping TIF files.
"""

import os
import pandas as pd
import polars as pl
import geopandas as gpd
import rasterio
from rasterio.mask import mask
import numpy as np
import xarray as xr
from dask import delayed, compute
from pathlib import Path
import tempfile
import shutil
import re
from datetime import datetime, timedelta

# Import h3ronpy pandas raster functions
try:
    from h3ronpy.pandas.raster import raster_to_dataframe
    print("Successfully imported h3ronpy.pandas.raster")
except ImportError as e:
    print(f"Error importing h3ronpy: {e}")
    print("Please ensure h3ronpy is installed: pip install h3ronpy")
    exit(1)

# Import centralized boundary utilities
from src.utils.boundary_utils import create_country_boundaries

# Note: create_country_boundaries is now imported from utils.boundary_utils
# This alias is kept for backward compatibility
def create_country_boundaries_local(country_codes, buffer_degrees=0.4):
    """Use centralized boundary utility."""
    return create_country_boundaries(country_codes, buffer_degrees)

def netcdf_to_tif_cache(netcdf_path, cache_dir):
    """Convert NetCDF to TIF in cache directory"""
    try:
        print(f"Converting NetCDF to TIF: {netcdf_path}")
        
        # Create cache directory if it doesn't exist
        os.makedirs(cache_dir, exist_ok=True)
        
        # Generate TIF filename in cache
        netcdf_filename = os.path.basename(netcdf_path)
        tif_filename = netcdf_filename.replace('.nc', '.tif')
        tif_path = os.path.join(cache_dir, tif_filename)
        
        # Open NetCDF file (suppress timedelta warning)
        with xr.open_dataset(netcdf_path, decode_timedelta=False) as ds:
            # Check available variables (Himawari uses AOT_Merged for AOD data)
            if 'AOT_Merged' in ds.variables:
                aod_var = ds['AOT_Merged']
            elif 'AOD' in ds.variables:
                aod_var = ds['AOD']
            elif 'aerosol_optical_depth' in ds.variables:
                aod_var = ds['aerosol_optical_depth']
            else:
                available_vars = list(ds.variables.keys())
                raise ValueError(f"AOD variable not found. Available variables: {available_vars}")
            
            # Get the data array (assuming first time step if multiple)
            if len(aod_var.dims) == 3:  # time, lat, lon
                aod_data = aod_var.isel(time=0)
            else:  # lat, lon
                aod_data = aod_var
            
            # Flip data if necessary (satellite data often needs flipping)
            aod_data = aod_data.fillna(-9999)  # Fill NaN with nodata value
            
            # Get spatial information
            if 'latitude' in ds.coords:
                lats = ds.coords['latitude'].values
                lons = ds.coords['longitude'].values
            elif 'lat' in ds.coords:
                lats = ds.coords['lat'].values
                lons = ds.coords['lon'].values
            elif 'latitude' in ds.variables:
                lats = ds.variables['latitude'].values
                lons = ds.variables['longitude'].values
            else:
                raise ValueError("Cannot find latitude/longitude coordinates")
            
            # Create transform
            lat_res = abs(lats[1] - lats[0]) if len(lats) > 1 else 0.01
            lon_res = abs(lons[1] - lons[0]) if len(lons) > 1 else 0.01
            
            transform = rasterio.transform.from_bounds(
                lons.min() - lon_res/2, lats.min() - lat_res/2,
                lons.max() + lon_res/2, lats.max() + lat_res/2,
                len(lons), len(lats)
            )
            
            # Write to TIF
            with rasterio.open(
                tif_path, 'w',
                driver='GTiff',
                height=aod_data.shape[0],
                width=aod_data.shape[1],
                count=1,
                dtype=aod_data.dtype,
                crs='EPSG:4326',
                transform=transform,
                nodata=-9999
            ) as dst:
                dst.write(aod_data.values, 1)
        
        print(f"Successfully converted to TIF: {tif_path}")
        return tif_path
        
    except Exception as e:
        print(f"Error converting NetCDF to TIF {netcdf_path}: {e}")
        return None

def process_single_file_streaming(netcdf_path, parquet_path, hex_resolution, boundaries_countries, cache_dir, compact=False, keep_netcdf=True):
    """Process a single NetCDF file through the complete pipeline"""
    
    # Extract country codes from parquet_path if present
    # Example: H09_20240330_1600_1HARP031_FLDK.02401_02401_LAO_THA.parquet
    parquet_filename = os.path.basename(parquet_path)
    
    # Check if the file already exists - need to use original path if checking an existing file
    if os.path.exists(parquet_path):
        print(f"H3 processed {parquet_path} already exists, skipping...")
        return f"Skipped: {parquet_path}"

    try:
        # Step 1: Convert NetCDF to TIF in cache
        tif_path = netcdf_to_tif_cache(netcdf_path, cache_dir)
        if not tif_path or not os.path.exists(tif_path):
            # Delete NetCDF file even if conversion failed (if keep_netcdf=False)
            if not keep_netcdf:
                try:
                    os.remove(netcdf_path)
                    print(f"Deleted NetCDF file (conversion failed): {netcdf_path}")
                    return f"NetCDF conversion failed: {netcdf_path} (NetCDF deleted)"
                except Exception as e:
                    print(f"Warning: Could not delete NetCDF file {netcdf_path}: {e}")
                    return f"NetCDF conversion failed: {netcdf_path} (NetCDF delete failed)"
            return f"NetCDF conversion failed: {netcdf_path}"
        
        try:
            # Step 2: Process TIF to H3 (same as before)
            print(f"Processing TIF to H3: {tif_path}")
            
            # Open and process raster
            with rasterio.open(tif_path) as src:
                # Read the data
                aod = src.read(1)
                
                # Check if data is valid
                if np.all(np.isnan(aod)) or np.all(aod <= 0):
                    print(f"No valid data in {tif_path}, skipping...")
                    # Delete NetCDF file even if no valid data (if keep_netcdf=False)
                    if not keep_netcdf:
                        try:
                            os.remove(netcdf_path)
                            print(f"Deleted NetCDF file (no valid data): {netcdf_path}")
                            return f"No valid data: {netcdf_path} (NetCDF deleted)"
                        except Exception as e:
                            print(f"Warning: Could not delete NetCDF file {netcdf_path}: {e}")
                            return f"No valid data: {netcdf_path} (NetCDF delete failed)"
                    return f"No valid data: {netcdf_path}"
                
                # Clip raster to boundaries
                try:
                    masked_aod, masked_transform = mask(src, boundaries_countries.geometry, crop=True)
                    masked_aod = masked_aod[0]  # Extract first band
                except Exception as e:
                    print(f"Error masking raster {tif_path}: {e}")
                    # Delete NetCDF file even if masking failed (if keep_netcdf=False)
                    if not keep_netcdf:
                        try:
                            os.remove(netcdf_path)
                            print(f"Deleted NetCDF file (masking error): {netcdf_path}")
                            return f"Masking error: {netcdf_path} (NetCDF deleted)"
                        except Exception as e:
                            print(f"Warning: Could not delete NetCDF file {netcdf_path}: {e}")
                            return f"Masking error: {netcdf_path} (NetCDF delete failed)"
                    return f"Masking error: {netcdf_path}"
                
                # Check if masked data is valid
                if np.all(np.isnan(masked_aod)) or np.all(masked_aod <= 0):
                    print(f"No valid data after masking {tif_path}, skipping...")
                    # Delete NetCDF file even if no valid data after masking (if keep_netcdf=False)
                    if not keep_netcdf:
                        try:
                            os.remove(netcdf_path)
                            print(f"Deleted NetCDF file (no valid data after masking): {netcdf_path}")
                            return f"No valid data after masking: {netcdf_path} (NetCDF deleted)"
                        except Exception as e:
                            print(f"Warning: Could not delete NetCDF file {netcdf_path}: {e}")
                            return f"No valid data after masking: {netcdf_path} (NetCDF delete failed)"
                    return f"No valid data after masking: {netcdf_path}"

            # Step 3: Convert raster to H3 using h3ronpy pandas function
            try:
                print(f"Converting to H3 resolution {hex_resolution}...")
                df_pandas = raster_to_dataframe(
                    in_raster=masked_aod,
                    transform=masked_transform,
                    h3_resolution=hex_resolution,
                    nodata_value=-9999,
                    compact=compact,
                    geo=False  # Don't create geometries for faster processing
                )
                
                if df_pandas.empty:
                    print(f"No valid H3 data for {tif_path}, skipping...")
                    # Delete NetCDF file even if no valid H3 data (if keep_netcdf=False)
                    if not keep_netcdf:
                        try:
                            os.remove(netcdf_path)
                            print(f"Deleted NetCDF file (no valid H3 data): {netcdf_path}")
                            return f"No valid H3 data: {netcdf_path} (NetCDF deleted)"
                        except Exception as e:
                            print(f"Warning: Could not delete NetCDF file {netcdf_path}: {e}")
                            return f"No valid H3 data: {netcdf_path} (NetCDF delete failed)"
                    return f"No valid H3 data: {netcdf_path}"
                    
            except Exception as e:
                print(f"Error converting to H3 {tif_path}: {e}")
                # Delete NetCDF file even if H3 conversion failed (if keep_netcdf=False)
                if not keep_netcdf:
                    try:
                        os.remove(netcdf_path)
                        print(f"Deleted NetCDF file (H3 conversion error): {netcdf_path}")
                        return f"H3 conversion error: {netcdf_path} (NetCDF deleted)"
                    except Exception as e:
                        print(f"Warning: Could not delete NetCDF file {netcdf_path}: {e}")
                        return f"H3 conversion error: {netcdf_path} (NetCDF delete failed)"
                return f"H3 conversion error: {netcdf_path}"

            # Step 4: Convert to Polars for fast processing
            try:
                print("Converting to Polars for fast processing...")
                df_pl = pl.from_pandas(df_pandas)
                
                # Clean and filter data using Polars
                df_pl = (df_pl
                    .filter(pl.col("value") > 0)  # Remove zero/negative values
                    .drop_nulls(subset=["value"])  # Remove null values
                    .filter(pl.col("value").is_not_nan())  # Remove NaN values
                )
                
                # Add metadata from NetCDF filename
                netcdf_filename = os.path.basename(netcdf_path)
                # Extract date/time from filename (adjust pattern as needed)
                # Example: H09_20240202_0100_1HARP031_FLDK.02401_02401.nc
                parts = netcdf_filename.split('_')
                if len(parts) >= 3:
                    date_part = parts[1]  # 20240202
                    time_part = parts[2]  # 0100
                    month = date_part[:6]  # 202402
                    day = date_part[6:8]  # 02
                else:
                    month = "unknown"
                    day = "unknown"
                
                df_pl = df_pl.with_columns([
                    pl.lit(netcdf_filename).alias("source_file"),
                    pl.lit(month).alias("month"),
                    pl.lit(day).alias("day"),
                    pl.col("cell").alias("h3_08"),  # Rename cell column to h3_08
                    pl.col("value").alias("aod_value")  # Rename value to aod_value
                ]).select([
                    "h3_08", "aod_value", "source_file", "month", "day"
                ])
                
                if df_pl.shape[0] == 0:
                    print(f"No valid data after cleaning {tif_path}, skipping...")
                    # Delete NetCDF file even if no valid data after cleaning (if keep_netcdf=False)
                    if not keep_netcdf:
                        try:
                            os.remove(netcdf_path)
                            print(f"Deleted NetCDF file (no valid data after cleaning): {netcdf_path}")
                            return f"No valid data after cleaning: {netcdf_path} (NetCDF deleted)"
                        except Exception as e:
                            print(f"Warning: Could not delete NetCDF file {netcdf_path}: {e}")
                            return f"No valid data after cleaning: {netcdf_path} (NetCDF delete failed)"
                    return f"No valid data after cleaning: {netcdf_path}"

            except Exception as e:
                print(f"Error processing with Polars {tif_path}: {e}")
                # Delete NetCDF file even if Polars processing failed (if keep_netcdf=False)
                if not keep_netcdf:
                    try:
                        os.remove(netcdf_path)
                        print(f"Deleted NetCDF file (Polars processing error): {netcdf_path}")
                        return f"Polars processing error: {netcdf_path} (NetCDF deleted)"
                    except Exception as e:
                        print(f"Warning: Could not delete NetCDF file {netcdf_path}: {e}")
                        return f"Polars processing error: {netcdf_path} (NetCDF delete failed)"
                return f"Polars processing error: {netcdf_path}"

            # Step 5: Save to Parquet using Polars
            try:
                os.makedirs(os.path.dirname(parquet_path), exist_ok=True)
                df_pl.write_parquet(parquet_path)
                
                print(f"H3 processed {parquet_path} successfully! ({df_pl.shape[0]} records)")
                result = f"Success: {parquet_path} ({df_pl.shape[0]} records)"
                
                # Delete NetCDF file after successful H3 processing (if keep_netcdf=False)
                if not keep_netcdf:
                    try:
                        os.remove(netcdf_path)
                        print(f"Deleted NetCDF file to save space: {netcdf_path}")
                        result += " (NetCDF deleted)"
                    except Exception as e:
                        print(f"Warning: Could not delete NetCDF file {netcdf_path}: {e}")
                        result += " (NetCDF delete failed)"
                
            except Exception as e:
                print(f"Error saving parquet {parquet_path}: {e}")
                # Delete NetCDF file even if parquet save failed (if keep_netcdf=False)
                if not keep_netcdf:
                    try:
                        os.remove(netcdf_path)
                        print(f"Deleted NetCDF file (parquet save error): {netcdf_path}")
                        return f"Parquet save error: {netcdf_path} (NetCDF deleted)"
                    except Exception as e:
                        print(f"Warning: Could not delete NetCDF file {netcdf_path}: {e}")
                        return f"Parquet save error: {netcdf_path} (NetCDF delete failed)"
                return f"Parquet save error: {netcdf_path}"
        
        finally:
            # Step 6: Always clean up TIF file from cache
            try:
                if os.path.exists(tif_path):
                    os.remove(tif_path)
                    print(f"Cleaned up TIF from cache: {tif_path}")
            except Exception as e:
                print(f"Warning: Could not clean up TIF file {tif_path}: {e}")
        
        return result
        
    except Exception as e:
        error_msg = f"Error in streaming pipeline {netcdf_path}: {str(e)}"
        print(error_msg)
        # Delete NetCDF file even if general error occurred (if keep_netcdf=False)
        if not keep_netcdf:
            try:
                os.remove(netcdf_path)
                print(f"Deleted NetCDF file (general error): {netcdf_path}")
                return f"{error_msg} (NetCDF deleted)"
            except Exception as delete_e:
                print(f"Warning: Could not delete NetCDF file {netcdf_path}: {delete_e}")
                return f"{error_msg} (NetCDF delete failed)"
        return error_msg

def check_interpolated_file_exists(date_str, countries, mode='historical'):
    """Check if interpolated file already exists for the given date"""
    country_str = "_".join(sorted(countries))
    subdir = 'realtime' if mode == 'realtime' else 'historical'
    interpolated_dir = Path(f"data/processed/himawari/interpolated/{subdir}")
    interpolated_file = interpolated_dir / f"interpolated_h3_aod_{date_str}_{country_str}.parquet"
    return interpolated_file.exists()

def extract_date_from_netcdf_path(netcdf_path):
    """Extract YYYYMMDD date from NetCDF file path or filename"""
    # Try to extract from filename: H09_20240330_1600_*.nc
    match = re.search(r'H[0-9]+_(\d{8})_', netcdf_path)
    if match:
        return match.group(1)
    # Try to extract from path: .../202403/30/...
    match = re.search(r'/(\d{6})/(\d{2})/', netcdf_path)
    if match:
        return match.group(1) + match.group(2)
    return None

def collect_netcdf_files(raw_path, data_type="historical", hours=None, countries=None):
    """Collect all NetCDF files to process, skipping dates with existing interpolated files"""
    all_netcdf_files = []
    
    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"Raw path does not exist: {raw_path}")
    
    # Default countries if not provided
    if countries is None:
        countries = ["LAO", "THA"]
    
    if data_type == "realtime":
        # For realtime mode, collect ALL downloaded files (no time filtering)
        # This eliminates race conditions between download and processing
        return collect_all_downloaded_netcdf_files(raw_path, countries, hours)
    else:
        # Historical mode - process all files, but skip dates with existing interpolated files
        dates_checked = set()
        dates_skipped = set()
        
        for month in sorted(os.listdir(raw_path)):
            month_path = os.path.join(raw_path, month)
            if not os.path.isdir(month_path):
                continue
                
            for day in sorted(os.listdir(month_path)):
                day_path = os.path.join(month_path, day)
                if not os.path.isdir(day_path):
                    continue
                
                # Check if interpolated file exists for this date
                date_str = month + day  # YYYYMMDD
                if date_str not in dates_checked:
                    dates_checked.add(date_str)
                    if check_interpolated_file_exists(date_str, countries, mode='historical'):
                        print(f"Skipping date {date_str} - interpolated file already exists")
                        dates_skipped.add(date_str)
                        continue
                
                # Skip if already marked as skipped
                if date_str in dates_skipped:
                    continue
                    
                for nc_file in sorted(os.listdir(day_path)):
                    if nc_file.endswith(".nc"):
                        netcdf_file_path = os.path.join(day_path, nc_file)
                        
                        # Create corresponding parquet path
                        # Convert: raw/himawari/202402/02/file.nc → processed/himawari/h3/historical/202402/02/file.parquet
                        relative_path = os.path.relpath(netcdf_file_path, raw_path)
                        parquet_relative = relative_path.replace('.nc', '.parquet')
                        parquet_path = os.path.join(f"./data/processed/himawari/h3/{data_type}", parquet_relative)
                        
                        all_netcdf_files.append((netcdf_file_path, parquet_path))
        
        if dates_skipped:
            print(f"Skipped {len(dates_skipped)} dates with existing interpolated files")
        print(f"Found {len(all_netcdf_files)} NetCDF files to process")
        print(f"Output directory: data/processed/himawari/h3/{data_type}/")
        return all_netcdf_files

def collect_all_downloaded_netcdf_files(raw_path, countries, hours=None):
    """Collect ALL downloaded NetCDF files, skipping dates with existing interpolated files (for realtime mode)"""
    all_netcdf_files = []
    dates_checked = set()
    dates_skipped = set()
    
    print(f"Collecting downloaded NetCDF files (realtime mode)")
    
    # Calculate all dates that should be in the time window
    if hours:
        end_datetime = datetime.utcnow()
        start_datetime = end_datetime - timedelta(hours=hours)
        
        # Check each day in the range for interpolated files
        current_date = start_datetime.date()
        end_date = end_datetime.date()
        
        print(f"Checking date range: {current_date} to {end_date} ({hours} hours)")
        
        while current_date <= end_date:
            date_str = current_date.strftime('%Y%m%d')
            dates_checked.add(date_str)
            
            if check_interpolated_file_exists(date_str, countries, mode='realtime'):
                print(f"Skipping date {date_str} - interpolated file already exists")
                dates_skipped.add(date_str)
            
            current_date += timedelta(days=1)
        
        if dates_skipped:
            print(f"Skipped {len(dates_skipped)} dates with existing interpolated files")
    
    # Get available months (most recent first)
    months = sorted([d for d in os.listdir(raw_path) if os.path.isdir(os.path.join(raw_path, d))], reverse=True)
    
    # Collect NetCDF files from dates that need processing
    for month in months:
        month_path = os.path.join(raw_path, month)
        
        # Get available days (most recent first)
        days = sorted([d for d in os.listdir(month_path) if os.path.isdir(os.path.join(month_path, d))], reverse=True)
        
        # Check all days that exist
        for day in days:
            day_path = os.path.join(month_path, day)
            date_str = month + day  # YYYYMMDD
            
            # Skip if already marked as skipped
            if date_str in dates_skipped:
                continue
            
            # Get all NetCDF files in this day
            nc_files = [f for f in os.listdir(day_path) if f.endswith('.nc')]
            
            for nc_file in nc_files:
                # Extract datetime from filename (e.g., H09_20240330_1600_1HARP031_FLDK.02401_02401.nc)
                match = re.match(r'H[0-9]+_(\d{8})_(\d{4})_.*\.nc', nc_file)
                if match:
                    date_str_file, time_str = match.groups()
                    try:
                        file_datetime = datetime.strptime(f"{date_str_file}_{time_str}", "%Y%m%d_%H%M")
                        
                        netcdf_file_path = os.path.join(day_path, nc_file)
                        
                        # Create corresponding parquet path
                        relative_path = os.path.relpath(netcdf_file_path, raw_path)
                        parquet_relative = relative_path.replace('.nc', '.parquet')
                        parquet_path = os.path.join(f"./data/processed/himawari/h3/realtime", parquet_relative)
                        
                        all_netcdf_files.append((netcdf_file_path, parquet_path, file_datetime))
                        
                    except ValueError:
                        print(f"Could not parse datetime from filename: {nc_file}")
                        continue
    
    # Sort by datetime (most recent first)
    all_netcdf_files.sort(key=lambda x: x[2], reverse=True)
    
    # Return only the file paths (remove datetime)
    file_pairs = [(path[0], path[1]) for path in all_netcdf_files]
    
    if dates_skipped:
        print(f"Skipped {len(dates_skipped)} dates with existing interpolated files")
    print(f"Found {len(file_pairs)} downloaded NetCDF files to process")
    print(f"Output directory: data/processed/himawari/h3/realtime/")
    
    if all_netcdf_files:
        print(f"Date range: {all_netcdf_files[-1][2]} to {all_netcdf_files[0][2]}")
    
    return file_pairs

def main():
    """Main streaming processing function"""
    import sys
    
    # Parse command line arguments for data type
    data_type = "historical"  # Default
    hours = 24  # Default for realtime
    keep_netcdf = False  # Default: delete NetCDF files
    country_codes = ["LAO", "THA"]  # Default countries
    
    # Parse arguments
    i = 1
    while i < len(sys.argv):
        arg = sys.argv[i]
        if arg == "realtime":
            data_type = "realtime"
            # Check if next argument is hours
            if i + 1 < len(sys.argv) and sys.argv[i + 1].isdigit():
                try:
                    hours = int(sys.argv[i + 1])
                    i += 1  # Skip next argument as it's hours
                except ValueError:
                    print("Invalid hours parameter, using default 24")
                    hours = 24
        elif arg == "--keep-netcdf":
            keep_netcdf = True
        elif arg == "--delete-netcdf":
            keep_netcdf = False
        elif arg == "--countries":
            # Parse country codes from next arguments until we hit another flag or end
            if i + 1 < len(sys.argv):
                country_codes = []
                j = i + 1
                while j < len(sys.argv) and not sys.argv[j].startswith("--"):
                    country_codes.append(sys.argv[j].upper())
                    j += 1
                i = j - 1  # Set i to last country code processed
                if not country_codes:
                    print("No countries specified after --countries flag, using default: LAO, THA")
                    country_codes = ["LAO", "THA"]
        i += 1
    
    # If no keep/delete flag specified, default to delete for space efficiency
    # Default is already set to False (delete NetCDF files)
    print(f"Starting Himawari AOD Streaming H3 Pipeline ({data_type} mode)...")
    if data_type == "realtime":
        print(f"Processing ALL downloaded files (no time filtering)")
    print(f"Countries: {', '.join(country_codes)}")
    print(f"NetCDF files will be {'kept' if keep_netcdf else 'deleted after processing'}")
    print(f"TIF files will always be deleted after processing (temporary files)")
    
    # Configuration
    hex_resolution = 8
    raw_path = "./data/raw/himawari"
    cache_dir = "./data/cache/himawari/tif"  # Temporary TIF storage
    compact = False
    
    # Create cache directory
    os.makedirs(cache_dir, exist_ok=True)
    
    # Create country boundaries (once) - ensure sorted for consistency
    try:
        sorted_country_codes = sorted(country_codes)
        boundaries_countries = create_country_boundaries(sorted_country_codes, buffer_degrees=0.4)
    except Exception as e:
        print(f"Error creating boundaries: {e}")
        return
    
    # Collect NetCDF files (skipping dates with existing interpolated files)
    try:
        all_netcdf_files = collect_netcdf_files(raw_path, data_type, hours, country_codes)
    except Exception as e:
        print(f"Error collecting NetCDF files: {e}")
        return
    
    if not all_netcdf_files:
        print("No NetCDF files found to process")
        return
    
    # Process files sequentially to avoid memory issues
    print(f"Processing {len(all_netcdf_files)} files sequentially...")
    
    results = []
    
    for i, (netcdf, parquet) in enumerate(all_netcdf_files):
        print(f"Processing file {i+1}/{len(all_netcdf_files)}: {os.path.basename(netcdf)}")
        
        # Update parquet path to include country codes (sorted alphabetically)
        sorted_country_codes = sorted(country_codes)
        countries_str = "_".join(sorted_country_codes)
        parquet_dir = os.path.dirname(parquet)
        parquet_filename = os.path.basename(parquet)
        parquet_base, parquet_ext = os.path.splitext(parquet_filename)
        new_parquet_filename = f"{parquet_base}_{countries_str}{parquet_ext}"
        new_parquet_path = os.path.join(parquet_dir, new_parquet_filename)
        
        try:
            result = process_single_file_streaming(netcdf, new_parquet_path, hex_resolution, boundaries_countries, cache_dir, compact, keep_netcdf)
            results.append(result)
            
            # Print progress every 50 files
            if (i + 1) % 50 == 0:
                success_count = sum(1 for r in results if r.startswith("Success"))
                skip_count = sum(1 for r in results if r.startswith("Skipped"))
                error_count = len(results) - success_count - skip_count
                print(f"Progress: {i+1}/{len(all_netcdf_files)} - Success: {success_count}, Skipped: {skip_count}, Errors: {error_count}")
                
        except Exception as e:
            error_msg = f"Error processing {netcdf}: {str(e)}"
            print(error_msg)
            results.append(error_msg)
    
    try:
        # Print summary
        success_count = sum(1 for r in results if r.startswith("Success"))
        skip_count = sum(1 for r in results if r.startswith("Skipped"))
        error_count = len(results) - success_count - skip_count
        
        print(f"\nStreaming processing complete!")
        print(f"Success: {success_count}")
        print(f"Skipped: {skip_count}")
        print(f"Errors: {error_count}")
        
        # Count NetCDF deletions for space savings report
        if not keep_netcdf:
            deleted_count = sum(1 for r in results if "NetCDF deleted" in r)
            failed_delete_count = sum(1 for r in results if "NetCDF delete failed" in r)
            if deleted_count > 0 or failed_delete_count > 0:
                print(f"\nSpace efficiency:")
                print(f"NetCDF files deleted: {deleted_count}")
                if failed_delete_count > 0:
                    print(f"NetCDF deletion failures: {failed_delete_count}")
                print(f"Storage savings: {deleted_count} NetCDF files removed to save space")
        
        if error_count > 0:
            print("\nErrors encountered:")
            for r in results:
                if not r.startswith("Success") and not r.startswith("Skipped"):
                    print(f"  {r}")
        
        # Clean up cache directory
        try:
            if os.path.exists(cache_dir):
                shutil.rmtree(cache_dir)
                print(f"\nCleaned up cache directory: {cache_dir}")
        except Exception as e:
            print(f"Warning: Could not clean up cache directory: {e}")
                    
    except Exception as e:
        print(f"Error during parallel processing: {e}")

def test_single_file():
    """Test streaming processing with a single file"""
    print("Testing single file streaming processing...")
    
    # Configuration
    hex_resolution = 8
    country_codes = ["LAO", "THA"]  # Default countries for testing
    cache_dir = "./data/cache/himawari/tif_test"
    
    # Find first NetCDF file
    raw_path = "./data/raw/himawari"
    test_file = None
    
    for month in sorted(os.listdir(raw_path)):
        month_path = os.path.join(raw_path, month)
        if not os.path.isdir(month_path):
            continue
        for day in sorted(os.listdir(month_path)):
            day_path = os.path.join(month_path, day)
            if not os.path.isdir(day_path):
                continue
            for nc_file in sorted(os.listdir(day_path)):
                if nc_file.endswith(".nc"):
                    test_file = os.path.join(day_path, nc_file)
                    break
            if test_file:
                break
        if test_file:
            break
    
    if not test_file:
        print("No NetCDF files found to test")
        return
    
    print(f"Testing with NetCDF file: {test_file}")
    
    # Create country boundaries
    try:
        boundaries_countries = create_country_boundaries(country_codes, buffer_degrees=0.4)
    except Exception as e:
        print(f"Error creating boundaries: {e}")
        return
    
    # Process the test file
    test_parquet = "./data/processed/himawari/h3/historical/test_streaming_output.parquet"
    result = process_single_file_streaming(test_file, test_parquet, hex_resolution, boundaries_countries, cache_dir, compact=False, keep_netcdf=True)
    print(f"Test result: {result}")
    
    # Verify result
    if os.path.exists(test_parquet) and result.startswith("Success"):
        df_read = pl.read_parquet(test_parquet)
        print(f"✅ SUCCESS! Streaming pipeline test passed")
        print(f"Test parquet shape: {df_read.shape}")
        print("Sample data:")
        print(df_read.head())
    
    # Clean up test cache
    try:
        if os.path.exists(cache_dir):
            shutil.rmtree(cache_dir)
            print(f"Cleaned up test cache: {cache_dir}")
    except Exception as e:
        print(f"Warning: Could not clean up test cache: {e}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test_single_file()
    else:
        main() 