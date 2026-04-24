#!/usr/bin/env python3
"""
Updated Himawari AOD H3 Processing Script using h3ronpy 0.22.0

This script uses h3ronpy's pandas raster_to_dataframe function but immediately 
converts to Polars for fast data processing and parquet output.
"""

import os
import pandas as pd
import polars as pl
import geopandas as gpd
import rasterio
from rasterio.mask import mask
import numpy as np
from dask import delayed, compute
from pathlib import Path

# Import h3ronpy pandas raster functions (available in 0.22.0)
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

def process_tif_to_h3_standard(tif_file_path, parquet_path, hex_resolution, boundaries_countries, compact=False):
    """Process a single TIF file to H3-indexed Parquet using h3ronpy pandas then Polars"""
    
    if os.path.exists(parquet_path):
        print(f"H3 processed {parquet_path} already exists, skipping...")
        return f"Skipped: {parquet_path}"

    try:
        print(f"Processing: {tif_file_path}")
        
        # Open and process raster
        with rasterio.open(tif_file_path) as src:
            # Read the data
            aod = src.read(1)
            
            # Check if data is valid
            if np.all(np.isnan(aod)) or np.all(aod == 0):
                print(f"No valid data in {tif_file_path}, skipping...")
                return f"No valid data: {tif_file_path}"
            
            # Clip raster to boundaries
            try:
                masked_aod, masked_transform = mask(src, boundaries_countries.geometry, crop=True)
                masked_aod = masked_aod[0]  # Extract first band
            except Exception as e:
                print(f"Error masking raster {tif_file_path}: {e}")
                return f"Masking error: {tif_file_path}"
            
            # Check if masked data is valid
            if np.all(np.isnan(masked_aod)) or np.all(masked_aod == 0):
                print(f"No valid data after masking {tif_file_path}, skipping...")
                return f"No valid data after masking: {tif_file_path}"

        # Convert raster to H3 using h3ronpy pandas function
        try:
            print(f"Converting to H3 resolution {hex_resolution}...")
            df_pandas = raster_to_dataframe(
                in_raster=masked_aod,
                transform=masked_transform,
                h3_resolution=hex_resolution,
                nodata_value=np.nan,
                compact=compact,
                geo=False  # Don't create geometries for faster processing
            )
            
            if df_pandas.empty:
                print(f"No valid H3 data for {tif_file_path}, skipping...")
                return f"No valid H3 data: {tif_file_path}"
                
        except Exception as e:
            print(f"Error converting to H3 {tif_file_path}: {e}")
            return f"H3 conversion error: {tif_file_path}"

        # Convert to Polars for fast processing
        try:
            print("Converting to Polars for fast processing...")
            df_pl = pl.from_pandas(df_pandas)
            
            # Clean and filter data using Polars (much faster than pandas)
            df_pl = (df_pl
                .filter(pl.col("value") > 0)  # Remove zero/negative values
                .drop_nulls(subset=["value"])  # Remove null values
                .filter(pl.col("value").is_not_nan())  # Remove NaN values
            )
            
            # Add metadata
            df_pl = df_pl.with_columns([
                pl.lit(os.path.basename(tif_file_path)).alias("source_file"),
                pl.lit(tif_file_path.split('/')[-3]).alias("month"),
                pl.lit(tif_file_path.split('/')[-2]).alias("day"),
                pl.col("cell").alias("h3_08"),  # Rename cell column to h3_08
                pl.col("value").alias("aod_value")  # Rename value to aod_value
            ]).select([
                "h3_08", "aod_value", "source_file", "month", "day"
            ])
            
            if df_pl.shape[0] == 0:
                print(f"No valid data after cleaning {tif_file_path}, skipping...")
                return f"No valid data after cleaning: {tif_file_path}"

        except Exception as e:
            print(f"Error processing with Polars {tif_file_path}: {e}")
            return f"Polars processing error: {tif_file_path}"

        # Save to Parquet using Polars (very fast)
        try:
            os.makedirs(os.path.dirname(parquet_path), exist_ok=True)
            df_pl.write_parquet(parquet_path)
            
            print(f"H3 processed {parquet_path} successfully! ({df_pl.shape[0]} records)")
            return f"Success: {parquet_path} ({df_pl.shape[0]} records)"
            
        except Exception as e:
            print(f"Error saving parquet {parquet_path}: {e}")
            return f"Parquet save error: {parquet_path}"
        
    except Exception as e:
        error_msg = f"Error processing {tif_file_path}: {str(e)}"
        print(error_msg)
        return error_msg

def collect_tif_files(base_path, data_type="historical", country_codes=None):
    """Collect all TIF files to process"""
    all_tif_files = []
    
    if not os.path.exists(base_path):
        raise FileNotFoundError(f"Base path does not exist: {base_path}")
    
    # Ensure country codes are sorted alphabetically to prevent duplicates
    if country_codes is None:
        country_codes = ["LAO", "THA"]  # Default
    country_codes = sorted(country_codes)
    countries_str = "_".join(country_codes)
    
    for month in sorted(os.listdir(base_path)):
        month_path = os.path.join(base_path, month)
        if not os.path.isdir(month_path):
            continue
            
        for day in sorted(os.listdir(month_path)):
            day_path = os.path.join(month_path, day)
            if not os.path.isdir(day_path):
                continue
                
            for hour_file in sorted(os.listdir(day_path)):
                if hour_file.endswith(".tif"):
                    tif_file_path = os.path.join(day_path, hour_file)
                    
                    # Create correct parquet path structure with country codes
                    # Replace /tif/ with /h3/{data_type}/ and add country codes to filename
                    base_parquet_path = tif_file_path.replace("/tif/", f"/h3/{data_type}/").replace(".tif", ".parquet")
                    parquet_dir = os.path.dirname(base_parquet_path)
                    parquet_filename = os.path.basename(base_parquet_path)
                    parquet_base, parquet_ext = os.path.splitext(parquet_filename)
                    new_parquet_filename = f"{parquet_base}_{countries_str}{parquet_ext}"
                    parquet_path = os.path.join(parquet_dir, new_parquet_filename)
                    
                    all_tif_files.append((tif_file_path, parquet_path))
    
    print(f"Found {len(all_tif_files)} TIF files to process")
    print(f"Output directory: data/processed/himawari/h3/{data_type}/")
    print(f"Country codes: {countries_str}")
    return all_tif_files

def test_single_file():
    """Test processing a single file"""
    print("Testing single file processing with h3ronpy 0.22.0...")
    
    # Configuration
    hex_resolution = 8
    country_codes = ["LAO", "THA"]
    
    # Find multiple TIF files to test with
    base_path = "./data/processed/himawari/tif"
    test_files = []
    
    for month in sorted(os.listdir(base_path)):
        month_path = os.path.join(base_path, month)
        if not os.path.isdir(month_path):
            continue
        for day in sorted(os.listdir(month_path)):
            day_path = os.path.join(month_path, day)
            if not os.path.isdir(day_path):
                continue
            for hour_file in sorted(os.listdir(day_path)):
                if hour_file.endswith(".tif"):
                    test_files.append(os.path.join(day_path, hour_file))
                    if len(test_files) >= 10:  # Try up to 10 files
                        break
            if len(test_files) >= 10:
                break
        if len(test_files) >= 10:
            break
    
    if not test_files:
        print("No TIF files found to test")
        return
    
    print(f"Found {len(test_files)} test files")
    
    # Create country boundaries - ensure sorted for consistency
    try:
        sorted_country_codes = sorted(country_codes)
        boundaries_countries = create_country_boundaries(sorted_country_codes, buffer_degrees=0.02)
    except Exception as e:
        print(f"Error creating boundaries: {e}")
        return
    
    # Try multiple files until we find one with valid data
    for i, test_file in enumerate(test_files):
        print(f"\nTesting file {i+1}/{len(test_files)}: {test_file}")
        
        # Process the test file
        test_parquet = f"./data/processed/himawari/h3/historical/test_output_{i}.parquet"
        result = process_tif_to_h3_standard(test_file, test_parquet, hex_resolution, boundaries_countries)
        print(f"Test result: {result}")
        
        # If successful, read back and verify using Polars
        if os.path.exists(test_parquet) and result.startswith("Success"):
            df_read = pl.read_parquet(test_parquet)
            print(f"✅ SUCCESS! Test parquet shape: {df_read.shape}")
            print("Sample data:")
            print(df_read.head())
            print(f"Data types:")
            print(df_read.dtypes)
            print(f"Value statistics:")
            print(df_read.select("aod_value").describe())
            break
        else:
            # Clean up failed test file
            if os.path.exists(test_parquet):
                os.remove(test_parquet)
    else:
        print("❌ No test files had valid data in the target region")

def main():
    """Main processing function"""
    print("Starting Himawari AOD H3 processing with h3ronpy 0.22.0 + Polars...")
    
    # Configuration
    hex_resolution = 8
    country_codes = ["LAO", "THA"]
    base_path = "./data/processed/himawari/tif"
    compact = False  # Set to True for mixed resolutions but smaller file sizes
    data_type = "historical"  # Change to "realtime" for current data
    
    # Create country boundaries - ensure sorted for consistency
    try:
        sorted_country_codes = sorted(country_codes)
        boundaries_countries = create_country_boundaries(sorted_country_codes, buffer_degrees=0.02)
    except Exception as e:
        print(f"Error creating boundaries: {e}")
        return
    
    # Collect TIF files
    try:
        all_tif_files = collect_tif_files(base_path, data_type, country_codes)
    except Exception as e:
        print(f"Error collecting TIF files: {e}")
        return
    
    if not all_tif_files:
        print("No TIF files found to process")
        return
    
    # Process files using Dask for parallel processing
    print(f"Creating {len(all_tif_files)} processing tasks...")
    tasks = [
        delayed(process_tif_to_h3_standard)(tif, parquet, hex_resolution, boundaries_countries, compact) 
        for tif, parquet in all_tif_files
    ]
    
    print("Starting parallel processing...")
    try:
        results = compute(*tasks)
        
        # Print summary
        success_count = sum(1 for r in results if r.startswith("Success"))
        skip_count = sum(1 for r in results if r.startswith("Skipped"))
        error_count = len(results) - success_count - skip_count
        
        print(f"\nProcessing complete!")
        print(f"Success: {success_count}")
        print(f"Skipped: {skip_count}")
        print(f"Errors: {error_count}")
        
        if error_count > 0:
            print("\nErrors encountered:")
            for r in results:
                if not r.startswith("Success") and not r.startswith("Skipped"):
                    print(f"  {r}")
                    
    except Exception as e:
        print(f"Error during parallel processing: {e}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test_single_file()
    else:
        main() 