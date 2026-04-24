#!/usr/bin/env python3
"""
Silver Dataset Generator

This script processes ERA5, Himawari, and FIRMS data into a clean "silver" dataset.
Supports both real-time and historical processing modes.

Usage:
    python src/make_silver.py --mode realtime --hours 24 --countries THA LAO
    python src/make_silver.py --mode historical --start-date 2024-06-01 --end-date 2024-06-03 --countries THA LAO
"""

import argparse
import logging
import os
import sys
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple
import io, requests
import numpy as np
import geopandas as gpd
import pandas as pd
import polars as pl
import polars_h3 as plh3
from h3ronpy.pandas.vector import geodataframe_to_cells
import pyarrow.parquet as pq

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Import the new configuration system
from src.utils.config_loader import ConfigLoader
from src.utils.logging_utils import setup_with_config
from src.utils.boundary_utils import create_country_boundaries


def create_date_dimension(start: date, end: date) -> pl.DataFrame:
    """
    Create a DataFrame containing a date column and extracts year, month, and day from each date.

    Parameters:
        start (date): Start date.
        end (date): End date.

    Returns:
        pl.DataFrame: DataFrame with 'date', 'year', 'month', and 'day' columns.
    """
    num_days = (end - start).days + 1
    dates = [start + timedelta(days=i) for i in range(num_days)]
    dates_df = pl.DataFrame({"date": dates})
    dates_df = dates_df.with_columns([
        pl.col("date").dt.year().alias("year"),
        pl.col("date").dt.month().alias("month"),
        pl.col("date").dt.day().alias("day"),
    ])
    return dates_df


def make_h3_index(
    country_iso3_list: List[str],
    resolution: int = None,
    geojson_url: str = None,
    cache_dir: str = None,
    config_loader: ConfigLoader = None
) -> pl.DataFrame:
    """
    Generate H3 indices for the specified countries based on ISO3 codes.

    Parameters:
        country_iso3_list (list of str): List of ISO3 country codes used to filter the dataset.
        resolution (int): H3 resolution level to use for cell generation (None = use config default).
        geojson_url (str): URL of the GeoJSON file containing country boundaries (None = use config default).
        cache_dir (str): Directory to store cached GeoJSON files (None = use config default).
        config_loader (ConfigLoader): Configuration loader instance.

    Returns:
        pl.DataFrame: A Polars DataFrame with columns 'ISO3' and 'cell' representing the country code and its corresponding H3 cell.
    """
    if config_loader is None:
        config_loader = ConfigLoader()
    
    # Use config defaults if not provided
    if resolution is None:
        resolution = config_loader.get_h3_resolution()
    
    if geojson_url is None:
        # Get boundaries config
        boundaries_config = config_loader.config.get('data_processing', {}).get('boundaries', {})
        geojson_url = boundaries_config.get('geoboundaries_url', 
                     "https://github.com/wmgeolab/geoBoundaries/raw/main/releaseData/CGAZ/geoBoundariesCGAZ_ADM0.geojson")
    


    if cache_dir is None:
        cache_dir = config_loader.get_path('cache.silver', create_if_missing=True)
    
    cache_path = cache_dir / "geoBoundariesCGAZ_ADM0.geojson"

    if cache_path.exists():
        world = gpd.read_file(cache_path)

    else:
        resp = requests.get(geojson_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=90)
        resp.raise_for_status()
        world = gpd.read_file(io.BytesIO(resp.content))
        world.to_file(cache_path, driver="GeoJSON")

    world_filtered = world[world["shapeGroup"].isin(country_iso3_list)]

    h3_cells = geodataframe_to_cells(world_filtered, resolution=resolution)
    h3_df = pl.DataFrame(h3_cells)
    h3_df = h3_df.with_columns(h3_df["shapeGroup"].alias("ISO3"))
    return h3_df.select("ISO3", "cell")


def make_base_frame(iso3_countries: List[str], resolution: int, start_date: date, end_date: date, config_loader: ConfigLoader = None) -> pl.LazyFrame:
    """
    Create the base frame with H3 cells and dates for all specified countries.
    
    Parameters:
        iso3_countries: List of ISO3 country codes
        resolution: H3 resolution level
        start_date: Start date for processing
        end_date: End date for processing
        config_loader: Configuration loader instance
        
    Returns:
        pl.LazyFrame: Base frame with cell, ISO3, and date columns
    """
    h3_df = make_h3_index(country_iso3_list=iso3_countries, resolution=resolution, config_loader=config_loader)
    dates_df = create_date_dimension(start_date, end_date)
    base_frame = h3_df.join(dates_df, how="cross")
    return base_frame.lazy()


def clean_himawari_aod_data(
    silver_df: pl.LazyFrame, 
    mode: str, 
    countries: List[str],
    cache_dir: str,
    logger,
    config_loader: ConfigLoader = None
) -> Tuple[str, pl.LazyFrame]:
    """
    Clean and align Himawari AOD data with the silver dataset.
    
    Updated to use interpolated data instead of kriged data.
    Expected input columns: ['h3_08', 'aod_1day_interpolated', 'date']
    Output columns: ['h3_08', 'aod_1day_interpolated', 'date']
    """
    if config_loader is None: 
        config_loader = ConfigLoader()
    
    try:
        cache_file = Path(cache_dir) / "himawari_aod.parquet"
        
        # Create country pattern
        countries_str = "_".join(sorted(countries))
        
        if mode == "realtime":
            # For realtime mode, look in interpolated realtime directory
            himawari_dir = Path("data/processed/himawari/interpolated/realtime")
            pattern = f"interpolated_h3_aod_*{countries_str}*.parquet"
        else:
            # For historical mode, look in interpolated historical directory  
            himawari_dir = Path("data/processed/himawari/interpolated/historical")
            pattern = f"interpolated_h3_aod_*{countries_str}*.parquet"
        
        # Get date range from silver_df to filter files
        silver_df_collected = silver_df.collect()
        if silver_df_collected.shape[0] > 0:
            min_date = silver_df_collected['date'].min()
            max_date = silver_df_collected['date'].max()
            logger.info(f"Filtering Himawari interpolated files for date range: {min_date} to {max_date}")
        else:
            logger.warning("No date range found in silver_df, will load all files")
            min_date = None
            max_date = None
        
        # Find matching files
        if himawari_dir.exists():
            himawari_files = list(himawari_dir.glob(pattern))
            logger.info(f"Found {len(himawari_files)} Himawari interpolated files matching pattern: {pattern}")
            
            # Filter by date range if available
            if min_date and max_date:
                filtered_himawari_files = []
                for file_path in himawari_files:
                    try:
                        # Extract date from filename
                        # Pattern: interpolated_h3_aod_YYYYMMDD_LAO_THA.parquet
                        filename = file_path.name
                        date_match = re.search(r'_(\d{8})_', filename)
                        
                        if date_match:
                            date_str = date_match.group(1)
                            file_date = datetime.strptime(date_str, "%Y%m%d").date()
                            if min_date <= file_date <= max_date:
                                filtered_himawari_files.append(file_path)
                    except Exception:
                        continue
                himawari_files = filtered_himawari_files
                logger.info(f"After date filtering: {len(himawari_files)} Himawari interpolated files")
                if himawari_files:
                    logger.info(f"Himawari interpolated files to process: {[f.name for f in himawari_files]}")
            
            if himawari_files:
                # Load and process files
                himawari_dfs = []
                
                for file_path in himawari_files:
                    try:
                        # Extract date from filename
                        filename = file_path.name
                        # Pattern: interpolated_h3_aod_YYYYMMDD_LAO_THA.parquet
                        date_match = re.search(r'_(\d{8})_', filename)
                        
                        if not date_match:
                            logger.warning(f"Could not extract date from filename: {filename}")
                            continue
                            
                        date_str = date_match.group(1)
                        file_date = datetime.strptime(date_str, "%Y%m%d").date()
                        
                        # Load data
                        df = pl.read_parquet(file_path)
                        
                        # Verify expected columns exist
                        if 'h3_08' not in df.columns or 'aod_1day_interpolated' not in df.columns:
                            logger.warning(f"Missing expected columns in {file_path}. Available: {df.columns}")
                            continue
                        
                        # Convert h3_08 to integer and include AOD column with flexible type handling
                        df = df.with_columns([
                            pl.col("h3_08").cast(pl.UInt64).alias("cell"),
                            # Use flexible casting for AOD column to handle type mismatches
                            pl.col("aod_1day_interpolated").cast(pl.Float64, strict=False),
                            pl.lit(file_date).alias("date")
                        ]).select(["cell", "date", "aod_1day_interpolated"])
                        
                        himawari_dfs.append(df)
                        logger.info(f"Loaded {df.shape[0]} AOD records from {filename}")
                        
                    except Exception as e:
                        logger.warning(f"Error processing Himawari file {file_path}: {e}")
                        continue
                
                if himawari_dfs:
                    # Combine all dataframes
                    combined_df = pl.concat(himawari_dfs)
                    
                    # CRITICAL: Remove duplicates (same cell-date can appear in multiple files)
                    initial_count = combined_df.shape[0]
                    combined_df = combined_df.unique(subset=["cell", "date"])
                    final_count = combined_df.shape[0]
                    
                    if initial_count != final_count:
                        logger.warning(f"Removed {initial_count - final_count} duplicate cell-date combinations from Himawari data")
                    
                    # Save to cache
                    Path(cache_dir).mkdir(parents=True, exist_ok=True)
                    combined_df.write_parquet(cache_file)
                    logger.info(f"Cached {combined_df.shape[0]} Himawari records to {cache_file}")
                    
                    return "success", combined_df.lazy()
                else:
                    logger.warning("No valid Himawari files could be processed")
            else:
                logger.warning(f"No Himawari files found matching pattern: {pattern}")
        else:
            logger.warning(f"Himawari directory does not exist: {himawari_dir}")
        
        # Return empty dataframe if no data found
        empty_df = pl.DataFrame({
            "cell": pl.Series([], dtype=pl.UInt64),
            "date": pl.Series([], dtype=pl.Date),
            "aod_1day_interpolated": pl.Series([], dtype=pl.Float64)
        })
        return "no_data", empty_df.lazy()
        
    except Exception as e:
        logger.error(f"Error processing Himawari data: {e}")
        # Provide more specific error information for debugging
        if "Float64" in str(e) and "Float32" in str(e):
            logger.error("This appears to be a data type compatibility issue between Float64 and Float32.")
            logger.error("This commonly occurs when processing data files created with different Polars versions or data sources.")
        empty_df = pl.DataFrame({
            "cell": pl.Series([], dtype=pl.UInt64),
            "date": pl.Series([], dtype=pl.Date),
            "aod_1day_interpolated": pl.Series([], dtype=pl.Float64)
        })
        return "error", empty_df.lazy()


def clean_himawari_raw_data(
    silver_df: pl.LazyFrame, 
    mode: str, 
    countries: List[str],
    cache_dir: str,
    logger,
    config_loader: ConfigLoader = None
) -> Tuple[str, pl.LazyFrame]:
    """
    Clean and align raw (non-kriged) Himawari AOD data with the silver dataset.
    
    Loads data from both for_kriging and incomplete_data directories.
    Expected input columns: ['h3_08', 'aod_avg', 'type']
    Output columns: ['cell', 'date', 'daily_mean_aod_raw']
    """
    if config_loader is None:
        config_loader = ConfigLoader()
    
    try:
        cache_file = Path(cache_dir) / "himawari_raw_aod.parquet"
        
        # Create country pattern - for raw data, we don't filter by country in filename
        # as the files contain all regions and we'll filter spatially later
        
        # Define base directories
        if mode == "realtime":
            base_dir = Path("data/processed/himawari/daily_aggregated/realtime")
        else:
            base_dir = Path("data/processed/himawari/daily_aggregated/historical")
        
        for_kriging_dir = base_dir / "for_kriging"
        incomplete_dir = base_dir / "incomplete_data"
        
        # Find matching files in both directories
        himawari_files = []
        
        # Get date range from silver_df to filter files
        silver_df_collected = silver_df.collect()
        if silver_df_collected.shape[0] > 0:
            min_date = silver_df_collected['date'].min()
            max_date = silver_df_collected['date'].max()
            logger.info(f"Filtering files for date range: {min_date} to {max_date}")
        else:
            logger.warning("No date range found in silver_df, will load all files")
            min_date = None
            max_date = None
        
        # Check for_kriging directory
        if for_kriging_dir.exists():
            if mode == "realtime":
                # For realtime, files might be named differently - check what's available
                for_kriging_files = list(for_kriging_dir.glob("*.parquet"))
            else:
                # For historical, files are named daily_h3_aod_YYYYMMDD_LAO_THA.parquet
                # Country codes are always in alphabetical order in filenames
                countries_str = "_".join(sorted(countries))
                for_kriging_files = list(for_kriging_dir.glob(f"daily_h3_aod_*_{countries_str}.parquet"))
            
            # Filter by date range if available
            if min_date and max_date:
                filtered_for_kriging = []
                for file_path in for_kriging_files:
                    try:
                        # Extract date from filename
                        filename = file_path.name
                        date_match = re.search(r'_(\d{8})_', filename)
                        if date_match:
                            file_date_str = date_match.group(1)
                            file_date = datetime.strptime(file_date_str, "%Y%m%d").date()
                            if min_date <= file_date <= max_date:
                                filtered_for_kriging.append(file_path)
                    except Exception:
                        continue
                for_kriging_files = filtered_for_kriging
            
            himawari_files.extend(for_kriging_files)
            logger.info(f"Found {len(for_kriging_files)} files in for_kriging directory")
            if for_kriging_files:
                logger.info(f"Files in for_kriging: {[f.name for f in for_kriging_files]}")
        
        # Check incomplete_data directory
        if incomplete_dir.exists():
            if mode == "realtime":
                # For realtime, files are now named incomplete_h3_aod_YYYYMMDD.parquet (daily format)
                incomplete_files = list(incomplete_dir.glob("incomplete_h3_aod_*.parquet"))
            else:
                # For historical, files are named incomplete_h3_aod_YYYYMMDD_LAO_THA.parquet
                # Country codes are always in alphabetical order in filenames
                countries_str = "_".join(sorted(countries))
                incomplete_files = list(incomplete_dir.glob(f"incomplete_h3_aod_*_{countries_str}.parquet"))
            
            # Filter by date range if available
            if min_date and max_date:
                filtered_incomplete = []
                for file_path in incomplete_files:
                    try:
                        # Extract date from filename
                        filename = file_path.name
                        date_match = re.search(r'_(\d{8})_', filename)
                        if date_match:
                            file_date_str = date_match.group(1)
                            file_date = datetime.strptime(file_date_str, "%Y%m%d").date()
                            if min_date <= file_date <= max_date:
                                filtered_incomplete.append(file_path)
                    except Exception:
                        continue
                incomplete_files = filtered_incomplete
            
            himawari_files.extend(incomplete_files)
            logger.info(f"Found {len(incomplete_files)} files in incomplete_data directory")
            if incomplete_files:
                logger.info(f"Files in incomplete_data: {[f.name for f in incomplete_files]}")
        
        logger.info(f"Found {len(himawari_files)} total raw Himawari files for date range")
        
        if himawari_files:
            # Load and process files
            himawari_dfs = []
            
            for file_path in himawari_files:
                try:
                    # Extract date from filename
                    filename = file_path.name
                    
                    # Pattern: daily_h3_aod_YYYYMMDD_LAO_THA.parquet or incomplete_h3_aod_YYYYMMDD.parquet
                    # Both historical and realtime now use the same daily format
                    date_match = re.search(r'_(\d{8})_', filename) or re.search(r'_(\d{8})\.parquet', filename)
                    
                    if not date_match:
                        logger.warning(f"Could not extract date from filename: {filename}")
                        continue
                        
                    date_str = date_match.group(1)
                    file_date = datetime.strptime(date_str, "%Y%m%d").date()
                    
                    # Load data
                    df = pl.read_parquet(file_path)
                    
                    # Verify expected columns exist
                    if 'h3_08' not in df.columns or 'aod_avg' not in df.columns:
                        logger.warning(f"Missing expected columns in {file_path}. Available: {df.columns}")
                        continue
                    
                    # Convert h3_08 to consistent format and rename columns
                    df = df.with_columns([
                        pl.col("h3_08").cast(pl.UInt64).alias("cell"),
                        pl.col("aod_avg").alias("daily_mean_aod_raw"),
                        pl.lit(file_date).alias("date")
                    ]).select(["cell", "date", "daily_mean_aod_raw"])
                    
                    himawari_dfs.append(df)
                    logger.info(f"Loaded {df.shape[0]} raw AOD records from {filename}")
                    
                except Exception as e:
                    logger.warning(f"Error processing raw Himawari file {file_path}: {e}")
                    continue
            
            if himawari_dfs:
                # Combine all dataframes
                combined_df = pl.concat(himawari_dfs)
                
                # Remove duplicates if any (same cell and date)
                combined_df = combined_df.unique(subset=["cell", "date"])
                
                # Save to cache
                Path(cache_dir).mkdir(parents=True, exist_ok=True)
                combined_df.write_parquet(cache_file)
                logger.info(f"Cached {combined_df.shape[0]} raw Himawari records to {cache_file}")
                
                return "success", combined_df.lazy()
            else:
                logger.warning("No valid raw Himawari files could be processed")
        else:
            logger.warning("No raw Himawari files found in either directory")
        
        # Return empty dataframe if no data found
        empty_df = pl.DataFrame({
            "cell": pl.Series([], dtype=pl.UInt64),
            "date": pl.Series([], dtype=pl.Date),
            "daily_mean_aod_raw": pl.Series([], dtype=pl.Float64)
        })
        return "no_data", empty_df.lazy()
        
    except Exception as e:
        logger.error(f"Error processing raw Himawari data: {e}")
        empty_df = pl.DataFrame({
            "cell": pl.Series([], dtype=pl.UInt64),
            "date": pl.Series([], dtype=pl.Date),
            "daily_mean_aod_raw": pl.Series([], dtype=pl.Float64)
        })
        return "error", empty_df.lazy()


def clean_firms_data(
    silver_df: pl.LazyFrame,
    mode: str,
    countries: List[str], 
    cache_dir: str,
    logger,
    config_loader: ConfigLoader = None
) -> Tuple[str, pl.LazyFrame]:
    """
    Clean and align FIRMS fire data with the silver dataset.
    
    Expected input columns: ['h3_08', 'fire_hotspot_strength', 'date']
    Output columns: ['cell', 'date', 'fire_hotspot_strength']
    """
    if config_loader is None:
        config_loader = ConfigLoader()
    
    try:
        cache_file = Path(cache_dir) / "firms_fire.parquet"
        
        # Create country patterns for both orders (LAO_THA and THA_LAO)
        countries_str_sorted = "_".join(sorted(countries))
        countries_str_reversed = "_".join(sorted(countries, reverse=True))
        
        if mode == "realtime":
            # For realtime mode, look in FIRMS realtime directory
            firms_dir = config_loader.get_path('processed.firms.h3.realtime')
            patterns = [
                f"firms_kde_h308_*{countries_str_sorted}*.parquet",
                f"firms_kde_h308_*{countries_str_reversed}*.parquet"
            ]
        else:
            # For historical mode, look in FIRMS historical directory
            firms_dir = config_loader.get_path('processed.firms.h3.historical')
            patterns = [
                f"firms_kde_h308_*{countries_str_sorted}*.parquet",
                f"firms_kde_h308_*{countries_str_reversed}*.parquet"
            ]
        
        # Get date range from silver_df to filter files
        silver_df_collected = silver_df.collect()
        if silver_df_collected.shape[0] > 0:
            min_date = silver_df_collected['date'].min()
            max_date = silver_df_collected['date'].max()
            logger.info(f"Filtering FIRMS files for date range: {min_date} to {max_date}")
        else:
            logger.warning("No date range found in silver_df, will load all files")
            min_date = None
            max_date = None
        
        # Find matching files
        if firms_dir and firms_dir.exists():
            firms_files = []
            for pattern in patterns:
                files = list(firms_dir.glob(pattern))
                firms_files.extend(files)
                logger.info(f"Pattern {pattern}: found {len(files)} files")
            
            # Remove duplicates
            firms_files = list(set(firms_files))
            logger.info(f"Total unique FIRMS files found: {len(firms_files)}")
            
            # Filter by date range if available
            if min_date and max_date:
                filtered_firms_files = []
                for file_path in firms_files:
                    try:
                        # Extract date from filename
                        filename = file_path.name
                        # Pattern: firms_kde_h308_*_YYYYMMDD.parquet (date is at the end)
                        date_match = re.search(r'_(\d{8})\.parquet', filename)
                        if date_match:
                            file_date_str = date_match.group(1)
                            file_date = datetime.strptime(file_date_str, "%Y%m%d").date()
                            if min_date <= file_date <= max_date:
                                filtered_firms_files.append(file_path)
                    except Exception:
                        continue
                firms_files = filtered_firms_files
                logger.info(f"After date filtering: {len(firms_files)} FIRMS files")
                if firms_files:
                    logger.info(f"FIRMS files to process: {[f.name for f in firms_files]}")
            
            if firms_files:
                # Load and process files
                firms_dfs = []
                
                for file_path in firms_files:
                    try:
                        # Load data
                        df = pl.read_parquet(file_path)
                        
                        # Handle different FIRMS file schemas
                        if 'h3_08' in df.columns and 'fire_hotspot_strength' in df.columns:
                            # Standard schema with date column
                            df = df.with_columns([
                                pl.col('h3_08').cast(pl.UInt64).alias('cell'),
                                pl.col('fire_hotspot_strength').alias('fire_hotspot_strength'),
                                pl.col('date').cast(pl.Date)
                            ]).select(["cell", "date", "fire_hotspot_strength"])
                        elif 'h3_08_text' in df.columns and 'value' in df.columns:
                            # Alternative schema without date column - extract date from filename
                            filename = file_path.name
                            date_match = re.search(r'_(\d{8})\.parquet', filename)
                            if date_match:
                                file_date_str = date_match.group(1)
                                file_date = datetime.strptime(file_date_str, '%Y%m%d').date()
                            else:
                                logger.warning(f"Could not extract date from filename: {filename}")
                                continue
                            df = df.with_columns([
                                plh3.str_to_int("h3_08_text").alias('cell'),
                                pl.col('value').alias('fire_hotspot_strength'),
                                pl.lit(file_date).cast(pl.Date).alias('date')
                            ]).select(["cell", "date", "fire_hotspot_strength"])
                        else:
                            logger.warning(f"Unexpected FIRMS schema in {file_path}. Available: {df.columns}")
                            continue
                        
                        firms_dfs.append(df)
                        logger.info(f"Loaded {df.shape[0]} fire records from {file_path.name}")
                        
                    except Exception as e:
                        logger.warning(f"Error processing FIRMS file {file_path}: {e}")
                        continue
                
                if firms_dfs:
                    # Combine all dataframes
                    combined_df = pl.concat(firms_dfs)
                    
                    # CRITICAL: Remove duplicates (same cell-date can appear in multiple files or multiple fires)
                    initial_count = combined_df.shape[0]
                    # For multiple fires in same cell-date, sum the fire_hotspot_strength
                    combined_df = combined_df.group_by(["cell", "date"]).agg([
                        pl.col("fire_hotspot_strength").sum().alias("fire_hotspot_strength")
                    ])
                    final_count = combined_df.shape[0]
                    
                    if initial_count != final_count:
                        logger.warning(f"Aggregated {initial_count} FIRMS records into {final_count} unique cell-date combinations (summed fire strengths)")
                    
                    # Save to cache
                    Path(cache_dir).mkdir(parents=True, exist_ok=True)
                    combined_df.write_parquet(cache_file)
                    logger.info(f"Cached {combined_df.shape[0]} FIRMS records to {cache_file}")
                    
                    return "success", combined_df.lazy()
                else:
                    logger.warning("No valid FIRMS files could be processed")
            else:
                logger.warning(f"No FIRMS files found matching pattern: {pattern}")
        else:
            logger.warning(f"FIRMS directory does not exist: {firms_dir}")
        
        # Return empty dataframe if no data found
        empty_df = pl.DataFrame({
            "cell": pl.Series([], dtype=pl.UInt64),
            "date": pl.Series([], dtype=pl.Date),
            "fire_hotspot_strength": pl.Series([], dtype=pl.Float64)
        })
        return "no_data", empty_df.lazy()
        
    except Exception as e:
        logger.error(f"Error processing FIRMS data: {e}")
        empty_df = pl.DataFrame({
            "cell": pl.Series([], dtype=pl.UInt64),
            "date": pl.Series([], dtype=pl.Date),
            "fire_hotspot_strength": pl.Series([], dtype=pl.Float64)
        })
        return "error", empty_df.lazy()


def clean_era5_data(
    silver_df: pl.LazyFrame,
    mode: str,
    countries: List[str],
    cache_dir: str,
    logger,
    config_loader: ConfigLoader = None
) -> Tuple[str, pl.LazyFrame]:
    """
    Clean and align ERA5 meteorological data with the silver dataset.
    
    Updated for IDW Integration:
    - Real-time: expects era5_daily_mean_YYYY-MM-DD_THA_LAO.parquet files
    - Historical: expects era5_daily_mean_YYYY-MM-DD_THA_LAO.parquet files
    
    Expected input columns: ['h3_08', '10u', '10v', '2d', '2t', 'date'] or ['h3_08', '10u', '10v', '2d', '2t', 'window_start', 'window_end']
    Output columns: ['cell', 'date', 'temperature_2m', 'wind_u_10m', 'wind_v_10m', 'dewpoint_2m']
    """
    if config_loader is None:
        config_loader = ConfigLoader()
    
    try:
        cache_file = Path(cache_dir) / "era5_weather.parquet"
        
        if mode == "realtime":
            era5_dir = config_loader.get_path('processed.era5.daily_aggregated.realtime')
            # Use wildcard pattern that matches our new IDW naming convention
            patterns = [
                f"era5_daily_mean_*.parquet"  # Updated: matches our IDW real-time output
            ]
        else:
            era5_dir = config_loader.get_path('processed.era5.daily_aggregated.historical')
            # Use wildcard pattern that matches any country combination
            patterns = [
                f"era5_daily_mean_*.parquet"
            ]
        
        # Get date range from silver_df to filter files
        silver_df_collected = silver_df.collect()
        if silver_df_collected.shape[0] > 0:
            min_date = silver_df_collected['date'].min()
            max_date = silver_df_collected['date'].max()
            logger.info(f"Filtering ERA5 files for date range: {min_date} to {max_date}")
        else:
            logger.warning("No date range found in silver_df, will load all files")
            min_date = None
            max_date = None
        
        # Find matching files
        if era5_dir and era5_dir.exists():
            era5_files = []
            for pattern in patterns:
                files = list(era5_dir.glob(pattern))
                era5_files.extend(files)
                logger.info(f"Pattern {pattern}: found {len(files)} files")
            
            # CRITICAL: Remove duplicate file paths (can happen with wildcard patterns)
            era5_files = list(set(era5_files))
            logger.info(f"Total unique ERA5 files found: {len(era5_files)}")
            
            # Filter by date range if available
            if min_date and max_date and (mode == "historical" or mode == "realtime"):
                filtered_era5_files = []
                for file_path in era5_files:
                    try:
                        # Extract date from filename
                        filename = file_path.name
                        # Pattern: era5_daily_mean_YYYY-MM-DD_*.parquet (works for both historical and real-time IDW)
                        date_match = re.search(r'_(\d{4}-\d{2}-\d{2})_', filename)
                        if date_match:
                            file_date_str = date_match.group(1)
                            file_date = datetime.strptime(file_date_str, "%Y-%m-%d").date()
                            if min_date <= file_date <= max_date:
                                filtered_era5_files.append(file_path)
                    except Exception:
                        continue
                era5_files = filtered_era5_files
                logger.info(f"After date filtering: {len(era5_files)} ERA5 files")
                if era5_files:
                    logger.info(f"ERA5 files to process: {[f.name for f in era5_files]}")
            
            logger.info(f"Found {len(era5_files)} ERA5 files total")
            
            if era5_files:
                era5_dfs = []
                
                for file_path in era5_files:
                    try:
                        # Load data
                        df = pl.read_parquet(file_path)
                        
                        # Handle h3_08 column type
                        if 'h3_08' in df.columns:
                            if df['h3_08'].dtype == pl.UInt64:
                                df = df.with_columns(pl.col('h3_08').cast(pl.Utf8))
                        
                        # Handle date column
                        if 'date' not in df.columns:
                            if 'window_start' in df.columns and 'window_end' in df.columns:
                                # Use window_end as the date
                                df = df.with_columns(pl.col('window_end').cast(pl.Date).alias('date'))
                            else:
                                logger.warning(f"Missing date columns in {file_path}. Available: {df.columns}")
                                continue
                        
                        # Verify expected columns exist
                        required_cols = ['h3_08', '10u', '10v', '2d', '2t', 'date']
                        missing_cols = [col for col in required_cols if col not in df.columns]
                        if missing_cols:
                            logger.warning(f"Missing columns {missing_cols} in {file_path}. Available: {df.columns}")
                            continue
                        
                        # Map columns to standardized names
                        logger.info(f"Processing ERA5 file {file_path.name}")
                        logger.info(f"Initial h3_08 dtype: {df['h3_08'].dtype}")
                        logger.info(f"Initial shape: {df.shape}")
                        
                        # Map columns to standardized names - ensure cell is UInt64
                        df = df.with_columns([
                            pl.col('h3_08').cast(pl.UInt64).alias('cell'),  # Cast to UInt64
                            pl.col("2t").alias("temperature_2m"),
                            pl.col("10u").alias("wind_u_10m"),
                            pl.col("10v").alias("wind_v_10m"),
                            pl.col("2d").alias("dewpoint_2m"),
                            pl.col("date").cast(pl.Date)
                        ]).select(["cell", "date", "temperature_2m", "wind_u_10m", "wind_v_10m", "dewpoint_2m"])
                        
                        logger.info(f"After processing - shape: {df.shape}")
                        logger.info(f"After processing - cell dtype: {df['cell'].dtype}")
                        logger.info(f"After processing - unique cells: {df['cell'].n_unique()}")
                        
                        era5_dfs.append(df)
                        logger.info(f"Loaded {df.shape[0]} ERA5 records from {file_path.name}")
                        
                    except Exception as e:
                        logger.warning(f"Error processing ERA5 file {file_path}: {e}")
                        continue
                
                if era5_dfs:
                    # Combine all dataframes
                    combined_df = pl.concat(era5_dfs)
                    logger.info(f"Combined ERA5 data - shape: {combined_df.shape}")
                    logger.info(f"Combined ERA5 data - unique cells: {combined_df['cell'].n_unique()}")
                    
                    if mode == "realtime":
                        # For realtime mode with IDW, data is already daily aggregated per cell
                        # Just ensure we have one record per cell (remove any duplicates)
                        logger.info("Processing realtime IDW data (already daily aggregated)...")
                        combined_df = combined_df.unique(subset=["cell", "date"])
                        logger.info(f"After deduplication - shape: {combined_df.shape}")
                        logger.info(f"After deduplication - unique cells: {combined_df['cell'].n_unique()}")
                        logger.info(f"After deduplication - sample of data:\n{combined_df.head()}")
                    else:
                        # For historical mode, remove duplicates (in case both THA_LAO and LAO_THA files exist)
                        combined_df = combined_df.unique(subset=["cell", "date"])
                    
                    # Save to cache
                    Path(cache_dir).mkdir(parents=True, exist_ok=True)
                    combined_df.write_parquet(cache_file)
                    logger.info(f"Cached {combined_df.shape[0]} ERA5 records to {cache_file}")
                    
                    return "success", combined_df.lazy()
                else:
                    logger.warning("No valid ERA5 files could be processed")
            else:
                logger.warning(f"No ERA5 files found with any of the patterns: {patterns}")
        else:
            logger.warning(f"ERA5 directory does not exist: {era5_dir}")
        
        # Return empty dataframe if no data found
        empty_df = pl.DataFrame({
            "cell": pl.Series([], dtype=pl.UInt64),
            "date": pl.Series([], dtype=pl.Date),
            "temperature_2m": pl.Series([], dtype=pl.Float64),
            "wind_u_10m": pl.Series([], dtype=pl.Float64),
            "wind_v_10m": pl.Series([], dtype=pl.Float64),
            "dewpoint_2m": pl.Series([], dtype=pl.Float64)
        })
        return "no_data", empty_df.lazy()
        
    except Exception as e:
        logger.error(f"Error processing ERA5 data: {e}")
        empty_df = pl.DataFrame({
            "cell": pl.Series([], dtype=pl.UInt64),
            "date": pl.Series([], dtype=pl.Date),
            "temperature_2m": pl.Series([], dtype=pl.Float64),
            "wind_u_10m": pl.Series([], dtype=pl.Float64),
            "wind_v_10m": pl.Series([], dtype=pl.Float64),
            "dewpoint_2m": pl.Series([], dtype=pl.Float64)
        })
        return "error", empty_df.lazy()


def clean_air_quality_data(
    silver_df: pl.LazyFrame,
    mode: str,
    countries: List[str],
    cache_dir: str,
    logger,
    config_loader: ConfigLoader = None
) -> Tuple[str, pl.LazyFrame]:
    """
    Clean and align air quality data (PM2.5) with the silver dataset.
    
    Expected input columns: ['h3_08_text', 'value', 'date_utc']
    Output columns: ['cell', 'date', 'pm25_value']
    """
    if config_loader is None:
        config_loader = ConfigLoader()
    
    try:
        cache_file = Path(cache_dir) / "air_quality_pm25.parquet"
        
        # Create country pattern
        countries_str = "_".join(sorted(countries))
        
        # Look in the processed air quality directory
        aq_dir = config_loader.get_path(f'processed.airquality.{mode}')
        pattern = f"air_quality_{mode}_{countries_str}_*.parquet"
        
        # Get date range from silver_df to filter files
        silver_df_collected = silver_df.collect()
        if silver_df_collected.shape[0] > 0:
            min_date = silver_df_collected['date'].min()
            max_date = silver_df_collected['date'].max()
            logger.info(f"Filtering air quality files for date range: {min_date} to {max_date}")
        else:
            logger.warning("No date range found in silver_df, will load all files")
            min_date = None
            max_date = None
        
        # Find matching files
        if aq_dir and aq_dir.exists():
            aq_files = list(aq_dir.glob(pattern))
            logger.info(f"Found {len(aq_files)} air quality files matching pattern: {pattern}")
            
                    # Filter by date range if available
        if min_date and max_date:
            filtered_aq_files = []
            for file_path in aq_files:
                try:
                    # Extract date from filename
                    filename = file_path.name
                    
                    # For realtime files, check if the file contains data for our target date
                    # Pattern: air_quality_realtime_LAO_THA_YYYYMMDD.parquet
                    date_match = re.search(r'_(\d{8})\.parquet', filename)
                    if date_match:
                        file_date_str = date_match.group(1)
                        file_date = datetime.strptime(file_date_str, "%Y%m%d").date()
                        # Check if our target date matches the file date
                        if min_date <= file_date <= max_date:
                            filtered_aq_files.append(file_path)
                    else:
                        # Try alternative patterns as fallback
                        # Pattern: air_quality_historical_THA_LAO_YYYY-MM-DD_to_YYYY-MM-DD.parquet (date range)
                        date_range_match = re.search(r'_(\d{4}-\d{2}-\d{2})_to_(\d{4}-\d{2}-\d{2})\.parquet', filename)
                        if date_range_match:
                            start_str = date_range_match.group(1)
                            end_str = date_range_match.group(2)
                            file_start = datetime.strptime(start_str, "%Y-%m-%d").date()
                            file_end = datetime.strptime(end_str, "%Y-%m-%d").date()
                            # Check if our target date range overlaps with file date range
                            if min_date <= file_end and max_date >= file_start:
                                filtered_aq_files.append(file_path)
                        else:
                            # Try single date pattern as fallback
                            date_match = re.search(r'_(\d{8})_', filename)
                            if date_match:
                                file_date_str = date_match.group(1)
                                file_date = datetime.strptime(file_date_str, "%Y%m%d").date()
                                if min_date <= file_date <= max_date:
                                    filtered_aq_files.append(file_path)
                except Exception:
                    continue
            aq_files = filtered_aq_files
            logger.info(f"After date filtering: {len(aq_files)} air quality files")
            if aq_files:
                logger.info(f"Air quality files to process: {[f.name for f in aq_files]}")
            
            if aq_files:
                # Load and process files
                aq_dfs = []
                
                for file_path in aq_files:
                    try:
                        # Load data
                        df = pl.read_parquet(file_path)
                        
                        # Verify expected columns exist
                        if 'h3_08_text' not in df.columns or 'value' not in df.columns:
                            logger.warning(f"Missing expected columns in {file_path}. Available: {df.columns}")
                            continue
                        
                        # Convert h3_08_text to integer for consistency
                        df = df.with_columns([
                            plh3.str_to_int("h3_08_text").alias("cell"),
                            pl.col("value").alias("pm25_value"),
                            pl.col("date_utc").cast(pl.Date).alias("date"),
                            pl.col("source").alias("pm25_source")
                        ]).select(["cell", "date", "pm25_value", "pm25_source"])
                        
                        aq_dfs.append(df)
                        logger.info(f"Loaded {df.shape[0]} air quality records from {file_path.name}")
                        
                    except Exception as e:
                        logger.warning(f"Error processing air quality file {file_path}: {e}")
                        continue
                
                if aq_dfs:
                    # Combine all dataframes
                    combined_df = pl.concat(aq_dfs)
                    
                    # Remove duplicates and calculate daily mean per cell
                    # For source, take the first one if multiple sources exist for same cell-date
                    combined_df = combined_df.group_by(["cell", "date"]).agg([
                        pl.col("pm25_value").mean().alias("pm25_value"),
                        pl.col("pm25_source").first().alias("pm25_source")
                    ])
                    
                    # Save to cache
                    Path(cache_dir).mkdir(parents=True, exist_ok=True)
                    combined_df.write_parquet(cache_file)
                    logger.info(f"Cached {combined_df.shape[0]} air quality records to {cache_file}")
                    
                    return "success", combined_df.lazy()
                else:
                    logger.warning("No valid air quality files could be processed")
            else:
                logger.warning(f"No air quality files found matching pattern: {pattern}")
        else:
            logger.warning(f"Air quality directory does not exist: {aq_dir}")
        
        # Return empty dataframe if no data found
        empty_df = pl.DataFrame({
            "cell": pl.Series([], dtype=pl.UInt64),
            "date": pl.Series([], dtype=pl.Date),
            "pm25_value": pl.Series([], dtype=pl.Float64),
            "pm25_source": pl.Series([], dtype=pl.Utf8)
        })
        return "no_data", empty_df.lazy()
        
    except Exception as e:
        logger.error(f"Error processing air quality data: {e}")
        empty_df = pl.DataFrame({
            "cell": pl.Series([], dtype=pl.UInt64),
            "date": pl.Series([], dtype=pl.Date),
            "pm25_value": pl.Series([], dtype=pl.Float64),
            "pm25_source": pl.Series([], dtype=pl.Utf8)
        })
        return "error", empty_df.lazy()


def clean_elevation_data(
    silver_df: pl.LazyFrame,
    countries: List[str],
    logger,
    config_loader: ConfigLoader = None
) -> Tuple[str, pl.LazyFrame]:
    """
    Process elevation data and merge it with the silver dataset.
    
    Parameters:
        silver_df (LazyFrame): Base silver dataset
        countries (List[str]): List of country codes
        logger: Logger instance
        config_loader: Configuration loader instance
        
    Returns:
        Tuple[str, pl.LazyFrame]: Status and elevation data
    """
    if config_loader is None:
        config_loader = ConfigLoader()
        
    try:
        # Only process if countries include THA or LAO
        if not any(country in ['THA', 'LAO'] for country in countries):
            logger.info("Skipping elevation data - not available for specified countries")
            return "skipped", pl.DataFrame({
                "cell": pl.Series([], dtype=pl.UInt64),
                "elevation": pl.Series([], dtype=pl.Float64)
            }).lazy()
            
        # Load elevation data using config path
        elevation_file = config_loader.get_path('assets.dem') / "LAO_THA_elevation.csv"
        if not elevation_file.exists():
            logger.warning(f"Elevation file not found: {elevation_file}")
            return "no_data", pl.DataFrame({
                "cell": pl.Series([], dtype=pl.UInt64),
                "elevation": pl.Series([], dtype=pl.Float64)
            }).lazy()
            
        # Read and process elevation data
        elevation_df = pl.read_csv(elevation_file).with_columns([
            pl.col("cell").cast(pl.UInt64),
            pl.col("elevation").cast(pl.Float64)
        ]).drop(["hex_id", ""])  # Drop unnecessary columns as in original implementation
        
        # CRITICAL: Remove duplicates to prevent row multiplication in joins
        initial_count = elevation_df.shape[0]
        elevation_df = elevation_df.unique(subset=["cell"])
        final_count = elevation_df.shape[0]
        
        if initial_count != final_count:
            logger.warning(f"Removed {initial_count - final_count} duplicate cells from elevation data!")
        
        logger.info(f"Loaded {elevation_df.shape[0]} unique elevation records")
        return "success", elevation_df.lazy()
        
    except Exception as e:
        logger.error(f"Error processing elevation data: {e}")
        return "error", pl.DataFrame({
            "cell": pl.Series([], dtype=pl.UInt64),
            "elevation": pl.Series([], dtype=pl.Float64)
        }).lazy()


def clean_landcover_data(
    silver_df: pl.LazyFrame,
    countries: List[str],
    logger,
    config_loader: ConfigLoader = None
) -> Tuple[str, pl.LazyFrame]:
    """
    Process landcover data and merge it with the silver dataset.
    
    Parameters:
        silver_df (LazyFrame): Base silver dataset
        countries (List[str]): List of country codes
        logger: Logger instance
        config_loader: Configuration loader instance
        
    Returns:
        Tuple[str, pl.LazyFrame]: Status and landcover data
    """
    if config_loader is None:
        config_loader = ConfigLoader()
        
    try:
        # Only process if countries include THA or LAO
        if not any(country in ['THA', 'LAO'] for country in countries):
            logger.info("Skipping landcover data - not available for specified countries")
            return "skipped", pl.DataFrame({
                "cell": pl.Series([], dtype=pl.UInt64),
                "trees": pl.Series([], dtype=pl.Float64),
                "grass": pl.Series([], dtype=pl.Float64),
                "shrub_and_scrub": pl.Series([], dtype=pl.Float64),
                "crops": pl.Series([], dtype=pl.Float64),
                "bare": pl.Series([], dtype=pl.Float64),
                "snow_and_ice": pl.Series([], dtype=pl.Float64),
                "flooded_vegetation": pl.Series([], dtype=pl.Float64),
                "built": pl.Series([], dtype=pl.Float64),
                "water": pl.Series([], dtype=pl.Float64)
            }).lazy()
            
        # Load landcover data using config path
        landcover_file = config_loader.get_path('assets.landcover') / "LAO_THA_landcover.csv"
        if not landcover_file.exists():
            logger.warning(f"Landcover file not found: {landcover_file}")
            return "no_data", pl.DataFrame({
                "cell": pl.Series([], dtype=pl.UInt64),
                "trees": pl.Series([], dtype=pl.Float64),
                "grass": pl.Series([], dtype=pl.Float64),
                "shrub_and_scrub": pl.Series([], dtype=pl.Float64),
                "crops": pl.Series([], dtype=pl.Float64),
                "bare": pl.Series([], dtype=pl.Float64),
                "snow_and_ice": pl.Series([], dtype=pl.Float64),
                "flooded_vegetation": pl.Series([], dtype=pl.Float64),
                "built": pl.Series([], dtype=pl.Float64),
                "water": pl.Series([], dtype=pl.Float64)
            }).lazy()
            
        # Read and process landcover data
        landcover_df = pl.read_csv(landcover_file).with_columns([
            pl.col("cell").cast(pl.UInt64),
            pl.col("trees").cast(pl.Float64),
            pl.col("grass").cast(pl.Float64),
            pl.col("shrub_and_scrub").cast(pl.Float64),
            pl.col("crops").cast(pl.Float64),
            pl.col("bare").cast(pl.Float64),
            pl.col("snow_and_ice").cast(pl.Float64),
            pl.col("flooded_vegetation").cast(pl.Float64),
            pl.col("built").cast(pl.Float64),
            pl.col("water").cast(pl.Float64)
        ]).select([
            "cell", "trees", "grass", "shrub_and_scrub", "crops", "bare",
            "snow_and_ice", "flooded_vegetation", "built", "water"
        ])
        
        # CRITICAL: Remove duplicates to prevent row multiplication in joins
        initial_count = landcover_df.shape[0]
        landcover_df = landcover_df.unique(subset=["cell"])
        final_count = landcover_df.shape[0]
        
        if initial_count != final_count:
            logger.warning(f"Removed {initial_count - final_count} duplicate cells from landcover data!")
        
        logger.info(f"Loaded {landcover_df.shape[0]} unique landcover records")
        return "success", landcover_df.lazy()
        
    except Exception as e:
        logger.error(f"Error processing landcover data: {e}")
        return "error", pl.DataFrame({
            "cell": pl.Series([], dtype=pl.UInt64),
            "trees": pl.Series([], dtype=pl.Float64),
            "grass": pl.Series([], dtype=pl.Float64),
            "shrub_and_scrub": pl.Series([], dtype=pl.Float64),
            "crops": pl.Series([], dtype=pl.Float64),
            "bare": pl.Series([], dtype=pl.Float64),
            "snow_and_ice": pl.Series([], dtype=pl.Float64),
            "flooded_vegetation": pl.Series([], dtype=pl.Float64),
            "built": pl.Series([], dtype=pl.Float64),
            "water": pl.Series([], dtype=pl.Float64)
        }).lazy()


def clean_worldpop_data(
    silver_df: pl.LazyFrame,
    countries: List[str],
    logger,
    config_loader: ConfigLoader = None
) -> Tuple[str, pl.LazyFrame]:
    """
    Process WorldPop population data and merge it with the silver dataset.
    
    Parameters:
        silver_df (LazyFrame): Base silver dataset
        countries (List[str]): List of country codes
        logger: Logger instance
        config_loader: Configuration loader instance
        
    Returns:
        Tuple[str, pl.LazyFrame]: Status and WorldPop data
    """
    if config_loader is None:
        config_loader = ConfigLoader()
        
    try:
        # Only process if countries include THA or LAO
        if not any(country in ['THA', 'LAO'] for country in countries):
            logger.info("Skipping WorldPop data - not available for specified countries")
            return "skipped", pl.DataFrame({
                "cell": pl.Series([], dtype=pl.UInt64),
                "worldpop_population": pl.Series([], dtype=pl.Float64)
            }).lazy()
            
        # Load WorldPop data using config path
        worldpop_file = config_loader.get_path('assets.worldpop') / "LAO_THA_worldpop_h3_08.csv"
        if not worldpop_file.exists():
            logger.warning(f"WorldPop file not found: {worldpop_file}")
            return "no_data", pl.DataFrame({
                "cell": pl.Series([], dtype=pl.UInt64),
                "worldpop_population": pl.Series([], dtype=pl.Float64)
            }).lazy()
            
        # Read and process WorldPop data
        worldpop_df = pl.read_csv(worldpop_file)
        
        # Check if we need to convert h3_08 to cell
        if "h3_08" in worldpop_df.columns:
            # Use polars_h3.str_to_int to convert h3_08 to cell
            worldpop_df = worldpop_df.with_columns([
                plh3.str_to_int("h3_08").alias("cell"),
                pl.col("general").cast(pl.Float64).alias("worldpop_population")
            ]).select(["cell", "worldpop_population"])
        else:
            worldpop_df = worldpop_df.with_columns([
                pl.col("cell").cast(pl.UInt64),
                pl.col("worldpop_population").cast(pl.Float64)
            ]).select(["cell", "worldpop_population"])
        
        # CRITICAL: Remove duplicates to prevent row multiplication in joins
        initial_count = worldpop_df.shape[0]
        worldpop_df = worldpop_df.unique(subset=["cell"])
        final_count = worldpop_df.shape[0]
        
        if initial_count != final_count:
            logger.warning(f"Removed {initial_count - final_count} duplicate cells from WorldPop data!")
        
        logger.info(f"Loaded {worldpop_df.shape[0]} unique WorldPop records")
        return "success", worldpop_df.lazy()
        
    except Exception as e:
        logger.error(f"Error processing WorldPop data: {e}")
        return "error", pl.DataFrame({
            "cell": pl.Series([], dtype=pl.UInt64),
            "worldpop_population": pl.Series([], dtype=pl.Float64)
        }).lazy()


def merge_silver_dataset(
    base_frame: pl.LazyFrame,
    himawari_lf: pl.LazyFrame, 
    himawari_raw_lf: pl.LazyFrame,
    firms_lf: pl.LazyFrame,
    era5_lf: pl.LazyFrame,
    air_quality_lf: pl.LazyFrame,
    elevation_lf: pl.LazyFrame,
    landcover_lf: pl.LazyFrame,
    worldpop_lf: pl.LazyFrame,
    mode: str,
    logger
) -> pl.DataFrame:
    """
    Merge all data sources into the final silver dataset.
    
    All input LazyFrames should have 'cell' and 'date' columns for joining.
    Static datasets (elevation, landcover, worldpop) are joined only on 'cell'.
    """
    logger.info("="*80)
    logger.info("STARTING MERGE_SILVER_DATASET - ROW COUNT DIAGNOSTICS")
    logger.info("="*80)
    
    # Start with base frame
    silver_lf = base_frame
    
    # Get shapes for logging
    base_shape = silver_lf.collect().shape
    himawari_shape = himawari_lf.collect().shape  
    firms_shape = firms_lf.collect().shape
    era5_shape = era5_lf.collect().shape
    air_quality_shape = air_quality_lf.collect().shape
    elevation_shape = elevation_lf.collect().shape
    landcover_shape = landcover_lf.collect().shape
    worldpop_shape = worldpop_lf.collect().shape
    
    logger.info(f"Base frame shape: {base_shape}")
    logger.info(f"  - Unique cells in base: {silver_lf.select('cell').unique().collect().shape[0]}")
    logger.info(f"  - Unique dates in base: {silver_lf.select('date').unique().collect().shape[0]}")
    logger.info(f"Himawari kriged data shape: {himawari_shape}")
    logger.info(f"FIRMS data shape: {firms_shape}")
    logger.info(f"ERA5 data shape: {era5_shape}")
    if era5_shape[0] > 0:
        era5_collected = era5_lf.collect()
        logger.info(f"  - Unique cells in ERA5: {era5_collected.select('cell').unique().shape[0]}")
        logger.info(f"  - Unique dates in ERA5: {era5_collected.select('date').unique().shape[0]}")
    logger.info(f"Air quality data shape: {air_quality_shape}")
    logger.info(f"Elevation data shape: {elevation_shape}")
    logger.info(f"Landcover data shape: {landcover_shape}")
    logger.info(f"WorldPop data shape: {worldpop_shape}")
    
    # First collect the base frame to pandas for static joins
    df_base_frame = silver_lf.collect().to_pandas()
    logger.info(f"After converting base frame to pandas: {df_base_frame.shape[0]:,} rows")
    
    # Join static datasets (elevation, landcover, worldpop)
    static_datasets = [
        ("elevation", elevation_lf, elevation_shape),
        ("landcover", landcover_lf, landcover_shape),
        ("worldpop", worldpop_lf, worldpop_shape)
    ]
    
    for name, dataset, shape in static_datasets:
        before_rows = df_base_frame.shape[0]
        if shape[0] > 0:
            logger.info(f"Joining {name} data...")
            df_dataset = dataset.collect().to_pandas()
            df_base_frame = df_base_frame.merge(df_dataset, on=["cell"], how="left")
            after_rows = df_base_frame.shape[0]
            logger.info(f"  After {name} join: {after_rows:,} rows (change: {after_rows - before_rows:+,})")
        else:
            logger.info(f"Added null {name} columns (no data)")
            if name == "elevation":
                df_base_frame["elevation"] = None
            elif name == "landcover":
                for col in ["trees", "grass", "shrub_and_scrub", "crops", "bare", 
                          "snow_and_ice", "flooded_vegetation", "built", "water"]:
                    df_base_frame[col] = None
            elif name == "worldpop":
                df_base_frame["worldpop_population"] = None
    
    # Convert back to polars for time-series joins
    silver_lf = pl.from_pandas(df_base_frame).lazy()
    logger.info(f"After static joins, converted back to polars LazyFrame")
    
    # Ensure consistent date types
    silver_lf = silver_lf.with_columns(pl.col("date").cast(pl.Date))
    
    # Join time-series data
    if himawari_shape[0] > 0:
        logger.info("Joining Himawari interpolated AOD data...")
        before_rows = silver_lf.select(pl.len()).collect().item()
        himawari_lf = himawari_lf.with_columns(pl.col("date").cast(pl.Date))
        silver_lf = silver_lf.join(
            himawari_lf,
            on=["cell", "date"], 
            how="left"
        )
        after_rows = silver_lf.select(pl.len()).collect().item()
        logger.info(f"  After Himawari join: {after_rows:,} rows (change: {after_rows - before_rows:+,})")
    else:
        logger.info("No Himawari interpolated AOD data available - using null values")
        logger.info("This indicates complete cloud cover or satellite data unavailability")
        silver_lf = silver_lf.with_columns([
            pl.lit(None, dtype=pl.Float64).alias("aod_1day_interpolated")
        ])
    
    
    # Join FIRMS data
    if firms_shape[0] > 0:
        logger.info("Joining FIRMS fire data...")
        before_rows = silver_lf.select(pl.len()).collect().item()
        firms_lf = firms_lf.with_columns(pl.col("date").cast(pl.Date))
        silver_lf = silver_lf.join(
            firms_lf,
            on=["cell", "date"],
            how="left"
        )
        after_rows = silver_lf.select(pl.len()).collect().item()
        logger.info(f"  After FIRMS join: {after_rows:,} rows (change: {after_rows - before_rows:+,})")
    else:
        logger.info("Added null FIRMS column (no data)")
        silver_lf = silver_lf.with_columns(
            pl.lit(None, dtype=pl.Float64).alias("fire_hotspot_strength")
        )
    
    # Join ERA5 data
    if era5_shape[0] > 0:
        logger.info("Joining ERA5 meteorological data...")
        before_rows = silver_lf.select(pl.len()).collect().item()
        era5_lf = era5_lf.with_columns(pl.col("date").cast(pl.Date))
        
        # CRITICAL: Always use cell+date join to preserve row count
        # Even in realtime mode, ERA5 data should have the same dates as base_frame
        logger.info(f"  Mode: {mode} - Using cell+date join (standard join for both modes)")
        silver_lf = silver_lf.join(
            era5_lf,
            on=["cell", "date"],
            how="left"
        )
        after_rows = silver_lf.select(pl.len()).collect().item()
        logger.info(f"  After ERA5 join: {after_rows:,} rows (change: {after_rows - before_rows:+,})")
        
        # Sanity check: row count should not change with left join
        if after_rows != before_rows:
            logger.warning(f"  ⚠️  ERA5 join changed row count from {before_rows:,} to {after_rows:,}")
            logger.warning(f"  ⚠️  This suggests duplicate cell+date combinations in ERA5 data or missing dates")
            # Get diagnostic info
            era5_collected = era5_lf.collect()
            logger.warning(f"  ERA5 unique cells: {era5_collected.select('cell').unique().shape[0]}")
            logger.warning(f"  ERA5 unique dates: {era5_collected.select('date').unique().shape[0]}")
            logger.warning(f"  ERA5 total rows: {era5_collected.shape[0]}")
    else:
        logger.info("Added null ERA5 columns (no data)")
        silver_lf = silver_lf.with_columns([
            pl.lit(None, dtype=pl.Float64).alias("temperature_2m"),
            pl.lit(None, dtype=pl.Float64).alias("wind_u_10m"),
            pl.lit(None, dtype=pl.Float64).alias("wind_v_10m"),
            pl.lit(None, dtype=pl.Float64).alias("dewpoint_2m")
        ])
    
    # Join Air Quality data
    if air_quality_shape[0] > 0:
        logger.info("Joining air quality data...")
        before_rows = silver_lf.select(pl.len()).collect().item()
        air_quality_lf = air_quality_lf.with_columns(pl.col("date").cast(pl.Date))
        silver_lf = silver_lf.join(
            air_quality_lf,
            on=["cell", "date"],
            how="left"
        )
        after_rows = silver_lf.select(pl.len()).collect().item()
        logger.info(f"  After Air Quality join: {after_rows:,} rows (change: {after_rows - before_rows:+,})")
    else:
        logger.info("Added null air quality columns (no data)")
        silver_lf = silver_lf.with_columns([
            pl.lit(None, dtype=pl.Float64).alias("pm25_value"),
            pl.lit(None, dtype=pl.Utf8).alias("pm25_source")
        ])
    
    # Collect final result
    silver_df = silver_lf.collect()
    logger.info(f"After collecting LazyFrame: {silver_df.shape[0]:,} rows")
    
    # Remove duplicates based on cell and date (keep first occurrence)
    logger.info("Removing duplicate cell-date combinations...")
    initial_count = silver_df.shape[0]
    silver_df = silver_df.unique(subset=["cell", "date"], keep="first")
    final_count = silver_df.shape[0]
    duplicates_removed = initial_count - final_count
    
    if duplicates_removed > 0:
        logger.info(f"Removed {duplicates_removed:,} duplicate records")
        logger.info(f"Records after deduplication: {final_count:,}")
    else:
        logger.info("No duplicate records found")
    
    logger.info(f"Final silver dataset shape: {silver_df.shape}")
    logger.info(f"  - Unique cells in final: {silver_df.select('cell').unique().shape[0]}")
    logger.info(f"  - Unique dates in final: {silver_df.select('date').unique().shape[0]}")
    logger.info("="*80)
    logger.info("END MERGE_SILVER_DATASET - ROW COUNT DIAGNOSTICS")
    logger.info("="*80)
    return silver_df


def apply_feature_engineering(
    silver_df_lazy: pl.LazyFrame,
    logger,
    clip_start_date: Optional[date] = None,
    clip_end_date: Optional[date] = None
) -> pl.LazyFrame:
    """
    Apply all feature engineering steps to silver dataset.
    
    Includes:
    - Fill NULL fires with 0
    - Create next_day_targets (PM2.5 offset by -1 day)
    - Calculate yesterday_parent_pm25 (parent H3-04 level)
    - Sort by cell and date
    - Calculate rolling averages (3-day and 7-day for 6 variables)
    - Clip to requested date range (if specified)
    
    Parameters:
        silver_df_lazy: Input silver dataset
        logger: Logger instance
        clip_start_date: Start date for final output (removes buffer if provided)
        clip_end_date: End date for final output (removes buffer if provided)
        
    Returns:
        LazyFrame with all engineered features
    """
    logger.info("="*80)
    logger.info("FEATURE ENGINEERING - ROW COUNT DIAGNOSTICS")
    logger.info("="*80)
    
    # Fill NULL fire_hotspot_strength with 0 BEFORE rolling calculations
    # This treats missing fire data as "no fires" in the rolling averages
    logger.info("Replacing NULL fire_hotspot_strength values with 0.0 (before rolling calculations)")
    null_count = silver_df_lazy.select(pl.col("fire_hotspot_strength").is_null().sum()).collect().item()
    logger.info(f"  Found {null_count:,} NULL values in fire_hotspot_strength")
    silver_df_lazy = silver_df_lazy.with_columns(
        pl.col("fire_hotspot_strength").fill_null(0.0)
    )
    logger.info("  Replaced with 0.0 (no fires)")
    logger.info(f"After filling fire nulls: {silver_df_lazy.select(pl.len()).collect().item():,} rows")

    # offset by one day 
    next_day_targets = (
        silver_df_lazy
        .select(["cell", "date", "pm25_value"])
        .with_columns(pl.col("date").dt.offset_by("-1d"))
    )

    silver_df_lazy_prepared = (
        silver_df_lazy
        .rename({"pm25_value": "current_day_pm25"})
        .join(next_day_targets, on=["cell", "date"], how="left")
        .unique()
    )
    
    logger.info(f"After joining next_day_targets and dedup: {silver_df_lazy_prepared.select(pl.len()).collect().item():,} rows")

    # Add air quality for previous day - parent h304
    silver_df_lazy_prepared = silver_df_lazy_prepared.with_columns(parent_h3_04 = plh3.cell_to_parent("cell", 4))
    yesterday_parent_pm25 = silver_df_lazy_prepared.select(["cell","date","current_day_pm25","parent_h3_04"])
    
    # Come back to this - To be rigorous we should *exclude* the current h3_08 from the calculation
    yesterday_parent_pm25 = yesterday_parent_pm25.group_by(["date", "parent_h3_04"]).agg([
        pl.col("current_day_pm25").mean().alias("parent_h3_04_pm25")
    ])
    
    # Sort by parent_h3_04 and date, then shift the parent_h3_04_pm25 by 1 day to get "yesterday's" value
    yesterday_parent_pm25 = (
        yesterday_parent_pm25
        .sort(["parent_h3_04", "date"])
        .with_columns(pl.col("parent_h3_04_pm25").shift(1).alias("yesterday_parent_h3_04_pm25")))
    
    # Join yesterday's parent h3_04 PM2.5 back to the main dataset
    silver_df_lazy_prepared = silver_df_lazy_prepared.join(yesterday_parent_pm25.drop("parent_h3_04_pm25"), on=["date", "parent_h3_04"], how="left")
    
    logger.info(f"After joining yesterday_parent_pm25: {silver_df_lazy_prepared.select(pl.len()).collect().item():,} rows")


    # Sort by cell and date before rolling calculations (CRITICAL!)
    # Without sorting, rolling windows process rows in random order
    logger.info("Sorting data by cell and date for rolling calculations...")
    silver_df_lazy_prepared = silver_df_lazy_prepared.sort(["cell", "date"])
    
    # Log date range in dataset before rolling calculations
    temp_df = silver_df_lazy_prepared.select(["date"]).collect()
    unique_dates = sorted(temp_df["date"].unique().to_list())
    logger.info(f"Dataset contains {len(unique_dates)} unique dates for rolling calculations:")
    logger.info(f"  Date range: {unique_dates[0]} to {unique_dates[-1]}")
    logger.info(f"  Dates: {unique_dates}")
    
    # rolling columns 
    rolling_cols = [
        "aod_1day_interpolated",
        "fire_hotspot_strength",
        "temperature_2m",
        "dewpoint_2m",
        "wind_u_10m",
        "wind_v_10m",
    ]

    logger.info("Calculating rolling 3-day and 7-day averages (allowing gaps)...")
    for col in rolling_cols:
        silver_df_lazy_prepared = silver_df_lazy_prepared.with_columns([
            pl.col(col).rolling_mean(window_size=3, min_periods=1).over("cell").alias(f"{col}_roll3"),
            pl.col(col).rolling_mean(window_size=7, min_periods=1).over("cell").alias(f"{col}_roll7"),
        ])
    
    logger.info(f"After rolling calculations: {silver_df_lazy_prepared.select(pl.len()).collect().item():,} rows")
    
    # Clip to requested date range (removes buffer dates)
    if clip_start_date and clip_end_date:
        logger.info(f"Clipping dataset to requested range: {clip_start_date} to {clip_end_date}")
        initial_count = silver_df_lazy_prepared.select(pl.len()).collect().item()
        
        silver_df_lazy_prepared = silver_df_lazy_prepared.filter(
            (pl.col("date") >= clip_start_date) & 
            (pl.col("date") <= clip_end_date)
        )
        
        final_count = silver_df_lazy_prepared.select(pl.len()).collect().item()
        logger.info(f"  Filtered from {initial_count:,} to {final_count:,} rows (removed {initial_count - final_count:,} buffer rows)")
    
    logger.info("="*80)
    return silver_df_lazy_prepared


def save_silver_dataset(
    silver_df: pl.LazyFrame,
    mode: str,
    countries: List[str],
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    logger = None,
    config_loader: ConfigLoader = None,
    chunk_output_dir: str = None
) -> str:
    """
    Save the silver dataset to the appropriate directory using configuration paths.
    
    NEW BEHAVIOR: Saves one file per day (not one file for entire range)
    NOTE: Clipping should be done in apply_feature_engineering() before calling this.
    
    Parameters:
        silver_df: Silver dataset LazyFrame (already clipped to desired range)
        mode: Processing mode ('realtime' or 'historical')
        countries: List of country codes
        start_date: Start date (optional, for filename generation)
        end_date: End date (optional, for filename generation)
        logger: Logger instance
        config_loader: Configuration loader instance
        chunk_output_dir: Chunk output directory (for chunked processing)
        
    Returns:
        str: Path to output directory (contains multiple daily files)
    """
    if config_loader is None:
        config_loader = ConfigLoader()
    
    # Use chunk output directory if provided
    if chunk_output_dir:
        output_dir = Path(chunk_output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
    else:
        # Create output directory using config paths
        output_dir = config_loader.get_path(f'silver.{mode}', create_if_missing=True)

    # Collect the LazyFrame once
    if logger:
        logger.info("Collecting LazyFrame for per-day saving...")
    silver_df_collected = silver_df.collect()
    
    # Get unique dates in the filtered dataset
    unique_dates = sorted(silver_df_collected.select("date").unique().to_series().to_list())
    
    if logger:
        logger.info(f"Saving {len(unique_dates)} daily files to: {output_dir}")
    
    # Generate filename pattern
    countries_str = "_".join(sorted(countries))
    
    # Save one file per day
    saved_files = []
    for single_date in unique_dates:
        # Filter to this specific date
        daily_df = silver_df_collected.filter(pl.col("date") == single_date)
        
        # Generate filename for this date
        date_str = single_date.strftime("%Y%m%d")
        if mode == "realtime":
            filename = f"silver_realtime_{countries_str}_{date_str}.parquet"
        else:
            filename = f"silver_historical_{countries_str}_{date_str}.parquet"
        
        output_path = output_dir / filename
        
        # Remove existing file if it exists
        if output_path.exists():
            output_path.unlink()
        
        # Save this day's data
        daily_df.write_parquet(output_path)
        
        saved_files.append(str(output_path))
        
        if logger:
            n_rows = daily_df.shape[0]
            logger.info(f"  Saved {single_date}: {output_path.name} ({n_rows:,} rows)")
    
    if logger:
        total_rows = silver_df_collected.shape[0]
        n_cols = silver_df_collected.shape[1]
        logger.info(f"Successfully saved {len(saved_files)} daily files")
        logger.info(f"Total dataset: {total_rows:,} records across {n_cols} columns")
    
    # Return the output directory path (or first file for backward compatibility)
    return str(output_dir)


def reaggregate_chunks(
    chunk_files: List[str],
    countries: List[str],
    start_date: date,
    end_date: date,
    logger,
    config_loader: ConfigLoader = None
) -> str:
    """Simple memory-efficient reaggregation using lazy evaluation."""
    
    logger.info(f"Reaggregating {len(chunk_files)} chunk files...")
    
    # Validate chunk files exist
    valid_chunks = []
    for chunk_file in chunk_files:
        if Path(chunk_file).exists():
            valid_chunks.append(chunk_file)
            logger.info(f"Found chunk: {Path(chunk_file).name}")
        else:
            logger.warning(f"Chunk file not found: {chunk_file}")
    
    if not valid_chunks:
        raise FileNotFoundError("No valid chunk files found")
    
    # Create output path
    if config_loader is None:
        config_loader = ConfigLoader()
    
    output_dir = config_loader.get_path('silver.historical', create_if_missing=True)
    countries_str = "_".join(sorted(countries))
    start_str = start_date.strftime("%Y%m%d")
    end_str = end_date.strftime("%Y%m%d")
    
    if start_date == end_date:
        filename = f"silver_historical_{countries_str}_{end_str}.parquet"
    else:
        filename = f"silver_historical_{countries_str}_{start_str}_to_{end_str}.parquet"
    
    output_path = output_dir / filename
    temp_output_path = output_dir / f"{filename}.tmp"
    
    # Remove existing files
    for path in [output_path, temp_output_path]:
        if path.exists():
            path.unlink()
    
    # Create lazy frames for all chunks - keep everything lazy
    logger.info("Creating lazy pipeline...")
    lazy_frames = []
    for chunk_file in valid_chunks:
        lf = pl.scan_parquet(chunk_file)
        # Ensure consistent schema by casting potential problematic columns
        lf = lf.with_columns([
            pl.col("aod_1day_interpolated").cast(pl.Float64)
        ])
        lazy_frames.append(lf)
    
    # Simple concatenation - skip expensive unique() and sort() operations
    logger.info("Building simple concatenation pipeline...")
    final_lf = pl.concat(lazy_frames)
    
    # Stream directly to output file - no expensive operations
    logger.info("Streaming to output file...")
    final_lf.sink_parquet(temp_output_path)
    
    # Atomic rename to final location
    temp_output_path.rename(output_path)
    logger.info(f"Reaggregated dataset saved to: {output_path}")
    
    return str(output_path)


def process_chunked(
    start_date: date,
    end_date: date,
    countries: List[str],
    chunk_days: int,
    mode: str,
    resolution: int,
    cache_dir: str,
    log_level: str,
    config_file: str,
    logger,
    config_loader: ConfigLoader = None
) -> str:
    """
    Process large date ranges using chunked approach.
    
    Splits the date range into smaller chunks, processes each chunk separately,
    and saves daily files directly to the final output directory.
    No reaggregation needed - daily files are the desired output.
    """
    
    from pathlib import Path
    
    # Calculate total days and number of chunks
    total_days = (end_date - start_date).days + 1
    logger.info(f"Processing {total_days} days in {chunk_days}-day chunks")
    logger.info(f"Daily files will be saved directly to final output directory")
    
    # Get final output directory
    if config_loader is None:
        config_loader = ConfigLoader()
    output_dir = config_loader.get_path(f'silver.{mode}', create_if_missing=True)
    logger.info(f"Output directory: {output_dir}")
    
    # Generate date chunks
    chunks = []
    current_date = start_date
    chunk_num = 1
    
    while current_date <= end_date:
        chunk_end = min(current_date + timedelta(days=chunk_days - 1), end_date)
        chunks.append((current_date, chunk_end, chunk_num))
        current_date = chunk_end + timedelta(days=1)
        chunk_num += 1
    
    logger.info(f"Created {len(chunks)} chunks to process")
    
    # Process each chunk
    processed_chunks = 0
    for chunk_start, chunk_end, chunk_num in chunks:
        logger.info(f"Processing chunk {chunk_num}/{len(chunks)}: {chunk_start} to {chunk_end}")
        
        # Build command arguments for chunk processing
        chunk_args = argparse.Namespace(
            mode=mode,
            start_date=chunk_start.strftime("%Y-%m-%d"),
            end_date=chunk_end.strftime("%Y-%m-%d"),
            countries=countries,
            resolution=resolution,
            cache_dir=cache_dir,
            log_level=log_level,
            config=config_file,
            chunk_output=None,  # Save directly to final directory
            reaggregate=False,
            chunk_files=None,
            chunked=False,
            chunk_days=chunk_days
        )
        
        # Process this chunk by calling the main processing logic
        try:
            chunk_output = process_single_chunk(chunk_args, logger, config_loader)
            if chunk_output and Path(chunk_output).exists():
                processed_chunks += 1
                logger.info(f"Chunk {chunk_num} completed - daily files saved to {chunk_output}")
            else:
                raise RuntimeError(f"Chunk {chunk_num} processing failed - no output generated")
                
        except Exception as e:
            logger.error(f"Error processing chunk {chunk_num}: {e}")
            raise
    
    if processed_chunks == 0:
        raise RuntimeError("No chunks were successfully processed")
    
    logger.info(f"All {processed_chunks} chunks processed successfully")
    logger.info(f"Chunked processing completed - daily files saved to: {output_dir}")
    
    return str(output_dir)


def process_single_chunk(args, logger, config_loader: ConfigLoader = None) -> str:
    """
    Process a single chunk using the normal processing logic with buffers for rolling calculations.
    
    Adds 7-day buffer before and 1-day buffer after for accurate feature engineering,
    then clips to the actual requested date range before saving.
    """
    
    # Convert string dates back to date objects (these are the actual range to save)
    actual_start = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    actual_end = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    
    # Add buffers for feature engineering
    buffer_start = actual_start - timedelta(days=7)   # 7 days for rolling averages
    buffer_end = actual_end + timedelta(days=1)       # 1 day for next_day_targets
    
    logger.info(f"Processing chunk with buffers:")
    logger.info(f"  Actual range to save: {actual_start} to {actual_end}")
    logger.info(f"  Buffered processing range: {buffer_start} to {buffer_end}")
    
    # Create base H3 frame WITH BUFFER
    logger.info(f"Creating base H3 frame for chunk with buffer: {buffer_start} to {buffer_end}")
    base_frame = make_base_frame(args.countries, args.resolution, buffer_start, buffer_end, config_loader)
    
    # Check how many records we created
    record_count = base_frame.collect().shape[0]
    logger.info(f"Created {record_count} records for this chunk (with buffer)")
    
    # Process each data source for this chunk
    himawari_status, himawari_lf = clean_himawari_aod_data(
        base_frame, args.mode, args.countries, args.cache_dir, logger, config_loader
    )
    
    firms_status, firms_lf = clean_firms_data(
        base_frame, args.mode, args.countries, args.cache_dir, logger, config_loader
    )
    
    era5_status, era5_lf = clean_era5_data(
        base_frame, args.mode, args.countries, args.cache_dir, logger, config_loader
    )
    
    air_quality_status, air_quality_lf = clean_air_quality_data(
        base_frame, args.mode, args.countries, args.cache_dir, logger, config_loader
    )
    
    # Process additional datasets (these are static, so they're the same for all chunks)
    elevation_status, elevation_lf = clean_elevation_data(
        base_frame, args.countries, logger, config_loader
    )
    
    landcover_status, landcover_lf = clean_landcover_data(
        base_frame, args.countries, logger, config_loader
    )
    
    worldpop_status, worldpop_lf = clean_worldpop_data(
        base_frame, args.countries, logger, config_loader
    )
    
    # Merge all datasets for this chunk
    silver_df = merge_silver_dataset(
        base_frame, himawari_lf, None, firms_lf, era5_lf, air_quality_lf,
        elevation_lf, landcover_lf, worldpop_lf, args.mode, logger
    )
    
    logger.info(f"After merge_silver_dataset: {silver_df.shape[0]:,} rows")
    
    # Apply feature engineering WITH CLIPPING to actual range
    silver_df_lazy = silver_df.lazy()
    silver_df_lazy_prepared = apply_feature_engineering(
        silver_df_lazy,
        logger,
        clip_start_date=actual_start,  # Clip buffer dates
        clip_end_date=actual_end
    )
    
    # Save chunk dataset (already clipped to actual range)
    output_path = save_silver_dataset(
        silver_df_lazy_prepared, args.mode, args.countries, actual_start, actual_end,
        logger, config_loader, args.chunk_output
    )
    
    return output_path


def main():
    """Main entry point for silver dataset generation with configuration integration."""
    # Load configuration for defaults
    try:
        config_loader = ConfigLoader()
        default_countries = config_loader.get_countries()
        default_resolution = config_loader.get_h3_resolution()
        default_cache_dir = config_loader.get_path('cache.silver', create_if_missing=True)
        logging_config = config_loader.get_logging_config()
        default_log_level = logging_config.get('level', 'INFO')
    except Exception as e:
        print(f"Warning: Could not load configuration, using fallback defaults: {e}")
        default_countries = ["THA", "LAO"]
        default_resolution = 8
        default_cache_dir = Path("./data/cache/silver")
        default_log_level = "INFO"
    
    parser = argparse.ArgumentParser(
        description="Generate silver dataset from processed data sources",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate real-time silver dataset
  python src/make_silver.py --mode realtime --hours 24 --countries THA LAO

  # Generate historical silver dataset for specific dates
  python src/make_silver.py --mode historical --start-date 2024-01-01 --end-date 2024-01-02

  # Use custom configuration file
  python src/make_silver.py --mode realtime --config config/custom_config.yaml

  # Generate with different H3 resolution
  python src/make_silver.py --mode historical --start-date 2024-01-01 --end-date 2024-01-01 --resolution 6
        """
    )
    
    parser.add_argument("--mode", choices=["realtime", "historical"], required=True,
                        help="Processing mode")
    parser.add_argument("--start-date", type=str, help="Start date (YYYY-MM-DD) for historical mode")
    parser.add_argument("--end-date", type=str, help="End date (YYYY-MM-DD) for historical mode")
    parser.add_argument("--hours", type=int, default=24, help="Hours to look back for realtime mode")
    parser.add_argument("--countries", nargs="+", default=default_countries, 
                        help=f"Country codes to process (default: {' '.join(default_countries)})")
    parser.add_argument("--resolution", type=int, default=default_resolution, 
                        help=f"H3 resolution (default: {default_resolution})")
    parser.add_argument("--cache-dir", type=str, default=str(default_cache_dir),
                        help=f"Cache directory (default: {default_cache_dir})")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], 
                        default=default_log_level, help=f"Logging level (default: {default_log_level})")
    parser.add_argument("--config", type=str, help="Path to configuration file (optional)")
    parser.add_argument("--chunk-output", type=str, help="Directory for chunk output files")
    parser.add_argument("--reaggregate", action="store_true", help="Reaggregate chunk files")
    parser.add_argument("--chunk-files", nargs="+", help="List of chunk files to reaggregate")
    parser.add_argument("--chunked", action="store_true", help="Enable chunked processing for large date ranges")
    parser.add_argument("--chunk-days", type=int, default=30, help="Number of days per chunk (default: 30)")
    parser.add_argument("--original-start-date", type=str, help="Original user-requested start date (for clipping after rolling calculations)")
    parser.add_argument("--original-end-date", type=str, help="Original user-requested end date (for clipping after rolling calculations)")
    
    args = parser.parse_args()
    
    # Handle reaggregation mode
    if args.reaggregate:
        if not args.chunk_files or not args.start_date or not args.end_date:
            parser.error("Reaggregate mode requires --chunk-files, --start-date, and --end-date")
        
        # Initialize configuration and logging
        config_loader = ConfigLoader(args.config) if args.config else ConfigLoader()
        logger = setup_with_config(config_loader, __name__)
        logger.setLevel(getattr(logging, args.log_level))
        
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
        
        # Reaggregate chunks
        output_path = reaggregate_chunks(
            args.chunk_files, args.countries, start_date, end_date, logger, config_loader
        )
        
        logger.info(f"Reaggregation completed: {output_path}")
        return
    
    # Handle chunked processing mode
    if args.chunked:
        if not args.start_date or not args.end_date:
            parser.error("Chunked mode requires --start-date and --end-date")
        
        # Initialize configuration and logging
        config_loader = ConfigLoader(args.config) if args.config else ConfigLoader()
        logger = setup_with_config(config_loader, __name__)
        logger.setLevel(getattr(logging, args.log_level))
        
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
        
        logger.info("="*80)
        logger.info("CHUNKED SILVER DATASET PROCESSING STARTING")
        logger.info("="*80)
        
        # Process in chunks
        output_path = process_chunked(
            start_date, end_date, args.countries, args.chunk_days,
            args.mode, args.resolution, args.cache_dir, args.log_level,
            args.config, logger, config_loader
        )
        
        logger.info("="*80)
        logger.info("CHUNKED SILVER DATASET PROCESSING COMPLETED")
        logger.info(f"Final output saved to: {output_path}")
        logger.info("="*80)
        return
    
    # Initialize configuration loader with custom config if provided
    if args.config:
        config_loader = ConfigLoader(args.config)
    else:
        config_loader = ConfigLoader()
    
    # Setup logging using configuration
    logger = setup_with_config(config_loader, __name__)
    logger.setLevel(getattr(logging, args.log_level))
    
    # Validate arguments
    if args.mode == "historical":

        if not args.start_date or not args.end_date:
            parser.error("Historical mode requires --start-date and --end-date")
        
        # Validate date format
        try:
            datetime.strptime(args.start_date, '%Y-%m-%d')
            datetime.strptime(args.end_date, '%Y-%m-%d')
        except ValueError:
            parser.error("Date format must be YYYY-MM-DD")
    
    logger.info("="*80)
    logger.info("SILVER DATASET GENERATOR STARTING")
    logger.info("="*80)
    logger.info(f"Mode: {args.mode}")
    logger.info(f"Countries: {', '.join(args.countries)}")
    logger.info(f"H3 Resolution: {args.resolution}")
    logger.info(f"Cache Directory: {args.cache_dir}")
    
    try:
        # Determine date range
        if args.mode == "realtime":
            end_date = date.today()
            start_date = end_date - timedelta(days=7)  # Need 7 days for rolling calculations
            original_start_date = end_date  # Only want today in final output
            original_end_date = end_date
            
            logger.info(f"Real-time mode:")
            logger.info(f"  Processing date range: {start_date} to {end_date} (7 days for rolling calculations)")
            logger.info(f"  Final output will be clipped to: {original_start_date}")
        else:
            # Historical mode: check if buffer was already applied by shell script
            if args.original_start_date and args.original_end_date:
                # Buffer already applied by shell script
                start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
                end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
                original_start_date = datetime.strptime(args.original_start_date, "%Y-%m-%d").date()
                original_end_date = datetime.strptime(args.original_end_date, "%Y-%m-%d").date()
                
                logger.info(f"Historical mode (buffer pre-applied by shell):")
                logger.info(f"  User-requested date range: {original_start_date} to {original_end_date}")
                logger.info(f"  Processing date range: {start_date} to {end_date} (with 7-day buffer)")
                logger.info(f"  Final output will be clipped to: {original_start_date} to {original_end_date}")
            else:
                # No buffer applied yet - apply it now
                original_start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
                original_end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
                
                # Add 7-day buffer BEFORE start date for rolling calculations
                start_date = original_start_date - timedelta(days=7)
                end_date = original_end_date
                
                logger.info(f"Historical mode:")
                logger.info(f"  User-requested date range: {original_start_date} to {original_end_date}")
                logger.info(f"  Processing date range: {start_date} to {end_date} (with 7-day buffer for rolling calculations)")
                logger.info(f"  Final output will be clipped to: {original_start_date} to {original_end_date}")
        
        # Create base H3 frame
        logger.info("="*80)
        logger.info("BASE FRAME CREATION - ROW COUNT DIAGNOSTICS")
        logger.info("="*80)
        logger.info("Creating base H3 frame...")
        base_frame = make_base_frame(args.countries, args.resolution, start_date, end_date, config_loader)
        
        # Check how many records we created
        base_collected = base_frame.collect()
        record_count = base_collected.shape[0]
        unique_cells = base_collected.select('cell').unique().shape[0]
        unique_dates = base_collected.select('date').unique().shape[0]
        
        logger.info(f"Created {record_count:,} records")
        logger.info(f"  - Unique H3 cells: {unique_cells:,}")
        logger.info(f"  - Unique dates: {unique_dates}")
        logger.info(f"  - Expected rows (cells × dates): {unique_cells * unique_dates:,}")
        if unique_cells * unique_dates != record_count:
            logger.warning(f"  ⚠️  Mismatch! Expected {unique_cells * unique_dates:,} but got {record_count:,}")
        logger.info("="*80)
        
        # Process each data source
        himawari_status, himawari_lf = clean_himawari_aod_data(
            base_frame, args.mode, args.countries, args.cache_dir, logger, config_loader
        )
        
        
        firms_status, firms_lf = clean_firms_data(
            base_frame, args.mode, args.countries, args.cache_dir, logger, config_loader
        )
        
        era5_status, era5_lf = clean_era5_data(
            base_frame, args.mode, args.countries, args.cache_dir, logger, config_loader
        )
        
        air_quality_status, air_quality_lf = clean_air_quality_data(
            base_frame, args.mode, args.countries, args.cache_dir, logger, config_loader
        )
        
        # Process additional datasets
        elevation_status, elevation_lf = clean_elevation_data(
            base_frame, args.countries, logger, config_loader
        )
        
        landcover_status, landcover_lf = clean_landcover_data(
            base_frame, args.countries, logger, config_loader
        )
        
        worldpop_status, worldpop_lf = clean_worldpop_data(
            base_frame, args.countries, logger, config_loader
        )
        
        # Merge all datasets
        silver_df = merge_silver_dataset(
            base_frame, himawari_lf, None, firms_lf, era5_lf, air_quality_lf,
            elevation_lf, landcover_lf, worldpop_lf, args.mode, logger
        )
        
        logger.info(f"After merge_silver_dataset: {silver_df.shape[0]:,} rows")

        # Apply all feature engineering with clipping
        silver_df_lazy = silver_df.lazy()
        silver_df_lazy_prepared = apply_feature_engineering(
            silver_df_lazy,
            logger,
            clip_start_date=original_start_date,
            clip_end_date=original_end_date
        )

        # Check if we have original dates (indicating buffer was applied upstream)
        if args.original_start_date and args.original_end_date:
            original_start_date = datetime.strptime(args.original_start_date, "%Y-%m-%d").date()
            original_end_date = datetime.strptime(args.original_end_date, "%Y-%m-%d").date() 
            logger.info(f"Processing extended range: {start_date} to {end_date}")
            logger.info(f"Will clip final output to: {original_start_date} to {original_end_date}")
        else:
            logger.info(f"Date range: {start_date} to {end_date}")
        
        # Save silver dataset (already clipped in feature engineering)
        logger.info("="*80)
        logger.info("SAVING SILVER DATASET - ROW COUNT DIAGNOSTICS")
        logger.info("="*80)
        logger.info(f"Before save_silver_dataset: {silver_df_lazy_prepared.select(pl.len()).collect().item():,} rows")
        
        output_path = save_silver_dataset(
                silver_df_lazy_prepared, args.mode, args.countries, start_date, end_date,
                logger, config_loader, args.chunk_output
)
        
        logger.info("="*80)
        logger.info("SILVER DATASET GENERATOR COMPLETED")
        logger.info(f"Output saved to: {output_path}")
        logger.info("="*80)
        
    except Exception as e:
        logger.error("="*80)
        logger.error("SILVER DATASET GENERATOR FAILED")
        logger.error(f"Error: {str(e)}")
        logger.error("="*80)
        import traceback
        logger.error(traceback.format_exc())
        exit(1)


if __name__ == "__main__":
    main() 
