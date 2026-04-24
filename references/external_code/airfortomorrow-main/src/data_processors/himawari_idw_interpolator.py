#!/usr/bin/env python3
"""
Himawari Inverse Distance Weighting (IDW) Interpolator

This script performs spatial interpolation of daily aggregated Himawari AOD data using
Inverse Distance Weighting (IDW) with H3 hexagonal grids for efficient processing.

Replaces the kriging approach with a faster, validated IDW implementation.
"""

import os
import argparse
import pandas as pd
import numpy as np
import polars as pl
import polars_h3 as plh3
import h3
from pathlib import Path
from datetime import datetime
import logging
import glob
from typing import Tuple, Optional, List
import geopandas as gpd
from h3ronpy.pandas.vector import geodataframe_to_cells
import requests
import io
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.utils.boundary_utils import create_country_boundaries
from src.utils.logging_utils import setup_basic_logging


def _create_retry_session(retries=5, backoff_factor=2.0):
    """
    Create a requests session with retry logic for handling transient network errors.
    
    Args:
        retries: Number of retry attempts
        backoff_factor: Factor for exponential backoff between retries
        
    Returns:
        requests.Session with retry adapter configured
    """
    session = requests.Session()
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    
    # Mount adapter with retry strategy
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session




def inverse_distance_weighting(
    ds: pl.LazyFrame,
    boundary: pl.LazyFrame,
    RINGS: int = 10,  # Reduced from 10 to 7 to save memory
    weight_power: float = 1.5,
    h3_column: str = "h3_08",
):

    # ds=ds.select([h3_column,'aod_avg'])

    ## Variables and expressions for weighting / aggregation
    exclusion_cols = ["latitude", "longitude", h3_column, "time", "valid_time"]
    value_cols = [c for c in ds.collect_schema().names() if c not in exclusion_cols]

    print(f"value_cols: {value_cols}")
    weight_expr = 1.0 / (pl.col("dist").cast(pl.Float32) ** weight_power)
    wv_exprs = [
        (pl.col(value) * weight_expr).alias(f"wv_{value}") for value in value_cols
    ]
    agg_exprs = [pl.sum("w").alias("wsum")] + [
        pl.sum(f"wv_{value}").alias(f"sum_{value}") for value in value_cols
    ]

    print(f"""len ds {len(ds.collect())}""")
    print(f"""len boundary {len(boundary.collect())}""")

    # Ensure both h3_08 columns have the same data type (cast to u64)
    ds_fixed = ds.with_columns(pl.col(h3_column).cast(pl.UInt64).alias(h3_column))
    boundary_fixed = boundary.with_columns(pl.col(h3_column).cast(pl.UInt64).alias(h3_column))

    missing_hexes = (
        boundary_fixed.select(pl.col(h3_column))
        .unique()
        .join(ds_fixed.select(h3_column), on=h3_column, how="anti")
        .select(h3_column)
        .lazy()
    )

    print(f"""len missing hexes {len(missing_hexes.collect())}""")

    # Debug: Check memory usage before the heavy operation
    import psutil
    process = psutil.Process()
    print(f"Memory usage before grid_disk: {process.memory_info().rss / 1024 / 1024:.1f} MB")

    # Doing this next bit with a sink_parquet because the streaming join materializes the frame and blows up my memory
    tmp = "targets_tmp.parquet"

    print("Starting grid_disk operation...")
    
    # Use sink_parquet directly without collecting to avoid memory explosion
    (
        ds_fixed.select([h3_column] + value_cols)
        .with_columns(plh3.grid_disk(h3_column, RINGS).alias("targets"))
        .explode("targets")
    ).sink_parquet(tmp)
    
    print(f"Memory usage after sink_parquet: {process.memory_info().rss / 1024 / 1024:.1f} MB")

    print("sink parquet done")

    contrib = (
        pl.scan_parquet(tmp)
        .join(missing_hexes, left_on="targets", right_on=h3_column, how="inner")
        .collect(engine="streaming")
    )

    contrib = contrib.with_columns(
        plh3.grid_distance(pl.col(h3_column), pl.col("targets")).alias("dist")
    )

    contrib = contrib.with_columns(
        [
            weight_expr.alias("w"),
            *[(pl.col(v) * weight_expr).alias(f"wv_{v}") for v in value_cols],
        ]
    )
    contrib = contrib.with_columns(pl.col("targets").alias(h3_column))
    contrib = contrib.select(h3_column, "w", *[f"wv_{v}" for v in value_cols])

    predicted = (
        contrib.group_by(h3_column)
        .agg(agg_exprs)
        .with_columns(
            [
                (pl.col(f"sum_{value}") / pl.col("wsum")).alias(
                    value
                )  # final prediction per var
                for value in value_cols
            ]
        )
        .select(h3_column, *value_cols)
    )

    known_vars = ds_fixed.select(h3_column, *value_cols)
    filled = (
        pl.concat([known_vars, predicted.lazy()])
        .group_by(h3_column, maintain_order=True)
        .agg(
            [pl.col(value).max().alias(value) for value in value_cols]
        )  # keep the original AOD value where present
    )

    return filled


def create_boundary_grid(countries: List[str] = ["LAO", "THA"], buffer_degrees: float = 0.4) -> pl.LazyFrame:
    """
    Create H3 boundary grid for specified countries.
    Duplicates the exact structure from the validated notebook (no buffer for h3ronpy compatibility).
    
    Args:
        countries: List of country codes
        buffer_degrees: Buffer around boundaries in degrees (not used with h3ronpy)
        
    Returns:
        Polars LazyFrame with H3 boundary cells
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Creating H3 boundary grid for countries: {countries}")
    
    # Duplicate the exact structure from the notebook
    country_codes = countries  # Match notebook variable name
    all_boundaries = []
    session = _create_retry_session(retries=5, backoff_factor=2.0)

    for country_code in country_codes:
        max_retries = 3
        retry_delay = 5  # seconds
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Processing {country_code}... (attempt {attempt + 1}/{max_retries})")
                
                # Use requests.get() approach (works in Docker) instead of gpd.read_file() directly
                url = f"https://github.com/wmgeolab/geoBoundaries/raw/fcccfab7523d4d5e55dfc7f63c166df918119fd1/releaseData/gbOpen/{country_code}/ADM0/geoBoundaries-{country_code}-ADM0.geojson"
                
                resp = session.get(
                    url, 
                    headers={"User-Agent": "Mozilla/5.0"}, 
                    timeout=60,
                    verify=True
                )
                resp.raise_for_status()
                boundary = gpd.read_file(io.BytesIO(resp.content))
                
                # Skip buffer operation - h3ronpy can't handle buffered geometries properly
                # Keep exact notebook approach: raw boundaries only
                logger.info(f"Loaded boundary for {country_code} (no buffer applied for h3ronpy compatibility)")
                
                # Exact same conversion as notebook
                boundary_gdf = geodataframe_to_cells(
                    boundary,
                    8,  # H3 resolution 8
                )
                
                # Exact same Polars conversion as notebook
                boundary = pl.DataFrame(boundary_gdf).select("cell")
                boundary = boundary.lazy()
                all_boundaries.append(boundary)
                
                logger.info(f"Added {len(boundary_gdf)} H3 cells for {country_code}")
                break  # Success, exit retry loop
                
            except (requests.exceptions.SSLError, 
                    requests.exceptions.ConnectionError,
                    requests.exceptions.RequestException) as e:
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (attempt + 1)
                    logger.warning(f"Network error for {country_code} (attempt {attempt + 1}/{max_retries}): {e}")
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to load boundary for {country_code} after {max_retries} attempts: {e}")
                    logger.error("This may be a DNS/network issue. Check Docker network connectivity.")
                    # Don't break the loop, continue to next country
                    continue
            except Exception as e:
                logger.error(f"Error processing country {country_code}: {e}")
                break  # Exit retry loop for non-network errors

    if not all_boundaries:
        raise ValueError("No valid country boundaries found")
    
    # Exact same concatenation as notebook
    final_boundary = pl.concat(all_boundaries).unique(subset=["cell"])
    
    # Rename to match our convention
    final_boundary = final_boundary.rename({"cell": "h3_08"})
    
    total_cells = final_boundary.select(pl.len()).collect().item()
    logger.info(f"Created boundary grid with {total_cells} unique H3 cells (no buffer)")
    
    return final_boundary


def process_daily_file(
    file_path: str,
    boundary_grid: pl.LazyFrame,
    rings: int = 10,
    weight_power: float = 1.5
) -> pl.DataFrame:
    """
    Process a single daily aggregated file with IDW interpolation.
    
    Args:
        file_path: Path to daily aggregated parquet file
        boundary_grid: Complete H3 boundary grid
        rings: Number of H3 rings for IDW
        weight_power: Power for inverse distance weighting
        
    Returns:
        DataFrame with original and interpolated data
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Processing file: {os.path.basename(file_path)}")
    
    # Load daily aggregated data and replace string by Int64 for the h3 index
    df = pl.scan_parquet(file_path).with_columns(
    pl.col("h3_08").map_elements(lambda x: h3.string_to_h3(x), return_dtype=pl.Int64).alias("h3_08")
)
    
    # Extract date from filename
    filename = os.path.basename(file_path)
    # Expected format: daily_h3_aod_YYYYMMDD_LAO_THA.parquet
    try:
        date_str = filename.split('_')[3]  # YYYYMMDD
        date_formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    except (IndexError, ValueError):
        logger.error(f"Could not extract date from filename: {filename}")
        return pl.DataFrame()
    
    # Start with complete boundary grid
    result = boundary_grid.select("h3_08").with_columns(pl.lit(date_formatted).alias("date"))
    
    print("start IDW")


    # Process 1-day AOD data
    if "aod_1day" in df.collect_schema().names():
        logger.info("Processing 1-day AOD interpolation...")
        df_1day = df.filter(pl.col("aod_1day").is_not_null()).select("h3_08", "aod_1day")


        if df_1day.select(pl.len()).collect().item() > 0:
            filled_1day = inverse_distance_weighting(
                df_1day, boundary_grid, rings, weight_power, "h3_08"
            )
            filled_1day = filled_1day.rename({"aod_1day": "aod_1day_interpolated"})     
            
            # result = result.join(df_1day, on="h3_08", how="left")
            result = result.join(filled_1day, on="h3_08", how="left")

            print("filled result structure")
            print(result.collect_schema())

        else:
            logger.warning("No valid 1-day AOD data found")
            result = result.with_columns([
                pl.lit(None, dtype=pl.Float64).alias("aod_1day_interpolated")
            ])
    
    # Select only the interpolated columns for final output
    final_df = result.select([
        "h3_08", 
        "date", 
        "aod_1day_interpolated"
    ]).collect()
    

    return final_df


def process_daily_aggregated_data(
    input_dir: str,
    output_dir: str,
    countries: List[str] = ["LAO", "THA"],
    mode: str = "historical",
    rings: int = 10,
    weight_power: float = 1.5,
    buffer_degrees: float = 0.4,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> None:
    """
    Process all daily aggregated files with IDW interpolation.
    
    Args:
        input_dir: Directory containing daily aggregated parquet files
        output_dir: Output directory for interpolated files
        countries: List of country codes
        mode: Processing mode ('historical' or 'realtime')
        rings: Number of H3 rings for IDW interpolation
        weight_power: Power for inverse distance weighting
        buffer_degrees: Buffer around country boundaries in degrees
        start_date: Start date filter (YYYY-MM-DD)
        end_date: End date filter (YYYY-MM-DD)
    """
    logger = logging.getLogger(__name__)
    
    # Create mode-specific input and output directories
    mode_input_dir = os.path.join(input_dir, mode)
    mode_output_dir = os.path.join(output_dir, mode)
    os.makedirs(mode_output_dir, exist_ok=True)
    
    logger.info(f"Processing {mode} mode")
    logger.info(f"Input directory: {mode_input_dir}")
    logger.info(f"Output directory: {mode_output_dir}")
    
    # Find daily aggregated files
    countries_str = "_".join(sorted(countries))
    pattern = os.path.join(mode_input_dir, f"daily_h3_aod_*_{countries_str}.parquet")
    files = glob.glob(pattern)

    
    if not files:
        logger.warning(f"No daily aggregated files found matching pattern: {pattern}")
        return
    
    # Filter files by date range if specified
    if start_date or end_date:
        filtered_files = []
        for file_path in files:
            filename = os.path.basename(file_path)
            try:
                date_str = filename.split('_')[3]  # YYYYMMDD
                file_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                
                if start_date and file_date < start_date:
                    continue
                if end_date and file_date > end_date:
                    continue
                    
                filtered_files.append(file_path)
            except (IndexError, ValueError):
                logger.warning(f"Could not parse date from filename: {filename}")
                continue
        files = filtered_files
    
    if not files:
        logger.warning("No files found after date filtering")
        return
    
    logger.info(f"Found {len(files)} files after date filtering")
    
    # Filter out files that already have interpolated output
    files_to_process = []
    files_skipped = []
    for file_path in files:
        filename = os.path.basename(file_path)
        try:
            date_str = filename.split('_')[3]  # YYYYMMDD from daily_h3_aod_YYYYMMDD_*.parquet
            country_str = "_".join(sorted(countries))
            subdir = mode
            interpolated_dir = Path(f"data/processed/himawari/interpolated/{subdir}")
            interpolated_file = interpolated_dir / f"interpolated_h3_aod_{date_str}_{country_str}.parquet"
            
            if interpolated_file.exists():
                logger.info(f"Skipping {date_str} - interpolated file already exists")
                files_skipped.append(file_path)
            else:
                files_to_process.append(file_path)
        except (IndexError, ValueError):
            logger.warning(f"Could not parse date from filename: {filename}, will process anyway")
            files_to_process.append(file_path)
    
    if files_skipped:
        logger.info(f"Skipped {len(files_skipped)} files with existing interpolated output")
    
    if not files_to_process:
        logger.info("No files to process - all dates already have interpolated output")
        return
    
    logger.info(f"Will process {len(files_to_process)} files")
    
    # Create boundary grid once for all files (now with buffer_degrees)
    logger.info("Creating boundary grid...")
    boundary_grid = create_boundary_grid(countries, buffer_degrees)
    print(boundary_grid.collect().head())
    
    # Process each file
    for i, file_path in enumerate(sorted(files_to_process), 1):
        try:
            logger.info(f"Processing file {i}/{len(files_to_process)}: {os.path.basename(file_path)}")
            
            # Process with IDW interpolation
            result_df = process_daily_file(file_path, boundary_grid, rings, weight_power)
            
            if result_df.is_empty():
                logger.warning(f"No results for {file_path}, skipping...")
                continue
            
            # Generate output filename
            input_filename = os.path.basename(file_path)
            # Extract date from input filename (works for both historical and realtime)
            # Input format: daily_h3_aod_YYYYMMDD_COUNTRIES.parquet
            date_str = input_filename.split('_')[3]  # YYYYMMDD
            output_filename = f"interpolated_h3_aod_{date_str}_{countries_str}.parquet"
            output_path = os.path.join(mode_output_dir, output_filename)
            
            # Save result
            result_df.write_parquet(output_path)
            logger.info(f"Saved: {output_path}")
            
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")
            continue
    
    logger.info(f"Completed processing {len(files)} files")


def main():
    """Main function for command line execution"""
    parser = argparse.ArgumentParser(description='Himawari IDW Interpolator')
    
    parser.add_argument('--mode', type=str, choices=['historical', 'realtime'], default='historical',
                       help='Processing mode (default: historical)')
    parser.add_argument('--input-dir', type=str, default='./data/processed/himawari/daily_aggregated',
                       help='Input directory containing daily aggregated data')
    parser.add_argument('--output-dir', type=str, default='./data/processed/himawari/interpolated',
                       help='Output directory for interpolated data')
    parser.add_argument('--countries', type=str, nargs='+', default=['LAO', 'THA'],
                       help='Country codes for boundaries (default: LAO THA)')
    parser.add_argument('--rings', type=int, default=10,
                       help='Number of H3 rings for IDW interpolation (default: 10)')
    parser.add_argument('--weight-power', type=float, default=1.5,
                       help='Power for inverse distance weighting (default: 1.5)')
    parser.add_argument('--buffer-degrees', type=float, default=0.4,
                       help='Buffer around country boundaries in degrees (default: 0.4)')
    parser.add_argument('--start-date', type=str,
                       help='Start date for processing (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str,
                       help='End date for processing (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_basic_logging(__name__)
    
    logger.info(f"Running Himawari IDW Interpolator")
    logger.info(f"Mode: {args.mode}")
    logger.info(f"Countries: {args.countries}")
    logger.info(f"IDW Parameters: rings={args.rings}, weight_power={args.weight_power}")
    logger.info(f"Buffer Degrees: {args.buffer_degrees}")
    
    # Validate date parameters
    if args.start_date or args.end_date:
        logger.info(f"Date range: {args.start_date} to {args.end_date}")
    
    # Run IDW interpolation
    process_daily_aggregated_data(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        countries=args.countries,
        mode=args.mode,
        rings=args.rings,
        weight_power=args.weight_power,
        buffer_degrees=args.buffer_degrees,
        start_date=args.start_date,
        end_date=args.end_date
    )
    
    logger.info("IDW interpolation completed successfully!")


if __name__ == "__main__":
    main() 