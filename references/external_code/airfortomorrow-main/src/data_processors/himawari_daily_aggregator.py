#!/usr/bin/env python3
"""
Himawari Daily Aggregator

This script processes H3-indexed Himawari AOD data to create daily averages.
Creates comprehensive datasets with both 1-day and 2-day averages for all boundary hexagons.
"""

import os
import argparse
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import logging
import glob
from typing import Tuple, Optional, List, Dict, Union
import geopandas as gpd
import h3
from shapely.geometry import Point
from shapely.ops import unary_union
from geowrangler import grids
from src.utils.boundary_utils import create_country_boundaries
from src.utils.logging_utils import setup_basic_logging

def create_boundaries_countries(country_code_list: List[str], buffer_degrees: float = 0.4) -> gpd.GeoDataFrame:
    """Use centralized boundary utility."""
    return create_country_boundaries(country_code_list, buffer_degrees)

def generate_boundaries_grid(countries: List[str] = ["LAO", "THA"], buffer_degrees: float = 0.4) -> pd.DataFrame:
    """Generate boundaries grid from country boundaries with buffer and H3 indexing"""
    logger = logging.getLogger(__name__)
    logger.info(f"Generating boundaries grid for countries {countries} with {buffer_degrees}° buffer...")
    
    # Create merged country boundaries
    boundaries_countries = create_boundaries_countries(countries, buffer_degrees)
    
    # Generate H3 grid using geowrangler
    h3_generator = grids.H3GridGenerator(resolution=8)  # resolution 8
    boundaries_grid = h3_generator.generate_grid(boundaries_countries)
    
    # Debug: Check what columns are actually present
    logger.info(f"Generated grid columns: {list(boundaries_grid.columns)}")
    logger.info(f"Generated grid shape: {boundaries_grid.shape}")
    
    # Rename the H3 column to h3_08 for consistency with our data
    # Check for different possible column names
    h3_column = None
    if 'h3_index' in boundaries_grid.columns:
        h3_column = 'h3_index'
    elif 'hex_id' in boundaries_grid.columns:
        h3_column = 'hex_id'
    else:
        # Search for any column that might contain H3 info
        for col in boundaries_grid.columns:
            if 'h3' in col.lower() or 'hex' in col.lower():
                h3_column = col
                break
    
    if h3_column:
        boundaries_grid = boundaries_grid.rename(columns={h3_column: 'h3_08'})
        logger.info(f"Renamed {h3_column} to h3_08")
    else:
        logger.error("No H3/hex column found in boundaries grid!")
        raise ValueError("Could not find H3 column in generated boundaries grid")
    
    # Final check
    logger.info(f"Final grid columns: {list(boundaries_grid.columns)}")
    
    logger.info(f"Generated boundaries grid with {len(boundaries_grid)} H3 cells")
    return boundaries_grid

def get_daily_folders(h3_dir: str) -> List[str]:
    """Get all daily folders sorted by date"""
    daily_folders = []
    
    # Check both realtime and historical subdirectories
    for subdir in ['realtime', 'historical']:
        pattern = os.path.join(h3_dir, subdir, "*", "*")
        folders = glob.glob(pattern)
        daily_folders.extend(folders)
    
    # Filter to only include directories that look like dates
    valid_folders = []
    for folder in daily_folders:
        try:
            parts = folder.split(os.sep)
            year_month, day = parts[-2], parts[-1]
            
            # Parse YYYYMM format
            if len(year_month) == 6 and len(day) == 2:
                year = year_month[:4]
                month = year_month[4:6]
                # Validate it's a valid date
                datetime.strptime(f"{year}-{month}-{day}", "%Y-%m-%d")
                valid_folders.append(folder)
        except (ValueError, IndexError):
            continue
    
    return sorted(valid_folders)

def load_daily_data(folder_path: str) -> Tuple[pd.DataFrame, str]:
    """Load all H3 data files for a single day"""
    parquet_files = glob.glob(os.path.join(folder_path, "*.parquet"))
    
    if not parquet_files:
        logging.warning(f"No parquet files found in {folder_path}")
        return pd.DataFrame(), ""
    
    # Extract date from folder path
    parts = folder_path.split(os.sep)
    year_month, day = parts[-2], parts[-1]
    
    # Parse YYYYMM format
    year = year_month[:4]
    month = year_month[4:6]
    date_str = f"{year}{month}{day}"
    
    all_data = []
    for file_path in parquet_files:
        try:
            df = pd.read_parquet(file_path)
            
            # Extract time from filename if available
            filename = os.path.basename(file_path)
            # Expected format: H09_YYYYMMDD_HHMM_1HARP031_FLDK.02401_02401.parquet
            if "_" in filename:
                parts_filename = filename.split("_")
                if len(parts_filename) >= 3:
                    time_str = parts_filename[2]  # HHMM
                    df['time'] = time_str
                    df['date'] = date_str
            
            all_data.append(df)
            
        except Exception as e:
            logging.warning(f"Error reading {file_path}: {e}")
            continue
    
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        return combined_df, date_str
    else:
        return pd.DataFrame(), date_str

def get_all_data_files(h3_dir: str) -> List[Tuple[str, datetime]]:
    """Get all H3 data files with their timestamps"""
    all_files = []
    
    # Get all parquet files recursively
    pattern = os.path.join(h3_dir, "**", "*.parquet")
    files = glob.glob(pattern, recursive=True)
    
    for file_path in files:
        try:
            filename = os.path.basename(file_path)
            # Expected format: H09_YYYYMMDD_HHMM_1HARP031_FLDK.02401_02401.parquet
            if "_" in filename:
                parts = filename.split("_")
                if len(parts) >= 3:
                    date_str = parts[1]  # YYYYMMDD
                    time_str = parts[2]  # HHMM
                    
                    # Parse datetime
                    dt = datetime.strptime(f"{date_str}{time_str}", "%Y%m%d%H%M")
                    all_files.append((file_path, dt))
        except (ValueError, IndexError):
            continue
    
    return sorted(all_files, key=lambda x: x[1])

def get_value_column(df: pd.DataFrame) -> str:
    """Determine which column contains AOD values"""
    if 'aod_value' in df.columns:
        return 'aod_value'
    elif 'aod' in df.columns:
        return 'aod'
    elif 'value' in df.columns:
        return 'value'
    else:
        raise ValueError("No AOD/value column found in data")

def get_daily_data_groups(h3_dir: str, mode: str, start_date: Optional[str] = None, 
                         end_date: Optional[str] = None, hours_lookback: int = 24) -> Dict[str, Union[List[str], List[Tuple[str, datetime]]]]:
    """Get daily data groups for both historical and realtime modes"""
    logger = logging.getLogger(__name__)
    
    if mode == 'historical':
        # Use folder-based approach for historical data
        daily_folders = get_daily_folders(h3_dir)
        
        # Filter by date range if specified
        # For 2-day averaging, we need to include one day before start_date
        if start_date or end_date:
            filtered_folders = []
            
            # Calculate actual start date (one day before for 2-day averaging)
            actual_start_date = start_date
            if start_date:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                prev_day_dt = start_dt - timedelta(days=1)
                actual_start_date = prev_day_dt.strftime('%Y-%m-%d')
                logger.info(f"Including previous day {actual_start_date} for 2-day averaging")
            
            for folder in daily_folders:
                try:
                    parts = folder.split(os.sep)
                    year_month, day = parts[-2], parts[-1]
                    
                    if len(year_month) == 6 and len(day) == 2:
                        year = year_month[:4]
                        month = year_month[4:6]
                        folder_date = f"{year}-{month}-{day}"
                        
                        # Use actual_start_date (which includes previous day)
                        if actual_start_date and folder_date < actual_start_date:
                            continue
                        if end_date and folder_date > end_date:
                            continue
                            
                        filtered_folders.append(folder)
                except (ValueError, IndexError):
                    continue
            daily_folders = filtered_folders
        
        # Convert folder paths to date strings and return
        daily_groups = {}
        for folder in daily_folders:
            try:
                parts = folder.split(os.sep)
                year_month, day = parts[-2], parts[-1]
                year = year_month[:4]
                month = year_month[4:6]
                date_str = f"{year}{month}{day}"
                daily_groups[date_str] = [folder]
            except (ValueError, IndexError):
                continue
                
        return daily_groups
        
    else:  # realtime mode
        # Use timestamp-based approach for realtime data
        all_files = get_all_data_files(h3_dir)
        
        if not all_files:
            logger.warning("No data files found")
            return {}
        
        latest_time = max(all_files, key=lambda x: x[1])[1]
        start_time = latest_time - timedelta(hours=hours_lookback)
        
        logger.info(f"Latest data timestamp: {latest_time}")
        logger.info(f"Processing window: {start_time} to {latest_time}")
        
        # Group by calendar day
        daily_groups = {}
        for file_path, timestamp in all_files:
            if start_time <= timestamp <= latest_time:
                day_key = timestamp.strftime('%Y%m%d')
                if day_key not in daily_groups:
                    daily_groups[day_key] = []
                daily_groups[day_key].append((file_path, timestamp))
        
        return daily_groups

def load_day_data(day_files_info: Union[List[str], List[Tuple[str, datetime]]]) -> pd.DataFrame:
    """Load all data for a single day regardless of mode"""
    logger = logging.getLogger(__name__)
    all_data = []
    
    if not day_files_info:
        return pd.DataFrame()
    
    # Check if this is realtime mode (tuples) or historical mode (strings)
    if isinstance(day_files_info[0], tuple):
        # Realtime mode: (file_path, timestamp) tuples
        for file_path, timestamp in day_files_info:
            try:
                df = pd.read_parquet(file_path)
                df['timestamp'] = timestamp
                df['file_path'] = file_path
                all_data.append(df)
            except Exception as e:
                logger.warning(f"Error reading {file_path}: {e}")
                continue
    else:
        # Historical mode: folder paths
        for folder_path in day_files_info:
            df, _ = load_daily_data(folder_path)  # reuse existing function
            if not df.empty:
                all_data.append(df)
    
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    else:
        return pd.DataFrame()

def check_interpolated_file_exists(date_str, countries, mode='historical'):
    """Check if interpolated file already exists for the given date"""
    from pathlib import Path
    country_str = "_".join(sorted(countries))
    subdir = 'realtime' if mode == 'realtime' else 'historical'
    interpolated_dir = Path(f"data/processed/himawari/interpolated/{subdir}")
    interpolated_file = interpolated_dir / f"interpolated_h3_aod_{date_str}_{country_str}.parquet"
    return interpolated_file.exists()

def aggregate_daily_data(
    h3_dir: str,
    output_dir: str,
    boundaries_grid: pd.DataFrame,
    mode: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    hours_lookback: int = 24,
    countries: List[str] = ["LAO", "THA"]
) -> None:
    """
    Unified function to aggregate Himawari H3 data into daily averages for both modes
    
    Args:
        h3_dir: Directory containing H3 indexed data
        output_dir: Base output directory
        boundaries_grid: DataFrame with grid boundaries  
        mode: Processing mode ('historical' or 'realtime')
        start_date: Start date for historical processing (YYYY-MM-DD)
        end_date: End date for historical processing (YYYY-MM-DD)
        hours_lookback: Hours to look back for realtime processing
        countries: List of country codes
    """
    logger = logging.getLogger(__name__)
    countries_str = "_".join(sorted(countries))
    
    # Create mode-specific output directory
    mode_output_dir = os.path.join(output_dir, mode)
    os.makedirs(mode_output_dir, exist_ok=True)
    
    logger.info(f"Processing {mode} mode")
    logger.info(f"Output directory: {mode_output_dir}")
    
    # Debug: Check boundaries grid data types
    logger.info(f"Boundaries grid h3_08 dtype: {boundaries_grid['h3_08'].dtype}")
    logger.info(f"Boundaries grid h3_08 sample: {boundaries_grid['h3_08'].head(3).tolist()}")
    
    # Get daily data groups (includes previous day for 2-day averaging)
    daily_data_groups = get_daily_data_groups(h3_dir, mode, start_date, end_date, hours_lookback)
    
    if not daily_data_groups:
        logger.warning("No data found to process")
        return
    
    logger.info(f"Found {len(daily_data_groups)} days of data available")
    
    # For historical mode, filter to only process the actual requested date range
    if mode == 'historical' and start_date and end_date:
        # Convert original date range for processing filter
        requested_start = datetime.strptime(start_date, '%Y-%m-%d')
        requested_end = datetime.strptime(end_date, '%Y-%m-%d')
        
        # Filter dates to process (only the originally requested range)
        dates_to_process = []
        dates_skipped = []
        for date_str in sorted(daily_data_groups.keys()):
            try:
                date_obj = datetime.strptime(date_str, '%Y%m%d')
                if requested_start <= date_obj <= requested_end:
                    # Check if interpolated file already exists
                    if check_interpolated_file_exists(date_str, countries, mode='historical'):
                        logger.info(f"Skipping {date_str} - interpolated file already exists")
                        dates_skipped.append(date_str)
                    else:
                        dates_to_process.append(date_str)
            except ValueError:
                continue
        
        if dates_skipped:
            logger.info(f"Skipped {len(dates_skipped)} dates with existing interpolated files")
        logger.info(f"Will process {len(dates_to_process)} days in requested range")
    else:
        # For realtime mode, process all available dates (skipping those with existing interpolated files)
        dates_to_process = []
        dates_skipped = []
        for date_str in sorted(daily_data_groups.keys()):
            if check_interpolated_file_exists(date_str, countries, mode='realtime'):
                logger.info(f"Skipping {date_str} - interpolated file already exists")
                dates_skipped.append(date_str)
            else:
                dates_to_process.append(date_str)
        
        if dates_skipped:
            logger.info(f"Skipped {len(dates_skipped)} dates with existing interpolated files")
        logger.info(f"Will process {len(dates_to_process)} days")
    
    for date_str in dates_to_process:
        logger.info(f"Processing {date_str} ({len(daily_data_groups[date_str])} files)")
        
        # Load all data for this calendar day
        df_current = load_day_data(daily_data_groups[date_str])
        
        if df_current.empty:
            logger.warning(f"No data found for {date_str}")
            continue
        
        # Debug: Check data types and sample values
        logger.info(f"Current data columns: {list(df_current.columns)}")
        if 'h3_08' in df_current.columns:
            logger.info(f"Current data h3_08 dtype: {df_current['h3_08'].dtype}")
            logger.info(f"Current data h3_08 sample: {df_current['h3_08'].head(3).tolist()}")
        else:
            logger.error(f"h3_08 column not found in current data! Available columns: {list(df_current.columns)}")
            continue
        
        # Start with complete boundary grid (all hexagons)
        df_final = boundaries_grid[['h3_08']].copy()
        
        # Get value column
        try:
            value_col = get_value_column(df_current)
        except ValueError as e:
            logger.error(f"Error for {date_str}: {e}")
            continue
        
        # Convert data types to match for merging
        logger.info("Converting data types for consistent merging...")
        
        # Convert H3 integer indices to H3 hex strings for consistent merging
        if df_current['h3_08'].dtype in ['uint64', 'int64']:
            logger.info("Converting H3 integer indices to hex strings...")
            df_current['h3_08'] = df_current['h3_08'].apply(lambda x: h3.h3_to_string(x))
        else:
            df_current['h3_08'] = df_current['h3_08'].astype(str)
        
        # Ensure boundaries grid is also string format
        df_final['h3_08'] = df_final['h3_08'].astype(str)
        
        logger.info(f"After conversion - boundaries h3_08 dtype: {df_final['h3_08'].dtype}")
        logger.info(f"After conversion - current data h3_08 dtype: {df_current['h3_08'].dtype}")
        logger.info(f"Boundaries h3_08 sample: {df_final['h3_08'].head(3).tolist()}")
        logger.info(f"Current data h3_08 sample: {df_current['h3_08'].head(3).tolist()}")
        
        # Calculate 1-day average
        df_1day = df_current.groupby('h3_08')[value_col].mean().reset_index()
        df_1day.rename(columns={value_col: 'aod_1day'}, inplace=True)
        df_final = df_final.merge(df_1day, on='h3_08', how='left')
        
        logger.info(f"1-day average: {len(df_1day)} hexagons with data")
        
        # Save to mode-specific directory
        output_file = os.path.join(mode_output_dir, f"daily_h3_aod_{date_str}_{countries_str}.parquet")
        df_final.to_parquet(output_file)
        
        logger.info(f"Saved: {output_file}")
        logger.info(f"Total hexagons: {len(df_final)}")
        logger.info(f"1-day data coverage: {df_final['aod_1day'].notna().sum()}/{len(df_final)} hexagons")
        
        logger.info("=" * 50)

def main():
    """Main function for command line execution"""
    parser = argparse.ArgumentParser(description='Aggregate Himawari H3 data into daily averages')
    
    parser.add_argument('--mode', type=str, choices=['historical', 'realtime'], default='historical',
                       help='Processing mode: historical (calendar days) or realtime (rolling time windows) (default: historical)')
    parser.add_argument('--h3-dir', type=str, default='./data/processed/himawari/h3',
                       help='Directory containing H3 indexed data')
    parser.add_argument('--output-dir', type=str, default='./data/processed/himawari/daily_aggregated',
                       help='Output directory for aggregated data')
    parser.add_argument('--start-date', type=str,
                       help='Start date for processing (YYYY-MM-DD) - only used in historical mode')
    parser.add_argument('--end-date', type=str,
                       help='End date for processing (YYYY-MM-DD) - only used in historical mode')
    parser.add_argument('--hours-lookback', type=int, default=24,
                       help='Hours to look back from latest data (default: 24) - only used in realtime mode')
    parser.add_argument('--countries', type=str, nargs='+', default=['LAO', 'THA'],
                       help='Country codes for boundaries (default: LAO THA)')
    parser.add_argument('--buffer-degrees', type=float, default=0.4,
                       help='Buffer around country boundaries in degrees (default: 0.4)')
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_basic_logging(__name__)
    
    logger.info(f"Running in {args.mode} mode")
    
    # Generate grid boundaries (always auto-generate for simplicity)
    logger.info("Generating grid boundaries from country boundaries...")
    boundaries_grid = generate_boundaries_grid(
        countries=args.countries,
        buffer_degrees=args.buffer_degrees
    )
    
    # Save generated boundaries for reference
    boundaries_output = os.path.join(args.output_dir, "generated_boundaries_grid.parquet")
    os.makedirs(args.output_dir, exist_ok=True)
    boundaries_grid.to_parquet(boundaries_output)
    logger.info(f"Saved generated boundaries grid to: {boundaries_output}")
    
    logger.info(f"Using {len(boundaries_grid)} grid cells")
    
    # Run unified aggregation for both modes
    logger.info(f"Running {args.mode} aggregation")
    aggregate_daily_data(
        h3_dir=args.h3_dir,
        output_dir=args.output_dir,
        boundaries_grid=boundaries_grid,
        mode=args.mode,
        start_date=args.start_date,
        end_date=args.end_date,
        hours_lookback=args.hours_lookback,
        countries=args.countries
    )
    
    logger.info("Processing completed successfully!")

if __name__ == "__main__":
    main() 