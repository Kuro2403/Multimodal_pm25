#!/usr/bin/env python3
"""
ERA5 Daily Aggregator

This module aggregates H3-indexed ERA5 meteorological data into daily averages,
combining multiple parameters (temperature, wind, dewpoint) and countries into 
single daily files. Duplicate H3 hexagons from overlapping countries are 
automatically removed by averaging.

Supports both real-time and historical aggregation modes.
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Set
import warnings
import time

import pandas as pd
import polars as pl
from glob import glob

# Suppress warnings
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)


class ERA5DailyAggregator:
    """
    Aggregates H3-indexed ERA5 data into daily means.
    
    Combines data from multiple countries and parameters into unified daily 
    meteorological datasets. Automatically handles overlapping H3 hexagons 
    between countries by averaging their values.
    """
    
    # Default parameters to aggregate
    DEFAULT_PARAMS = ["2d", "2t", "10u", "10v"]
    
    # Parameter descriptions for metadata
    PARAM_DESCRIPTIONS = {
        "2t": "2-meter temperature",
        "10u": "10-meter u-component of wind",
        "10v": "10-meter v-component of wind", 
        "2d": "2-meter dewpoint temperature"
    }
    
    def __init__(self,
                 h3_dir: str = "./data/processed/era5/h3",
                 output_dir: str = "./data/processed/era5/daily_aggregated",
                 params: List[str] = None,
                 countries: List[str] = None,
                 cleanup_h3: bool = False):
        """
        Initialize ERA5 Daily Aggregator.
        
        Args:
            h3_dir: Directory containing H3-indexed parameter files
            output_dir: Directory for daily aggregated output
            params: List of parameters to aggregate
            countries: List of country codes
            cleanup_h3: Whether to delete H3 data after successful aggregation
        """
        self.h3_dir = Path(h3_dir)
        self.output_dir = Path(output_dir)
        self.params = params or self.DEFAULT_PARAMS.copy()
        self.countries = countries or ["THA", "LAO"]
        self.cleanup_h3 = cleanup_h3
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
        
        # Create output directories
        self.output_dir.mkdir(parents=True, exist_ok=True)
        (self.output_dir / "realtime").mkdir(exist_ok=True)
        (self.output_dir / "historical").mkdir(exist_ok=True)
        
        self.logger.info(f"ERA5 Daily Aggregator initialized")
        self.logger.info(f"H3 directory: {self.h3_dir}")
        self.logger.info(f"Output directory: {self.output_dir}")
        self.logger.info(f"Parameters: {self.params}")
        self.logger.info(f"Countries to process: {self.countries}")
        self.logger.info(f"Approach: Combining all countries into single daily files")
        self.logger.info(f"Cleanup H3 data after aggregation: {self.cleanup_h3}")
    
    def aggregate_realtime(self, hours_lookback: int = 24) -> List[str]:
        """
        Aggregate real-time ERA5 data from the past N hours.
        
        Unlike historical processing which works with complete calendar days,
        real-time processing works with exact time windows that may span
        multiple calendar dates.
        
        Args:
            hours_lookback: Hours to look back for data
            
        Returns:
            List of output file paths
        """
        self.logger.info(f"Aggregating real-time ERA5 data (past {hours_lookback} hours)")
        
        # Calculate exact time window
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours_lookback)
        
        self.logger.info(f"Time window: {start_time.strftime('%Y-%m-%d %H:%M')} to {end_time.strftime('%Y-%m-%d %H:%M')}")
        
        # Find all files within the exact time window across all countries
        merged_files = []
        countries_with_data = []
        
        for country_code in self.countries:
            realtime_dir = self.h3_dir / "realtime" / country_code
            if not realtime_dir.exists():
                self.logger.warning(f"Realtime directory not found for {country_code}: {realtime_dir}")
                continue
            
            # Find files within time window for this country
            country_files = self._find_files_in_time_window(realtime_dir, start_time, end_time, country_code)
            if country_files:
                merged_files.extend(country_files)
                countries_with_data.append(country_code)
                self.logger.info(f"Found {len(country_files)} files for {country_code} in time window")
        
        if not merged_files:
            self.logger.warning("No real-time data found for any country in time window")
            return []
        
        self.logger.info(f"Found {len(merged_files)} total files across {len(countries_with_data)} countries: {countries_with_data}")
        
        # Aggregate all files in the time window into a single output
        output_path = self._aggregate_realtime_time_window(merged_files, start_time, end_time, countries_with_data)
        
        return [output_path] if output_path else []
    
    def aggregate_historical(self, start_date: str, end_date: str) -> List[str]:
        """
        Aggregate historical ERA5 data for date range.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            List of output file paths
        """
        self.logger.info(f"Aggregating historical ERA5 data from {start_date} to {end_date}")
        
        # Parse dates
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        output_paths = []
        
        # Process each date (combining all countries)
        current_date = start_dt
        while current_date <= end_dt:
            date_str = current_date.strftime("%Y%m%d")
            
            try:
                output_path = self._aggregate_single_date_all_countries(date_str, "historical")
                if output_path:
                    output_paths.append(output_path)
            except Exception as e:
                self.logger.error(f"Error aggregating date {date_str}: {e}")
                
            current_date += timedelta(days=1)
        
        return output_paths
    
    def _find_available_dates(self, base_dir: Path, start_time: datetime, end_time: datetime) -> List[str]:
        """
        Find available dates within time window.
        
        Args:
            base_dir: Base directory to search
            start_time: Start of time window
            end_time: End of time window
            
        Returns:
            List of date strings (YYYYMMDD format)
        """
        available_dates = set()
        
        # Search for date directories
        for year_month_dir in base_dir.glob("*"):
            if not year_month_dir.is_dir():
                continue
                
            for day_dir in year_month_dir.glob("*"):
                if not day_dir.is_dir():
                    continue
                
                # Parse date from directory structure (YYYYMM/DD)
                try:
                    year_month = year_month_dir.name
                    day = day_dir.name
                    date_str = f"{year_month}{day}"
                    
                    # Validate date format
                    date_obj = datetime.strptime(date_str, "%Y%m%d")
                    
                    # Check if within time window
                    if start_time <= date_obj <= end_time:
                        available_dates.add(date_str)
                        
                except ValueError:
                    continue
        
        return sorted(list(available_dates))
    
    def _find_files_in_time_window(self, base_dir: Path, start_time: datetime, end_time: datetime, country_code: str) -> List[Path]:
        """
        Find files within exact time window, parsing timestamps from filenames.
        
        Args:
            base_dir: Base directory to search
            start_time: Start of time window
            end_time: End of time window
            country_code: Country code for filtering files
            
        Returns:
            List of file paths within the time window
        """
        matching_files = []
        
        # Search through date directories that might overlap with our time window
        # We need to check a few days around our time window to be safe
        search_start = start_time - timedelta(days=1)
        search_end = end_time + timedelta(days=1)
        
        current_date = search_start.replace(hour=0, minute=0, second=0, microsecond=0)
        while current_date <= search_end:
            year_month = current_date.strftime("%Y%m")
            day = current_date.strftime("%d")
            
            day_dir = base_dir / year_month / day
            if day_dir.exists():
                # Find all merged files for this country in this directory
                pattern = f"h3_era5_merged_*_{country_code}.parquet"
                for file_path in day_dir.glob(pattern):
                    # Extract timestamp from filename: h3_era5_merged_YYYY-MM-DD-HHMM_COUNTRY.parquet
                    try:
                        filename = file_path.stem
                        # Extract timestamp part: h3_era5_merged_2025-06-04-0000_THA -> 2025-06-04-0000
                        timestamp_part = filename.split('_')[3]  # Gets YYYY-MM-DD-HHMM
                        
                        # Parse timestamp
                        file_time = datetime.strptime(timestamp_part, "%Y-%m-%d-%H%M")
                        
                        # Check if file is within our time window
                        if start_time <= file_time <= end_time:
                            matching_files.append(file_path)
                            self.logger.debug(f"File {file_path.name} timestamp {file_time} is within window")
                        else:
                            self.logger.debug(f"File {file_path.name} timestamp {file_time} is outside window")
                            
                    except (ValueError, IndexError) as e:
                        self.logger.warning(f"Could not parse timestamp from filename {file_path.name}: {e}")
                        continue
            
            current_date += timedelta(days=1)
        
        return sorted(matching_files)
    
    def _aggregate_single_date_all_countries(self, date_str: str, mode: str) -> Optional[str]:
        """
        Aggregate data for a single date across all countries.
        
        Args:
            date_str: Date string in YYYYMMDD format
            mode: Processing mode ('realtime' or 'historical')
            
        Returns:
            Output file path or None if failed
        """
        self.logger.info(f"Aggregating {mode} data for {date_str}")
        
        # Parse date for directory structure
        date_obj = datetime.strptime(date_str, "%Y%m%d")
        year_month = date_obj.strftime("%Y%m")
        day = date_obj.strftime("%d")
        
        # Find files for this date across all countries
        all_merged_files = []
        countries_with_data = []
        
        for country_code in self.countries:
            data_dir = self.h3_dir / mode / country_code / year_month / day
            
            if not data_dir.exists():
                self.logger.debug(f"Data directory not found for {country_code}: {data_dir}")
                continue
            
            # Find merged files for this country
            # Handle both real-time (merged) and historical (reanalysis) file patterns
            country_merged_files = list(data_dir.glob(f"h3_era5_merged_*_{country_code}.parquet"))
            country_reanalysis_files = list(data_dir.glob(f"h3_era5_reanalysis_*_{country_code}.parquet"))
            
            # Combine both patterns - this ensures compatibility with both real-time and historical
            country_files = country_merged_files + country_reanalysis_files
            
            if country_files:
                all_merged_files.extend(country_files)
                countries_with_data.append(country_code)
                self.logger.debug(f"Found {len(country_files)} files for {country_code}")
        
        if not all_merged_files:
            self.logger.warning(f"No merged files found for {date_str}")
            return None
        
        self.logger.info(f"Found {len(all_merged_files)} merged files for {date_str} across countries: {countries_with_data}")
        
        # Load and combine all merged files for this date
        all_dataframes = []
        for file_path in all_merged_files:
            try:
                df = pl.read_parquet(file_path)
                all_dataframes.append(df)
                self.logger.debug(f"Loaded {file_path}: {df.shape}")
            except Exception as e:
                self.logger.error(f"Error reading {file_path}: {e}")
                continue
        
        if not all_dataframes:
            self.logger.warning(f"No valid data files found for {date_str}")
            return None
        
        # Ensure consistent column ordering before concatenation
        # Standard order: h3_08 first, then parameters in alphabetical order
        if all_dataframes:
            # Get all unique columns from all dataframes
            all_columns = set()
            for df in all_dataframes:
                all_columns.update(df.columns)
            
            # Define standard column order: h3_08 first, then parameters alphabetically
            h3_col = "h3_08"
            param_cols = sorted([col for col in all_columns if col != h3_col])
            standard_order = [h3_col] + param_cols
            
            # Reorder all dataframes to match standard order
            standardized_dataframes = []
            for df in all_dataframes:
                # Only select columns that exist in this dataframe
                available_cols = [col for col in standard_order if col in df.columns]
                df_reordered = df.select(available_cols)
                standardized_dataframes.append(df_reordered)
            
            all_dataframes = standardized_dataframes
        
        # Concatenate all time steps for this date
        combined_df = pl.concat(all_dataframes)
        self.logger.info(f"Combined {len(all_dataframes)} time steps: {combined_df.shape}")
        
        # Group by H3 cell and calculate daily mean for all parameters
        # Get all parameter columns (exclude h3_08)
        param_columns = [col for col in combined_df.columns if col != "h3_08"]
        
        if not param_columns:
            self.logger.warning(f"No parameter columns found in data for {date_str}")
            return None
        
        # Aggregate by H3 cell - calculate mean for all parameters
        # This automatically handles duplicate H3 cells from overlapping countries
        daily_aggregated = combined_df.group_by("h3_08").agg(
            [pl.col(param).mean().alias(param) for param in param_columns]
        )
        
        self.logger.info(f"Daily aggregation completed: {daily_aggregated.shape} - Columns: {daily_aggregated.columns}")
        self.logger.info(f"Removed duplicates - unique H3 cells: {len(daily_aggregated)}")
        
        # Add date column
        daily_aggregated = daily_aggregated.with_columns([
            pl.lit(date_obj.date()).alias("date")
        ])
        
        # Save output
        output_path = self._save_daily_aggregated(daily_aggregated, date_str, mode)
        
        if output_path:
            self.logger.info(f"Successfully aggregated {date_str}: {len(daily_aggregated)} unique H3 cells from {len(countries_with_data)} countries")
            
            # Cleanup H3 data if requested and aggregation was successful
            if self.cleanup_h3:
                self._cleanup_h3_data_for_date(date_str, mode)
        
        return output_path
    
    def _save_daily_aggregated(self, df: pl.DataFrame, date_str: str, mode: str) -> Optional[str]:
        """
        Save daily aggregated data for all countries.
        
        Args:
            df: Aggregated DataFrame
            date_str: Date string in YYYYMMDD format
            mode: Processing mode
            
        Returns:
            Output file path or None if failed
        """
        try:
            # Create output directory for this mode
            output_mode_dir = self.output_dir / mode
            output_mode_dir.mkdir(parents=True, exist_ok=True)
            
            # Format date for filename
            date_obj = datetime.strptime(date_str, "%Y%m%d")
            date_formatted = date_obj.strftime("%Y-%m-%d")
            
            # Create country string for filename (sorted for consistency)
            countries_str = "_".join(sorted(self.countries))
            
            # Create output paths
            parquet_path = output_mode_dir / f"era5_daily_mean_{date_formatted}_{countries_str}.parquet"
            
            # Convert to pandas for saving
            df_pandas = df.to_pandas()
            
            # Save files
            df_pandas.to_parquet(parquet_path, index=False)
            
            self.logger.debug(f"Saved: {parquet_path}")
            
            return str(parquet_path)
            
        except Exception as e:
            self.logger.error(f"Error saving aggregated data: {e}")
            return None
    
    def _cleanup_h3_data_for_date(self, date_str: str, mode: str) -> None:
        """
        Delete H3 data for a specific date across all countries after successful aggregation.
        
        Args:
            date_str: Date string in YYYYMMDD format
            mode: Processing mode ('realtime' or 'historical')
        """
        try:
            # Parse date for directory structure
            date_obj = datetime.strptime(date_str, "%Y%m%d")
            year_month = date_obj.strftime("%Y%m")
            day = date_obj.strftime("%d")
            
            total_deleted = 0
            total_size = 0
            
            # Process each country
            for country_code in self.countries:
                # Find data directory for this date and country
                data_dir = self.h3_dir / mode / country_code / year_month / day
                
                if not data_dir.exists():
                    self.logger.debug(f"H3 data directory not found for cleanup: {data_dir}")
                    continue
                
                # Count files before deletion
                h3_files = list(data_dir.glob(f"h3_era5_merged_*_{country_code}.*"))
                file_count = len(h3_files)
                
                if file_count == 0:
                    self.logger.debug(f"No H3 files found to cleanup for {date_str} in {country_code}")
                    continue
                
                # Calculate total size before deletion
                country_size = sum(f.stat().st_size for f in h3_files)
                total_size += country_size
                
                # Delete the files
                deleted_count = 0
                for file_path in h3_files:
                    try:
                        file_path.unlink()
                        deleted_count += 1
                        total_deleted += 1
                    except Exception as e:
                        self.logger.warning(f"Failed to delete {file_path}: {e}")
                
                # Try to remove empty directories
                try:
                    if not any(data_dir.iterdir()):  # Directory is empty
                        data_dir.rmdir()
                        self.logger.debug(f"Removed empty directory: {data_dir}")
                        
                        # Try to remove parent month directory if empty
                        month_dir = data_dir.parent
                        if not any(month_dir.iterdir()):
                            month_dir.rmdir()
                            self.logger.debug(f"Removed empty directory: {month_dir}")
                except OSError:
                    # Directory not empty or other issue, ignore
                    pass
                
                self.logger.debug(f"Cleaned up {deleted_count} files for {country_code}")
            
            size_mb = total_size / (1024 * 1024)
            self.logger.info(f"Cleanup: Deleted {total_deleted} H3 files for {date_str} ({size_mb:.1f} MB freed)")
            
        except Exception as e:
            self.logger.error(f"Error during H3 cleanup for {date_str}: {e}")
    
    def cleanup_all_h3_data(self, mode: str = None) -> Dict[str, int]:
        """
        Delete all H3 data for specified mode(s). Use with caution!
        
        Args:
            mode: Specific mode to cleanup ('realtime' or 'historical'), or None for both
            
        Returns:
            Dictionary with cleanup statistics
        """
        stats = {"files_deleted": 0, "mb_freed": 0, "directories_removed": 0}
        
        modes_to_cleanup = [mode] if mode else ["realtime", "historical"]
        
        for cleanup_mode in modes_to_cleanup:
            mode_dir = self.h3_dir / cleanup_mode
            if not mode_dir.exists():
                continue
                
            self.logger.warning(f"Starting cleanup of ALL H3 data in {cleanup_mode} mode...")
            
            # Find all H3 files
            h3_files = list(mode_dir.glob("**/h3_era5_merged_*.parquet")) + \
                      list(mode_dir.glob("**/h3_era5_merged_*.csv"))
            
            for file_path in h3_files:
                try:
                    file_size = file_path.stat().st_size
                    file_path.unlink()
                    stats["files_deleted"] += 1
                    stats["mb_freed"] += file_size / (1024 * 1024)
                except Exception as e:
                    self.logger.warning(f"Failed to delete {file_path}: {e}")
            
            # Remove empty directories
            for root, dirs, files in os.walk(mode_dir, topdown=False):
                for dir_name in dirs:
                    dir_path = Path(root) / dir_name
                    try:
                        if not any(dir_path.iterdir()):
                            dir_path.rmdir()
                            stats["directories_removed"] += 1
                    except OSError:
                        pass
        
        self.logger.warning(f"H3 cleanup completed: {stats['files_deleted']} files deleted, "
                           f"{stats['mb_freed']:.1f} MB freed, {stats['directories_removed']} directories removed")
        
        return stats
    
    def generate_summary_stats(self, mode: str = "historical") -> Dict:
        """
        Generate summary statistics for aggregated data.
        
        Args:
            mode: Processing mode to analyze
            
        Returns:
            Dictionary of summary statistics
        """
        summary = {
            "mode": mode,
            "total_files": 0,
            "date_range": None,
            "parameters": self.params,
            "avg_h3_cells_per_day": 0,
            "countries_processed": self.countries
        }
        
        mode_dir = self.output_dir / mode
        if not mode_dir.exists():
            return summary
        
        # Create expected filename pattern with country codes
        countries_str = "_".join(sorted(self.countries))
        filename_pattern = f"era5_daily_mean_*_{countries_str}.parquet"
        
        # Find all parquet files with the expected pattern
        parquet_files = list(mode_dir.glob(filename_pattern))
        summary["total_files"] = len(parquet_files)
        
        if parquet_files:
            # Extract dates from filenames
            dates = []
            h3_counts = []
            
            for file_path in parquet_files:
                try:
                    # Extract date from filename: era5_daily_mean_YYYY-MM-DD_{countries}.parquet
                    filename = file_path.stem
                    # Split by underscore and get the date part
                    parts = filename.split("_")
                    date_part = parts[3]  # era5_daily_mean_YYYY-MM-DD_{countries}
                    dates.append(date_part)
                    
                    # Count H3 cells
                    df = pl.read_parquet(file_path)
                    h3_counts.append(len(df))
                    
                except Exception as e:
                    self.logger.warning(f"Error analyzing {file_path}: {e}")
                    continue
            
            if dates:
                summary["date_range"] = f"{min(dates)} to {max(dates)}"
                summary["avg_h3_cells_per_day"] = sum(h3_counts) / len(h3_counts)
                summary["total_h3_cells"] = sum(h3_counts)
        
        return summary
    
    def _aggregate_realtime_time_window(self, file_paths: List[Path], start_time: datetime, end_time: datetime, countries_with_data: List[str]) -> Optional[str]:
        """
        Aggregate files from a specific time window into a single real-time output.
        
        Args:
            file_paths: List of file paths to aggregate
            start_time: Start of time window
            end_time: End of time window
            countries_with_data: List of countries that have data
            
        Returns:
            Output file path or None if failed
        """
        self.logger.info(f"Aggregating {len(file_paths)} files from time window")
        
        # Load and combine all files
        all_dataframes = []
        for file_path in file_paths:
            try:
                df = pl.read_parquet(file_path)
                all_dataframes.append(df)
                self.logger.debug(f"Loaded {file_path}: {df.shape}")
            except Exception as e:
                self.logger.error(f"Error reading {file_path}: {e}")
                continue
        
        if not all_dataframes:
            self.logger.warning("No valid data files found in time window")
            return None
        
        # Ensure consistent column ordering before concatenation
        # Standard order: h3_08 first, then parameters in alphabetical order
        if all_dataframes:
            # Get all unique columns from all dataframes
            all_columns = set()
            for df in all_dataframes:
                all_columns.update(df.columns)
            
            # Define standard column order: h3_08 first, then parameters alphabetically
            h3_col = "h3_08"
            param_cols = sorted([col for col in all_columns if col != h3_col])
            standard_order = [h3_col] + param_cols
            
            # Reorder all dataframes to match standard order
            standardized_dataframes = []
            for df in all_dataframes:
                # Only select columns that exist in this dataframe
                available_cols = [col for col in standard_order if col in df.columns]
                df_reordered = df.select(available_cols)
                standardized_dataframes.append(df_reordered)
            
            all_dataframes = standardized_dataframes
        
        # Concatenate all time steps
        combined_df = pl.concat(all_dataframes)
        self.logger.info(f"Combined {len(all_dataframes)} time steps: {combined_df.shape}")
        
        # Group by H3 cell and calculate mean for all parameters
        # Get all parameter columns (exclude h3_08)
        param_columns = [col for col in combined_df.columns if col != "h3_08"]
        
        if not param_columns:
            self.logger.warning("No parameter columns found in data")
            return None
        
        # Aggregate by H3 cell - calculate mean for all parameters
        # This automatically handles duplicate H3 cells from overlapping countries
        aggregated_df = combined_df.group_by("h3_08").agg(
            [pl.col(param).mean().alias(param) for param in param_columns]
        )
        
        self.logger.info(f"Time window aggregation completed: {aggregated_df.shape} - Columns: {aggregated_df.columns}")
        self.logger.info(f"Removed duplicates - unique H3 cells: {len(aggregated_df)}")
        
        # Add time window metadata
        aggregated_df = aggregated_df.with_columns([
            pl.lit(start_time).alias("window_start"),
            pl.lit(end_time).alias("window_end"),
            pl.lit((end_time - start_time).total_seconds() / 3600).alias("window_duration_hours"),
            pl.lit(end_time.date()).alias("date")
        ])
        
        # Save output with real-time naming convention
        output_path = self._save_realtime_aggregated(aggregated_df, end_time, countries_with_data)
        
        if output_path:
            self.logger.info(f"Successfully aggregated time window: {len(aggregated_df)} unique H3 cells from {len(countries_with_data)} countries")
            
            # Cleanup H3 data if requested and aggregation was successful
            if self.cleanup_h3:
                self._cleanup_realtime_h3_data(file_paths)
        
        return output_path
    
    def _save_realtime_aggregated(self, df: pl.DataFrame, end_time: datetime, countries_with_data: List[str]) -> Optional[str]:
        """
        Save real-time aggregated data with timestamp-based naming.
        
        Args:
            df: Aggregated DataFrame
            end_time: End time of the aggregation window
            countries_with_data: List of countries included
            
        Returns:
            Output file path or None if failed
        """
        try:
            # Create output directory for realtime
            output_realtime_dir = self.output_dir / "realtime"
            output_realtime_dir.mkdir(parents=True, exist_ok=True)
            
            # Create timestamp string for filename
            timestamp_str = end_time.strftime("%Y-%m-%d")
            
            # Create country string for filename (sorted for consistency)
            countries_str = "_".join(sorted(countries_with_data))
            
            # Create output paths with real-time naming convention
            parquet_path = output_realtime_dir / f"era5_realtime_24h_{timestamp_str}_{countries_str}.parquet"
            
            # Convert to pandas for saving
            df_pandas = df.to_pandas()
            
            # Save files
            df_pandas.to_parquet(parquet_path, index=False)
            
            self.logger.debug(f"Saved: {parquet_path}")
            
            return str(parquet_path)
            
        except Exception as e:
            self.logger.error(f"Error saving real-time aggregated data: {e}")
            return None
    
    def _cleanup_realtime_h3_data(self, file_paths: List[Path]) -> None:
        """
        Delete specific H3 files after successful real-time aggregation.
        
        Args:
            file_paths: List of file paths that were successfully aggregated
        """
        try:
            total_deleted = 0
            total_size = 0
            
            for file_path in file_paths:
                try:
                    file_size = file_path.stat().st_size
                    file_path.unlink()
                    total_deleted += 1
                    total_size += file_size
                    self.logger.debug(f"Deleted: {file_path}")
                except Exception as e:
                    self.logger.warning(f"Failed to delete {file_path}: {e}")
            
            size_mb = total_size / (1024 * 1024)
            self.logger.info(f"Real-time cleanup: Deleted {total_deleted} H3 files ({size_mb:.1f} MB freed)")
            
        except Exception as e:
            self.logger.error(f"Error during real-time H3 cleanup: {e}")


def setup_logging(log_dir: str = "./logs", log_level: str = "INFO"):
    """Setup logging configuration."""
    os.makedirs(log_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"era5_aggregator_{timestamp}.log")
    
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return log_file


def main():
    """Main function for command-line execution."""
    parser = argparse.ArgumentParser(description='ERA5 Daily Aggregator')
    
    # Mode selection
    parser.add_argument('--mode', type=str, choices=['realtime', 'historical'],
                       required=True, help='Aggregation mode')
    
    # Date parameters for historical mode
    parser.add_argument('--start-date', type=str,
                       help='Start date for historical mode (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str,
                       help='End date for historical mode (YYYY-MM-DD)')
    
    # Real-time parameters
    parser.add_argument('--hours-lookback', type=int, default=24,
                       help='Hours to look back for real-time mode (default: 24)')
    
    # Data parameters
    parser.add_argument('--params', nargs='+',
                       default=ERA5DailyAggregator.DEFAULT_PARAMS,
                       help='ERA5 parameters to aggregate')
    
    # Countries
    parser.add_argument('--countries', nargs='+', default=["THA", "LAO"],
                       help='Country codes for processing')
    
    # Directory parameters
    parser.add_argument('--h3-dir', type=str, default="./data/processed/era5/h3",
                       help='H3 data directory')
    parser.add_argument('--output-dir', type=str, default="./data/processed/era5/daily_aggregated",
                       help='Output directory for aggregated data')
    
    # Options
    parser.add_argument('--generate-stats', action='store_true',
                       help='Generate summary statistics')
    
    # Cleanup options
    parser.add_argument('--cleanup-h3', action='store_true',
                       help='Delete H3 data after successful daily aggregation (saves disk space)')
    parser.add_argument('--cleanup-all-h3', action='store_true',
                       help='Delete ALL existing H3 data immediately (use with caution!)')
    
    # Logging
    parser.add_argument('--log-dir', type=str, default="./logs",
                       help='Log directory')
    parser.add_argument('--log-level', type=str, default="INFO",
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Logging level')
    
    args = parser.parse_args()
    
    # Setup logging
    log_file = setup_logging(args.log_dir, args.log_level)
    logger = logging.getLogger(__name__)
    
    logger.info("=== ERA5 Daily Aggregator Starting ===")
    logger.info(f"Mode: {args.mode}")
    logger.info(f"Log file: {log_file}")
    
    # Validate arguments
    if args.mode == 'historical':
        if not args.start_date or not args.end_date:
            logger.error("Historical mode requires --start-date and --end-date")
            return 1
    
    try:
        # Initialize aggregator
        aggregator = ERA5DailyAggregator(
            h3_dir=args.h3_dir,
            output_dir=args.output_dir,
            params=args.params,
            countries=args.countries,
            cleanup_h3=args.cleanup_h3
        )
        
        # Handle immediate cleanup if requested
        if args.cleanup_all_h3:
            logger.warning("⚠️  CLEANUP ALL H3 DATA REQUESTED - This will delete all H3 files!")
            logger.warning("This action cannot be undone. Proceeding in 3 seconds...")
            time.sleep(3)
            
            stats = aggregator.cleanup_all_h3_data(args.mode if args.mode != 'historical' else None)
            logger.info(f"Cleanup completed: {stats}")
            return 0
        
        # Run aggregation based on mode
        if args.mode == 'realtime':
            output_paths = aggregator.aggregate_realtime(args.hours_lookback)
        else:
            output_paths = aggregator.aggregate_historical(args.start_date, args.end_date)
        
        logger.info(f"=== Aggregation Complete ===")
        logger.info(f"Generated {len(output_paths)} files")
        
        # Generate statistics if requested
        if args.generate_stats:
            stats = aggregator.generate_summary_stats(args.mode)
            logger.info("=== Summary Statistics ===")
            for key, value in stats.items():
                logger.info(f"{key}: {value}")
        
        logger.info(f"Log file: {log_file}")
        return 0
        
    except Exception as e:
        logger.error(f"Aggregation failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main()) 