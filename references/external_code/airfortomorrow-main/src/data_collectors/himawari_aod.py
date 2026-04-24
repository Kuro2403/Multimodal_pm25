#!/usr/bin/env python3
"""
Himawari Aerosol Optical Depth (AOD) Data Collection

This script downloads AOD data from the Himawari-8 satellite via JAXA's FTP server,
processes it into GeoTIFF format, and prepares it for further analysis.

It can operate in two modes:
- Historical: Downloads data for a specified date range
- Real-time: Downloads the most recent data for a specified number of hours
"""

import os
import argparse
import logging
import time
from datetime import datetime, timedelta
from ftplib import FTP
import numpy as np
import xarray as xr
import rasterio
from rasterio.transform import from_origin
import geopandas as gpd
import pandas as pd
from shapely.ops import unary_union
from dotenv import load_dotenv
from pathlib import Path

# Import centralized boundary utilities
from src.utils.boundary_utils import create_country_boundaries

def setup_logging(log_dir, mode):
    """Set up logging configuration"""
    os.makedirs(log_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"himawari_aod_{mode}_{timestamp}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger()

def connect_to_ftp(user, password):
    """Connect to JAXA FTP server and navigate to Himawari-8 AOD data directory"""
    ftp = FTP('ftp.ptree.jaxa.jp')
    ftp.login(user=user, passwd=password)
    
    # Navigate to the directory for L3 Aerosol Property data
    overall_path = "/pub/himawari/L3/ARP/031/"
    directories = overall_path.strip("/").split("/")
    
    for directory in directories:
        ftp.cwd(directory)
    
    return ftp

def parse_date_from_path(path):
    """Parse date from path in format YYYYMM/DD"""
    parts = path.split('/')
    if len(parts) == 2:
        year_month = parts[0]
        day = parts[1]
        year = year_month[:4]
        month = year_month[4:6]
        return datetime(int(year), int(month), int(day))
    return None

def get_historical_data_periods(ftp, start_date, end_date):
    """
    Get a list of available data periods (month/day) from the FTP server for historical data
    
    Args:
        ftp: FTP connection
        start_date: Start date in format YYYY-MM-DD
        end_date: End date in format YYYY-MM-DD
    """
    # List all available months
    print(f"DEBUG: Current FTP directory: {ftp.pwd()}")
    date_month_files = ftp.nlst()
    print(f"DEBUG: Available months: {date_month_files}")
    print(f"DEBUG: Number of months found: {len(date_month_files)}")
    
    if not date_month_files:
        raise ValueError(f"No month directories found on FTP server. Current directory: {ftp.pwd()}")
    
    date_month_files.sort(reverse=False)
    print(f"DEBUG: Month range: {date_month_files[0]} to {date_month_files[-1]}")
    
    # Convert requested date range to datetime objects
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    print(f"DEBUG: Requested date range: {start_dt} to {end_dt}")
    
    # Get the available date range
    available_dates = []
    for month_dir in date_month_files:
        try:
            year = int(month_dir[:4])
            month = int(month_dir[4:6])
            month_first_day = datetime(year, month, 1)
            available_dates.append(month_first_day)
        except (ValueError, IndexError):
            # Skip if not a valid month directory format
            continue
    
    if available_dates:
        min_date = min(available_dates)
        max_date = max(available_dates) + timedelta(days=31)  # Approximate end of last month
        print(f"DEBUG: Available date range on server: {min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}")
        
        # Check if requested range is completely outside available range
        if end_dt < min_date or start_dt > max_date:
            print(f"WARNING: Requested date range ({start_date} to {end_date}) is outside available range "
                  f"({min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')})")
            return []
    
    # For historical collection, we'll take the specified date range
    all_day_paths = []
    months_to_check = date_month_files  # Check all months instead of just the last 12
    print(f"DEBUG: Months to check: {months_to_check}")
    
    for idx, month in enumerate(months_to_check, 1):
        try:
            print(f"DEBUG: [{idx}/{len(months_to_check)}] Checking month: {month}")
            ftp.cwd(month)
            print(f"DEBUG: Changed to directory: {ftp.pwd()}")
            
            date_day_files = ftp.nlst()
            print(f"DEBUG: Raw items in {month}: {len(date_day_files)} items")
            date_day_files.sort(reverse=False)
            
            # Filter out daily and monthly summary files
            day_dirs = [day for day in date_day_files if 'daily' not in day and 'monthly' not in day]
            print(f"DEBUG: Filtered days in {month}: {len(day_dirs)} days - {day_dirs}")
            
            # Combine month/day paths
            month_day_paths = [f"{month}/{day}" for day in day_dirs]
            all_day_paths.extend(month_day_paths)
            print(f"DEBUG: Added {len(month_day_paths)} day paths, total so far: {len(all_day_paths)}")
            
            ftp.cwd("..")
        except Exception as e:
            print(f"DEBUG: Error processing month {month}: {str(e)}")
            import traceback
            traceback.print_exc()
            try:
                ftp.cwd("/pub/himawari/L3/ARP/031/")  # Return to base directory
                print(f"DEBUG: Returned to base directory after error")
            except:
                pass  # Ignore if this fails
    
    print(f"DEBUG: All day paths before date filtering: {len(all_day_paths)} paths")
    
    filtered_paths = []
    skipped_count = 0
    for path in all_day_paths:
        path_date = parse_date_from_path(path)
        if path_date and start_dt <= path_date <= end_dt:
            filtered_paths.append(path)
        else:
            skipped_count += 1
    
    print(f"DEBUG: Date filtering complete:")
    print(f"  - Paths matching date range: {len(filtered_paths)}")
    print(f"  - Paths outside date range: {skipped_count}")
    
    if filtered_paths:
        print(f"DEBUG: First path: {filtered_paths[0]}")
        print(f"DEBUG: Last path: {filtered_paths[-1]}")
        print(f"DEBUG: Total days to download: {len(filtered_paths)}")
    else:
        print(f"WARNING: No data found for the requested date range ({start_date} to {end_date})")
        if available_dates:
            print(f"WARNING: Available date range on server is approximately {min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}")
    
    return filtered_paths

def get_realtime_data_periods(ftp, hours=24):
    """Get the most recent data periods (month/day/hour) from the FTP server for real-time data"""
    # List all available months
    date_month_files = ftp.nlst()
    print(f"DEBUG: Current FTP directory: {ftp.pwd()}")
    print(f"DEBUG: date_month_files = {date_month_files}")
    print(f"DEBUG: Number of months found: {len(date_month_files)}")
    
    if not date_month_files:
        raise ValueError(f"No month directories found on FTP server. Current directory: {ftp.pwd()}")
    
    date_month_files.sort(reverse=False)
    
    # Get the most recent month
    latest_month = date_month_files[-1]
    print(f"DEBUG: Latest month: {latest_month}")
    ftp.cwd(latest_month)
    
    # List all available days in the most recent month
    date_day_files = ftp.nlst()
    date_day_files.sort(reverse=False)
    print(f"DEBUG: date_day_files in {latest_month} = {date_day_files}")
    
    # Filter out daily and monthly summary files
    day_dirs = [day for day in date_day_files if 'daily' not in day and 'monthly' not in day]
    print(f"DEBUG: day_dirs after filtering = {day_dirs}")
    
    if not day_dirs:
        raise ValueError(f"No day directories found in month {latest_month}")
    
    # Get the most recent day
    latest_day = day_dirs[-1]
    print(f"DEBUG: Latest day: {latest_day}")
    
    # Navigate to the day directory
    ftp.cwd(latest_day)
    
    # List all hourly files
    hourly_files = ftp.nlst()
    hourly_files.sort(reverse=False)
    print(f"DEBUG: Found {len(hourly_files)} hourly files in {latest_month}/{latest_day}")
    
    # Return to the month directory
    ftp.cwd("..")
    
    # We might need to look at the previous day too if we need more hours
    needed_files = []
    
    # Add files from the latest day (most recent first)
    latest_files = [(latest_month, latest_day, hour_file) for hour_file in reversed(hourly_files)]
    needed_files.extend(latest_files)
    


        # If we need more hours and there are previous days available
    previous_day_indice = 2
    while len(needed_files) < hours and previous_day_indice <= len(day_dirs):
        previous_day = day_dirs[-previous_day_indice]
        ftp.cwd(previous_day)
        prev_hourly_files = ftp.nlst()
        prev_hourly_files.sort(reverse=False)
        ftp.cwd("..")

        # Add files from the previous day (most recent first)
        prev_files = [
            (latest_month, previous_day, hour_file)
            for hour_file in reversed(prev_hourly_files)
        ]
        needed_files.extend(prev_files)
        previous_day_indice = previous_day_indice + 1

    # Return to the base directory
    ftp.cwd("..")
    
    # If we still need more hours, go to the previous month
    if len(needed_files) < hours and len(date_month_files) > 1:
        print(f"DEBUG: Need more files ({len(needed_files)}/{hours}), going to previous month")
        previous_month = date_month_files[-2]  # Second to last month
        print(f"DEBUG: Accessing previous month: {previous_month}")
        ftp.cwd(previous_month)
        
        # Get all days in the previous month
        prev_month_days = ftp.nlst()
        prev_month_days = [day for day in prev_month_days if 'daily' not in day and 'monthly' not in day]
        prev_month_days.sort(reverse=True)  # Start from most recent day
        print(f"DEBUG: Days in previous month {previous_month}: {prev_month_days}")
        
        # Iterate through days in reverse order (most recent first)
        for prev_day in prev_month_days:
            if len(needed_files) >= hours:
                break
            
            ftp.cwd(prev_day)
            prev_month_hourly_files = ftp.nlst()
            prev_month_hourly_files.sort(reverse=False)
            ftp.cwd("..")
            
            # Add files from previous month (most recent first)
            prev_month_files = [
                (previous_month, prev_day, hour_file)
                for hour_file in reversed(prev_month_hourly_files)
            ]
            needed_files.extend(prev_month_files)
            print(f"DEBUG: Added {len(prev_month_hourly_files)} files from {previous_month}/{prev_day}, total now: {len(needed_files)}")
        
        # Return to base directory
        ftp.cwd("..")
    
    ftp.cwd("/pub/himawari/L3/ARP/031/")  # Ensure we're back at the base directory
    
    # Limit to the requested number of hours
    return needed_files[:hours]

def check_interpolated_file_exists(date_str, countries=['LAO', 'THA'], mode='historical'):
    """Check if interpolated file already exists for the given date"""
    from pathlib import Path
    country_str = "_".join(sorted(countries))
    subdir = 'realtime' if mode == 'realtime' else 'historical'
    interpolated_dir = Path(f"data/processed/himawari/interpolated/{subdir}")
    interpolated_file = interpolated_dir / f"interpolated_h3_aod_{date_str}_{country_str}.parquet"
    return interpolated_file.exists()

def download_historical_files(ftp, day_paths, output_dir, logger, force_download=False, check_h3_exists=False, h3_base_dir=None, mode='historical', countries=['LAO', 'THA']):
    """Download all hourly files for a list of days"""
    total_downloaded = 0
    total_skipped = 0
    days_skipped = 0
    
    for day_path in day_paths:
        try:
            # Extract date from day_path (e.g., "202510/22" -> "20251022")
            date_str = day_path.replace('/', '')
            
            # Check if interpolated file already exists for this date
            if check_interpolated_file_exists(date_str, countries, mode):
                logger.info(f"Skipping {day_path} - interpolated file already exists")
                days_skipped += 1
                continue
            
            # Navigate to the day directory
            ftp.cwd(day_path)
            logger.info(f"Processing directory: {day_path}")
            
            # Get list of hourly files
            hourly_files = ftp.nlst()
            
            # Create local directory for the files
            local_dir = os.path.join(output_dir, day_path)
            os.makedirs(local_dir, exist_ok=True)
            
            downloaded_files = []
            skipped_files = []
            
            # Download each hourly file
            for hourly_file in hourly_files:
                target_file_path = os.path.join(local_dir, hourly_file)
                
                # Check if H3 file already exists (if enabled)
                if check_h3_exists and h3_base_dir:
                    if check_h3_file_exists(target_file_path, h3_base_dir, mode):
                        logger.info(f"Skipping {hourly_file} - H3 file already exists")
                        skipped_files.append(hourly_file)
                        continue
                
                # Check if NetCDF file already exists
                if os.path.exists(target_file_path) and not force_download:
                    skipped_files.append(hourly_file)
                    continue
                
                # Download the file
                with open(target_file_path, "wb") as local_file:
                    ftp.retrbinary(f"RETR {hourly_file}", local_file.write)
                
                downloaded_files.append(hourly_file)
            
            logger.info(f"Downloaded {len(downloaded_files)} new files for {day_path}")
            if skipped_files:
                logger.info(f"Skipped {len(skipped_files)} existing files for {day_path}")
            
            total_downloaded += len(downloaded_files)
            total_skipped += len(skipped_files)
            
            # Return to parent directory
            ftp.cwd("..")
            ftp.cwd("..")
            
        except Exception as e:
            logger.error(f"Error downloading files for {day_path}: {str(e)}")
            # Try to return to root directory
            try:
                ftp.cwd("/pub/himawari/L3/ARP/031/")
            except:
                # If that fails, just reconnect
                pass
    
    if days_skipped > 0:
        logger.info(f"Skipped {days_skipped} days with existing interpolated files")
    
    return total_downloaded, total_skipped

def download_realtime_files(ftp, file_paths, output_dir, logger, force_download=False, check_h3_exists=False, h3_base_dir=None, mode='realtime', countries=['LAO', 'THA']):
    """Download the most recent hourly files"""
    downloaded_files = []
    skipped_files = []
    dates_with_interpolated = set()
    
    for month, day, hourly_file in file_paths:
        # Extract date (e.g., "202510" + "22" = "20251022")
        date_str = month + day
        
        # Check once per date if interpolated file exists
        if date_str not in dates_with_interpolated:
            if check_interpolated_file_exists(date_str, countries, mode):
                logger.info(f"Skipping date {month}/{day} - interpolated file already exists")
                dates_with_interpolated.add(date_str)
        
        # Skip all files for dates that have interpolated files
        if date_str in dates_with_interpolated:
            skipped_files.append((month, day, hourly_file))
            continue
        
        try:
            # Create local directory for the files
            local_dir = os.path.join(output_dir, month, day)
            os.makedirs(local_dir, exist_ok=True)
            
            target_file_path = os.path.join(local_dir, hourly_file)
            
            # Check if H3 file already exists (if enabled)
            if check_h3_exists and h3_base_dir:
                if check_h3_file_exists(target_file_path, h3_base_dir, mode):
                    logger.info(f"Skipping {month}/{day}/{hourly_file} - H3 file already exists")
                    skipped_files.append((month, day, hourly_file))
                    continue
            
            # Check if NetCDF file already exists
            if os.path.exists(target_file_path) and not force_download:
                logger.info(f"Skipping existing file: {month}/{day}/{hourly_file}")
                skipped_files.append((month, day, hourly_file))
                continue
            
            # Navigate to the file's directory
            ftp.cwd(f"{month}/{day}")
            
            # Download the file
            with open(target_file_path, "wb") as local_file:
                ftp.retrbinary(f"RETR {hourly_file}", local_file.write)
            
            logger.info(f"Downloaded {month}/{day}/{hourly_file}")
            downloaded_files.append((month, day, hourly_file))
            
            # Return to the base directory
            ftp.cwd("/pub/himawari/L3/ARP/031/")
            
        except Exception as e:
            logger.error(f"Error downloading {month}/{day}/{hourly_file}: {str(e)}")
            # Try to return to the base directory
            try:
                ftp.cwd("/pub/himawari/L3/ARP/031/")
            except:
                # If that fails, reconnect
                pass
    
    if dates_with_interpolated:
        logger.info(f"Skipped {len(dates_with_interpolated)} dates with existing interpolated files")
    
    return downloaded_files, skipped_files

def transform_nc_to_tif(raw_data_dir, tif_output_dir, logger, mode='historical', time_limit=None, sort_reverse=False, start_date=None, end_date=None):
    """Transform downloaded NetCDF files to GeoTIFF format"""
    # Get list of month directories
    month_dirs = os.listdir(raw_data_dir)
    month_dirs.sort(reverse=sort_reverse)
    
    if time_limit is None and mode == 'realtime':
        # Process all available data
        time_limit = float('inf')
    
    total_processed = 0
    total_errors = 0
    total_skipped = 0
    units_processed = 0  # days for historical, hours for realtime
    
    # Prepare date filters if specified
    start_dt = None
    end_dt = None
    if start_date and end_date and mode == 'historical':
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        logger.info(f"Filtering files between {start_date} and {end_date}")
    
    # Process each month directory
    for month_dir in month_dirs:
        month_path = os.path.join(raw_data_dir, month_dir)
        
        # Skip if not a directory
        if not os.path.isdir(month_path):
            continue
        
        # Skip if month is outside date range
        if start_dt and end_dt:
            try:
                year = int(month_dir[:4])
                month = int(month_dir[4:6])
                month_start = datetime(year, month, 1)
                month_end = datetime(year, month + 1, 1) - timedelta(days=1) if month < 12 else datetime(year + 1, 1, 1) - timedelta(days=1)
                
                # Skip this month if it's entirely outside the date range
                if month_end < start_dt or month_start > end_dt:
                    logger.info(f"Skipping month {month_dir} (outside date range)")
                    continue
            except ValueError:
                # If month_dir is not in expected format, just continue
                pass
        
        logger.info(f"Processing month: {month_dir}")
        
        # Get list of day directories
        day_dirs = os.listdir(month_path)
        day_dirs.sort(reverse=sort_reverse)
        
        # Process each day directory
        for day_dir in day_dirs:
            day_path = os.path.join(month_path, day_dir)
            
            # Skip if not a directory
            if not os.path.isdir(day_path):
                continue
            
            # Skip if day is outside date range
            if start_dt and end_dt:
                try:
                    day_date = datetime(int(month_dir[:4]), int(month_dir[4:6]), int(day_dir))
                    if day_date < start_dt or day_date > end_dt:
                        logger.info(f"Skipping day {month_dir}/{day_dir} (outside date range)")
                        continue
                except ValueError:
                    # If date is not in expected format, just continue
                    pass
            
            logger.info(f"Processing day: {day_dir}")
            
            # Create output directory for TIFs
            tif_day_dir = os.path.join(tif_output_dir, month_dir, day_dir)
            os.makedirs(tif_day_dir, exist_ok=True)
            
            # Get list of hourly files
            hourly_files = os.listdir(day_path)
            hourly_files.sort(reverse=sort_reverse)
            
            # Process each hourly file
            for hourly_file in hourly_files:
                if not hourly_file.endswith('.nc'):
                    continue
                
                nc_file_path = os.path.join(day_path, hourly_file)
                tif_file_name = hourly_file.replace(".nc", ".tif")
                tif_file_path = os.path.join(tif_day_dir, tif_file_name)
                
                # Skip if TIF already exists
                if os.path.exists(tif_file_path):
                    logger.info(f"Skipping existing TIF: {tif_file_path}")
                    total_skipped += 1
                    if mode == 'realtime':
                        units_processed += 1
                        if units_processed >= time_limit:
                            break
                    continue
                
                try:
                    # Open NetCDF dataset
                    dataset = xr.open_dataset(nc_file_path, engine='netcdf4')
                    
                    # Get the Aerosol Optical Depth variable (usually second variable)
                    variable_name = list(dataset.data_vars.keys())[1]  # Merged AOT product
                    data = dataset[variable_name]
                    
                    # Extract coordinates
                    lon = dataset['longitude'] if 'longitude' in dataset.coords else None
                    lat = dataset['latitude'] if 'latitude' in dataset.coords else None
                    
                    # Handle missing coordinates
                    if lon is None or lat is None:
                        lon_start, lon_step = -180, 0.05
                        lat_start, lat_step = 90, -0.05
                        lon = xr.DataArray(lon_start + lon_step * np.arange(data.shape[-1]), dims=['x'])
                        lat = xr.DataArray(lat_start + lat_step * np.arange(data.shape[-2]), dims=['y'])
                    
                    # Create georeferencing transform
                    transform = from_origin(
                        lon.min().item(), 
                        lat.max().item(), 
                        abs(lon[1] - lon[0]).item(), 
                        abs(lat[0] - lat[1]).item()
                    )
                    
                    # Write GeoTIFF
                    with rasterio.open(
                        tif_file_path,
                        'w',
                        driver='GTiff',
                        height=data.shape[-2],
                        width=data.shape[-1],
                        count=1,
                        dtype=data.dtype.name,
                        crs='EPSG:4326',
                        transform=transform
                    ) as dst:
                        dst.write(data.values, 1)
                    
                    logger.info(f"Created TIF: {tif_file_path}")
                    total_processed += 1
                    
                except Exception as e:
                    logger.error(f"Error processing {nc_file_path}: {str(e)}")
                    total_errors += 1
                
                if mode == 'realtime':
                    units_processed += 1
                    if units_processed >= time_limit:
                        break
            
            if mode == 'realtime' and units_processed >= time_limit:
                break
        
        if mode == 'realtime' and units_processed >= time_limit:
            break
    
    logger.info(f"Transformation complete: {total_processed} files processed, {total_errors} errors, {total_skipped} skipped")
    return total_processed, total_errors, total_skipped

def check_h3_file_exists(file_path, h3_base_dir, mode='historical'):
    """Check if H3 parquet file already exists for a given NetCDF file path"""
    try:
        # Extract relative path from NetCDF file
        # Convert: ./data/raw/himawari/202402/02/H09_20240202_0100_1HARP031_FLDK.02401_02401.nc
        # To: ./data/processed/himawari/h3/historical/202402/02/H09_20240202_0100_1HARP031_FLDK.02401_02401.parquet
        
        # Get filename without extension
        filename = os.path.basename(file_path)
        filename_no_ext = filename.replace('.nc', '')
        
        # Extract date components from path
        path_parts = file_path.split(os.sep)
        
        # Find month and day in path (e.g., ['data', 'raw', 'himawari', '202402', '02', 'filename.nc'])
        month = None
        day = None
        for i, part in enumerate(path_parts):
            if len(part) == 6 and part.isdigit():  # Month format YYYYMM
                month = part
                if i + 1 < len(path_parts) and len(path_parts[i + 1]) == 2 and path_parts[i + 1].isdigit():
                    day = path_parts[i + 1]
                break
        
        if not month or not day:
            return False
        
        # Construct H3 parquet path
        h3_subdir = 'historical' if mode == 'historical' else 'realtime'
        h3_file_path = os.path.join(h3_base_dir, h3_subdir, month, day, f"{filename_no_ext}.parquet")
        
        exists = os.path.exists(h3_file_path)
        return exists
        
    except Exception as e:
        # If any error occurs, assume file doesn't exist
        return False

# Note: create_country_boundaries is now imported from utils.boundary_utils
# This alias is kept for backward compatibility
def create_country_boundaries_local(countries, buffer_degrees=0.4):
    """Use centralized boundary utility."""
    return create_country_boundaries(countries, buffer_degrees)

def main():
    """Main function to download and process Himawari AOD data"""
    # Load environment variables from .env file if it exists
    env_path = Path('.env')
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
        
    parser = argparse.ArgumentParser(description='Himawari AOD Data Collection')
    
    # Mode selection
    parser.add_argument('--mode', type=str, choices=['historical', 'realtime'], default='realtime',
                        help='Operation mode: historical or realtime (default: realtime)')
    
    # FTP parameters
    parser.add_argument('--user', type=str,
                        help='Username for JAXA FTP server (can also be set via HIMAWARI_FTP_USER env var)')
    parser.add_argument('--password', type=str,
                        help='Password for JAXA FTP server (can also be set via HIMAWARI_FTP_PASSWORD env var)')
    
    # Date parameters for historical mode
    parser.add_argument('--start-date', type=str,
                        help='Start date for historical collection (format: YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str,
                        help='End date for historical collection (format: YYYY-MM-DD)')
    
    # Real-time parameters
    parser.add_argument('--hours', type=int, default=24,
                        help='Number of hours to collect in realtime mode (default: 24)')
    parser.add_argument('--force-download', action='store_true',
                        help='Force re-download of existing files')
    
    # Path parameters
    parser.add_argument('--raw-data-dir', type=str, default='./data/raw/himawari',
                        help='Directory to store raw NetCDF files')
    parser.add_argument('--tif-dir', type=str, default='./data/processed/himawari/tif',
                        help='Directory to store GeoTIFF files')
    parser.add_argument('--log-dir', type=str, default='./logs',
                        help='Directory to store log files')
    
    # Processing flags
    parser.add_argument('--download-only', action='store_true',
                        help='Only download data without conversion to TIF')
    parser.add_argument('--transform-only', action='store_true',
                        help='Only convert existing NetCDF files to TIF')
    parser.add_argument('--skip-if-h3-exists', action='store_true',
                        help='Skip downloading files if corresponding H3 parquet files already exist')
    parser.add_argument('--h3-dir', type=str, default='./data/processed/himawari/h3',
                        help='Directory containing H3 indexed parquet files')
    parser.add_argument('--countries', nargs='+', default=['LAO', 'THA'],
                        help='Country codes for checking interpolated files (default: LAO THA)')
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging(args.log_dir, args.mode)
    logger.info(f"Starting Himawari AOD {args.mode} data collection")
    
    # Get credentials from environment variables if not provided via command line
    user = args.user or os.environ.get('HIMAWARI_FTP_USER')
    password = args.password or os.environ.get('HIMAWARI_FTP_PASSWORD')
    
    if not user or not password:
        logger.error("FTP username and password must be provided either via command line arguments or environment variables (HIMAWARI_FTP_USER, HIMAWARI_FTP_PASSWORD)")
        return 1
    
    # Validate date parameters for historical mode
    if args.mode == 'historical':
        if not args.start_date or not args.end_date:
            logger.error("For historical mode, both start_date and end_date must be provided")
            return 1
        
        try:
            start_dt = datetime.strptime(args.start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(args.end_date, "%Y-%m-%d")
            if start_dt > end_dt:
                logger.error("Start date must be before or equal to end date")
                return 1
            logger.info(f"Date range: {args.start_date} to {args.end_date}")
        except ValueError as e:
            logger.error(f"Invalid date format: {str(e)}")
            return 1
    else:
        logger.info(f"Hours to collect: {args.hours}")
        
    logger.info("FTP credentials provided; username redacted")
    logger.info(f"Raw data directory: {args.raw_data_dir}")
    logger.info(f"TIF directory: {args.tif_dir}")
    
    if args.skip_if_h3_exists:
        logger.info(f"H3 checking enabled - will skip files if H3 parquet exists in: {args.h3_dir}")
    else:
        logger.info("H3 checking disabled - will download all available files")
    
    # Create directories
    os.makedirs(args.raw_data_dir, exist_ok=True)
    os.makedirs(args.tif_dir, exist_ok=True)
    
    start_time = time.time()
    
    # Download data if not in transform-only mode
    if not args.transform_only:
        try:
            logger.info("Connecting to FTP server...")
            ftp = connect_to_ftp(user, password)
            
            if args.mode == 'historical':
                # Historical mode - download specified date range
                logger.info(f"Getting available data periods for date range {args.start_date} to {args.end_date}...")
                day_paths = get_historical_data_periods(ftp, args.start_date, args.end_date)
                
                logger.info(f"Found {len(day_paths)} days to process: {', '.join(day_paths)}")
                
                total_downloaded, total_skipped = download_historical_files(
                    ftp, day_paths, args.raw_data_dir, logger, args.force_download,
                    check_h3_exists=args.skip_if_h3_exists, h3_base_dir=args.h3_dir, mode='historical',
                    countries=args.countries
                )
                
                logger.info(f"Download complete: {total_downloaded} files downloaded, {total_skipped} files skipped")
                
            else:
                # Realtime mode - download latest hours
                logger.info(f"Getting latest data for the past {args.hours} hours...")
                file_paths = get_realtime_data_periods(ftp, args.hours)
                
                logger.info(f"Found {len(file_paths)} files to process")
                
                downloaded, skipped = download_realtime_files(
                    ftp, file_paths, args.raw_data_dir, logger, args.force_download,
                    check_h3_exists=args.skip_if_h3_exists, h3_base_dir=args.h3_dir, mode='realtime',
                    countries=args.countries
                )
                
                logger.info(f"Download complete: {len(downloaded)} files downloaded, {len(skipped)} files skipped")
            
            try:
                ftp.quit()
            except:
                pass
            
        except Exception as e:
            logger.error(f"Error in download process: {str(e)}")
            if not args.download_only:
                logger.info("Continuing to transformation step despite download errors")
    
    # Transform data if not in download-only mode
    if not args.download_only:
        try:
            logger.info("Starting transformation of NetCDF to GeoTIFF...")
            
            if args.mode == 'historical':
                # Historical mode - process by date range
                processed, errors, skipped = transform_nc_to_tif(
                    args.raw_data_dir, args.tif_dir, logger, 
                    mode='historical', start_date=args.start_date, end_date=args.end_date, sort_reverse=False
                )
            else:
                # Realtime mode - process hours, most recent first
                processed, errors, skipped = transform_nc_to_tif(
                    args.raw_data_dir, args.tif_dir, logger, 
                    mode='realtime', time_limit=args.hours, sort_reverse=True
                )
            
            logger.info(f"Transformation summary: {processed} files processed, {errors} errors, {skipped} skipped")
            
        except Exception as e:
            logger.error(f"Error in transformation process: {str(e)}")
    
    elapsed_time = time.time() - start_time
    logger.info(f"Processing completed in {elapsed_time:.2f} seconds")
    
    return 0

if __name__ == "__main__":
    exit(main()) 
