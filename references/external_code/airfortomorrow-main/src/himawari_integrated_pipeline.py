#!/usr/bin/env python3
"""
Integrated Himawari AOD Pipeline

This script combines downloading Himawari data and H3 processing into a single pipeline.
It automatically checks for existing H3 files before downloading and processes new data.
"""

import os
import subprocess
import sys
import argparse
import shlex
from pathlib import Path
from datetime import datetime, timedelta
import logging
from typing import List, Sequence, Union

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent))
from src.utils.logging_utils import setup_logging
from src.utils.config_loader import ConfigLoader

def run_command(command: Union[Sequence[str], str], logger, description=""):
    """Run a command and log the results without invoking a shell."""
    logger.info(f"Starting: {description}")
    if isinstance(command, str):
        # Backward-compatible conversion for any legacy string callers.
        command = shlex.split(command)
    logger.info(f"Command: {' '.join(shlex.quote(part) for part in command)}")
    
    try:
        result = subprocess.run(
            command, 
            capture_output=True, 
            text=True, 
            check=True
        )
        
        if result.stdout:
            logger.info(f"STDOUT:\n{result.stdout}")
        if result.stderr:
            logger.warning(f"STDERR:\n{result.stderr}")
            
        logger.info(f"Completed successfully: {description}")
        return True, result.stdout, result.stderr
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed: {description}")
        logger.error(f"Return code: {e.returncode}")
        logger.error(f"STDOUT:\n{e.stdout}")
        logger.error(f"STDERR:\n{e.stderr}")
        return False, e.stdout, e.stderr

def check_existing_interpolated_file(date_str: str, countries: List[str], config_loader: ConfigLoader = None, mode: str = 'historical') -> bool:
    """Check if interpolated file already exists for the given date and countries"""
    if config_loader is None:
        config_loader = ConfigLoader()
    
    country_str = "_".join(sorted(countries))
    # Determine subdirectory based on mode
    subdir = 'realtime' if mode == 'realtime' else 'historical'
    interpolated_dir = Path(f"data/processed/himawari/interpolated/{subdir}")
    interpolated_file = interpolated_dir / f"interpolated_h3_aod_{date_str}_{country_str}.parquet"
    return interpolated_file.exists()

def main():
    """Main integrated pipeline function"""
    parser = argparse.ArgumentParser(description='Integrated Himawari AOD Pipeline')
    
    # Mode selection
    parser.add_argument('--mode', type=str, choices=['historical', 'realtime'], required=True,
                        help='Operation mode: historical or realtime')
    
    # Date parameters for historical mode
    parser.add_argument('--start-date', type=str,
                        help='Start date for historical collection (format: YYYY-MM-DD) - required for historical mode')
    parser.add_argument('--end-date', type=str,
                        help='End date for historical collection (format: YYYY-MM-DD) - required for historical mode')
    
    # Real-time parameters
    parser.add_argument('--hours', type=int, default=24,
                        help='Number of hours to collect in realtime mode (default: 24)')
    
    # Processing options
    parser.add_argument('--keep-originals', action='store_true',
                        help='Keep original NetCDF and TIF files in permanent locations (default: use cache, auto-delete)')
    parser.add_argument('--skip-download', action='store_true',
                        help='Skip download step and only process existing files')
    parser.add_argument('--skip-h3', action='store_true',
                        help='Skip H3 processing step')
    parser.add_argument('--force-download', action='store_true',
                        help='Force re-download of existing files')
    parser.add_argument('--countries', nargs='+', default=['LAO', 'THA'],
                        help='Country codes for geographic boundaries (default: LAO THA)')
    
    # Directory parameters
    parser.add_argument('--raw-data-dir', type=str, default='./data/raw/himawari',
                        help='Directory to store raw NetCDF files')
    parser.add_argument('--tif-dir', type=str, default='./data/processed/himawari/tif',
                        help='Directory to store GeoTIFF files')
    parser.add_argument('--h3-dir', type=str, default='./data/processed/himawari/h3',
                        help='Directory to store H3 indexed parquet files')
    parser.add_argument('--log-dir', type=str, default='./logs',
                        help='Directory to store log files')
    
    args = parser.parse_args()
    
    # Validate arguments based on mode
    if args.mode == 'historical':
        if not args.start_date or not args.end_date:
            parser.error("For historical mode, both --start-date and --end-date are required")
    
    # Setup logging first
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(args.log_dir, f"integrated_{args.mode}_pipeline_{timestamp}.log")
    logger = setup_logging(
        level="INFO",
        logger_name=__name__,
        log_file=log_file
    )
    
    # Initialize configuration loader
    try:
        config_loader = ConfigLoader()
        logger.info("Configuration system: Available")
    except Exception as e:
        logger.warning(f"Configuration system: Not available - {e}")
        config_loader = None
    logger.info(f"Logging initialized - log file: {log_file}")
    logger.info("="*80)
    logger.info("HIMAWARI AOD INTEGRATED PIPELINE STARTING")
    logger.info("="*80)
    logger.info(f"Mode: {args.mode}")
    if args.keep_originals:
        logger.info(f"Storage: Keep original files (NetCDF + TIF + H3)")
    else:
        logger.info(f"Storage: Cache-based (NetCDF → cache → delete, TIF → cache → delete, H3 → permanent)")
    logger.info(f"Countries: {', '.join(args.countries)}")
    
    # Log date range (buffer is now handled by bash script wrapper)
    if args.mode == 'historical':
        logger.info(f"Date range for data collection: {args.start_date} to {args.end_date}")
    else:
        if args.mode == 'realtime':
            # Use UTC time for consistency with satellite data timestamps
            cutoff_time = datetime.utcnow() - timedelta(hours=args.hours)
            logger.info(f"Realtime mode: downloading files from the past {args.hours} hours (since {cutoff_time} UTC)")
            logger.info(f"H3 processing: will process ALL downloaded files (no time filtering)")
        else:
            logger.info(f"Date range: {args.start_date} to {args.end_date}")
    
    # Create directories
    os.makedirs(args.raw_data_dir, exist_ok=True)
    os.makedirs(args.tif_dir, exist_ok=True)
    os.makedirs(args.h3_dir, exist_ok=True)
    
    pipeline_success = True
    
    # Check which dates need processing (for both historical and realtime modes)
    # Note: Buffer is handled by bash script wrapper, so we check the entire range passed to us
    dates_to_process = []
    
    if args.mode == 'historical':
        logger.info(f"Checking for existing interpolated files in date range: {args.start_date} to {args.end_date}")
        current_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        end_date_obj = datetime.strptime(args.end_date, '%Y-%m-%d')
        
        while current_date <= end_date_obj:
            date_str = current_date.strftime('%Y%m%d')
            if check_existing_interpolated_file(date_str, args.countries, config_loader):
                logger.info(f"Skipping {date_str} - interpolated file already exists")
            else:
                logger.info(f"Will process {date_str} - no existing interpolated file found")
                dates_to_process.append(current_date.strftime('%Y-%m-%d'))
            current_date += timedelta(days=1)
        
        if not dates_to_process:
            logger.info("All dates already have interpolated files. No processing needed.")
            return 0
        
        logger.info(f"Will process the following dates: {', '.join(dates_to_process)}")
    
    elif args.mode == 'realtime':
        logger.info(f"Checking for existing interpolated files in realtime mode (past {args.hours} hours)")
        
        # Calculate date range for the requested hours
        end_datetime = datetime.utcnow()
        start_datetime = end_datetime - timedelta(hours=args.hours)
        
        # Check each day in the range
        current_date = start_datetime.date()
        end_date = end_datetime.date()
        
        while current_date <= end_date:
            date_str = current_date.strftime('%Y%m%d')
            if check_existing_interpolated_file(date_str, args.countries, config_loader, mode='realtime'):
                logger.info(f"Skipping {date_str} - interpolated file already exists in realtime/")
            else:
                logger.info(f"Will process {date_str} - no existing interpolated file found")
                dates_to_process.append(current_date.strftime('%Y-%m-%d'))
            current_date += timedelta(days=1)
        
        if not dates_to_process:
            logger.info("All dates already have interpolated files in realtime/. No processing needed.")
            return 0
        
        logger.info(f"Will process {len(dates_to_process)} dates: {', '.join(dates_to_process)}")
    
    # Step 1: Download data (with H3 checking if not using streaming)
    if not args.skip_download:
        logger.info("\n" + "="*60)
        logger.info("STEP 1: DOWNLOADING HIMAWARI DATA")
        logger.info("="*60)
        
        # Log download parameters
        if args.mode == 'historical':
            logger.info(f"Download mode: Historical")
            logger.info(f"Date range: {args.start_date} to {args.end_date}")
            logger.info(f"Dates to process: {len(dates_to_process)} dates")
        else:
            logger.info(f"Download mode: Realtime")
            logger.info(f"Hours lookback: {args.hours}")
            logger.info(f"Dates to process: {len(dates_to_process)} dates")
        
        logger.info(f"H3 existence checking: ENABLED (skip existing H3 files)")
        logger.info(f"Force download: {'ENABLED' if args.force_download else 'DISABLED'}")
        logger.info(f"Countries: {', '.join(args.countries)}")
        
        download_cmd = ["python", "src/data_collectors/himawari_aod.py", "--mode", args.mode]
        
        if args.mode == 'historical':
            download_cmd.extend(["--start-date", args.start_date, "--end-date", args.end_date])
        else:
            download_cmd.extend(["--hours", str(args.hours)])
        
        download_cmd.extend(["--raw-data-dir", args.raw_data_dir])
        download_cmd.extend(["--tif-dir", args.tif_dir])
        download_cmd.extend(["--log-dir", args.log_dir])
        
        if args.force_download:
            download_cmd.append("--force-download")
        
        # Always enable H3 checking to skip existing files and save bandwidth
        download_cmd.append("--skip-if-h3-exists")
        download_cmd.extend(["--h3-dir", args.h3_dir])
        
        # Add countries for interpolated file checking
        if args.countries:
            download_cmd.append("--countries")
            download_cmd.extend(args.countries)
        
        # Configure download based on processing strategy
        if not args.keep_originals:
            # Use cache: Download only (no permanent TIF)
            download_cmd.append("--download-only")
        
        logger.info(f"Starting download...")
        success, stdout, stderr = run_command(
            download_cmd, 
            logger, 
            "Downloading Himawari AOD data"
        )
        
        if not success:
            logger.error("Download step failed!")
            pipeline_success = False
        else:
            logger.info("Download step completed successfully")
            # Try to extract summary from stdout
            if stdout:
                for line in stdout.split('\n'):
                    if 'Downloaded' in line or 'Skipped' in line or 'Total' in line:
                        logger.info(f"  {line.strip()}")
    
    # Step 2: H3 Processing
    if not args.skip_h3 and pipeline_success:
        logger.info("\n" + "="*60)
        logger.info("STEP 2: H3 INDEXING")
        logger.info("="*60)
        
        if args.keep_originals:
            # Use traditional pipeline (existing TIF files → H3)
            logger.info("Using traditional pipeline: permanent NetCDF and TIF files...")
            
            h3_cmd = ["python", "src/data_processors/process_himawari_h3_standard.py"]
            
            success, stdout, stderr = run_command(
                h3_cmd,
                logger,
                "Processing TIF files to H3 format"
            )
        else:
            # Use cache-based pipeline (NetCDF cache → TIF cache → H3 → delete both)
            logger.info("Using cache-based pipeline: temporary files, auto-delete NetCDF and TIF...")
            
            h3_cmd = ["python", "src/data_processors/process_himawari_h3_streaming.py"]
            
            if args.mode == 'realtime':
                h3_cmd.extend(["realtime", str(args.hours)])
            # For historical mode, no additional parameters needed (default behavior)
            
            # For cache-based approach, always delete NetCDF files
            h3_cmd.append("--delete-netcdf")
            
            # Add countries parameter if specified
            if args.countries:
                h3_cmd.append("--countries")
                h3_cmd.extend(args.countries)
            
            success, stdout, stderr = run_command(
                h3_cmd,
                logger,
                "Processing with cache-based pipeline"
            )
        
        if not success:
            logger.error("H3 processing step failed!")
            pipeline_success = False
        else:
            logger.info("H3 processing completed successfully")
    
    # Final summary
    logger.info("\n" + "="*80)
    if pipeline_success:
        logger.info("🎉 INTEGRATED PIPELINE COMPLETED SUCCESSFULLY!")
    else:
        logger.error("❌ INTEGRATED PIPELINE FAILED!")
    logger.info("="*80)
    
    # Show final directory sizes
    if os.path.exists(args.raw_data_dir):
        success, stdout, stderr = run_command(
            ["du", "-sh", args.raw_data_dir],
            logger,
            "Checking raw data directory size"
        )
    
    if os.path.exists(args.h3_dir):
        success, stdout, stderr = run_command(
            ["du", "-sh", args.h3_dir],
            logger,
            "Checking H3 data directory size"
        )
    
    # Show TIF directory size for traditional processing
    if args.keep_originals and os.path.exists(args.tif_dir):
        success, stdout, stderr = run_command(
            ["du", "-sh", args.tif_dir],
            logger,
            "Checking TIF data directory size"
        )
    
    return 0 if pipeline_success else 1

if __name__ == "__main__":
    exit(main()) 
