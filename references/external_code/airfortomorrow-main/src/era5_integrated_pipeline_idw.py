#!/usr/bin/env python3
"""
ERA5 Integrated Pipeline with IDW Interpolation

This module provides an integrated pipeline for ERA5 meteorological data collection
and processing using Inverse Distance Weighting (IDW) interpolation.

The pipeline combines data collection and daily aggregation into a single workflow,
eliminating the need for intermediate H3 files and directly producing daily aggregated
data with IDW interpolation.
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional
import warnings

from src.data_collectors.era5_meteorological_idw import ERA5MeteorologicalCollectorIDW

# Suppress warnings
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)


def run_era5_integrated_pipeline_idw(
    mode: str,
    start_date: str = None,
    end_date: str = None,
    hours: int = 24,
    params: List[str] = None,
    countries: List[str] = None,
    output_dir: str = "./data/processed/era5",
    raw_data_dir: str = "./data/raw/era5",
    idw_rings: int = 10,
    idw_weight_power: float = 1.5,
    force_reprocess: bool = False,
    log_level: str = "INFO"
) -> dict:
    """
    Run the complete ERA5 pipeline with IDW interpolation.
    
    This pipeline combines data collection and processing into a single step,
    directly producing daily aggregated data with IDW interpolation.
    
    Args:
        mode: Processing mode ('realtime' or 'historical')
        start_date: Start date for historical mode (YYYY-MM-DD)
        end_date: End date for historical mode (YYYY-MM-DD)
        hours: Hours to look back for real-time mode
        params: List of ERA5 parameters to collect
        countries: List of country codes
        output_dir: Base output directory
        raw_data_dir: Raw data directory
        idw_rings: Number of rings for IDW interpolation
        idw_weight_power: Power for distance weighting in IDW
        force_reprocess: If True, reprocess files even if they already exist
        log_level: Logging level
        
    Returns:
        Dictionary with pipeline results
    """
    # Setup logging
    log_dir = Path(output_dir).parent.parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"era5_integrated_idw_{timestamp}.log"
    
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger(__name__)
    
    # Initialize results dictionary
    results = {
        'start_time': datetime.now(),
        'mode': mode,
        'countries': countries,
        'params': params,
        'idw_rings': idw_rings,
        'idw_weight_power': idw_weight_power,
        'output_files': [],
        'success': False,
        'error': None
    }
    
    try:
        logger.info("=" * 80)
        logger.info("ERA5 INTEGRATED PIPELINE WITH IDW STARTING")
        logger.info("=" * 80)
        logger.info(f"Mode: {mode}")
        logger.info(f"Countries: {countries}")
        logger.info(f"Parameters: {params}")
        logger.info(f"IDW rings: {idw_rings}")
        logger.info(f"IDW weight power: {idw_weight_power}")
        
        # Apply 7-day buffer for rolling calculations
        BUFFER_DAYS = 7
        original_start_date = None
        original_end_date = None
        
        if mode == 'historical':
            # Store original dates
            original_start_date = start_date
            original_end_date = end_date
            
            # Calculate buffered start date (7 days before)
            start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
            buffer_start_date_obj = start_date_obj - timedelta(days=BUFFER_DAYS)
            buffer_start_date = buffer_start_date_obj.strftime('%Y-%m-%d')
            
            logger.info(f"Original date range requested: {original_start_date} to {original_end_date}")
            logger.info(f"Collecting with {BUFFER_DAYS}-day buffer: {buffer_start_date} to {end_date}")
            logger.info(f"Buffer allows calculation of {BUFFER_DAYS}-day rolling averages from day 1")
            
            # Use buffered date for collection
            start_date = buffer_start_date
        else:
            # For realtime, ensure we collect at least 7 days for rolling calculations
            if hours < 7 * 24:
                logger.info(f"Realtime mode: requested {hours} hours, extending to {7*24} hours for rolling calculations")
                hours = 7 * 24
            else:
                logger.info(f"Real-time mode: past {hours} hours")
        
        # Set up output directory
        aggregated_dir = f"{output_dir}/daily_aggregated"
        
        # STEP 1: Data Collection and IDW Processing
        logger.info("")
        logger.info("=" * 60)
        logger.info("STEP 1: ERA5 DATA COLLECTION WITH IDW INTERPOLATION")
        logger.info("=" * 60)
        
        collector = ERA5MeteorologicalCollectorIDW(
            output_dir=aggregated_dir,
            raw_data_dir=raw_data_dir,
            params=params,
            countries=countries,
            idw_rings=idw_rings,
            idw_weight_power=idw_weight_power,
            force_reprocess=force_reprocess
        )
        
        if mode == 'realtime':
            output_files = collector.collect_realtime_data(hours)
        else:
            output_files = collector.collect_historical_data(start_date, end_date)
        
        results['output_files'] = output_files
        logger.info(f"Collection and IDW processing completed: {len(output_files)} files generated")
        
        # Calculate final results
        results['end_time'] = datetime.now()
        results['duration'] = results['end_time'] - results['start_time']
        results['success'] = True
        
        logger.info("")
        logger.info("=" * 80)
        logger.info("🎉 ERA5 INTEGRATED PIPELINE WITH IDW COMPLETED SUCCESSFULLY!")
        logger.info("=" * 80)
        logger.info(f"Total duration: {results['duration']}")
        logger.info(f"Output files: {len(results['output_files'])}")
        logger.info(f"Log file: {log_file}")
        
        return results
        
    except Exception as e:
        results['error'] = str(e)
        results['end_time'] = datetime.now()
        results['duration'] = results['end_time'] - results['start_time']
        logger.error(f"Pipeline failed: {e}")
        return results


def get_mode_specific_idw_defaults(mode: str) -> tuple[int, float]:
    """
    Get mode-specific IDW defaults from configuration.
    
    Args:
        mode: Processing mode ('realtime' or 'historical')
        
    Returns:
        Tuple of (rings, weight_power) for the specified mode
    """
    try:
        from src.utils.config_loader import ConfigLoader
        config_loader = ConfigLoader()
        era5_config = config_loader.get_data_collection_config('era5')
        idw_config = era5_config.get('idw', {})
        
        if mode == 'historical':
            historical_config = idw_config.get('historical', {})
            rings = historical_config.get('rings', 15)
            weight_power = historical_config.get('weight_power', 2.0)
        else:  # realtime
            realtime_config = idw_config.get('realtime', {})
            rings = realtime_config.get('rings', 8)
            weight_power = realtime_config.get('weight_power', 1.5)
            
        return rings, weight_power
        
    except Exception as e:
        # Fallback defaults if config loading fails
        if mode == 'historical':
            return 15, 2.0
        else:
            return 8, 1.5


def main():
    """Main function for command-line execution."""
    parser = argparse.ArgumentParser(
        description='ERA5 Integrated Pipeline with IDW Interpolation',
        epilog='''
This pipeline combines ERA5 data collection and IDW interpolation into a single workflow.
It directly produces daily aggregated data without intermediate H3 files.

CDS API Setup for Historical Mode:
Historical mode requires Climate Data Store (CDS) API credentials.
Set environment variables: CDSAPI_KEY="your-uid:your-api-key" and CDSAPI_URL="https://cds.climate.copernicus.eu/api"
Or create ~/.cdsapirc file. See docs/CDS_API_SETUP.md for details.
Get API key from: https://cds.climate.copernicus.eu/user
        ''',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Mode selection
    parser.add_argument('--mode', type=str, choices=['realtime', 'historical'], 
                       required=True, help='Processing mode')
    
    # Date parameters for historical mode
    parser.add_argument('--start-date', type=str,
                       help='Start date for historical mode (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str,
                       help='End date for historical mode (YYYY-MM-DD)')
    
    # Real-time parameters
    parser.add_argument('--hours', type=int, default=24,
                       help='Hours to look back for real-time mode (default: 24)')
    
    # Data parameters
    parser.add_argument('--params', nargs='+', 
                       default=ERA5MeteorologicalCollectorIDW.DEFAULT_PARAMS,
                       help='ERA5 parameters to collect')
    parser.add_argument('--steps', nargs='+', type=int,
                       default=ERA5MeteorologicalCollectorIDW.DEFAULT_STEPS,
                       help='Forecast steps in hours (for real-time mode)')
    
    # Countries
    parser.add_argument('--countries', nargs='+', default=["THA", "LAO"],
                       help='Country codes for processing')
    
    # IDW parameters
    parser.add_argument('--idw-rings', type=int, default=None,
                       help='Number of rings for IDW interpolation (historical: 15, realtime: 8)')
    parser.add_argument('--idw-weight-power', type=float, default=None,
                       help='Power for distance weighting in IDW (historical: 2.0, realtime: 1.5)')
    
    # Directory parameters
    parser.add_argument('--output-dir', type=str, default="./data/processed/era5",
                       help='Base output directory')
    parser.add_argument('--raw-data-dir', type=str, default="./data/raw/era5",
                       help='Directory for raw data metadata')
    
    # Processing options
    parser.add_argument('--force', action='store_true',
                       help='Force reprocessing even if output files already exist')
    
    # Logging
    parser.add_argument('--log-level', type=str, default="INFO",
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Logging level')
    
    args = parser.parse_args()
    
    # Get mode-specific IDW defaults if not specified
    if args.idw_rings is None or args.idw_weight_power is None:
        default_rings, default_weight_power = get_mode_specific_idw_defaults(args.mode)
        if args.idw_rings is None:
            args.idw_rings = default_rings
            print(f"📊 Using {args.mode} mode default: IDW rings = {default_rings}")
        if args.idw_weight_power is None:
            args.idw_weight_power = default_weight_power
            print(f"📊 Using {args.mode} mode default: IDW weight power = {default_weight_power}")
    
    # Validate arguments
    if args.mode == 'historical':
        if not args.start_date or not args.end_date:
            print("ERROR: Historical mode requires --start-date and --end-date")
            return 1
    
    try:
        # Run the integrated pipeline
        results = run_era5_integrated_pipeline_idw(
            mode=args.mode,
            start_date=args.start_date,
            end_date=args.end_date,
            hours=args.hours,
            params=args.params,
            countries=args.countries,
            output_dir=args.output_dir,
            raw_data_dir=args.raw_data_dir,
            idw_rings=args.idw_rings,
            idw_weight_power=args.idw_weight_power,
            force_reprocess=args.force,
            log_level=args.log_level
        )
        
        if results['success']:
            print(f"✅ Pipeline completed successfully!")
            print(f"📁 Generated {len(results['output_files'])} files")
            print(f"⏱️  Duration: {results['duration']}")
            return 0
        else:
            print(f"❌ Pipeline failed: {results['error']}")
            return 1
            
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
