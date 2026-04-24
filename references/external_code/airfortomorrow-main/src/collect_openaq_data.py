#!/usr/bin/env python3
"""Script to collect OpenAQ data, both historical and real-time."""

import argparse
from datetime import datetime, timedelta
import logging
from pathlib import Path

from src.data_collectors.openaq_collector import OpenAQCollector
from src.utils.logging_utils import setup_basic_logging

def get_country_codes_string(collector: OpenAQCollector) -> str:
    """Get a string of country codes from the configuration."""
    country_codes = collector.config['data_collection'].get('country_codes_openaq', 
                                                             collector.config['data_collection']['country_codes'])
    
    # Map numeric OpenAQ country codes to ISO country codes
    code_mapping = {
        68: "LAO",   # Laos
        111: "THA"   # Thailand
    }
    
    # Convert numeric codes to ISO codes
    iso_codes = []
    for code in sorted(country_codes):
        if code in code_mapping:
            iso_codes.append(code_mapping[code])
        else:
            # Fallback to original code if mapping not found
            iso_codes.append(str(code))
    
    return '_'.join(iso_codes)

def collect_historical_data(start_date: str, end_date: str):
    """Collect historical data for the specified date range."""
    print("\n=== Starting Historical Data Collection ===")
    print(f"Time period: {start_date} to {end_date}")
    
    print("\n1. Initializing OpenAQ collector...")
    collector = OpenAQCollector()
    
    print("\n2. Setting up file paths...")
    countries = get_country_codes_string(collector)
    filename = f"openaq_historical_{countries}_from_{start_date}_to_{end_date}.parquet"
    print(f"Output file will be: {filename}")
    
    print("\n3. Starting data collection...")
    collector.collect(start_date, end_date, filename, mode="historical")
    
    print("\n=== Data Collection Complete ===")

def collect_realtime_data(days: int = 2):
    """Collect real-time data for the specified number of days.
    
    Args:
        days: Number of days to look back for data collection (default: 2)
    """
    print("\n=== Starting Real-time Data Collection with Parallel Processing ===")
    
    print("\n1. Initializing OpenAQ collector...")
    collector = OpenAQCollector()
    
    # Get data for specified number of days
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    print(f"\n2. Time period: {start_date} to {end_date} ({days} days)")
    
    print("\n3. Setting up file paths...")
    # Use descriptive date range in filename instead of timestamp
    countries = get_country_codes_string(collector)
    filename = f"openaq_realtime_{countries}_from_{start_date}_to_{end_date}.parquet"
    print(f"Output file will be: {filename}")
    
    print(f"\n4. Starting parallel data collection (up to {collector.MAX_WORKERS} concurrent workers)...")
    collector.collect(start_date, end_date, filename, mode="realtime")
    
    print("\n=== Data Collection Complete ===")

def main():
    """Main function to handle command line arguments."""
    parser = argparse.ArgumentParser(description="Collect OpenAQ data with parallel processing support.")
    parser.add_argument(
        "--mode",
        choices=["historical", "realtime"],
        required=True,
        help="Collection mode: historical or realtime"
    )
    parser.add_argument(
        "--start-date",
        help="Start date for historical data collection (YYYY-MM-DD)",
        required=False
    )
    parser.add_argument(
        "--end-date",
        help="End date for historical data collection (YYYY-MM-DD)",
        required=False
    )
    parser.add_argument(
        "--days",
        type=int,
        default=2,
        help="Number of days to look back for real-time data collection (default: 2)"
    )
    
    args = parser.parse_args()
    
    print("\n=== OpenAQ Data Collection Script ===")
    print(f"Mode: {args.mode}")
    if args.mode == "realtime":
        print(f"Days to collect: {args.days}")
    
    setup_basic_logging(__name__)
    
    if args.mode == "historical":
        if not args.start_date or not args.end_date:
            parser.error("Historical mode requires --start-date and --end-date")
        collect_historical_data(args.start_date, args.end_date)
    else:
        collect_realtime_data(args.days)

if __name__ == "__main__":
    main()