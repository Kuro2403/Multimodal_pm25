#!/usr/bin/env python3
"""Script to collect AirGradient data, both historical and real-time."""

import argparse
from datetime import datetime, timedelta
import logging
from pathlib import Path

from src.data_collectors.airgradient_collector import AirGradientCollector
from src.utils.logging_utils import setup_basic_logging

def get_country_codes_string(collector: AirGradientCollector) -> str:
    """Get a string of country codes from the configuration."""
    country_codes = collector.config['data_collection']['country_codes']
    return '_'.join(sorted(str(code) for code in country_codes))

def collect_historical_data(start_date: str, end_date: str):
    """Collect historical data for the specified date range."""
    collector = AirGradientCollector()
    countries = get_country_codes_string(collector)
    filename = f"airgradient_historical_{countries}_{start_date}_{end_date}.parquet"
    collector.collect(start_date, end_date, filename, mode="historical")

def collect_realtime_data():
    """Collect real-time data for the last few days."""
    collector = AirGradientCollector()
    
    # Get data for last 2 days
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
    
    # Use date in filename
    date = datetime.now().strftime("%Y%m%d")
    countries = get_country_codes_string(collector)
    filename = f"airgradient_realtime_{countries}_{date}.parquet"
    collector.collect(start_date, end_date, filename, mode="realtime")

def main():
    parser = argparse.ArgumentParser(description='Collect AirGradient data')
    parser.add_argument('--mode', choices=['historical', 'realtime'], required=True,
                      help='Data collection mode')
    parser.add_argument('--start-date', help='Start date for historical data (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='End date for historical data (YYYY-MM-DD)')
    
    args = parser.parse_args()
    setup_basic_logging(__name__)
    
    if args.mode == 'historical':
        if not args.start_date or not args.end_date:
            parser.error("Historical mode requires --start-date and --end-date")
        collect_historical_data(args.start_date, args.end_date)
    else:
        collect_realtime_data()

if __name__ == '__main__':
    main() 