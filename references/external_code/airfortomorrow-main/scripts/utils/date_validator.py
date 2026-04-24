#!/usr/bin/env python3
"""
Cross-platform date validation utility.

This utility provides reliable date validation that works consistently
across different operating systems (macOS, Linux, Windows).

Usage:
    python scripts/utils/date_validator.py validate "2024-07-07"
    python scripts/utils/date_validator.py range "2024-07-07" "2024-07-14"
"""

import argparse
import datetime
import sys
from typing import Optional, Tuple


def validate_date(date_str: str) -> Tuple[bool, Optional[str]]:
    """
    Validate a date string in YYYY-MM-DD format.
    
    Args:
        date_str: Date string to validate
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    if not date_str:
        return False, "Date string is empty"
    
    # Check basic format
    if len(date_str) != 10 or date_str.count('-') != 2:
        return False, f"Invalid date format: {date_str} (expected YYYY-MM-DD)"
    
    try:
        # Parse the date
        parsed_date = datetime.datetime.strptime(date_str, '%Y-%m-%d')
        
        # Additional validation checks
        year = parsed_date.year
        if year < 1900 or year > 2100:
            return False, f"Invalid year: {year} (must be between 1900-2100)"
        
        return True, None
        
    except ValueError as e:
        return False, f"Invalid date: {date_str} ({str(e)})"


def validate_date_range(start_date: str, end_date: str) -> Tuple[bool, Optional[str]]:
    """
    Validate a date range.
    
    Args:
        start_date: Start date string (YYYY-MM-DD)
        end_date: End date string (YYYY-MM-DD)
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    # Validate individual dates
    start_valid, start_error = validate_date(start_date)
    if not start_valid:
        return False, f"Start date error: {start_error}"
    
    end_valid, end_error = validate_date(end_date)
    if not end_valid:
        return False, f"End date error: {end_error}"
    
    # Check if start <= end
    try:
        start_dt = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        
        if start_dt > end_dt:
            return False, f"Start date {start_date} must be before or equal to end date {end_date}"
        
        # Check if range is reasonable (not more than 5 years)
        if (end_dt - start_dt).days > (5 * 365):
            return False, f"Date range too large: {(end_dt - start_dt).days} days (maximum 5 years)"
        
        return True, None
        
    except ValueError as e:
        return False, f"Date range validation error: {str(e)}"


def get_date_info(date_str: str) -> dict:
    """
    Get information about a date.
    
    Args:
        date_str: Date string (YYYY-MM-DD)
        
    Returns:
        Dictionary with date information
    """
    try:
        dt = datetime.datetime.strptime(date_str, '%Y-%m-%d')
        return {
            'date': date_str,
            'year': dt.year,
            'month': dt.month,
            'day': dt.day,
            'weekday': dt.strftime('%A'),
            'day_of_year': dt.timetuple().tm_yday,
            'iso_week': dt.isocalendar()[1],
            'timestamp': dt.timestamp(),
            'is_leap_year': dt.year % 4 == 0 and (dt.year % 100 != 0 or dt.year % 400 == 0)
        }
    except ValueError:
        return {}


def main():
    """Command line interface for date validation."""
    parser = argparse.ArgumentParser(
        description='Cross-platform date validation utility',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python scripts/utils/date_validator.py validate "2024-07-07"
    python scripts/utils/date_validator.py range "2024-07-07" "2024-07-14"
    python scripts/utils/date_validator.py info "2024-07-07"
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Validate single date
    validate_parser = subparsers.add_parser('validate', help='Validate a single date')
    validate_parser.add_argument('date', help='Date to validate (YYYY-MM-DD)')
    
    # Validate date range
    range_parser = subparsers.add_parser('range', help='Validate a date range')
    range_parser.add_argument('start_date', help='Start date (YYYY-MM-DD)')
    range_parser.add_argument('end_date', help='End date (YYYY-MM-DD)')
    
    # Get date info
    info_parser = subparsers.add_parser('info', help='Get information about a date')
    info_parser.add_argument('date', help='Date to analyze (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    if args.command == 'validate':
        is_valid, error = validate_date(args.date)
        if is_valid:
            print(f"✅ Valid date: {args.date}")
            sys.exit(0)
        else:
            print(f"❌ {error}")
            sys.exit(1)
    
    elif args.command == 'range':
        is_valid, error = validate_date_range(args.start_date, args.end_date)
        if is_valid:
            start_dt = datetime.datetime.strptime(args.start_date, '%Y-%m-%d')
            end_dt = datetime.datetime.strptime(args.end_date, '%Y-%m-%d')
            days = (end_dt - start_dt).days + 1
            print(f"✅ Valid date range: {args.start_date} to {args.end_date} ({days} days)")
            sys.exit(0)
        else:
            print(f"❌ {error}")
            sys.exit(1)
    
    elif args.command == 'info':
        is_valid, error = validate_date(args.date)
        if not is_valid:
            print(f"❌ {error}")
            sys.exit(1)
        
        info = get_date_info(args.date)
        print(f"📅 Date Information for {args.date}:")
        print(f"   Year: {info['year']}")
        print(f"   Month: {info['month']}")
        print(f"   Day: {info['day']}")
        print(f"   Weekday: {info['weekday']}")
        print(f"   Day of year: {info['day_of_year']}")
        print(f"   ISO week: {info['iso_week']}")
        print(f"   Leap year: {info['is_leap_year']}")
        sys.exit(0)


if __name__ == '__main__':
    main() 