import os
import sys
import logging
import polars as pl
import pandas as pd
import geopandas as gpd
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional, Tuple
import time
import h3

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

# Import the new configuration system
from src.utils.config_loader import ConfigLoader

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AirQualityProcessor:
    def __init__(
        self,
        mode: str = "realtime",
        hours: int = 24,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        countries: List[str] = None,
        minimum_threshold: float = None,
        maximum_threshold: float = None,
        config_path: str = None
    ):
        """
        Initialize the Air Quality Processor with centralized configuration.
        
        Args:
            mode: Processing mode ("realtime" or "historical")
            hours: Hours to look back in realtime mode
            start_date: Start date for historical mode (YYYY-MM-DD)
            end_date: End date for historical mode (YYYY-MM-DD)
            countries: List of country codes to process (None = use config default)
            minimum_threshold: Minimum PM2.5 value threshold (None = use config default)
            maximum_threshold: Maximum PM2.5 value threshold (None = use config default)
            config_path: Path to configuration file (optional)
        """
        # Initialize configuration system
        self.config_loader = ConfigLoader(config_path)
        
        # Set parameters with config fallbacks
        self.mode = mode
        self.hours = hours
        self.start_date = start_date
        self.end_date = end_date
        self.countries = countries or self.config_loader.get_countries()
        
        # Get air quality processing config
        aq_processing_config = self.config_loader.get_processing_config('air_quality') or {}
        self.minimum_threshold = minimum_threshold if minimum_threshold is not None else aq_processing_config.get('minimum_threshold', 0.0)
        self.maximum_threshold = maximum_threshold if maximum_threshold is not None else aq_processing_config.get('maximum_threshold', 500.0)
        
        # Set up directories using configuration system
        self.raw_dir = self.config_loader.get_path('raw.base', create_if_missing=True)
        self.processed_dir = self.config_loader.get_path('processed.airquality.base', create_if_missing=True)
        self.output_dir = self.processed_dir / mode
        
        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Set up data source specific paths using configuration
        self.openaq_dir = self.config_loader.get_path(f'raw.openaq.{mode}', create_if_missing=True)
        self.airgradient_dir = self.config_loader.get_path(f'raw.airgradient.{mode}', create_if_missing=True)
        
        # AirGradient mapping file path
        self.airgradient_mapping_file = self.config_loader.get_path('raw.airgradient.base', create_if_missing=True) / "location_list.csv"
        
        logger.info(f"AirQualityProcessor initialized:")
        logger.info(f"  Mode: {self.mode}")
        logger.info(f"  Countries: {self.countries}")
        logger.info(f"  PM2.5 thresholds: {self.minimum_threshold} - {self.maximum_threshold}")
        logger.info(f"  OpenAQ data: {self.openaq_dir}")
        logger.info(f"  AirGradient data: {self.airgradient_dir}")
        logger.info(f"  Output: {self.output_dir}")

    def _get_date_range(self) -> Tuple[datetime, datetime]:
        """Get the start and end dates for filtering."""
        if self.mode == "realtime":
            end_date = datetime.now(timezone.utc)
            start_date = end_date - timedelta(hours=self.hours)
        else:
            end_date = pd.to_datetime(self.end_date).tz_localize('UTC')
            start_date = pd.to_datetime(self.start_date).tz_localize('UTC')
        return start_date, end_date

    def _filter_files_by_date(self, files: List[Path], start_date: datetime, end_date: datetime) -> List[Path]:
        """Filter files based on their names to only include files that overlap with our target window."""
        relevant_files = []
        logger.info(f"Filtering files for time window: {start_date} to {end_date}")
        
        for file in files:
            try:
                # Handle OpenAQ file pattern (from_YYYY-MM-DD_to_YYYY-MM-DD)
                if 'from_' in file.name and '_to_' in file.name:
                    # Extract date parts from pattern: openaq_realtime_LAO_THA_from_2025-06-19_to_2025-06-20.parquet
                    parts = file.name.split('from_')[1]  # "2025-06-19_to_2025-06-20.parquet"
                    date_parts = parts.split('_to_')     # ["2025-06-19", "2025-06-20.parquet"]
                    start_date_str = date_parts[0]       # "2025-06-19"
                    end_date_str = date_parts[1].split('.')[0]  # "2025-06-20" (remove .parquet)
                    
                    file_start = datetime.strptime(start_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                    file_end = datetime.strptime(end_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                
                # Handle AirGradient historical pattern (YYYY-MM-DD_YYYY-MM-DD)
                elif '_historical_' in file.name:
                    dates = file.name.split('_')[-2:]
                    file_start = datetime.strptime(dates[0], '%Y-%m-%d').replace(tzinfo=timezone.utc)
                    file_end = datetime.strptime(dates[1].split('.')[0], '%Y-%m-%d').replace(tzinfo=timezone.utc)
                
                # Handle AirGradient realtime pattern (YYYYMMDD)
                elif '_realtime_' in file.name:
                    # New format: airgradient_realtime_LAO_THA_20250702.parquet
                    date_str = file.name.split('_')[-1].replace('.parquet', '')
                    file_start = datetime.strptime(date_str, '%Y%m%d').replace(tzinfo=timezone.utc)
                    file_end = file_start + timedelta(days=1)
                
                else:
                    logger.warning(f"Skipping {file.name} - doesn't match any known naming pattern")
                    continue
                
                # Check if file overlaps with target window
                if (file_start <= end_date and file_end >= start_date):
                    relevant_files.append(file)
                    logger.info(f"Including file {file.name} ({file_start} to {file_end})")
                else:
                    logger.debug(f"Skipping {file.name} - outside target window ({file_start} to {file_end})")
            
            except Exception as e:
                logger.warning(f"Error parsing date from filename {file.name}: {str(e)}")
                continue
        
        return relevant_files

    def _load_openaq_data(self) -> pd.DataFrame:
        """Load and combine OpenAQ data from CSV and Parquet files."""
        logger.info(f"Loading OpenAQ data from {self.openaq_dir}...")
        df_concat = pd.DataFrame()
        
        # Get date range for filtering
        start_date, end_date = self._get_date_range()
        
        # For historical mode, look in historical directory
        if self.mode == "historical":
            logger.info(f"Historical mode: Looking in {self.openaq_dir}")
            
            # List all location_*_year.parquet files
            files = list(self.openaq_dir.glob("location_*_*.parquet"))
            if not files:
                logger.warning(f"No historical files found in {self.openaq_dir}")
                return pd.DataFrame(columns=['location_id', 'sensors_id', 'latitude', 'longitude', 'datetime', 'value', 'parameter'])
            
            logger.info(f"Found {len(files)} historical OpenAQ files")
            
            for file in files:
                try:
                    logger.info(f"Processing OpenAQ file: {file.name}")
                    df = pd.read_parquet(file)
                    
                    # Filter for PM2.5 data
                    df = df[df["parameter"] == "pm25"]
                    
                    # Rename columns to match expected format
                    df = df.rename(columns={
                        'lat': 'latitude',
                        'lon': 'longitude',
                        'datetime': 'datetime_utc'
                    })
                    
                    # Filter for the requested date range
                    df['datetime_utc'] = pd.to_datetime(df['datetime_utc'])
                    mask = (df['datetime_utc'].dt.date >= start_date.date()) & (df['datetime_utc'].dt.date <= end_date.date())
                    df = df[mask]
                    
                    if len(df) > 0:
                        logger.info(f"Found {len(df)} records in date range")
                        df_concat = pd.concat([df_concat, df])
                    else:
                        logger.info("No records found in date range")
                    
                except Exception as e:
                    logger.error(f"Error processing file {file}: {str(e)}")
                    continue
        else:
            # Original realtime processing code
            files = list(self.openaq_dir.glob("*"))
            if not files:
                logger.warning(f"No files found in {self.openaq_dir}")
                return pd.DataFrame(columns=['location_id', 'sensors_id', 'latitude', 'longitude', 'datetime', 'value', 'parameter'])
            
            # Filter files by date range
            relevant_files = self._filter_files_by_date(files, start_date, end_date)
            if not relevant_files:
                logger.warning("No files found overlapping with the target time window")
                return pd.DataFrame(columns=['location_id', 'sensors_id', 'latitude', 'longitude', 'datetime', 'value', 'parameter'])
            
            logger.info(f"Found {len(relevant_files)} relevant OpenAQ files that overlap with our window")
            
            for file in relevant_files:
                try:
                    logger.info(f"Processing OpenAQ file: {file.name}")
                    if file.suffix == '.parquet':
                        df = pd.read_parquet(file)
                        df = df.rename(columns={
                            'sensor_id': 'sensors_id',
                            'datetime_from_utc': 'datetime',
                            'sensor_type': 'parameter'
                        })
                        df = df[df["parameter"] == "pm25"]
                    elif file.suffix == '.csv':
                        df = pd.read_csv(file)
                        df["datetime"] = pd.to_datetime(df["datetime"])
                        df = df[df["parameter"] == "pm25"]
                    else:
                        logger.warning(f"Skipping unsupported file format: {file}")
                        continue
                    
                    df_concat = pd.concat([df_concat, df])
                    
                except Exception as e:
                    logger.error(f"Error processing file {file}: {str(e)}")
                    continue
        
        if df_concat.empty:
            logger.warning("No valid data found in OpenAQ files")
            return pd.DataFrame(columns=['location_id', 'sensors_id', 'latitude', 'longitude', 'datetime', 'value', 'parameter'])
        
        # Add UTC datetime and date columns
        df_concat["datetime_utc"] = pd.to_datetime(df_concat["datetime_utc"] if "datetime_utc" in df_concat.columns else df_concat["datetime"], utc=True)
        df_concat["date_utc"] = df_concat["datetime_utc"].dt.date
        
        # Ensure consistent data types
        if 'sensors_id' in df_concat.columns:
            df_concat['sensors_id'] = df_concat['sensors_id'].astype(str)
        if 'location_id' in df_concat.columns:
            df_concat['location_id'] = df_concat['location_id'].astype(str)
        
        return df_concat

    def _load_airgradient_data(self) -> pd.DataFrame:
        """Load and combine AirGradient data from historical and NRT files."""
        logger.info(f"Loading AirGradient data from {self.airgradient_dir}...")
        
        # Initialize empty DataFrame with correct columns
        df_airgradient = pd.DataFrame(columns=[
            'location_id', 'latitude', 'longitude', 'value', 'date_utc', 'datetime_utc'
        ])
        
        try:
            # Load location mapping
            if self.airgradient_mapping_file.exists():
                df_mapping = pd.read_csv(self.airgradient_mapping_file)
                logger.info(f"Loaded location mapping with {len(df_mapping)} entries")
            else:
                logger.warning(f"Location mapping file not found: {self.airgradient_mapping_file}")
                df_mapping = pd.DataFrame(columns=['locationId', 'latitude', 'longitude'])
            
            # Get date range for filtering
            start_date, end_date = self._get_date_range()
            
            # Load data from the mode-specific directory
            if self.airgradient_dir.exists():
                files = list(self.airgradient_dir.glob("*.parquet"))
                logger.info(f"Found AirGradient files: {[f.name for f in files]}")
                
                # Filter files by date range
                relevant_files = self._filter_files_by_date(files, start_date, end_date)
                if not relevant_files:
                    logger.warning("No files found overlapping with the target time window")
                    return df_airgradient
                
                logger.info(f"Found {len(relevant_files)} relevant AirGradient files that overlap with our window")
                
                for file in relevant_files:
                    try:
                        logger.info(f"Processing AirGradient file: {file.name}")
                        df = pd.read_parquet(file)
                        logger.info(f"Records found: {len(df)}")
                        
                        # Merge with location mapping if needed
                        if "latitude" not in df.columns or "longitude" not in df.columns:
                            df = df.merge(df_mapping, on="locationId", how="left")
                        
                        df_airgradient = pd.concat([df_airgradient, df])
                    except Exception as e:
                        logger.error(f"Error processing file {file}: {str(e)}")
                        continue
            else:
                logger.warning(f"AirGradient directory not found: {self.airgradient_dir}")
            
            if df_airgradient.empty:
                logger.warning("No valid AirGradient data found")
                return pd.DataFrame(columns=[
                    'location_id', 'latitude', 'longitude', 'value', 'date_utc', 'datetime_utc'
                ])
            
            # Process AirGradient data
            df_airgradient["datetime_utc"] = pd.to_datetime(df_airgradient["timestamp"])
            df_airgradient["date_utc"] = df_airgradient["datetime_utc"].dt.date
            df_airgradient["value"] = df_airgradient["pm02"]
            df_airgradient["location_id"] = df_airgradient["locationId"].astype(str) + "_airgradient"
            
            # Select and rename columns
            df_airgradient = df_airgradient[[
                'location_id', 'latitude', 'longitude', 'value', 'date_utc', 'datetime_utc'
            ]]
            
        except Exception as e:
            logger.error(f"Error loading AirGradient data: {str(e)}")
            return pd.DataFrame(columns=[
                'location_id', 'latitude', 'longitude', 'value', 'date_utc', 'datetime_utc'
            ])
        
        return df_airgradient

    def _filter_by_date_range(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter data by date range based on mode."""
        if self.mode == "realtime":
            end_date = datetime.now(timezone.utc)
            start_date = end_date - timedelta(hours=self.hours)
            logger.info(f"Filtering data between {start_date} and {end_date}")
            mask = (df["datetime_utc"] >= start_date) & (df["datetime_utc"] <= end_date)
        else:
            start_date = pd.to_datetime(self.start_date).tz_localize('UTC')
            end_date = pd.to_datetime(self.end_date).tz_localize('UTC')
            logger.info(f"Filtering data between {start_date.date()} and {end_date.date()}")
            mask = (df["date_utc"] >= start_date.date()) & (df["date_utc"] <= end_date.date())
        
        filtered_df = df[mask]
        logger.info(f"Records after date filtering: {len(filtered_df)}")
        if not filtered_df.empty:
            logger.info("Sample of filtered data:")
            logger.info(f"Date range: {filtered_df['datetime_utc'].min()} to {filtered_df['datetime_utc'].max()}")
            logger.info(f"Unique locations: {filtered_df['location_id'].nunique()}")
            if 'source_file' in filtered_df.columns:
                logger.info("Records by source file:")
                logger.info(filtered_df['source_file'].value_counts())
        return filtered_df

    def _deduplicate_sensors(self, df: pd.DataFrame) -> pd.DataFrame:
        """Deduplicate sensors based on location and date."""
        logger.info("Deduplicating sensors...")
        logger.info(f"Before deduplication:")
        logger.info(f"Total records: {len(df)}")
        logger.info(f"Unique dates: {df['date_utc'].nunique()}")
        logger.info(f"Unique locations: {df['location_id'].nunique()}")
        logger.info(f"Unique lat/lon pairs: {df.groupby(['latitude', 'longitude']).ngroups}")
        
        # Show sample of records for a single location
        if not df.empty:
            sample_location = df['location_id'].iloc[0]
            sample_records = df[df['location_id'] == sample_location]
            logger.info(f"\nSample records for location {sample_location}:")
            logger.info(f"Number of records: {len(sample_records)}")
            logger.info(f"Unique dates: {sample_records['date_utc'].nunique()}")
            logger.info(f"Unique times: {sample_records['datetime_utc'].nunique()}")
            logger.info("First few records:")
            logger.info(sample_records[['datetime_utc', 'value', 'latitude', 'longitude']].head())
        
        # Group by location and date, keeping first occurrence
        df_dedup = df.drop_duplicates(
            subset=["date_utc", "latitude", "longitude"],
            keep="first"
        )
        
        logger.info(f"\nAfter deduplication:")
        logger.info(f"Total records: {len(df_dedup)}")
        logger.info(f"Unique dates: {df_dedup['date_utc'].nunique()}")
        logger.info(f"Unique locations: {df_dedup['location_id'].nunique()}")
        logger.info(f"Unique lat/lon pairs: {df_dedup.groupby(['latitude', 'longitude']).ngroups}")
        
        # Add source column
        df_dedup["location_id"] = df_dedup["location_id"].astype(str)
        df_dedup["source"] = "openaq"
        df_dedup.loc[
            df_dedup['location_id'].str.contains('_airgradient', na=False),
            "source"
        ] = "airgradient"
        
        return df_dedup

    def process(self) -> None:
        """Process and combine OpenAQ and AirGradient data."""
        logger.info(f"Starting air quality processing in {self.mode} mode")
        
        # Load data
        df_openaq = self._load_openaq_data()
        if not df_openaq.empty:
            df_openaq['source_file'] = 'openaq'
        df_airgradient = self._load_airgradient_data()
        if not df_airgradient.empty:
            df_airgradient['source_file'] = 'airgradient'
        
        # Combine datasets
        df_combined = pd.concat([df_openaq, df_airgradient], ignore_index=True)
        logger.info(f"Total combined records before filtering: {len(df_combined)}")
        
        # Filter by date range
        df_filtered = self._filter_by_date_range(df_combined)
        
        # Deduplicate sensors
        df_dedup = self._deduplicate_sensors(df_filtered)
        logger.info(f"Records after deduplication: {len(df_dedup)}")
        
        # Add H3 indexing using config-driven resolution
        h3_resolution = self.config_loader.get_h3_resolution()
        df_dedup["h3_08_text"] = df_dedup.apply(lambda row: h3.geo_to_h3(row.latitude, row.longitude, h3_resolution), axis=1)
        logger.info(f"Added H3 indexing at resolution {h3_resolution}")
        logger.info(f"Unique H3 cells: {df_dedup['h3_08_text'].nunique()}")
        
        # Apply thresholds to filter out invalid values
        initial_count = len(df_dedup)
        df_dedup = df_dedup[
            (df_dedup['value'] >= self.minimum_threshold) & 
            (df_dedup['value'] <= self.maximum_threshold)
        ]
        filtered_count = len(df_dedup)
        logger.info(f"Applied PM2.5 thresholds ({self.minimum_threshold}-{self.maximum_threshold}): "
                   f"{initial_count} → {filtered_count} records")
        
        # Ensure consistent column structure across all files
        expected_columns = [
            'location_id', 'sensors_id', 'datetime_from_local', 'datetime', 'latitude', 'longitude', 
            'parameter', 'sensor_units', 'value', 'provider_name', 'owner_name', 'country', 'name', 
            'sensor_grade', 'datetime_to_utc', 'datetime_to_local', 'datetime_utc', 'date_utc', 
            'source_file', 'source', 'h3_08_text'
        ]
        
        # Add missing columns with null values
        for col in expected_columns:
            if col not in df_dedup.columns:
                df_dedup[col] = None
                logger.info(f"Added missing column '{col}' with null values")
        
        # Reorder columns to ensure consistent structure
        df_dedup = df_dedup[expected_columns]
        
        # Create country codes string for filename
        countries_str = "_".join(sorted(self.countries))
        
        # Save output with country codes in filename
        if self.mode == "historical":
            output_file = self.output_dir / f"air_quality_{self.mode}_{countries_str}_{self.start_date}_to_{self.end_date}.parquet"
        else:
            output_file = self.output_dir / f"air_quality_{self.mode}_{countries_str}_{datetime.now().strftime('%Y%m%d')}.parquet"
        df_dedup.to_parquet(output_file)
        
        logger.info(f"Processing complete. Output saved to: {output_file}")
        
        # Log summary statistics
        logger.info(f"Total records: {len(df_dedup)}")
        logger.info(f"Unique locations: {df_dedup['location_id'].nunique()}")
        logger.info(f"Date range: {df_dedup['date_utc'].min()} to {df_dedup['date_utc'].max()}")
        logger.info(f"Source distribution:\n{df_dedup['source'].value_counts()}")

def main():
    """Main entry point for the script with configuration integration."""
    import argparse
    
    # Load configuration to get defaults
    try:
        config_loader = ConfigLoader()
        default_countries = config_loader.get_countries()
        processing_config = config_loader.get_processing_config('air_quality') or {}
        default_min_threshold = processing_config.get('minimum_threshold', 0.0)
        default_max_threshold = processing_config.get('maximum_threshold', 500.0)
    except Exception as e:
        logger.warning(f"Could not load configuration, using fallback defaults: {e}")
        default_countries = ["THA", "LAO"]
        default_min_threshold = 0.0
        default_max_threshold = 500.0
    
    parser = argparse.ArgumentParser(
        description="Process air quality data from OpenAQ and AirGradient",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process realtime data for last 24 hours
  python src/data_processors/process_air_quality.py --mode realtime --hours 24

  # Process historical data for specific date range
  python src/data_processors/process_air_quality.py --mode historical --start-date 2024-01-01 --end-date 2024-01-02

  # Process with custom countries and thresholds
  python src/data_processors/process_air_quality.py --countries THA LAO --min-threshold 5.0 --max-threshold 300.0

  # Use custom configuration file
  python src/data_processors/process_air_quality.py --config config/custom_config.yaml
        """
    )
    
    parser.add_argument("--mode", choices=["realtime", "historical"], default="realtime",
                      help="Processing mode (default: realtime)")
    parser.add_argument("--hours", type=int, default=24,
                      help="Hours to look back in realtime mode (default: 24)")
    parser.add_argument("--start-date", type=str,
                      help="Start date for historical mode (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str,
                      help="End date for historical mode (YYYY-MM-DD)")
    parser.add_argument("--countries", nargs="+", default=default_countries,
                      help=f"Country codes to process (default: {' '.join(default_countries)})")
    parser.add_argument("--min-threshold", type=float, default=default_min_threshold,
                      help=f"Minimum PM2.5 value threshold (default: {default_min_threshold})")
    parser.add_argument("--max-threshold", type=float, default=default_max_threshold,
                      help=f"Maximum PM2.5 value threshold (default: {default_max_threshold})")
    parser.add_argument("--config", type=str,
                      help="Path to configuration file (optional)")
    
    args = parser.parse_args()
    
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
    
    try:
        logger.info("="*60)
        logger.info("AIR QUALITY PROCESSOR STARTING")
        logger.info("="*60)
        logger.info(f"Mode: {args.mode}")
        logger.info(f"Countries: {args.countries}")
        logger.info(f"PM2.5 thresholds: {args.min_threshold} - {args.max_threshold}")
        
        if args.mode == "realtime":
            logger.info(f"Lookback hours: {args.hours}")
        else:
            logger.info(f"Date range: {args.start_date} to {args.end_date}")
        
        # Initialize processor
        processor = AirQualityProcessor(
            mode=args.mode,
            hours=args.hours,
            start_date=args.start_date,
            end_date=args.end_date,
            countries=args.countries,
            minimum_threshold=args.min_threshold,
            maximum_threshold=args.max_threshold,
            config_path=args.config
        )
        
        # Process data
        start_time = time.time()
        processor.process()
        end_time = time.time()
        
        logger.info("="*60)
        logger.info("AIR QUALITY PROCESSOR COMPLETED")
        logger.info(f"Total processing time: {end_time - start_time:.2f} seconds")
        logger.info("="*60)
        
    except Exception as e:
        logger.error("="*60)
        logger.error("AIR QUALITY PROCESSOR FAILED")
        logger.error(f"Error: {str(e)}")
        logger.error("="*60)
        import traceback
        logger.error(traceback.format_exc())
        exit(1)

if __name__ == "__main__":
    main() 