import logging
from typing import Dict, Any, List, Literal, Optional
import requests
import pandas as pd
from pandas import json_normalize
import geopandas as gpd
from shapely.geometry import Point
from datetime import datetime, timedelta
import time
from pathlib import Path
from openaq import OpenAQ
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from itertools import islice
from tqdm import tqdm
import pyarrow as pa
import pyarrow.parquet as pq
import awswrangler as wr
import os
import boto3
import yaml
from botocore import UNSIGNED
from botocore.config import Config
import gzip
import io
from dotenv import load_dotenv

from functools import wraps
from pandas import json_normalize


from src.data_collectors.base_collector import BaseCollector



class RateLimiter:
    """Thread-safe rate limiter for API requests."""
    
    def __init__(self, max_requests_per_minute: int = 30):
        self.max_requests = max_requests_per_minute
        self.requests = []
        self.lock = threading.Lock()
        
    def wait_if_needed(self):
        """Wait if necessary to respect rate limits."""
        with self.lock:
            now = time.time()
            # Remove requests older than 1 minute
            self.requests = [req_time for req_time in self.requests if now - req_time < 60]
            
            if len(self.requests) >= self.max_requests:
                # Calculate how long to wait
                oldest_request = min(self.requests)
                wait_time = 60 - (now - oldest_request) + 1  # Add 1 second buffer
                if wait_time > 0:
                    time.sleep(wait_time)
                    # Clean up old requests after waiting
                    now = time.time()
                    self.requests = [req_time for req_time in self.requests if now - req_time < 60]
            
            # Record this request
            self.requests.append(now)

def retry_on_rate_limit(max_retries: int = 3, base_delay: float = 60.0):
    """Decorator to retry requests on rate limit errors with exponential backoff."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except requests.HTTPError as e:
                    if e.response.status_code == 429 and attempt < max_retries:
                        # Exponential backoff: 60s, 120s, 240s
                        delay = base_delay * (2 ** attempt)
                        logging.warning(f"Rate limit hit, retrying in {delay}s (attempt {attempt + 1}/{max_retries + 1})")
                        time.sleep(delay)
                        continue
                    raise
                except Exception as e:
                    # Check if it's a rate limit error from OpenAQ SDK
                    if "429" in str(e) or "Too many requests" in str(e):
                        if attempt < max_retries:
                            delay = base_delay * (2 ** attempt)
                            logging.warning(f"Rate limit hit, retrying in {delay}s (attempt {attempt + 1}/{max_retries + 1})")
                            time.sleep(delay)
                            continue
                    raise
            return None
        return wrapper
    return decorator


class OpenAQCollector(BaseCollector):
    """Collector for OpenAQ air quality data."""
    
    def __init__(self, config_path: str = None):
        """Initialize the collector.
        
        Args:
            config_path: Path to configuration file (optional, uses default if None)
        """
        # Load environment variables from .env file if it exists
        env_path = Path('.env')
        if env_path.exists():
            load_dotenv(dotenv_path=env_path)
        
        # Initialize parent class with new config system
        super().__init__(config_path)
        
        # Get OpenAQ-specific configuration using new config loader
        self.openaq_config = self.config_loader.get_data_collection_config('openaq')

        self.rate_limiter = RateLimiter(max_requests_per_minute = self.openaq_config.get('rate_limit_requests_per_minute', 30))  # Conservative limit
    
        # Set OpenAQ-specific settings from config
        self.MEASUREMENTS_LIMIT = self.openaq_config.get('measurements_limit', 1000)
        self.BASE_URL = "https://api.openaq.org/v3"
        self.RATE_LIMIT = self.openaq_config.get('rate_limit_requests_per_minute', 30)
        # Reduce concurrency to avoid overwhelming the API
        self.MAX_WORKERS = self.openaq_config.get('max_workers', 3)
        self.BATCH_SIZE = self.openaq_config.get('batch_size', 10)   # Reduce batch size
        
        self._last_request_time = 0
        
        # Get API key from environment variable (like Himawari credentials)
        api_key = os.environ.get('OPENAQ_API_KEY')
        if not api_key:
            raise ValueError("OpenAQ API key not found. Please set OPENAQ_API_KEY environment variable.")
        
        # Ensure output directories exist using new config system
        hist_path = self.config_loader.get_path('raw.openaq.historical', create_if_missing=True)
        realtime_path = self.config_loader.get_path('raw.openaq.realtime', create_if_missing=True)
        
        # For backwards compatibility, update old-style config paths
        self.config['paths']['raw_data_openaq_historical'] = hist_path
        self.config['paths']['raw_data_openaq_realtime'] = realtime_path

    def get_country_codes(self) -> List[int]:
        """Get OpenAQ country codes from configuration."""
        return self.config_loader.get_countries('openaq')

    def get_indicators(self) -> List[str]:
        """Get indicators to filter for from configuration."""
        return self.openaq_config.get('indicators', ['pm25'])
        
    def get_api_key_from_config(self) -> str:
        """Get OpenAQ API key from environment variable."""
        return os.environ.get('OPENAQ_API_KEY')

    def _rate_limit_wait(self):
        """Wait to respect rate limits."""
        current_time = time.time()
        time_since_last = current_time - self._last_request_time
        if time_since_last < 60 / self.RATE_LIMIT:
            time.sleep((60 / self.RATE_LIMIT) - time_since_last)
        self._last_request_time = time.time()

    @retry_on_rate_limit(max_retries=3, base_delay=60.0)
    def _make_request(self, endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Make a request to the OpenAQ API."""
        # Apply rate limiting before making request
        self.rate_limiter.wait_if_needed()
        
        url = f"{self.BASE_URL}/{endpoint}"
        
        # Add API key to headers
        headers = {
            "X-API-Key": self.get_api_key_from_config()
        }
        
        response = requests.get(url, params=params, headers=headers)
        
        # Let the decorator handle 429 errors
        response.raise_for_status()
        return response.json()

    @retry_on_rate_limit(max_retries=3, base_delay=60.0)
    def _get_sensor_data(self, sensor_id: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Get data for a single sensor using OpenAQ SDK."""
        # Apply rate limiting before making request
        self.rate_limiter.wait_if_needed()
        
        try:
            # Use OpenAQ SDK client for measurements
            client = OpenAQ(api_key=self.get_api_key_from_config())
            
            response = client.measurements.list(
                sensors_id=sensor_id,
                datetime_from=start_date,
                datetime_to=end_date,
                limit=self.MEASUREMENTS_LIMIT
            )
            
            # Convert response to dictionary
            data_measurements = response.dict()
            
            if not data_measurements.get('results'):
                self.logger.warning(f"No data found for sensor {sensor_id}")
                return pd.DataFrame()
            
            # Process the results similar to openaq_realtime_client
           
            df_hourly_data = json_normalize(data_measurements['results'])
            
            if len(df_hourly_data) == 0:
                self.logger.warning(f"No data found for sensor {sensor_id}")
                return pd.DataFrame()
            
            # Rename columns to match expected format
            df_hourly_data = df_hourly_data.rename(columns={
                'period.datetime_from.local': 'datetime_from_local',
                'period.datetime_from.utc': 'datetime_from_utc',
                'period.datetime_to.local': 'datetime_to_local',
                'period.datetime_to.utc': 'datetime_to_utc',
                'parameter.name': 'sensor_type',
                'parameter.units': 'sensor_units'
            })
            
            # Add sensor_id column
            df_hourly_data['sensor_id'] = sensor_id
            
            # Keep relevant columns (ensuring they exist)
            desired_columns = ["value", "datetime_from_utc", "datetime_from_local", 
                            "datetime_to_utc", "datetime_to_local", "sensor_type", 
                            "sensor_units", "sensor_id"]
            existing_columns = [col for col in desired_columns if col in df_hourly_data.columns]
            df_hourly_data = df_hourly_data[existing_columns]
            
            self.logger.debug(f"Successfully collected {len(df_hourly_data)} records for sensor {sensor_id}")
            return df_hourly_data
        
        except Exception as e:
            self.logger.error(f"Error getting data for sensor {sensor_id}: {str(e)}")
            return pd.DataFrame()

    def _get_location_ids_for_countries(self, country_codes: List[int]) -> List[int]:
        """Get all location IDs for specified countries.
        
        Args:
            country_codes: List of country codes (e.g., [111, 68] for Thailand and Laos)
            
        Returns:
            List of location IDs
        """
        all_locations = []
        page = 1
        
        while True:
            try:
                # Construct countries_id parameters
                params = {
                    "limit": 1000,
                    "page": page,
                    "sort_order": "asc"
                }
                # Add country codes to params
                for code in country_codes:
                    params[f"countries_id"] = code
                    
                response = self._make_request("locations", params)
                
                if not response.get('results'):
                    break
                    
                locations = response['results']
                all_locations.extend([loc['id'] for loc in locations])
                
                # Check if we've received less than the limit (meaning no more pages)
                if len(locations) < 1000:
                    break
                    
                page += 1
                
            except Exception as e:
                self.logger.error(f"Error fetching locations for page {page}: {str(e)}")
                break
        
        self.logger.info(f"Found {len(all_locations)} locations for country codes {country_codes}")
        return all_locations

    def collect_historical_data(self, start_date: str, end_date: str, output_dir: str = "data/raw/openaq/historical") -> pd.DataFrame:
        """Collect historical data from OpenAQ AWS S3 buckets.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            output_dir: Directory to save individual location files
            
        Returns:
            Combined DataFrame with all historical data
        """
        collection_start_time = time.time()
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Get location IDs using OpenAQ Python package
        self.logger.info("Getting location IDs using OpenAQ Python package...")
        print("\nStep 1: Fetching location data...")
        gdf_sensors = self._get_sensors_country()
        
        if gdf_sensors.empty:
            self.logger.error("No locations found")
            print("Error: No location data found")
            return pd.DataFrame()
        
        location_ids = gdf_sensors['location_id'].unique().tolist()
        print(f"Found {len(location_ids)} unique locations")
        self.logger.info(f"Found {len(location_ids)} unique locations")
        
        # Parse dates
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        years = list(range(start.year, end.year + 1))
        print(f"Data will be collected for years: {years}")
        
        # Statistics for reporting
        successful = 0
        failed = 0
        no_data = 0
        total_locations = len(location_ids)
        
        print(f"\nStep 2: Collecting historical data from {total_locations} locations...")
        print(f"Time period: {start_date} to {end_date}")
        
        # Configure AWS client for anonymous access to openaq-data-archive
        print("Configuring AWS for anonymous S3 access...")
        s3_client = boto3.client(
            's3', 
            config=Config(signature_version=UNSIGNED),
            region_name='us-east-1'
        )
        
        # Set up progress tracking
        progress = tqdm(total=total_locations * len(years), desc="Downloading data")
        all_data = []
        
        for location_id in location_ids:
            for year in years:
                # Skip future years
                current_year = datetime.now().year
                if year > current_year:
                    progress.update(1)
                    continue
                
                # Parse S3 path for openaq-data-archive
                bucket_name = "openaq-data-archive"
                s3_key = f"records/csv.gz/locationid={location_id}/year={year}"
                
                try:
                    # List objects to find CSV files
                    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_key)
                    
                    if 'Contents' not in response:
                        self.logger.info(f"No data found for location {location_id} year {year}")
                        no_data += 1
                        progress.update(1)
                        continue
                    
                    # Find CSV.gz files
                    csv_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.csv.gz')]
                    
                    if not csv_files:
                        self.logger.info(f"No CSV files found for location {location_id} year {year}")
                        no_data += 1
                        progress.update(1)
                        continue
                    
                    # Combine all CSV files for this location/year
                    all_dataframes = []
                    
                    for csv_file in csv_files:
                        # Download the file
                        obj = s3_client.get_object(Bucket=bucket_name, Key=csv_file)
                        
                        # Read compressed CSV
                        with gzip.GzipFile(fileobj=io.BytesIO(obj['Body'].read())) as gz:
                            df = pd.read_csv(gz)
                            
                            # Filter by date if this is a boundary year
                            if year == start.year or year == end.year:
                                if 'utc' in df.columns:
                                    date_col = 'utc'
                                elif 'local' in df.columns:
                                    date_col = 'local'
                                else:
                                    # Try to find a date column
                                    date_cols = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
                                    if date_cols:
                                        date_col = date_cols[0]
                                    else:
                                        # No date column found, skip filtering
                                        date_col = None
                                
                                if date_col:
                                    df[date_col] = pd.to_datetime(df[date_col])
                                    if year == start.year:
                                        df = df[df[date_col] >= start]
                                    if year == end.year:
                                        df = df[df[date_col] <= end]
                            
                            all_dataframes.append(df)
                    
                    # Combine all dataframes
                    if all_dataframes:
                        combined_df = pd.concat(all_dataframes, ignore_index=True)
                        
                        # Save location data
                        output_file = os.path.join(output_dir, f"location_{location_id}_{year}.csv")
                        combined_df.to_csv(output_file, index=False)
                        
                        # Add to all data
                        all_data.append(combined_df)
                        
                        successful += 1
                        self.logger.info(f"Successfully downloaded {len(combined_df)} records for location {location_id} year {year}")
                    else:
                        no_data += 1
                        self.logger.info(f"No data found for location {location_id} year {year}")
                        
                except Exception as e:
                    self.logger.error(f"Error downloading data for location {location_id} year {year}: {str(e)}")
                    failed += 1
                
                # Update progress
                progress.update(1)
        
        progress.close()
        
        # Combine all data if any successful downloads
        final_df = pd.DataFrame()
        if all_data:
            self.logger.info(f"Combining data from {len(all_data)} successful downloads...")
            final_df = pd.concat(all_data, ignore_index=True)
            
            # Save combined data
            combined_file = os.path.join(output_dir, "combined_historical_data.csv")
            final_df.to_csv(combined_file, index=False)
            self.logger.info(f"Combined data saved to {combined_file}")
        
        # Print summary
        elapsed_time = time.time() - collection_start_time
        time_str = self._format_time(elapsed_time)
        
        print("\n" + "="*50)
        print(f"Collection Summary:")
        print(f"Total locations processed: {total_locations}")
        print(f"Years processed: {years}")
        print(f"Successful downloads: {successful}")
        print(f"Locations with no data: {no_data}")
        print(f"Failed downloads: {failed}")
        print(f"Time taken: {time_str}")
        print("="*50)
        
        return final_df

    def collect_realtime_data(self, sensor_ids: List[str], start_date: str, end_date: str) -> pd.DataFrame:
        """Collect real-time data for multiple sensors.
        
        Args:
            sensor_ids: List of sensor IDs
            start_date: Start date in ISO format
            end_date: End date in ISO format
            
        Returns:
            DataFrame with combined sensor data
        """
        all_data = []
        
        # Process sensors in batches with concurrent workers
        for i in range(0, len(sensor_ids), self.BATCH_SIZE):
            batch = sensor_ids[i:i + self.BATCH_SIZE]
            
            with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
                future_to_sensor = {
                    executor.submit(self._get_sensor_data, sensor_id, start_date, end_date): sensor_id 
                    for sensor_id in batch
                }
                
                for future in tqdm(
                    as_completed(future_to_sensor),
                    total=len(batch),
                    desc=f"Processing batch {i//self.BATCH_SIZE + 1}"
                ):
                    sensor_id = future_to_sensor[future]
                    try:
                        df = future.result()
                        if not df.empty:
                            all_data.append(df)
                    except Exception as e:
                        self.logger.error(f"Error processing sensor {sensor_id}: {str(e)}")
        
        if not all_data:
            return pd.DataFrame()
            
        return pd.concat(all_data, ignore_index=True)

    def collect(
        self,
        start_date: str,
        end_date: str,
        filename: str,
        mode: str = "historical"
    ) -> None:
        """Main method to collect, validate and save data.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            filename: Name of the output file
            mode: Collection mode ("historical" or "realtime")
        """
        print("\nStep 1: Initializing data collection...")
        print(f"Mode: {mode}")
        print(f"Time period: {start_date} to {end_date}")
        
        collection_start_time = time.time()
        
        print("\nStep 2: Collecting data...")
        if mode == "historical":
            # Use S3-based historical data collection
            output_dir = "data/raw/openaq/historical"
            data = self.collect_historical_data(start_date, end_date, output_dir)
        else:
            # Use API-based realtime data collection
            data = self._get_sensors_data(start_date, end_date)
        
        if isinstance(data, pd.DataFrame):
            print("\nStep 3: Validating collected data...")
            if self.validate_data(data):
                print("\nStep 4: Saving data...")
                self.save_data(data, filename, mode=mode)
                total_time = time.time() - collection_start_time
                print(f"\nData collection completed successfully!")
                print(f"Total process time: {self._format_time(total_time)}")
            else:
                print("\nError: Data validation failed!")

    def _format_time(self, seconds: float) -> str:
        """Format time in seconds to a human-readable string."""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        seconds = int(seconds % 60)
        if hours > 0:
            return f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        else:
            return f"{seconds}s"

    def _load_master_sensor_list(self) -> pd.DataFrame:
        """Load the master sensor list from CSV, create if doesn't exist."""
        if self.SENSOR_LIST_PATH.exists():
            df = pd.read_csv(self.SENSOR_LIST_PATH)
            self.logger.info(f"Loaded {len(df)} sensors from master list")
            return df
        else:
            self.logger.warning(f"Master sensor list not found at {self.SENSOR_LIST_PATH}, creating empty list")
            # Create the directory if it doesn't exist
            self.SENSOR_LIST_PATH.parent.mkdir(parents=True, exist_ok=True)
            # Create empty DataFrame with required columns
            df = pd.DataFrame(columns=[
                'location_id', 'sensor_id', 'name', 'locality', 'country',
                'latitude', 'longitude', 'sensor_type', 'sensor_grade',
                'provider_name', 'owner_name'
            ])
            df.to_csv(self.SENSOR_LIST_PATH, index=False)
            return df

    def _save_master_sensor_list(self):
        """Save the master sensor list to CSV."""
        if self.master_sensor_list is not None:
            # Ensure the directory exists
            self.SENSOR_LIST_PATH.parent.mkdir(parents=True, exist_ok=True)
            self.master_sensor_list.to_csv(self.SENSOR_LIST_PATH, index=False)
            self.logger.info(f"Saved {len(self.master_sensor_list)} sensors to master list at {self.SENSOR_LIST_PATH}")

    def _get_sensors_country(self) -> pd.DataFrame:
        """Get list of sensors for specified countries."""
        print("\nFetching sensors from OpenAQ API...")
        country_codes = self.get_country_codes()
        limit = 1000  # Maximum number of sensors to retrieve
        indicators = self.get_indicators()
        
        print(f"Country codes: {country_codes}")
        print(f"Filter indicators: {indicators if indicators else 'No filters - all sensor types will be included'}")
        print(f"Request limit: {limit} sensors per request")

        # Initialize OpenAQ client with API key
        client = OpenAQ(api_key=self.get_api_key_from_config())
        
        start_time = time.time()
        print("\nMaking API request to fetch locations...")

        # Get the locations in specified countries
        self.logger.info(f"Fetching locations for country codes: {country_codes}")
        try:
            # Get locations data
            locations = client.locations.list(
                countries_id=country_codes,
                limit=limit
            )
            
            # Convert response to dictionary and normalize
            data_locations = locations.dict()
            
            api_time = time.time() - start_time
            print(f"API request completed in {self._format_time(api_time)}")
            
            # Debug logging
            self.logger.info(f"API Response keys: {list(data_locations.keys())}")
            self.logger.info(f"Number of results: {len(data_locations.get('results', []))}")
            print(f"Received {len(data_locations.get('results', []))} locations from API")
            
            # Check if we have results
            if 'results' not in data_locations or not data_locations['results']:
                self.logger.error("No results found in the API response")
                print("Error: No location data found in API response")
                return pd.DataFrame()
                
            print("\nProcessing location data...")
            
            # Process the results into a dataframe
            df_sensors_country = pd.json_normalize(data_locations['results'])
            self.logger.info(f"DataFrame created with {len(df_sensors_country)} rows")
            self.logger.info(f"DataFrame columns: {df_sensors_country.columns.tolist()}")

            # Rename columns 
            df_sensors_country.rename(columns={
                'coordinates.latitude': 'latitude',
                'coordinates.longitude': 'longitude',
                'country.name': 'country',
                'provider.name': 'provider_name',
                'owner.name': 'owner_name'
            }, inplace=True)

            # Extract sensor grade from instruments 
            df_sensors_country['sensor_grade'] = df_sensors_country['instruments'].apply(
                lambda x: x[0]['name'] if isinstance(x, list) and len(x) > 0 and isinstance(x[0], dict) else None
            )

            # Create geometry column
            df_sensors_country['geometry'] = gpd.points_from_xy(
                df_sensors_country['longitude'],
                df_sensors_country['latitude']
            )

            # Transform to geodataframe
            gdf_sensors_country = gpd.GeoDataFrame(df_sensors_country, geometry='geometry')
            self.logger.info(f"Created GeoDataFrame with {len(gdf_sensors_country)} rows")
            print(f"Created GeoDataFrame with {len(gdf_sensors_country)} locations")

            # Check if 'sensors' column exists
            if 'sensors' not in gdf_sensors_country.columns:
                self.logger.error("No 'sensors' column in the response data")
                self.logger.info(f"Available columns: {gdf_sensors_country.columns.tolist()}")
                print("Error: No sensor data found in API response")
                return pd.DataFrame()

            # Debug: Check what sensor data looks like
            if not gdf_sensors_country.empty and len(gdf_sensors_country['sensors'].iloc[0]) > 0:
                self.logger.info(f"Sample sensor data: {gdf_sensors_country['sensors'].iloc[0][0]}")

            # Explode sensors and create new columns
            print("Extracting individual sensors from locations...")
            gdf_sensors_exploded = gdf_sensors_country.explode('sensors')
            self.logger.info(f"Exploded sensors DataFrame has {len(gdf_sensors_exploded)} rows")
            print(f"Found {len(gdf_sensors_exploded)} individual sensors across all locations")
            
            print("Processing sensor metadata...")
            gdf_sensors_exploded['sensor_type'] = gdf_sensors_exploded['sensors'].apply(lambda x: x['name'] if isinstance(x, dict) and 'name' in x else None)
            gdf_sensors_exploded['sensor_id'] = gdf_sensors_exploded['sensors'].apply(lambda x: x['id'] if isinstance(x, dict) and 'id' in x else None)

            # Clean up
            gdf_sensors_exploded.drop(["sensors", "bounds"], axis=1, inplace=True)
            gdf_sensors_exploded.rename(columns={'id': 'location_id'}, inplace=True)

            # Check what sensor types we have
            sensor_types = gdf_sensors_exploded['sensor_type'].unique().tolist()
            self.logger.info(f"Sensor types found: {sensor_types}")
            print(f"Sensor types found: {', '.join(sensor_types)}")
            
            # Keep only PM sensors with more flexible matching
            if indicators:
                print(f"Filtering for sensors matching indicators: {indicators}")
                # Use a more flexible match for PM2.5 or other indicators
                mask = gdf_sensors_exploded['sensor_type'].str.contains('|'.join([i.lower() for i in indicators]), case=False)
                gdf_sensors_exploded_pm_only = gdf_sensors_exploded[mask]
                self.logger.info(f"Found {len(gdf_sensors_exploded_pm_only)} sensors after filtering for {indicators}")
                print(f"After filtering: {len(gdf_sensors_exploded_pm_only)} sensors match the indicators")
                
                total_time = time.time() - start_time
                print(f"Sensor processing completed in {self._format_time(total_time)}")
                return gdf_sensors_exploded_pm_only
            
            total_time = time.time() - start_time
            print(f"Sensor processing completed in {self._format_time(total_time)}")
            return gdf_sensors_exploded
            
        except Exception as e:
            self.logger.error(f"Error fetching locations: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            print(f"Error fetching sensor data: {str(e)}")
            return pd.DataFrame()

    def _process_sensor_batch(self, sensor_batch: List[str], start_date: str, end_date: str) -> List[pd.DataFrame]:
        """Process sensors with better error handling."""
        results = []
        
        # Since we're using MAX_WORKERS=1, this will be sequential anyway
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            future_to_sensor = {
                executor.submit(self._get_sensor_data, sensor_id, start_date, end_date): sensor_id 
                for sensor_id in sensor_batch
            }
            
            for future in as_completed(future_to_sensor):
                sensor_id = future_to_sensor[future]
                try:
                    df = future.result()
                    if not df.empty:
                        results.append(df)
                        self.logger.info(f"Successfully processed sensor {sensor_id}: {len(df)} records")
                except Exception as e:
                    self.logger.error(f"Error processing sensor {sensor_id}: {str(e)}")
        
        return results

    def _get_sensors_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Get data for all sensors in batches using concurrent processing."""
        print("\nStep 1: Fetching list of sensors...")
        gdf_sensors = self._get_sensors_country()
        sensor_list = gdf_sensors['sensor_id'].tolist()
        print(f"Found {len(sensor_list)} sensors to process")

        print("\nStep 2: Fetching data for sensors in batches...")
        print(f"Time period: {start_date} to {end_date}")
        print(f"Processing in batches of {self.BATCH_SIZE} sensors with {self.MAX_WORKERS} concurrent workers")
        
        all_data = []
        completed = 0
        total = len(sensor_list)
        start_time = time.time()
        batch_times = []  # Keep track of batch processing times
        
        # Process sensors in batches
        for i in range(0, len(sensor_list), self.BATCH_SIZE):
            batch_start_time = time.time()
            batch = sensor_list[i:i + self.BATCH_SIZE]
            current_batch = (i//self.BATCH_SIZE) + 1
            total_batches = (total + self.BATCH_SIZE - 1)//self.BATCH_SIZE
            
            print(f"\nProcessing batch {current_batch}/{total_batches}...")
            
            batch_results = self._process_sensor_batch(batch, start_date, end_date)
            all_data.extend(batch_results)
            
            completed += len(batch)
            current_time = time.time()
            elapsed_time = current_time - start_time
            batch_time = current_time - batch_start_time
            batch_times.append(batch_time)
            
            # Calculate average batch time from the last 3 batches for more accurate ETA
            recent_avg_batch_time = sum(batch_times[-3:]) / len(batch_times[-3:]) if batch_times else batch_time
            remaining_batches = total_batches - current_batch
            eta = recent_avg_batch_time * remaining_batches
            
            print(f"Progress: {completed}/{total} sensors processed ({(completed/total)*100:.1f}%)")
            print(f"Elapsed time: {self._format_time(elapsed_time)}")
            print(f"Estimated time remaining: {self._format_time(eta)}")
            print(f"Batch processing time: {self._format_time(batch_time)}")
        
        total_time = time.time() - start_time
        
        if not all_data:
            print("\nNo data collected from any sensor")
            print(f"Total time elapsed: {self._format_time(total_time)}")
            return pd.DataFrame()
        
        print(f"\nStep 3: Combining data from {len(all_data)} sensors...")    
        result = pd.concat(all_data, ignore_index=True)
        print(f"Total measurements collected: {len(result)}")
        print(f"Total time elapsed: {self._format_time(total_time)}")
        
        return result

    def validate_data(self, data: pd.DataFrame) -> bool:
        """Validate the collected data."""
        if data.empty:
            self.logger.error("No data collected")
            return False

        # Check if we have the required columns
        required_columns = ['sensor_id', 'value']
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            self.logger.error(f"Missing required columns: {missing_columns}")
            return False

        # Quality check: sensors with no data
        nb_sensors = len(data['sensor_id'].unique())
        measures_by_sensor = data.groupby('sensor_id').count()
        zero_sensors = measures_by_sensor[measures_by_sensor['value'] == 0]
        zero_sensor_ratio = len(zero_sensors) / nb_sensors if nb_sensors > 0 else 0

        if zero_sensor_ratio > 0.10:
            self.logger.warning(f"More than 10% of sensors ({zero_sensor_ratio:.1%}) have no data")
            # Don't fail validation, just warn
            
        self.logger.info(f"Validation passed: {len(data)} measurements from {nb_sensors} sensors")
        return True

    def save_data(self, data: pd.DataFrame, filename: str, mode: str = "historical") -> None:
        """Save the collected data."""
        if data.empty:
            self.logger.warning("No data to save")
            return

        # Determine the save path based on mode
        if mode == "historical":
            save_path = self.config['paths']['raw_data_openaq_historical'] / filename
        else:
            save_path = self.config['paths']['raw_data_openaq_realtime'] / filename

        # Ensure the directory exists
        save_path.parent.mkdir(parents=True, exist_ok=True)

        # Save to parquet format
        data.to_parquet(save_path, index=False)
        self.logger.info(f"Saved {len(data)} records to {save_path}")

    def fetch_data(self, start_date: str, end_date: str, mode: str = "historical") -> List[Dict[str, Any]]:
        """Fetch data from OpenAQ API."""
        df = self._get_sensors_data(start_date, end_date)
        
        # Convert DataFrame to list of dictionaries
        if df.empty:
            return []
            
        # Convert datetime columns to string for JSON serialization
        for col in df.columns:
            if 'datetime' in col:
                df[col] = df[col].astype(str)
                
        return df.to_dict('records') 