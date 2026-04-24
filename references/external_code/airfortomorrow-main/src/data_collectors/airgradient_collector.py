import logging
from typing import Dict, Any, List, Literal, Tuple
import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from datetime import datetime, timedelta
import time
from pathlib import Path
import shutil

from src.data_collectors.base_collector import BaseCollector

class AirGradientCollector(BaseCollector):
    """Unified collector for both historical and real-time data from AirGradient API."""
    
    SEED_SENSOR_LIST_PATH = Path("assets/sensor_lists/airgradient_sensor_list.csv")
    RUNTIME_SENSOR_LIST_PATH = Path("data/raw/airgradient/sensor_cache/airgradient_sensor_list.csv")
    
    def __init__(self, config_path: str = None):
        """Initialize the collector.
        
        Args:
            config_path: Path to configuration file (optional, uses default if None)
        """
        # Initialize parent class with new config system
        super().__init__(config_path)
        
        # Get AirGradient-specific configuration using new config loader
        self.airgradient_config = self.config_loader.get_data_collection_config('airgradient')

        # Keep a mutable runtime sensor cache separate from the immutable bootstrap seed file.
        self.sensor_list_path = self._resolve_sensor_list_path()
        
        # Load master sensor list
        self.master_sensor_list = self._load_master_sensor_list()

    def _resolve_sensor_list_path(self) -> Path:
        """Resolve runtime sensor list path and seed it from bootstrap assets when needed."""
        runtime_path = self.RUNTIME_SENSOR_LIST_PATH
        runtime_path.parent.mkdir(parents=True, exist_ok=True)

        if runtime_path.exists():
            return runtime_path

        if self.SEED_SENSOR_LIST_PATH.exists():
            shutil.copy2(self.SEED_SENSOR_LIST_PATH, runtime_path)
            self.logger.info(
                f"Seeded AirGradient runtime sensor cache from {self.SEED_SENSOR_LIST_PATH} to {runtime_path}"
            )
        else:
            self.logger.warning(
                f"Bootstrap sensor list not found at {self.SEED_SENSOR_LIST_PATH}; creating runtime cache from scratch"
            )

        return runtime_path

    def get_country_codes(self) -> List[str]:
        """Get AirGradient country codes from configuration."""
        return self.config_loader.get_countries('airgradient')

    def get_buffer_degrees(self) -> float:
        """Get geographic buffer in degrees from configuration."""
        return self.config_loader.get_buffer_degrees()
        
    def get_base_url(self) -> str:
        """Get AirGradient API base URL from configuration."""
        return self.airgradient_config.get('base_url', 'https://api.airgradient.com/public/api/v1')
        
    def get_endpoints(self) -> Dict[str, str]:
        """Get AirGradient API endpoints from configuration."""
        return self.airgradient_config.get('endpoints', {
            'current': '/world/locations/measures/current',
            'historical': '/unicef/locations/{location_id}/measures/past'
        })
        
    def get_validation_rules(self) -> Dict[str, Any]:
        """Get validation rules from configuration."""
        return self.airgradient_config.get('validation', {
            'pm25_min': 0,
            'pm25_max': 1000
        })
        
    def get_rate_limiting_config(self) -> Dict[str, Any]:
        """Get rate limiting configuration."""
        return self.airgradient_config.get('rate_limiting', {
            'requests_per_minute': 30,
            'delay_between_requests': 2.0,
            'batch_size': 10,
            'max_retries': 3,
            'backoff_factor': 2.0
        })

    def _load_master_sensor_list(self) -> pd.DataFrame:
        """Load the master sensor list from CSV, create if doesn't exist."""
        if self.sensor_list_path.exists():
            df = pd.read_csv(self.sensor_list_path)
            self.logger.info(f"Loaded {len(df)} sensors from master list")
            return df
        else:
            self.logger.warning(f"Master sensor list not found at {self.sensor_list_path}, creating empty list")
            # Create the directory if it doesn't exist
            self.sensor_list_path.parent.mkdir(parents=True, exist_ok=True)
            # Create empty DataFrame
            df = pd.DataFrame(columns=['locationId', 'latitude', 'longitude'])
            df.to_csv(self.sensor_list_path, index=False)
            return df

    def _save_master_sensor_list(self):
        """Save the master sensor list to CSV."""
        if self.master_sensor_list is not None:
            # Ensure the directory exists
            self.sensor_list_path.parent.mkdir(parents=True, exist_ok=True)
            self.master_sensor_list.to_csv(self.sensor_list_path, index=False)
            self.logger.info(f"Saved {len(self.master_sensor_list)} sensors to master list at {self.sensor_list_path}")

    def _update_master_sensor_list(self, new_sensors: pd.DataFrame):
        """Update master sensor list with new sensors."""
        if new_sensors.empty:
            return

        # Ensure locationId is integer in both dataframes
        self.master_sensor_list['locationId'] = self.master_sensor_list['locationId'].astype(int)
        new_sensors['locationId'] = new_sensors['locationId'].astype(int)

        # Find new sensors not in master list
        existing_ids = set(self.master_sensor_list['locationId'])
        new_sensors_df = new_sensors[~new_sensors['locationId'].isin(existing_ids)]

        if not new_sensors_df.empty:
            self.logger.info(f"Found {len(new_sensors_df)} new sensors to add to master list")
            self.master_sensor_list = pd.concat([self.master_sensor_list, new_sensors_df], ignore_index=True)
            self._save_master_sensor_list()

    def _get_country_boundaries(self, country_code_list: List[str] = None) -> gpd.GeoDataFrame:
        """
        Get country boundaries for specified countries.
        
        Args:
            country_code_list: List of ISO3 country codes. If None, uses config defaults.
            
        Returns:
            GeoDataFrame with country boundaries
        """
        if country_code_list is None:
            country_code_list = self.get_country_codes()
            
        buffer_degrees = self.get_buffer_degrees()
        
        # Use the centralized boundary utility instead of broken CGAZ URL
        from src.utils.boundary_utils import create_country_boundaries
        
        self.logger.info(f"Downloading country boundaries for: {country_code_list}")
        country_boundaries = create_country_boundaries(country_code_list, buffer_degrees)
        
        self.logger.info(f"Retrieved boundaries for {len(country_code_list)} countries with {buffer_degrees}° buffer")
        return country_boundaries

    def _get_sensor_locations(self, boundaries_country: gpd.GeoDataFrame) -> pd.DataFrame:
        """Get list of sensor locations within country boundaries."""
        try:
            # Construct the current sensors URL
            base_url = self.get_base_url()
            endpoints = self.get_endpoints()
            current_url = base_url + endpoints['current']
            
            # First get current online sensors
            response = requests.get(current_url)
            response.raise_for_status()
            self.logger.info(f"Successfully fetched sensor data from {current_url}")
            
            data_all_sensors = response.json()
            if not data_all_sensors:
                self.logger.error("Received empty response from API")
                return pd.DataFrame()
                
            df_all_sensors = pd.DataFrame(data_all_sensors)
            
            # Ensure locationId is integer
            df_all_sensors['locationId'] = pd.to_numeric(df_all_sensors['locationId'], downcast='integer')
            
            self.logger.info(f"Found {len(df_all_sensors)} total online sensors")
            
            # Update master list with ALL sensors, regardless of boundaries
            sensors_for_master = df_all_sensors[['locationId', 'latitude', 'longitude']].copy()
            self._update_master_sensor_list(sensors_for_master)
            
            # Create geometry column for spatial filtering
            df_all_sensors['geometry'] = df_all_sensors.apply(
                lambda x: Point(x['longitude'], x['latitude']), axis=1
            )
            
            # Convert to GeoDataFrame
            df_all_sensors = gpd.GeoDataFrame(df_all_sensors, geometry='geometry')
            
            # Filter sensors within boundaries
            df_sensors_within = gpd.sjoin(
                df_all_sensors, boundaries_country, how="inner", predicate='within'
            )
            
            self.logger.info(f"Found {len(df_sensors_within)} online sensors within specified boundaries")
            
            # Now get all known sensors for this region from master list
            master_sensors_gdf = gpd.GeoDataFrame(
                self.master_sensor_list,
                geometry=[Point(x, y) for x, y in zip(self.master_sensor_list['longitude'], self.master_sensor_list['latitude'])]
            )
            
            master_sensors_within = gpd.sjoin(
                master_sensors_gdf, boundaries_country, how="inner", predicate='within'
            )
            
            result_df = master_sensors_within[['locationId', 'latitude', 'longitude']].copy()
            result_df['locationId'] = result_df['locationId'].astype(int)
            
            self.logger.info(f"Found {len(result_df)} total known sensors within specified boundaries")
            return result_df
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching sensor data: {str(e)}")
            return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Unexpected error processing sensor data: {str(e)}")
            return pd.DataFrame()

    def _split_date_range(self, start_date: str, end_date: str) -> List[Tuple[str, str]]:
        """
        Split a date range into configurable chunks for large historical collections.
        
        For full year collections (e.g., 2024-01-01 to 2024-12-31):
        - Default 2-month chunks = 6 chunks per year
        - Prevents API timeouts and memory issues
        - Each chunk processed separately with progress tracking
        
        Args:
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            
        Returns:
            List[Tuple[str, str]]: List of (chunk_start, chunk_end) date pairs
        """
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        # Get chunk size from configuration (default to 2 months)
        chunk_months = self.airgradient_config.get('historical_chunk_months', 2)
        
        chunks = []
        chunk_start = start
        
        while chunk_start < end:
            # Calculate chunk end (N months from chunk start)
            chunk_end = datetime(
                chunk_start.year + ((chunk_start.month + chunk_months - 1) // 12),
                ((chunk_start.month + chunk_months - 1) % 12) + 1,
                1
            ) - timedelta(days=1)
            
            # If chunk_end is beyond the overall end date, use the end date
            if chunk_end > end:
                chunk_end = end
                
            chunks.append((
                chunk_start.strftime("%Y-%m-%d"),
                chunk_end.strftime("%Y-%m-%d")
            ))
            
            # Move to next chunk
            chunk_start = chunk_end + timedelta(days=1)
            
        return chunks

    def _make_rate_limited_request(self, url: str, params: Dict[str, Any] = None, timeout: int = 30) -> requests.Response:
        """Make a rate-limited request to the AirGradient API with retry logic."""
        rate_config = self.get_rate_limiting_config()
        max_retries = rate_config['max_retries']
        backoff_factor = rate_config['backoff_factor']
        delay = rate_config['delay_between_requests']
        
        for attempt in range(max_retries + 1):
            try:
                # Apply rate limiting delay
                if hasattr(self, '_last_request_time'):
                    time_since_last = time.time() - self._last_request_time
                    if time_since_last < delay:
                        sleep_time = delay - time_since_last
                        self.logger.debug(f"Rate limiting: sleeping {sleep_time:.2f}s")
                        time.sleep(sleep_time)
                
                # Make the request
                self._last_request_time = time.time()
                response = requests.get(url, params=params, timeout=timeout)
                response.raise_for_status()
                return response
                
            except requests.exceptions.RequestException as e:
                if attempt == max_retries:
                    raise e
                    
                # Exponential backoff for retries
                wait_time = delay * (backoff_factor ** attempt)
                self.logger.warning(f"Request failed (attempt {attempt + 1}/{max_retries + 1}): {e}")
                self.logger.info(f"Retrying in {wait_time:.1f} seconds...")
                time.sleep(wait_time)
                
        raise Exception("Max retries exceeded")

    def _fetch_historical_data(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        Fetch historical data for the specified date range.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            List of data records
        """
        # Calculate date range and warn about large collections
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        total_days = (end - start).days + 1
        
        self.logger.info(f"Collecting AirGradient historical data: {start_date} to {end_date} ({total_days} days)")
        
        if total_days > 90:  # More than 3 months
            self.logger.warning(f"⚠️  Large historical collection: {total_days} days")
            self.logger.warning(f"⚠️  Using 2-month chunks to prevent API timeouts")
        
        # Get country boundaries using new config system
        country_codes = self.get_country_codes()
        boundaries = self._get_country_boundaries(country_codes)
        
        base_url = self.get_base_url()
        endpoints = self.get_endpoints()
        current_url = base_url + endpoints['current']
        
        try:
            # First get current online sensors + use master sensor list
            response = requests.get(current_url, timeout=30)
            response.raise_for_status()
            self.logger.info(f"Successfully fetched current sensor data from {current_url}")
            
            data_all_sensors = response.json()
            if not data_all_sensors:
                self.logger.error("Received empty response from API")
                return []
            
            df_all_sensors = pd.DataFrame(data_all_sensors)
            
            # Update master sensor list with current sensors
            sensors_for_master = df_all_sensors[['locationId', 'latitude', 'longitude']].copy()
            self._update_master_sensor_list(sensors_for_master)
            
            # Get all sensors within boundaries (including historical ones from master list)
            sensors_df = self._get_sensor_locations(boundaries)
            
            if sensors_df.empty:
                self.logger.warning("No sensors found within specified boundaries")
                return []
            
            self.logger.info(f"Found {len(sensors_df)} total sensors within boundaries for historical collection")
            
            # Split date range into 2-month chunks for large collections
            if total_days > 60:  # More than 2 months, use chunking
                date_chunks = self._split_date_range(start_date, end_date)
                self.logger.info(f"Split date range into {len(date_chunks)} chunks: {[f'{s} to {e}' for s, e in date_chunks]}")
            else:
                date_chunks = [(start_date, end_date)]
                self.logger.info("Using single date range (≤2 months)")
            
            # Fetch historical data for each sensor and date chunk
            historical_endpoint_template = base_url + endpoints['historical']
            all_data = []
            total_requests = len(sensors_df) * len(date_chunks)
            completed_requests = 0
            
            self.logger.info(f"Starting historical data collection: {len(sensors_df)} sensors × {len(date_chunks)} chunks = {total_requests} API requests")
            
            for chunk_idx, (chunk_start, chunk_end) in enumerate(date_chunks):
                self.logger.info(f"Processing chunk {chunk_idx + 1}/{len(date_chunks)}: {chunk_start} to {chunk_end}")
                chunk_data = []
                
                for sensor_idx, (_, sensor) in enumerate(sensors_df.iterrows()):
                    location_id = int(sensor['locationId'])
                    
                    # Progress logging every 10 sensors or at end of chunk
                    if (sensor_idx + 1) % 10 == 0 or sensor_idx == len(sensors_df) - 1:
                        self.logger.info(f"  Chunk {chunk_idx + 1}: Processing sensor {sensor_idx + 1}/{len(sensors_df)} (location {location_id})")
                    
                    # Format the historical endpoint URL
                    url = historical_endpoint_template.format(location_id=location_id)
                    params = {
                        'from': f"{chunk_start}T000000Z",
                        'to': f"{chunk_end}T235900Z"
                    }
                    
                    try:
                        response = self._make_rate_limited_request(url, params)
                        data = response.json()
                        
                        if data:  # If we got data back
                            # Add location info to each measurement
                            for measurement in data:
                                measurement.update({
                                    'locationId': location_id,
                                    'latitude': float(sensor['latitude']),
                                    'longitude': float(sensor['longitude'])
                                })
                            chunk_data.extend(data)
                            self.logger.debug(f"Fetched {len(data)} records for location {location_id} ({chunk_start} to {chunk_end})")
                        else:
                            self.logger.debug(f"No data for location {location_id} ({chunk_start} to {chunk_end})")
                            
                    except requests.exceptions.RequestException as e:
                        self.logger.error(f"Error fetching data for location {location_id} ({chunk_start} to {chunk_end}): {str(e)}")
                        continue
                    
                    completed_requests += 1
                
                # Add chunk data to all_data
                all_data.extend(chunk_data)
                
                # Progress update after each chunk
                progress_pct = (completed_requests / total_requests) * 100
                self.logger.info(f"Completed chunk {chunk_idx + 1}/{len(date_chunks)}: {len(chunk_data)} records, {progress_pct:.1f}% total progress")
                
                # Force garbage collection after each chunk to manage memory
                import gc
                gc.collect()
            
            self.logger.info(f"Historical data collection completed: {len(all_data)} total records from {len(date_chunks)} chunks")
            return all_data
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching sensor data: {str(e)}")
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error processing sensor data: {str(e)}")
            return []

    def _fetch_realtime_data(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        Fetch real-time data (past 24 hours) from AirGradient API using historical endpoint.
        
        Args:
            start_date: Start date in YYYY-MM-DD format (ignored, uses past 2 days)
            end_date: End date in YYYY-MM-DD format (ignored, uses current date)
            
        Returns:
            List of data records from the past 24 hours
        """
        # Calculate dates for past 24 hours: get 2 days of data then filter to 24 hours
        end_datetime = datetime.now()
        start_datetime = end_datetime - timedelta(days=2)  # Get 2 days to ensure 24h coverage
        
        from_date = start_datetime.strftime("%Y-%m-%d")
        to_date = end_datetime.strftime("%Y-%m-%d")
        
        # Calculate 24 hours ago for filtering (timezone-aware)
        import pytz
        utc = pytz.UTC
        hours_24_ago = utc.localize(end_datetime - timedelta(hours=24))
        
        self.logger.info(f"Fetching realtime data for past 24 hours (requesting {from_date} to {to_date})")
        
        # Get country boundaries using new config system
        country_codes = self.get_country_codes()
        boundaries = self._get_country_boundaries(country_codes)
        
        base_url = self.get_base_url()
        endpoints = self.get_endpoints()
        current_url = base_url + endpoints['current']
        rate_config = self.get_rate_limiting_config()
        
        try:
            # First get current online sensors to know which sensors exist
            self.logger.info(f"Fetching sensor list from {current_url}")
            response = self._make_rate_limited_request(current_url)
            
            data_all_sensors = response.json()
            if not data_all_sensors:
                self.logger.error("Received empty response from API")
                return []
            
            df_all_sensors = pd.DataFrame(data_all_sensors)
            
            # Create geometry column for spatial filtering
            df_all_sensors['geometry'] = df_all_sensors.apply(
                lambda x: Point(x['longitude'], x['latitude']), axis=1
            )
            
            # Convert to GeoDataFrame and set CRS
            df_all_sensors = gpd.GeoDataFrame(df_all_sensors, geometry='geometry', crs='EPSG:4326')
            
            # Filter sensors within boundaries
            df_sensors_within = gpd.sjoin(
                df_all_sensors, boundaries, how="inner", predicate='within'
            )
            
            self.logger.info(f"Found {len(df_sensors_within)} online sensors within specified boundaries")
            
            # Process sensors in batches with rate limiting
            all_data = []
            batch_size = rate_config['batch_size']
            total_sensors = len(df_sensors_within)
            
            for i in range(0, total_sensors, batch_size):
                batch = df_sensors_within.iloc[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                total_batches = (total_sensors + batch_size - 1) // batch_size
                
                self.logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} sensors)")
                
                for _, sensor in batch.iterrows():
                    location_id = int(sensor['locationId'])
                    self.logger.debug(f"Fetching past 24h data for location {location_id}")
                    
                    # Use the historical endpoint with specific date range
                    url = f"{base_url}/unicef/locations/{location_id}/measures/past"
                    params = {
                        'from': f"{from_date}T000000Z",
                        'to': f"{to_date}T235900Z"
                    }
                    
                    try:
                        response = self._make_rate_limited_request(url, params)
                        data = response.json()
                        
                        if data:  # If we got data back
                            # Add location info to each measurement
                            for measurement in data:
                                measurement.update({
                                    'locationId': location_id,
                                    'latitude': float(sensor['latitude']),
                                    'longitude': float(sensor['longitude'])
                                })
                            all_data.extend(data)
                            self.logger.debug(f"Successfully fetched {len(data)} records for location {location_id}")
                        else:
                            self.logger.warning(f"No data for location {location_id}")
                            
                    except requests.exceptions.RequestException as e:
                        self.logger.error(f"Error fetching data for location {location_id}: {str(e)}")
                        continue
                
                # Progress update
                processed = min(i + batch_size, total_sensors)
                self.logger.info(f"Progress: {processed}/{total_sensors} sensors processed ({100*processed/total_sensors:.1f}%)")
            
            # Filter to keep only the past 24 hours
            if all_data:
                df_all = pd.DataFrame(all_data)
                df_all['timestamp'] = pd.to_datetime(df_all['timestamp'], utc=True)
                
                # Filter to keep only data from the past 24 hours (timezone-aware comparison)
                df_filtered = df_all[df_all['timestamp'] >= hours_24_ago]
                
                self.logger.info(f"Filtered {len(df_all)} records to {len(df_filtered)} records from past 24 hours")
                
                return df_filtered.to_dict('records')
            
            return all_data
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching sensor data: {str(e)}")
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error processing sensor data: {str(e)}")
            return []

    def fetch_data(self, start_date: str, end_date: str, mode: Literal["historical", "realtime"] = "historical") -> List[Dict[str, Any]]:
        """Fetch data for the specified date range and mode."""
        if mode == "historical":
            return self._fetch_historical_data(start_date, end_date)
        else:
            return self._fetch_realtime_data(start_date, end_date)

    def validate_data(self, data: List[Dict[str, Any]]) -> bool:
        """Validate the collected data using configuration rules."""
        if not data:
            self.logger.error("No data to validate")
            return False

        # Get validation rules from configuration
        validation_rules = self.get_validation_rules()
        
        # Convert to DataFrame for validation
        df = pd.DataFrame(data)
        
        # Get required fields from configuration
        required_fields = self.airgradient_config.get('fields', {}).get('required', 
            ['locationId', 'timestamp', 'pm02', 'latitude', 'longitude'])
        
        # Check for required columns
        missing_columns = [col for col in required_fields if col not in df.columns]
        if missing_columns:
            self.logger.error(f"Missing required columns: {missing_columns}")
            return False

        # Validate PM2.5 values
        if 'pm02' in df.columns:
            pm25_min = validation_rules.get('pm25_min', 0)
            pm25_max = validation_rules.get('pm25_max', 1000)
            
            # Check for valid PM2.5 range
            invalid_pm25 = df[(df['pm02'] < pm25_min) | (df['pm02'] > pm25_max)]
            if len(invalid_pm25) > 0:
                invalid_ratio = len(invalid_pm25) / len(df)
                if invalid_ratio > 0.1:  # More than 10% invalid values
                    self.logger.error(f"Too many invalid PM2.5 values: {invalid_ratio:.1%}")
                    return False
                else:
                    self.logger.warning(f"Found {len(invalid_pm25)} invalid PM2.5 values ({invalid_ratio:.1%})")

        # Validate coordinates
        if 'latitude' in df.columns and 'longitude' in df.columns:
            invalid_coords = df[(df['latitude'].isna()) | (df['longitude'].isna()) |
                               (df['latitude'] < -90) | (df['latitude'] > 90) |
                               (df['longitude'] < -180) | (df['longitude'] > 180)]
            if len(invalid_coords) > 0:
                self.logger.warning(f"Found {len(invalid_coords)} records with invalid coordinates")

        # Check for duplicate records
        if 'locationId' in df.columns and 'timestamp' in df.columns:
            duplicates = df.duplicated(subset=['locationId', 'timestamp'])
            if duplicates.sum() > 0:
                self.logger.warning(f"Found {duplicates.sum()} duplicate records")

        self.logger.info(f"Data validation passed for {len(data)} records")
        return True

    def save_data(self, data: List[Dict[str, Any]], filename: str, mode: str = "historical") -> None:
        """Save the collected data."""
        df = pd.DataFrame(data)
        
        # Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Determine the save path based on mode
        if mode == "historical":
            save_path = self.config['paths']['raw_data_airgradient_historical'] / filename
        else:
            save_path = self.config['paths']['raw_data_airgradient_realtime'] / filename
            
        # Ensure the directory exists
        save_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save to parquet format
        df.to_parquet(save_path, index=False)
        self.logger.info(f"Saved {len(df)} records to {save_path}")

    def collect(self, start_date: str, end_date: str, filename: str, mode: str = "historical", **kwargs) -> None:
        """Main method to collect, validate and save data."""
        self.logger.info(f"Starting data collection from {start_date} to {end_date}")
        
        # Fetch data
        self.logger.info("Fetching data...")
        data = self.fetch_data(start_date, end_date, mode=mode, **kwargs)
        self.logger.info(f"Fetched {len(data)} records")
        
        if not data:
            self.logger.error("No data was collected")
            return
            
        # Validate data
        self.logger.info("Validating data...")
        if self.validate_data(data):
            self.logger.info("Data validation passed")
            
            # Save data
            self.logger.info(f"Saving data to {filename}...")
            self.save_data(data, filename, mode=mode)
            self.logger.info("Data collection completed successfully")
        else:
            self.logger.error("Data validation failed") 
