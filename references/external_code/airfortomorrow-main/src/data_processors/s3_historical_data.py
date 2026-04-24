import boto3
import pandas as pd
import gzip
import io
import os
import time
from datetime import datetime
import argparse
from tqdm import tqdm
from pathlib import Path
from botocore.exceptions import NoCredentialsError, ClientError
from botocore import UNSIGNED
from botocore.config import Config
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys

# Add the project root directory to Python path for reliable imports
script_dir = Path(__file__).resolve().parent
project_root = script_dir.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.data_collectors.openaq_collector import OpenAQCollector

# Set a maximum number of workers for parallel processing
MAX_WORKERS = 8

def process_location_year(location_id, year, output_dir, s3_client, max_retries=3):
    """Process a single location and year - returns result stats."""
    output_file = os.path.join(output_dir, f"location_{location_id}_{year}.parquet")
    
    # Check if file already exists
    if os.path.exists(output_file):
        # Read existing file to get record count for accurate statistics
        try:
            existing_df = pd.read_parquet(output_file)
            record_count = len(existing_df)
        except Exception as e:
            # If we can't read the file, assume 0 records
            record_count = 0
        
        return {
            "location_id": location_id,
            "year": year,
            "status": "skipped",
            "message": f"File already exists for location {location_id} year {year}",
            "record_count": record_count
        }
    
    # Parse S3 path
    bucket_name = "openaq-data-archive"
    s3_key = f"records/csv.gz/locationid={location_id}/year={year}"
    
    retry_count = 0
    success = False
    
    while not success and retry_count < max_retries:
        try:
            # List all objects with the given prefix to find CSV files
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_key)
            
            if 'Contents' not in response:
                if retry_count == 0:
                    retry_count += 1
                    continue
                return {
                    "location_id": location_id,
                    "year": year,
                    "status": "no_data",
                    "message": f"No data found for location {location_id} year {year}"
                }
            
            # Find CSV.gz files
            csv_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.csv.gz')]
            
            if not csv_files:
                if retry_count == 0:
                    retry_count += 1
                    continue
                return {
                    "location_id": location_id,
                    "year": year,
                    "status": "no_data",
                    "message": f"No CSV files found for location {location_id} year {year}"
                }
            
            # Combine all CSV files for this location/year
            all_dataframes = []
            
            for csv_file in csv_files:
                # Download the file
                obj = s3_client.get_object(Bucket=bucket_name, Key=csv_file)
                
                # Read compressed CSV
                with gzip.GzipFile(fileobj=io.BytesIO(obj['Body'].read())) as gz:
                    df = pd.read_csv(gz)
                    all_dataframes.append(df)
            
            # Combine all dataframes
            if all_dataframes:
                combined_df = pd.concat(all_dataframes, ignore_index=True)
                combined_df.to_parquet(output_file, index=False)
                
                return {
                    "location_id": location_id,
                    "year": year,
                    "status": "success",
                    "message": f"Successfully downloaded {len(combined_df)} records for location {location_id} year {year}",
                    "record_count": len(combined_df)
                }
            else:
                retry_count += 1
                continue
                
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchBucket':
                return {
                    "location_id": location_id,
                    "year": year,
                    "status": "failed",
                    "message": f"Bucket {bucket_name} does not exist"
                }
            elif error_code == 'AccessDenied':
                retry_count += 1
                time.sleep(1)  # Wait before retrying
            else:
                retry_count += 1
                time.sleep(1)  # Wait before retrying
        except NoCredentialsError:
            return {
                "location_id": location_id,
                "year": year,
                "status": "failed",
                "message": "AWS credentials not found. Using unsigned requests."
            }
        except Exception as e:
            retry_count += 1
            time.sleep(1)  # Wait before retrying
    
    if not success and retry_count >= max_retries:
        return {
            "location_id": location_id,
            "year": year,
            "status": "failed",
            "message": f"Failed to download data for location {location_id} year {year} after {max_retries} retries"
        }

def download_historical_data(location_ids, years, output_dir='./data/raw/openaq/historical', max_retries=3):
    """
    Download historical OpenAQ data from S3 for specified location IDs and years.
    Uses parallel processing for faster downloads and includes progress tracking.
    
    Args:
        location_ids: List of location IDs to download data for
        years: List of years to download data for
        output_dir: Directory to save the downloaded data
        max_retries: Maximum number of retries for failed downloads
    """
    # Create output directory
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Set up S3 client for public bucket access
    s3_client = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    
    # Statistics for reporting
    stats = {
        "successful": 0,
        "failed": 0,
        "no_data": 0,
        "skipped": 0,
        "total_records": 0
    }
    
    total_tasks = len(location_ids) * len(years)
    start_time = time.time()
    
    print(f"Starting download of historical data for {len(location_ids)} locations across {len(years)} years")
    print(f"Output directory: {output_dir}")
    print(f"Using {MAX_WORKERS} parallel workers for faster downloads")
    
    # Create tasks for all location-year combinations
    tasks = []
    for location_id in location_ids:
        for year in years:
            tasks.append((location_id, year))
    
    # Process tasks in parallel with a progress bar
    progress_bar = tqdm(total=total_tasks, desc="Downloading data", unit="location-year combination")
    
    # Create a shared counter for completed tasks (to estimate remaining time)
    completed_tasks = 0
    results = []
    
    # Function to update progress
    def update_progress(future):
        nonlocal completed_tasks
        completed_tasks += 1
        progress_bar.update(1)
        
        # Calculate and update ETA based on average time per task
        elapsed = time.time() - start_time
        if completed_tasks > 0:
            avg_time_per_task = elapsed / completed_tasks
            remaining_tasks = total_tasks - completed_tasks
            eta = avg_time_per_task * remaining_tasks
            
            # Update progress bar description with ETA
            progress_bar.set_description(
                f"Downloading data - ETA: {format_time(eta)} - "
                f"Success: {stats['successful']}, Failed: {stats['failed']}, "
                f"No data: {stats['no_data']}, Skipped: {stats['skipped']}"
            )
    
    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit all tasks
            future_to_task = {
                executor.submit(
                    process_location_year, loc_id, yr, output_dir, s3_client, max_retries
                ): (loc_id, yr) for loc_id, yr in tasks
            }
            
            # Process results as they complete
            for future in as_completed(future_to_task):
                result = future.result()
                results.append(result)
                
                # Update statistics based on result status
                status = result["status"]
                if status not in stats:
                    # If we encounter a new status type, add it to our stats
                    stats[status] = 0
                stats[status] += 1
                
                # Print success or failure messages and count records
                if status == "success":
                    record_count = result.get("record_count", 0)
                    stats["total_records"] += record_count
                    tqdm.write(result["message"])
                elif status == "skipped":
                    # Also count records from skipped files
                    record_count = result.get("record_count", 0)
                    stats["total_records"] += record_count
                elif status == "failed":
                    tqdm.write(f"Error: {result['message']}")
                
                # Update progress
                update_progress(future)
    except KeyboardInterrupt:
        print("\nDownload interrupted by user.")
        print("You can resume later - files that were successfully downloaded will be skipped.")
        sys.exit(1)
    
    progress_bar.close()
    
    # Print summary
    elapsed_time = time.time() - start_time
    print("\n" + "="*50)
    print(f"Download Summary:")
    print(f"Total unique locations: {len(location_ids)}")
    print(f"Years requested: {years}")
    print(f"Total location-year combinations: {len(location_ids) * len(years)}")
    print(f"Breakdown by location-year result:")
    print(f"  - Successfully downloaded: {stats['successful']}")
    print(f"  - Skipped (already downloaded): {stats['skipped']}")
    print(f"  - No data available: {stats['no_data']}")
    print(f"  - Failed downloads: {stats['failed']}")
    print(f"Total records downloaded: {stats['total_records']}")
    print(f"Time taken: {format_time(elapsed_time)}")
    print("="*50)
    
    return stats

def format_time(seconds):
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

def main():
    """Main function to run the script from command line."""
    # Get access to the global variable
    global MAX_WORKERS
    
    parser = argparse.ArgumentParser(description='Download historical OpenAQ data from S3')
    parser.add_argument('--years', type=int, nargs='+', default=[2024], 
                        help='Years to download data for (default: 2024)')
    parser.add_argument('--country-codes', type=int, nargs='+', default=[111],
                        help='Country codes to download data for (default: 111 for Thailand)')
    parser.add_argument('--output-dir', type=str, default='./data/raw/openaq/historical',
                        help='Directory to save the downloaded parquet files')
    parser.add_argument('--max-retries', type=int, default=3,
                        help='Maximum number of retries for failed downloads')
    parser.add_argument('--max-workers', type=int, default=MAX_WORKERS,
                        help=f'Maximum number of parallel downloads (default: {MAX_WORKERS})')
    args = parser.parse_args()
    
    # Update MAX_WORKERS if specified in arguments
    MAX_WORKERS = args.max_workers
    
    # Initialize OpenAQCollector to get location IDs
    print("Initializing OpenAQ collector to get location IDs...")
    collector = OpenAQCollector()
    
    # Get location IDs for specified countries
    print(f"Getting location IDs for country codes: {args.country_codes}")
    
    # Temporarily modify the collector's country codes to match the command line args
    original_country_codes = collector.config['data_collection']['country_codes']
    collector.config['data_collection']['country_codes'] = args.country_codes
    
    locations_gdf = collector._get_sensors_country()
    
    # Restore original country codes
    collector.config['data_collection']['country_codes'] = original_country_codes
    
    if locations_gdf.empty:
        print("Error: No locations found for the specified countries")
        return
    
    # Print country distribution to verify filtering
    country_counts = locations_gdf['country'].value_counts()
    print(f"Country distribution of locations: {country_counts.to_dict()}")
    
    # Extract location IDs
    location_ids = locations_gdf['location_id'].unique().tolist()
    print(f"Found {len(location_ids)} unique locations for countries {args.country_codes}")
    print(f"Will process {len(location_ids) * len(args.years)} location-year combinations ({len(location_ids)} locations × {len(args.years)} years)")
    
    try:
        # Download data
        download_historical_data(location_ids, args.years, args.output_dir, args.max_retries)
    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Exiting...")
        sys.exit(1)

if __name__ == "__main__":
    main() 