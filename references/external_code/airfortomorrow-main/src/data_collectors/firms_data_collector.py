#!/usr/bin/env python3
"""
FIRMS Fire Data Collection

This script handles FIRMS fire data collection:
1. Historical data - expected to be manually downloaded and placed in data/raw/firms/historical
2. Near real-time data - downloaded automatically for South East Asia region (past 7 days)
"""

import pandas as pd
import os
import argparse
import time
from datetime import datetime
from pathlib import Path

def download_nrt_firms_data(output_dir='data/raw/firms/nrt'):
    """
    Download near real-time FIRMS fire data for South East Asia (past 7 days)
    
    Args:
        output_dir: Directory to save the downloaded data
    
    Returns:
        A dictionary containing paths to downloaded files and summary statistics
    """
    print("Downloading near real-time FIRMS data for South East Asia...")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # URLs for different satellite data (7-day data for South East Asia)
    urls = {
        'MODIS': "https://firms.modaps.eosdis.nasa.gov/data/active_fire/modis-c6.1/csv/MODIS_C6_1_SouthEast_Asia_7d.csv",
        'VIIRS_SUOMI': "https://firms.modaps.eosdis.nasa.gov/data/active_fire/suomi-npp-viirs-c2/csv/SUOMI_VIIRS_C2_SouthEast_Asia_7d.csv",
        'VIIRS_J1': "https://firms.modaps.eosdis.nasa.gov/data/active_fire/noaa-20-viirs-c2/csv/J1_VIIRS_C2_SouthEast_Asia_7d.csv",
        'VIIRS_J2': "https://firms.modaps.eosdis.nasa.gov/data/active_fire/noaa-21-viirs-c2/csv/J2_VIIRS_C2_SouthEast_Asia_7d.csv"
    }
    
    # File paths for downloaded data
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_files = {
        satellite: os.path.join(output_dir, f"{satellite.lower()}_{timestamp}.csv")
        for satellite in urls.keys()
    }
    
    # Combined index file to help with processing
    index_file = os.path.join(output_dir, f"firms_nrt_index_{timestamp}.csv")
    
    # Download the data
    files_downloaded = 0
    total_points = 0
    index_data = []
    
    try:
        for satellite, url in urls.items():
            print(f"Downloading {satellite} data...")
            try:
                df = pd.read_csv(url)
                output_file = output_files[satellite]
                df.to_csv(output_file, index=False)
                files_downloaded += 1
                points = len(df)
                total_points += points
                print(f"Successfully downloaded {satellite} data with {points} fire points")
                
                index_data.append({
                    "satellite": satellite,
                    "file_path": output_file,
                    "points": points,
                    "timestamp": timestamp,
                    "data_type": "nrt"
                })
            except Exception as e:
                print(f"Error downloading {satellite} data: {str(e)}")
        
        # Create an index file to help with processing
        if index_data:
            pd.DataFrame(index_data).to_csv(index_file, index=False)
            print(f"\nCreated index file: {index_file}")
        
        return {
            "output_files": output_files,
            "index_file": index_file,
            "timestamp": timestamp,
            "total_files": files_downloaded,
            "total_points": total_points
        }
    
    except Exception as e:
        print(f"Error in download process: {str(e)}")
        return {
            "error": str(e),
            "total_files": files_downloaded
        }

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='FIRMS Fire Data Collection')
    parser.add_argument('--output-dir', type=str, default='data/raw/firms/nrt',
                        help='Directory to save downloaded near real-time files (default: data/raw/firms/nrt)')
    args = parser.parse_args()
    
    # Start timing
    start_time = time.time()
    
    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Download near real-time FIRMS data
    result = download_nrt_firms_data(args.output_dir)
    
    # Calculate elapsed time
    elapsed_time = time.time() - start_time
    print(f"\nData collection completed in {elapsed_time:.2f} seconds.")
    
    # Print summary
    if "error" not in result:
        print("\nCollection Summary:")
        print(f"Files downloaded: {result['total_files']}")
        print(f"Total fire points: {result['total_points']}")
        print(f"Data saved to: {args.output_dir}")
        print(f"Index file created: {result['index_file']}")
    else:
        print("\nCollection Summary:")
        print(f"Files downloaded: {result['total_files']}")
        print(f"Errors encountered: {result['error']}")
    
    return result

if __name__ == "__main__":
    main() 