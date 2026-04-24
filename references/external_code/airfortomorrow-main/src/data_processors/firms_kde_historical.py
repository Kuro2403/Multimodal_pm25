#!/usr/bin/env python3
"""
FIRMS KDE Historical Interpolation

This script provides daily KDE interpolation for historical FIRMS fire data,
processing each day separately.
"""

import os
import sys
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))

import pandas as pd
import numpy as np
import polars as pl
import polars_h3 as plh3
import geopandas as gpd
import matplotlib.pyplot as plt
import time
from datetime import datetime, timedelta
from scipy.stats import gaussian_kde
from scipy.signal import fftconvolve
import argparse
from tqdm import tqdm
from shapely.ops import unary_union

# Import centralized boundary utilities
from src.utils.boundary_utils import create_country_boundaries

def gaussian_kernel(size, sigma):
    """Generate a 2D Gaussian kernel."""
    ax = np.linspace(-(size // 2), size // 2, size)
    gauss = np.exp(-0.5 * (ax / sigma) ** 2)
    kernel = np.outer(gauss, gauss)
    return kernel / kernel.sum()

def fast_kde(df_day, boundaries_countries, bandwidth_factor=0.3, grid_size=6400):
    """
    Process KDE for fire data using binning + FFT-based convolution method.
    
    Args:
        df_day: DataFrame with fire data (must have longitude, latitude, frp columns)
        boundaries_countries: GeoDataFrame with country boundaries
        bandwidth_factor: Factor to adjust bandwidth (default 0.3)
        grid_size: Size of the grid for interpolation (default 6400)
        
    Returns:
        Dictionary with KDE results (Z, extent, coords, processing_time)
    """
    start_time = time.time()
    
    # Extract coordinates and weights
    coords = df_day[['longitude', 'latitude']].values
    weights = df_day['frp'].values
    
    # Define grid boundaries
    bbox = boundaries_countries.total_bounds
    lon_min, lon_max = bbox[0], bbox[2]
    lat_min, lat_max = bbox[1], bbox[3]
    
    # Make sure the grid will be square
    lon_extent = lon_max - lon_min
    lat_extent = lat_max - lat_min
    aspect_ratio = lon_extent / lat_extent 

    # Compute bandwidth adaptively
    kde = gaussian_kde(coords.T, weights=weights)
    bandwidth = kde.scotts_factor() * np.std(coords, axis=0).mean()
    
    # Create histogram (binning step)
    hist, x_edges, y_edges = np.histogram2d(
        coords[:, 0], coords[:, 1],
        bins=[int(grid_size*aspect_ratio), grid_size],
        range=[[lon_min, lon_max], [lat_min, lat_max]],
        weights=weights
    )
    
    # Convert bandwidth to standard deviation for Gaussian filter
    sigma = bandwidth * grid_size / (lon_max - lon_min) / np.sqrt(2)
    
    # Create FFT-optimized Gaussian kernel
    kernel_size = max(int(7 * sigma), 10)  # Ensure kernel size is large enough
    kernel = gaussian_kernel(kernel_size, sigma)
    
    # Apply FFT-based convolution
    kde_result = fftconvolve(hist, kernel, mode='same')
    
    processing_time = time.time() - start_time
    
    return {
        'Z': kde_result.T,  # Transpose to match plotting format
        'extent': [lon_min, lon_max, lat_min, lat_max],
        'coords': coords,
        'processing_time': processing_time
    }

# Note: create_country_boundaries is now imported from utils.boundary_utils
# Local alias with buffer adjustment for this file's needs
def create_country_boundaries_local(countries):
    """Use centralized boundary utility with specific buffer for fires."""
    return create_country_boundaries(countries, buffer_degrees=3.0)  # Using 3-degree buffer for fires

def process_single_day(day_df, boundaries_countries, output_dir, countries, date_str, save_kde_grids=False):
    """
    Process a single day of fire data using KDE interpolation.
    
    Args:
        day_df: DataFrame with fire data for a single day
        boundaries_countries: GeoDataFrame with country boundaries
        output_dir: Directory to save output files
        countries: List of country codes
        date_str: Date string in YYYYMMDD format
        save_kde_grids: Whether to save the large KDE grid files (default False)
    
    Returns:
        Dictionary with processing results
    """
    # Remove negative FRP values
    day_df = day_df[day_df['frp'] >= 0]
    
    # Check if we have enough data
    if len(day_df) <= 2:
        return {
            "date": date_str,
            "error": "Not enough fires to compute KDE interpolation (minimum 3 required)",
            "fires_processed": len(day_df)
        }
    
    # Run fast KDE interpolation
    kde_result = fast_kde(day_df, boundaries_countries)
    
    # Create output file paths
    countries_str = "_".join(countries)
    
    # Use new directory structure
    kde_dir = os.path.join(output_dir, "kde") if save_kde_grids else None
    h3_dir = os.path.join("data/processed/firms/h3/historical")
    plots_dir = os.path.join("data/processed/firms/plots/historical")
    
    kde_file = os.path.join(kde_dir, f"firms_kde_{countries_str}_{date_str}.parquet") if kde_dir else None
    h3_file = os.path.join(h3_dir, f"firms_kde_h308_{countries_str}_{date_str}.parquet")
    plot_file = os.path.join(plots_dir, f"firms_kde_plot_{countries_str}_{date_str}.png")
    
    # Convert KDE results to DataFrame
    Z = kde_result['Z']
    extent = kde_result['extent']
    lon_min, lon_max, lat_min, lat_max = extent
    grid_size = Z.shape
    
    # Create coordinate grids
    lon_coords = np.linspace(lon_min, lon_max, grid_size[1])
    lat_coords = np.linspace(lat_min, lat_max, grid_size[0])
    
    # Create meshgrid and flatten
    lon_grid, lat_grid = np.meshgrid(lon_coords, lat_coords)
    lon_values = lon_grid.flatten()
    lat_values = lat_grid.flatten()
    z_values = Z.flatten()
    
    # Create DataFrame with results
    result_df = pd.DataFrame(data={'longitude': lon_values, 'latitude': lat_values, 'value': z_values})
    
    # Save KDE results if requested
    if save_kde_grids:
        result_df.to_parquet(kde_file)
    
    # Convert to H3 grid using Polars
    result_lf = pl.from_pandas(result_df).lazy()
    result_lf = result_lf.with_columns(hex_id=plh3.latlng_to_cell("latitude", "longitude", 8))
    h3_result = result_lf.group_by("hex_id").agg(pl.col("value").mean().alias("value")).collect()
    
    # Filter positive values and convert to string format
    h3_viz = h3_result.filter(pl.col("value") > 0).with_columns(h3_08_text=plh3.int_to_str("hex_id")).select("h3_08_text", "value")
    h3_viz.write_parquet(h3_file)
    
    # Generate visualization
    if os.environ.get('GENERATE_PLOTS', 'true').lower() == 'true':
        # Plot the KDE results
        plt.figure(figsize=(10, 10))
        plt.contourf(Z, levels=20, cmap='Reds', extent=extent)
        plt.colorbar(label='Fire Density (weighted by radiative power)')
        plt.gca().set_aspect('equal')
        plt.scatter(kde_result['coords'][:, 0], kde_result['coords'][:, 1], s=1, c='red', alpha=0.3)
        
        # Plot the merged GeoDataFrame
        boundaries_countries.plot(ax=plt.gca(), facecolor="none", edgecolor="blue", linewidth=1)
        
        plt.title(f'Radiative Power-Weighted Fire Density ({date_str})')
        plt.xlabel('Longitude')
        plt.ylabel('Latitude')
        
        plt.savefig(plot_file)
        plt.close()
    
    return {
        "date": date_str,
        "kde_file": kde_file if save_kde_grids else None,
        "h3_file": h3_file,
        "plot_file": plot_file if os.environ.get('GENERATE_PLOTS', 'true').lower() == 'true' else None,
        "fires_processed": len(day_df),
        "processing_time": kde_result['processing_time'],
        "h3_cells": len(h3_viz)
    }

def process_historical_data(input_file, output_dir, countries, start_date=None, end_date=None, save_kde_grids=False, buffer_degrees=0.4):
    """
    Process historical FIRMS data with daily KDE interpolation.
    
    Args:
        input_file: Path to the prepared FIRMS data file
        output_dir: Directory to save output files
        countries: List of country codes
        start_date: Start date for filtering (YYYY-MM-DD format)
        end_date: End date for filtering (YYYY-MM-DD format)
        save_kde_grids: Whether to save the large KDE grid files
    
    Returns:
        Dictionary with processing results
    """
    print(f"Processing historical FIRMS data from {input_file}...")
    
    # Create output directories with new structure
    kde_dir = os.path.join(output_dir, "kde") if save_kde_grids else None
    h3_dir = os.path.join("data/processed/firms/h3/historical")
    plots_dir = os.path.join("data/processed/firms/plots/historical")
    
    for directory in [dir for dir in [kde_dir, h3_dir, plots_dir] if dir]:
        os.makedirs(directory, exist_ok=True)
    
    # Load prepared fire data
    df_firms = pd.read_parquet(input_file)
    print(f"Loaded {len(df_firms)} fire points from {input_file}")
    
    # Ensure acq_date is datetime
    df_firms['acq_date'] = pd.to_datetime(df_firms['acq_date'])
    
    # Filter by date range if provided
    if start_date:
        start_date = pd.to_datetime(start_date)
        df_firms = df_firms[df_firms['acq_date'] >= start_date]
        print(f"Filtered to dates >= {start_date.strftime('%Y-%m-%d')}")
    
    if end_date:
        end_date = pd.to_datetime(end_date)
        df_firms = df_firms[df_firms['acq_date'] <= end_date]
        print(f"Filtered to dates <= {end_date.strftime('%Y-%m-%d')}")
    
    # Get unique dates
    unique_dates = df_firms['acq_date'].dt.date.unique()
    unique_dates = sorted(unique_dates)
    print(f"Found {len(unique_dates)} unique dates to process")
    
    # Create country boundaries
    boundaries_countries = create_country_boundaries(countries, buffer_degrees=buffer_degrees)  # Use configurable buffer for interpolation area
    
    # Process each day
    results = []
    
    for date in tqdm(unique_dates, desc="Processing dates"):
        # Filter data for current date
        date_str = date.strftime("%Y%m%d")
        date_df = df_firms[df_firms['acq_date'].dt.date == date]
        
        # Process the day
        result = process_single_day(date_df, boundaries_countries, output_dir, countries, date_str, save_kde_grids)
        results.append(result)
        
        # Print progress message
        if "error" in result:
            print(f"  {date_str}: {result['error']} ({result['fires_processed']} fires)")
        else:
            print(f"  {date_str}: Processed {result['fires_processed']} fires, created {result['h3_cells']} H3 cells in {result['processing_time']:.2f}s")
    
    return {
        "days_processed": len(results),
        "days_successful": len([r for r in results if "error" not in r]),
        "days_failed": len([r for r in results if "error" in r]),
        "total_fires_processed": sum([r["fires_processed"] for r in results]),
    }

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='FIRMS KDE Historical Interpolation')
    parser.add_argument('--input-file', type=str, required=True,
                        help='Path to the prepared FIRMS data file')
    parser.add_argument('--output-dir', type=str, default='data/processed/firms/historical',
                        help='Directory to save output files')
    parser.add_argument('--countries', type=str, nargs='+', default=['THA', 'LAO'],
                        help='List of country codes to process')
    parser.add_argument('--start-date', type=str,
                        help='Start date for filtering (YYYY-MM-DD format)')
    parser.add_argument('--end-date', type=str,
                        help='End date for filtering (YYYY-MM-DD format)')
    parser.add_argument('--no-plots', action='store_true',
                        help='Skip generating plots to save time')
    parser.add_argument('--save-kde-grids', action='store_true',
                        help='Save the large KDE grid files (not recommended for large datasets)')
    parser.add_argument('--buffer', type=float, default=0.4,
                        help='Geographic buffer in degrees for interpolation area (default: 0.4)')
    args = parser.parse_args()
    
    # Set environment variable for plots
    if args.no_plots:
        os.environ['GENERATE_PLOTS'] = 'false'
    else:
        os.environ['GENERATE_PLOTS'] = 'true'
    
    # Process historical data
    start_time = time.time()
    result = process_historical_data(
        args.input_file, 
        args.output_dir, 
        args.countries,
        args.start_date,
        args.end_date,
        args.save_kde_grids,
        args.buffer
    )
    
    # Print summary
    elapsed_time = time.time() - start_time
    print("\nHistorical KDE Interpolation Summary:")
    print(f"Days processed: {result['days_processed']}")
    print(f"Days successful: {result['days_successful']}")
    print(f"Days failed: {result['days_failed']}")
    print(f"Total fires processed: {result['total_fires_processed']}")
    print(f"Total processing time: {elapsed_time:.2f} seconds")

if __name__ == "__main__":
    main() 