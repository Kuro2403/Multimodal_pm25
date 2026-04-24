#!/usr/bin/env python3
"""
FIRMS KDE Interpolation

This script provides fast KDE interpolation for FIRMS fire data,
optimized for real-time processing of the past 24 hours of fire data.
"""

import pandas as pd
import numpy as np
import polars as pl
import polars_h3 as plh3
import geopandas as gpd
import matplotlib.pyplot as plt
import time
from datetime import datetime, date, timedelta, timezone
from typing import Tuple, Optional, List
from scipy.stats import gaussian_kde
from scipy.signal import fftconvolve
import argparse
import os
from src.utils.boundary_utils import create_country_boundaries
from src.utils.logging_utils import setup_basic_logging

from pathlib import Path

# Note: create_boundaries_countries is now imported from utils.boundary_utils
def create_boundaries_countries(country_code_list: List[str], buffer_degrees: float = 0.4) -> gpd.GeoDataFrame:
    """Use centralized boundary utility."""
    return create_country_boundaries(country_code_list, buffer_degrees)

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
    print("Processing data with optimized fast KDE method...")
    
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
    print(f"Fast KDE completed in {processing_time:.2f} seconds")
    
    return {
        'Z': kde_result.T,  # Transpose to match plotting format
        'extent': [lon_min, lon_max, lat_min, lat_max],
        'coords': coords,
        'processing_time': processing_time
    }

def plot_kde(result, boundaries_countries, output_file=None):
    """
    Plot the KDE results.
    
    Args:
        result: Dictionary with KDE results
        boundaries_countries: GeoDataFrame with country boundaries
        output_file: Path to save the plot (if None, plot is displayed)
    """
    Z = result['Z']
    extent = result['extent']
    coords = result['coords']
    
    plt.figure(figsize=(10, 10))
    plt.contourf(Z, levels=20, cmap='Reds', extent=extent)
    plt.colorbar(label='Fire Density (weighted by radiative power)')
    plt.gca().set_aspect('equal')
    plt.scatter(coords[:, 0], coords[:, 1], s=1, c='red', alpha=0.3)
    
    # Plot the merged GeoDataFrame
    boundaries_countries.plot(ax=plt.gca(), facecolor="none", edgecolor="blue", linewidth=1, label="Region Boundaries")
    
    plt.title('Radiative Power-Weighted Fire Density')
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.legend()
    
    if output_file:
        plt.savefig(output_file)
        print(f"KDE plot saved to {output_file}")
    else:
        plt.show()
    
    plt.close()

def process_last_24h_fires(input_file, output_dir, countries, save_kde_grids=False, buffer_degrees=0.4):
    """
    Process recent fire data using KDE interpolation, creating one file per day.
    
    Args:
        input_file: Path to the prepared FIRMS data file
        output_dir: Directory to save output files
        countries: List of country codes
        save_kde_grids: Whether to save the large KDE grid files (default False)
        buffer_degrees: Geographic buffer in degrees for interpolation area (default 0.4)
    
    Returns:
        Dictionary with summary of processed files
    """
    print(f"Processing fire data from {input_file}...")
    
    # Create output directories with new structure
    kde_dir = os.path.join(output_dir, "kde") if save_kde_grids else None
    h3_dir = os.path.join("data/processed/firms/h3/realtime")
    plots_dir = os.path.join("data/processed/firms/plots/realtime")
    
    for directory in [dir for dir in [kde_dir, h3_dir, plots_dir] if dir]:
        os.makedirs(directory, exist_ok=True)
    
    # Load prepared fire data
    df_firms = pd.read_parquet(input_file)
    print(f"Loaded {len(df_firms)} fire points from {input_file}")
    
    # Ensure acq_date is datetime
    df_firms['acq_date'] = pd.to_datetime(df_firms['acq_date'])
    
    # Group by date
    df_firms['date_only'] = df_firms['acq_date'].dt.date
    unique_dates = sorted(df_firms['date_only'].unique())
    print(f"Found {len(unique_dates)} unique dates to process: {unique_dates}")
    
    # Create GeoDataFrame for country boundaries once
    print("Creating boundaries for countries:", countries)
    boundaries_countries = create_boundaries_countries(countries, buffer_degrees)
    countries_str = "_".join(countries)
    
    # Process each day separately
    results_summary = {
        "files_created": [],
        "total_fires_processed": 0,
        "dates_processed": []
    }
    
    for date_to_process in unique_dates:
        date_str = date_to_process.strftime("%Y%m%d")
        print(f"\n{'='*60}")
        print(f"Processing date: {date_str}")
        print(f"{'='*60}")
        
        # Filter to current date
        df_day = df_firms[df_firms['date_only'] == date_to_process].copy()
        print(f"Found {len(df_day)} fires for {date_str}")
        
        # Remove negative FRP values
        df_day = df_day[df_day['frp'] >= 0]
        print(f"After removing negative FRP: {len(df_day)} fires")
        
        # Check if we have enough data
        if len(df_day) <= 2:
            print(f"WARNING: Not enough fires for {date_str} (minimum 3 required), skipping...")
            continue
        
        try:
            # Run fast KDE interpolation
            kde_result = fast_kde(df_day, boundaries_countries, bandwidth_factor=0.3)
            
            # Create output file paths with date in filename
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
            if save_kde_grids and kde_file:
                result_df.to_parquet(kde_file)
                print(f"KDE interpolation saved to {kde_file}")
                results_summary["files_created"].append(kde_file)
            
            # Convert to H3 grid using Polars
            result_lf = pl.from_pandas(result_df).lazy()
            result_lf = result_lf.with_columns(hex_id=plh3.latlng_to_cell("latitude", "longitude", 8))
            h3_result = result_lf.group_by("hex_id").agg(pl.col("value").mean().alias("value")).collect()
            
            # Filter positive values and convert to string format
            h3_viz = h3_result.filter(pl.col("value") > 0).with_columns(h3_08_text=plh3.int_to_str("hex_id")).select("h3_08_text", "value")
            h3_viz.write_parquet(h3_file)
            print(f"H3 grid saved to {h3_file}")
            results_summary["files_created"].append(h3_file)
            
            # Generate visualization
            plot_kde(kde_result, boundaries_countries, output_file=plot_file)
            print(f"Plot saved to {plot_file}")
            results_summary["files_created"].append(plot_file)
            
            # Update summary
            results_summary["total_fires_processed"] += len(df_day)
            results_summary["dates_processed"].append(date_str)
            
            print(f"✓ Successfully processed {date_str} ({len(df_day)} fires, processing time: {kde_result['processing_time']:.2f}s)")
            
        except Exception as e:
            print(f"ERROR processing {date_str}: {str(e)}")
            continue
    
    # Print final summary
    print(f"\n{'='*60}")
    print(f"PROCESSING SUMMARY")
    print(f"{'='*60}")
    print(f"Total dates processed: {len(results_summary['dates_processed'])}")
    print(f"Total fires processed: {results_summary['total_fires_processed']}")
    print(f"Files created: {len(results_summary['files_created'])}")
    print(f"Dates: {', '.join(results_summary['dates_processed'])}")
    
    return results_summary

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='FIRMS KDE Realtime Interpolation')
    parser.add_argument('--input-file', type=str, required=True,
                        help='Path to the prepared FIRMS data file')
    parser.add_argument('--output-dir', type=str, default='data/processed/firms/realtime',
                        help='Directory to save output files')
    parser.add_argument('--countries', type=str, nargs='+', default=['THA', 'LAO'],
                        help='List of country codes to process')
    parser.add_argument('--save-kde-grids', action='store_true',
                        help='Save the large KDE grid files (not recommended for large datasets)')
    parser.add_argument('--buffer', type=float, default=0.4,
                        help='Geographic buffer in degrees for interpolation area (default: 0.4)')
    args = parser.parse_args()
    
    # Process last 24 hours of fire data
    result = process_last_24h_fires(
        args.input_file,
        args.output_dir,
        args.countries,
        save_kde_grids=args.save_kde_grids,
        buffer_degrees=args.buffer
    )
    
    # Print summary
    print("\nKDE Interpolation Summary:")
    for key, value in result.items():
        print(f"{key}: {value}")

if __name__ == "__main__":
    main() 