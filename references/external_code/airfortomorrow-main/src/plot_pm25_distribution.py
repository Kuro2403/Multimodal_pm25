#!/usr/bin/env python3
"""
Plot PM2.5 distribution histogram from predictions
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import argparse
import sys
from datetime import datetime
from pathlib import Path

def main():
    parser = argparse.ArgumentParser(description='Generate PM2.5 distribution histogram from predictions')
    parser.add_argument('--input-file', type=str, required=True,
                       help='Path to the predictions parquet file')
    parser.add_argument('--countries', nargs='+', default=['LAO', 'THA'],
                       help='Country codes for the plot title')
    parser.add_argument('--target-date', type=str, help='Target date for the chart (YYYY-MM-DD format)')
    args = parser.parse_args()
    
    try:
        # Read the predictions file
        df = pd.read_parquet(args.input_file)
        
        # Check if the column is 'predicted_pm25' or 'prediction_pm_25'
        pm25_col = 'predicted_pm25' if 'predicted_pm25' in df.columns else 'prediction_pm_25'
        
        if pm25_col not in df.columns:
            print(f"ERROR: Neither 'predicted_pm25' nor 'prediction_pm_25' column found in {args.input_file}")
            print(f"Available columns: {list(df.columns)}")
            sys.exit(1)
        
        # Set style
        plt.style.use('default')
        sns.set_theme(style="whitegrid")
        
        # Create figure with larger size
        plt.figure(figsize=(12, 6))
        
        # Define WHO guidelines for coloring
        who_breaks = [0, 15, 35, 55, 150, float('inf')]
        who_colors = ['#a8e05f', '#fdd64b', '#ff9b57', '#fe6a69', '#a97abc']
        who_labels = ['Good (≤15)', 'Moderate (15-35)', 'USG (35-55)', 'Unhealthy (55-150)', 'Very Unhealthy (>150)']
        
        # Create histogram with custom bins
        bins = np.linspace(0, 60, 61)  # Bins from 0 to 60 with 1 μg/m³ width
        n, bins, patches = plt.hist(df[pm25_col], bins=bins, alpha=0.7, edgecolor='black')
        
        # Color the bars according to WHO guidelines
        for i, patch in enumerate(patches):
            for j, break_point in enumerate(who_breaks[:-1]):
                if bins[i] >= break_point and bins[i] < who_breaks[j+1]:
                    patch.set_facecolor(who_colors[j])
                    break
        
        # Add vertical lines for WHO guidelines
        for break_point, color in zip(who_breaks[1:-1], who_colors[:-1]):
            plt.axvline(x=break_point, color=color, linestyle='--', alpha=0.5)
        
        # Generate dynamic title and filename
        countries_str = ", ".join(args.countries)
        # Use target date 
        target_datetime = datetime.strptime(args.target_date, "%Y-%m-%d")
        date_str = target_datetime.strftime("%Y%m%d")

        
        # Customize the plot
        plt.title(f'Distribution of PM2.5 Predictions ({date_str})\n{countries_str}', fontsize=14, pad=20)
        plt.xlabel('PM2.5 (μg/m³)', fontsize=12)
        plt.ylabel('Number of Grid Cells', fontsize=12)
        
        # Add statistics as text
        stats_text = f"""
Statistics:
Mean: {df[pm25_col].mean():.1f} μg/m³
Median: {df[pm25_col].median():.1f} μg/m³
Std: {df[pm25_col].std():.1f} μg/m³
Min: {df[pm25_col].min():.1f} μg/m³
Max: {df[pm25_col].max():.1f} μg/m³
"""
        plt.text(0.95, 0.95, stats_text,
                 transform=plt.gca().transAxes,
                 verticalalignment='top',
                 horizontalalignment='right',
                 bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
        
        # Add legend for WHO categories
        legend_elements = [plt.Rectangle((0,0),1,1, facecolor=color, alpha=0.7, edgecolor='black')
                          for color in who_colors]
        plt.legend(legend_elements, who_labels, title='WHO Guidelines',
                  loc='upper right', bbox_to_anchor=(1, 1))
        
        # Create output directory and filename
        plots_dir = Path('data/predictions/distribution')
        plots_dir.mkdir(parents=True, exist_ok=True)
        
        countries_filename = "_".join(sorted(args.countries))
        output_file = plots_dir / f'pm25_distribution_{date_str}_{countries_filename}.png'
        
        # Adjust layout and save
        plt.tight_layout()
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"✅ Histogram saved to: {output_file}")
        
    except Exception as e:
        print(f"ERROR: Failed to generate plot: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 