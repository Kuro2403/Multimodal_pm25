#!/usr/bin/env python3
"""
Sensor Validation Module for Air Quality Predictions

This module validates air quality predictions against real sensor measurements
and creates enhanced visualizations showing both predictions and sensor data.
"""

import logging
import sys
import os
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import List, Optional, Tuple, Dict
import warnings

import pandas as pd
import geopandas as gpd
import numpy as np
import h3
import matplotlib.pyplot as plt
from shapely.geometry import Polygon
from sklearn.metrics import mean_absolute_error, mean_squared_error

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.utils.config_loader import ConfigLoader

# Suppress warnings
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

# Configure logging
logger = logging.getLogger(__name__)

class SensorValidator:
    """
    Validates air quality predictions against real sensor measurements.
    
    This class loads processed sensor data, matches it with predictions,
    calculates validation metrics, and creates enhanced visualizations.
    """
    
    # AQI categories and colors (consistent with prediction maps)
    AQI_BREAKS = [0, 9.0, 35.4, 55.4, 125.4, 225.4, float("inf")]
    AQI_LABELS = ["1.Good", "2.Moderate", "3.USG", "4.Unhealthy", "5.Very Unhealthy", "6.Hazardous"]
    AQI_COLORS = {
        "1.Good": "#a8e05f",
        "2.Moderate": "#fdd64b",
        "3.USG": "#ff9b57",
        "4.Unhealthy": "#fe6a69",
        "5.Very Unhealthy": "#a97abc",
        "6.Hazardous": "#a87383"
    }
    
    def __init__(self, config_path: str = None):
        """
        Initialize the sensor validator.
        
        Args:
            config_path: Path to configuration file (optional)
        """
        self.config_loader = ConfigLoader(config_path)
        
        # Set up directories using configuration
        self.processed_aq_dir = self.config_loader.get_path('processed.airquality.base')
        self.predictions_dir = self.config_loader.get_path('predictions.base')
        self.validation_dir = self.config_loader.get_path('predictions.base')
        
        # Create validation subdirectories
        self.validation_data_dir = self.validation_dir / 'validation_data'
        self.validation_map_dir = self.validation_dir / 'validation_map'
        self.validation_scatter_dir = self.validation_dir / 'scatter'
        
        # Ensure all directories exist
        self.validation_dir.mkdir(parents=True, exist_ok=True)
        self.validation_data_dir.mkdir(parents=True, exist_ok=True)
        self.validation_map_dir.mkdir(parents=True, exist_ok=True)
        self.validation_scatter_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info("SensorValidator initialized")
        logger.info(f"  Processed AQ data: {self.processed_aq_dir}")
        logger.info(f"  Predictions data: {self.predictions_dir}")
        logger.info(f"  Validation output: {self.validation_dir}")
        logger.info(f"    → Data files: {self.validation_data_dir}")
        logger.info(f"    → Maps: {self.validation_map_dir}")
        logger.info(f"    → Scatter plots: {self.validation_scatter_dir}")
    
    def _assign_aqi_category(self, pm25_value: float) -> str:
        """Assign AQI category based on PM2.5 value."""
        for i, break_point in enumerate(self.AQI_BREAKS[1:]):
            if pm25_value <= break_point:
                return self.AQI_LABELS[i]
        return self.AQI_LABELS[-1]
    
    def load_sensor_data(
        self,
        countries: List[str],
        target_date: date = None,
        mode: str = "realtime"
    ) -> pd.DataFrame:
        """
        Load processed sensor data for validation.
        
        Args:
            countries: List of country codes
            target_date: Target date (defaults to today if not provided)
            mode: Processing mode ('realtime' or 'historical')
            
        Returns:
            DataFrame with sensor data including daily means per H3 cell
        """
        logger.info(f"Loading sensor data for {mode} validation")
        logger.info(f"Countries: {countries}")
        logger.info(f"Target date: {target_date}")
        logger.info(f"Processed AQ dir: {self.processed_aq_dir}")
        
        # Build file pattern based on mode
        countries_str = "_".join(sorted(countries))
        aq_data_dir = self.processed_aq_dir / mode  # Use mode instead of hardcoded "realtime"
        
        logger.info(f"Looking for sensor data in: {aq_data_dir}")
        logger.info(f"Countries input: {countries}")
        logger.info(f"Countries string: {countries_str}")
        
        if not aq_data_dir.exists():
            logger.warning(f"Processed air quality directory not found: {aq_data_dir}")
            return pd.DataFrame()
        
        # Show all available files for debugging
        all_files = list(aq_data_dir.glob("*.parquet"))
        logger.info(f"Available files in {aq_data_dir}: {[f.name for f in all_files]}")
        
        # Find matching files based on mode
        if target_date:
            date_str = target_date.strftime("%Y%m%d")
            date_str_hyphen = target_date.strftime("%Y-%m-%d")
            
            # Try single date format first (realtime style)
            pattern = f"air_quality_{mode}_{countries_str}_{date_str}.parquet"
            logger.info(f"Trying single-date pattern: {pattern}")
            matching_files = list(aq_data_dir.glob(pattern))
            logger.info(f"Single-date matched files: {[f.name for f in matching_files]}")
            
            # If no match, try date range format (historical style)
            if not matching_files:
                # Historical files use date ranges: air_quality_historical_LAO_THA_2025-01-01_to_2025-01-06.parquet
                # We need to find files where target_date falls within the range
                range_pattern = f"air_quality_{mode}_{countries_str}_*_to_*.parquet"
                logger.info(f"Trying date-range pattern: {range_pattern}")
                all_range_files = list(aq_data_dir.glob(range_pattern))
                logger.info(f"Found {len(all_range_files)} date-range files")
                
                # Filter files where target_date falls within the date range
                for file in all_range_files:
                    try:
                        # Extract start and end dates from filename
                        # Format: air_quality_historical_LAO_THA_YYYY-MM-DD_to_YYYY-MM-DD.parquet
                        import re
                        match = re.search(r'(\d{4}-\d{2}-\d{2})_to_(\d{4}-\d{2}-\d{2})\.parquet$', file.name)
                        if match:
                            file_start = datetime.strptime(match.group(1), '%Y-%m-%d').date()
                            file_end = datetime.strptime(match.group(2), '%Y-%m-%d').date()
                            if file_start <= target_date <= file_end:
                                matching_files.append(file)
                                logger.info(f"Target date {target_date} falls within {file.name} ({file_start} to {file_end})")
                    except Exception as e:
                        logger.debug(f"Could not parse date range from {file.name}: {e}")
                        continue
                
                logger.info(f"Date-range matched files: {[f.name for f in matching_files]}")
        else:
            # Use today's date as fallback
            date_str = datetime.now().strftime("%Y%m%d")
            pattern = f"air_quality_{mode}_{countries_str}_{date_str}.parquet"
            logger.info(f"Trying pattern: {pattern}")
            matching_files = list(aq_data_dir.glob(pattern))
            logger.info(f"Matched files: {[f.name for f in matching_files]}")
        
        if not matching_files:
            # Final fallback: try finding the most recent file
            fallback_pattern = f"air_quality_{mode}_{countries_str}_*.parquet"
            logger.info(f"No exact match found, trying fallback pattern: {fallback_pattern}")
            matching_files = sorted(aq_data_dir.glob(fallback_pattern))
            logger.info(f"Fallback matched files: {[f.name for f in matching_files]}")
            if matching_files:
                matching_files = [matching_files[-1]]  # Get most recent
                logger.info(f"Using most recent file: {matching_files[0].name}")
        
        if not matching_files:
            logger.warning(f"No processed air quality files found matching pattern in {aq_data_dir}")
            return pd.DataFrame()
        
        logger.info(f"Found {len(matching_files)} sensor data files")
        
        # Load and combine sensor data
        sensor_dfs = []
        for file in matching_files:
            try:
                logger.info(f"Loading sensor data from: {file.name}")
                df = pd.read_parquet(file)
                
                # Ensure required columns exist
                if not all(col in df.columns for col in ['latitude', 'longitude', 'value', 'h3_08_text']):
                    logger.warning(f"Missing required columns in {file.name}")
                    continue
                
                # If we loaded from a date-range file, filter to target_date
                if target_date and 'datetime_utc' in df.columns:
                    df['datetime_utc'] = pd.to_datetime(df['datetime_utc'])
                    df['date'] = df['datetime_utc'].dt.date
                    records_before = len(df)
                    df = df[df['date'] == target_date].copy()
                    records_after = len(df)
                    if records_after < records_before:
                        logger.info(f"Filtered to target date {target_date}: {records_before} -> {records_after} records")
                    
                    if df.empty:
                        logger.warning(f"No data for target date {target_date} in {file.name}")
                        continue
                
                # Add file source for tracking
                df['source_file'] = file.name
                sensor_dfs.append(df)
                
            except Exception as e:
                logger.error(f"Error loading sensor file {file}: {e}")
                continue
        
        if not sensor_dfs:
            logger.warning("No valid sensor data loaded")
            return pd.DataFrame()
        
        # Combine all sensor data
        combined_df = pd.concat(sensor_dfs, ignore_index=True)
        logger.info(f"Combined sensor data: {len(combined_df)} records")
        
        # Convert H3 cells to consistent format
        combined_df['h3_08'] = combined_df['h3_08_text'].astype(str)
        
        # Show data structure for debugging
        logger.info(f"Sensor data columns: {list(combined_df.columns)}")
        if 'datetime_utc' in combined_df.columns:
            logger.info(f"Datetime range: {combined_df['datetime_utc'].min()} to {combined_df['datetime_utc'].max()}")
        
        # Group hourly measurements to get one value per sensor location
        # First, identify unique sensor locations
        logger.info("Aggregating hourly measurements to daily means per sensor location...")
        
        # Group by sensor location (using location_id or h3_08 if available)
        if 'location_id' in combined_df.columns and combined_df['location_id'].notna().any():
            # Use location_id as primary grouping key
            grouping_cols = ['location_id', 'h3_08']
            logger.info("Grouping by location_id and H3 cell")
        else:
            # Fall back to H3 cell and lat/lon
            grouping_cols = ['h3_08']
            logger.info("Grouping by H3 cell only")
        
        # Calculate daily mean PM2.5 for each sensor location
        aggregation_dict = {
            'value': ['mean', 'count', 'std'],  # Mean, count of measurements, standard deviation
            'latitude': 'first',
            'longitude': 'first'
        }
        
        # Add location_id and source if available
        if 'location_id' in combined_df.columns:
            aggregation_dict['location_id'] = 'first'
        if 'source' in combined_df.columns:
            aggregation_dict['source'] = 'first'
        
        # Perform aggregation
        daily_means = combined_df.groupby(grouping_cols).agg(aggregation_dict).reset_index()
        
        # Flatten column names
        daily_means.columns = [
            col[0] if col[1] == '' else f"{col[0]}_{col[1]}" 
            for col in daily_means.columns
        ]
        
        # Rename value columns for clarity
        daily_means = daily_means.rename(columns={
            'value_mean': 'sensor_pm25',
            'value_count': 'measurement_count',
            'value_std': 'measurement_std'
        })
        
        # No minimum measurement threshold - accept all sensor locations
        logger.info(f"Keeping all sensor locations regardless of measurement count")
        
        logger.info(f"Sensor locations after aggregation: {len(daily_means)}")
        logger.info(f"Average measurements per location: {daily_means['measurement_count'].mean():.1f}")
        logger.info(f"Measurement count range: {daily_means['measurement_count'].min()} - {daily_means['measurement_count'].max()}")
        
        # Add AQI category for sensors
        daily_means['sensor_aqi_category'] = daily_means['sensor_pm25'].apply(self._assign_aqi_category)
        
        logger.info(f"Processed sensor data: {len(daily_means)} unique sensor locations with daily means")
        logger.info(f"PM2.5 range: {daily_means['sensor_pm25'].min():.1f} - {daily_means['sensor_pm25'].max():.1f} μg/m³")
        logger.info(f"Sensors with high variability (std > 5): {(daily_means['measurement_std'] > 5).sum()}")
        
        return daily_means
    
    def load_prediction_data(self, prediction_file: str) -> pd.DataFrame:
        """
        Load prediction data from parquet file.
        
        Args:
            prediction_file: Path to prediction parquet file
            
        Returns:
            DataFrame with prediction data
        """
        try:
            logger.info(f"Loading prediction data from: {prediction_file}")
            pred_df = pd.read_parquet(prediction_file)
            
            # Ensure required columns exist
            required_cols = ['cell', 'predicted_pm25']
            if not all(col in pred_df.columns for col in required_cols):
                raise ValueError(f"Missing required columns in prediction file: {required_cols}")
            
            # Convert H3 cell to string format for matching
            pred_df['h3_08'] = pred_df['cell'].astype(str)
            
            # Add AQI category for predictions
            pred_df['predicted_aqi_category'] = pred_df['predicted_pm25'].apply(self._assign_aqi_category)
            
            logger.info(f"Loaded predictions: {len(pred_df)} H3 cells")
            logger.info(f"Predicted PM2.5 range: {pred_df['predicted_pm25'].min():.1f} - {pred_df['predicted_pm25'].max():.1f} μg/m³")
            
            return pred_df
            
        except Exception as e:
            logger.error(f"Error loading prediction file {prediction_file}: {e}")
            return pd.DataFrame()
    
    def validate_predictions(
        self,
        sensor_df: pd.DataFrame,
        prediction_df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, Dict]:
        """
        Validate predictions against sensor measurements.
        
        Args:
            sensor_df: DataFrame with sensor measurements
            prediction_df: DataFrame with predictions
            
        Returns:
            Tuple of (matched_data_df, validation_metrics_dict)
        """
        logger.info("Validating predictions against sensor measurements...")
        
        if sensor_df.empty or prediction_df.empty:
            logger.warning("Empty sensor or prediction data - cannot validate")
            return pd.DataFrame(), {}
        
        # Merge predictions with sensor data on H3 cell
        matched_df = sensor_df.merge(
            prediction_df[['h3_08', 'predicted_pm25', 'predicted_aqi_category']],
            on='h3_08',
            how='inner'
        )
        
        if matched_df.empty:
            logger.warning("No matching H3 cells between sensors and predictions")
            return pd.DataFrame(), {}
        
        logger.info(f"Successfully matched {len(matched_df)} sensor locations with predictions")
        
        # Calculate validation metrics
        sensor_values = matched_df['sensor_pm25'].values
        predicted_values = matched_df['predicted_pm25'].values
        
        # Calculate deviations
        matched_df['absolute_error'] = np.abs(sensor_values - predicted_values)
        matched_df['relative_error'] = matched_df['absolute_error'] / np.maximum(sensor_values, 1.0)  # Avoid division by zero
        matched_df['bias'] = predicted_values - sensor_values  # Positive = overprediction
        
        # Calculate overall metrics including R²
        correlation = np.corrcoef(sensor_values, predicted_values)[0, 1]
        r_squared = correlation ** 2 if not np.isnan(correlation) else 0.0
        
        metrics = {
            'n_matched': len(matched_df),
            'mae': mean_absolute_error(sensor_values, predicted_values),
            'rmse': np.sqrt(mean_squared_error(sensor_values, predicted_values)),
            'mean_bias': np.mean(matched_df['bias']),
            'mean_relative_error': np.mean(matched_df['relative_error']),
            'correlation': correlation,
            'r_squared': r_squared,
            'sensor_mean': np.mean(sensor_values),
            'sensor_std': np.std(sensor_values),
            'prediction_mean': np.mean(predicted_values),
            'prediction_std': np.std(predicted_values)
        }
        
        # Add category-specific agreement
        category_agreement = (matched_df['sensor_aqi_category'] == matched_df['predicted_aqi_category']).mean()
        metrics['category_agreement'] = category_agreement
        
        logger.info("Validation metrics calculated:")
        logger.info(f"  Matched locations: {metrics['n_matched']}")
        logger.info(f"  MAE: {metrics['mae']:.2f} μg/m³")
        logger.info(f"  RMSE: {metrics['rmse']:.2f} μg/m³")
        logger.info(f"  Mean Bias: {metrics['mean_bias']:.2f} μg/m³")
        logger.info(f"  Correlation: {metrics['correlation']:.3f}")
        logger.info(f"  R²: {metrics['r_squared']:.3f}")
        logger.info(f"  AQI Category Agreement: {metrics['category_agreement']:.1%}")
        
        return matched_df, metrics
    
    def create_enhanced_map(
        self,
        prediction_df: pd.DataFrame,
        sensor_df: pd.DataFrame,
        matched_df: pd.DataFrame,
        countries: List[str],
        target_date: date = None,
        map_resolution: int = 6,
        output_path: str = None
    ) -> str:
        """
        Create enhanced map showing both predictions and sensor validation points.
        
        Args:
            prediction_df: DataFrame with predictions
            sensor_df: DataFrame with sensor measurements
            matched_df: DataFrame with matched predictions and sensors
            countries: List of country codes
            target_date: Target date for the map
            map_resolution: H3 resolution for map aggregation
            output_path: Custom output path (optional)
            
        Returns:
            Path to saved map file
        """
        logger.info("Creating enhanced validation map...")
        
        try:
            # Aggregate predictions to target resolution
            prediction_df['h3_res_low'] = prediction_df['cell'].apply(
                lambda x: h3.h3_to_parent(h3.h3_to_string(x), map_resolution)
            )
            
            # Calculate mean PM2.5 values per aggregated hexagon
            df_agg = prediction_df.groupby('h3_res_low', as_index=False)['predicted_pm25'].mean()
            logger.info(f"Aggregated predictions from {len(prediction_df)} cells to {len(df_agg)} hexagons")
            
            # Convert aggregated H3 cells to hexagon geometries
            hexagons = []
            pm25_values = []
            
            for _, row in df_agg.iterrows():
                try:
                    h3_id = row['h3_res_low']
                    # Get hexagon boundary
                    boundary = h3.h3_to_geo_boundary(h3_id, geo_json=True)
                    # Create polygon (h3 geo_json=True returns coords as [lng, lat])
                    coords = [(pt[0], pt[1]) for pt in boundary]
                    hexagon = Polygon(coords)
                    hexagons.append(hexagon)
                    pm25_values.append(row['predicted_pm25'])
                except Exception as e:
                    continue
            
            if not hexagons:
                logger.error("No valid hexagons for map generation")
                return None
            
            # Create GeoDataFrame for predictions
            gdf_hex = gpd.GeoDataFrame({
                'PM25': pm25_values,
                'geometry': hexagons
            })
            
            # Assign AQI categories to predictions
            gdf_hex['AQI_Category'] = gdf_hex['PM25'].apply(self._assign_aqi_category)
            
            # Create GeoDataFrame for sensors
            # Handle different possible column names for coordinates
            longitude_col = 'longitude_first' if 'longitude_first' in sensor_df.columns else 'longitude'
            latitude_col = 'latitude_first' if 'latitude_first' in sensor_df.columns else 'latitude'
            
            gdf_sensor = gpd.GeoDataFrame(
                sensor_df,
                geometry=gpd.points_from_xy(sensor_df[longitude_col], sensor_df[latitude_col]),
                crs="EPSG:4326"
            )
            
            # Create the enhanced map
            fig, ax = plt.subplots(figsize=(12, 10))
            
            # Plot prediction hexagons by AQI category
            legend_handles = []
            for category in self.AQI_LABELS:
                subset = gdf_hex[gdf_hex["AQI_Category"] == category]
                if not subset.empty:
                    subset.plot(
                        ax=ax,
                        color=self.AQI_COLORS[category],
                        edgecolor=None,
                        alpha=0.7,
                        label=f"{category} (Predicted)"
                    )
            
            # Plot sensor points by AQI category
            for category in self.AQI_LABELS:
                subset = gdf_sensor[gdf_sensor["sensor_aqi_category"] == category]
                if not subset.empty:
                    subset.plot(
                        ax=ax,
                        color=self.AQI_COLORS[category],
                        markersize=40,
                        marker="o",
                        alpha=0.9,
                        edgecolor="black",
                        linewidth=1,
                        label=f"{category} (Sensor)"
                    )
            
            # Add matched sensors with special styling (show validation quality)
            if not matched_df.empty:
                # Use same coordinate column detection for matched data
                longitude_col_matched = 'longitude_first' if 'longitude_first' in matched_df.columns else 'longitude'
                latitude_col_matched = 'latitude_first' if 'latitude_first' in matched_df.columns else 'latitude'
                
                gdf_matched = gpd.GeoDataFrame(
                    matched_df,
                    geometry=gpd.points_from_xy(matched_df[longitude_col_matched], matched_df[latitude_col_matched]),
                    crs="EPSG:4326"
                )
                
                # Plot matched sensors with black border to highlight validation points
                gdf_matched.plot(
                    ax=ax,
                    color='none',
                    markersize=50,
                    marker="o",
                    edgecolor="black",
                    linewidth=0.1,
                    alpha=1.0
                )
            
            # Customize the map
            date_str = target_date.strftime("%Y-%m-%d") if target_date else datetime.now().strftime("%Y-%m-%d")
            countries_str = ", ".join(countries)
            
            ax.set_title(
                f"PM2.5 Predictions vs Sensor Measurements\n{countries_str} - {date_str}\n"
                f"Hexagons: Predictions | Circles: Sensor Data | Black rings: Validation points",
                fontsize=14,
                pad=20
            )
            ax.axis("off")
            
            # Create custom legend
            from matplotlib.patches import Patch
            from matplotlib.lines import Line2D
            
            legend_elements = []
            
            # Add AQI category patches
            for category in self.AQI_LABELS:
                if (not gdf_hex[gdf_hex["AQI_Category"] == category].empty or 
                    not gdf_sensor[gdf_sensor["sensor_aqi_category"] == category].empty):
                    legend_elements.append(
                        Patch(facecolor=self.AQI_COLORS[category], label=category)
                    )
            
            # Add explanation elements
            legend_elements.extend([
                Line2D([0], [0], marker='s', color='w', markerfacecolor='gray', 
                       markersize=10, label='Predictions (Hexagons)', linestyle='None'),
                Line2D([0], [0], marker='o', color='w', markerfacecolor='gray', 
                       markersize=8, label='Sensors (Circles)', linestyle='None'),
                Line2D([0], [0], marker='o', color='w', markerfacecolor='none', 
                       markeredgecolor='black', markeredgewidth=0.1, markersize=10, 
                       label='Validation Points', linestyle='None')
            ])
            
            ax.legend(handles=legend_elements, loc="lower left", fontsize=10, frameon=True)
            
            plt.tight_layout()
            
            # Generate output path
            if not output_path:
                countries_filename = "_".join(sorted(countries))
                date_filename = target_date.strftime("%Y%m%d") if target_date else datetime.now().strftime("%Y%m%d")
                filename = f"enhanced_aqi_map_{date_filename}_{countries_filename}.png"
                output_path = self.validation_map_dir / filename
            
            # Save map
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            logger.info(f"Enhanced validation map saved to: {output_path}")
            logger.info(f"Map includes {len(gdf_hex)} prediction hexagons and {len(gdf_sensor)} sensor points")
            if not matched_df.empty:
                logger.info(f"Highlighted {len(matched_df)} validation points")
            
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Failed to generate enhanced map: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    def create_scatter_plot(
        self,
        matched_df: pd.DataFrame,
        metrics: Dict,
        countries: List[str],
        target_date: date = None,
        output_path: str = None
    ) -> str:
        """
        Create scatter plot of predicted vs actual PM2.5 values with key metrics.
        
        Args:
            matched_df: DataFrame with matched predictions and sensor measurements
            metrics: Dictionary with validation metrics
            countries: List of country codes
            target_date: Target date for the plot
            output_path: Custom output path (optional)
            
        Returns:
            Path to saved scatter plot file
        """
        logger.info("Creating validation scatter plot...")
        
        try:
            if matched_df.empty:
                logger.warning("No matched data available for scatter plot")
                return None
            
            # Create the scatter plot
            fig, ax = plt.subplots(figsize=(10, 8))
            
            # Get data
            sensor_values = matched_df['sensor_pm25'].values
            predicted_values = matched_df['predicted_pm25'].values
            
            # Create scatter plot colored by AQI category
            for category in self.AQI_LABELS:
                mask = matched_df['sensor_aqi_category'] == category
                if mask.any():
                    ax.scatter(
                        sensor_values[mask],
                        predicted_values[mask],
                        c=self.AQI_COLORS[category],
                        alpha=0.6,
                        s=50,
                        label=category,
                        edgecolors='black',
                        linewidth=0.5
                    )
            
            # Add 1:1 line (perfect prediction)
            max_val = max(sensor_values.max(), predicted_values.max())
            min_val = min(sensor_values.min(), predicted_values.min())
            ax.plot([min_val, max_val], [min_val, max_val], 'k--', alpha=0.7, linewidth=1, label='Perfect Prediction (1:1)')
            
            # Add trend line
            z = np.polyfit(sensor_values, predicted_values, 1)
            p = np.poly1d(z)
            ax.plot(sensor_values, p(sensor_values), "r-", alpha=0.8, linewidth=2, label=f'Trend Line (y = {z[0]:.2f}x + {z[1]:.2f})')
            
            # Set labels and title
            ax.set_xlabel('Actual PM2.5 (μg/m³)', fontsize=12, fontweight='bold')
            ax.set_ylabel('Predicted PM2.5 (μg/m³)', fontsize=12, fontweight='bold')
            
            countries_str = ", ".join(countries)
            date_str = target_date.strftime("%Y-%m-%d") if target_date else datetime.now().strftime("%Y-%m-%d")
            ax.set_title(f'PM2.5 Prediction Validation\n{countries_str} - {date_str}', fontsize=14, fontweight='bold', pad=20)
            
            # Add metrics text box
            metrics_text = (
                f"n = {metrics.get('n_matched', 0):,} points\n"
                f"R² = {metrics.get('r_squared', 0):.3f}\n"
                f"Correlation = {metrics.get('correlation', 0):.3f}\n"
                f"MAE = {metrics.get('mae', 0):.2f} μg/m³\n"
                f"RMSE = {metrics.get('rmse', 0):.2f} μg/m³\n"
                f"Mean Bias = {metrics.get('mean_bias', 0):.2f} μg/m³\n"
                f"Category Agreement = {metrics.get('category_agreement', 0):.1%}"
            )
            
            # Add text box with metrics
            props = dict(boxstyle='round', facecolor='white', alpha=0.8)
            ax.text(0.02, 0.98, metrics_text, transform=ax.transAxes, fontsize=10,
                   verticalalignment='top', bbox=props, family='monospace')
            
            # Set equal aspect ratio and grid
            ax.set_aspect('equal', adjustable='box')
            ax.grid(True, alpha=0.3)
            ax.legend(loc='lower right', fontsize=9)
            
            # Adjust layout
            plt.tight_layout()
            
            # Generate output path if not provided
            if not output_path:
                countries_filename = "_".join(sorted(countries))
                date_filename = target_date.strftime("%Y%m%d") if target_date else datetime.now().strftime("%Y%m%d")
                filename = f"validation_scatter_{date_filename}_{countries_filename}.png"
                output_path = self.validation_scatter_dir / filename
            
            # Save plot
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            logger.info(f"Validation scatter plot saved to: {output_path}")
            
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Failed to generate scatter plot: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    def save_validation_results(
        self,
        matched_df: pd.DataFrame,
        metrics: Dict,
        countries: List[str],
        target_date: date = None
    ) -> str:
        """
        Save validation results to files.
        
        Args:
            matched_df: DataFrame with matched predictions and sensors
            metrics: Dictionary with validation metrics
            countries: List of country codes
            target_date: Target date for realtime processing
            
        Returns:
            Path to saved validation file
        """
        try:
            # Generate filename
            countries_str = "_".join(sorted(countries))
            date_str = target_date.strftime("%Y%m%d") if target_date else datetime.now().strftime("%Y%m%d")
            
            # Save detailed validation data
            validation_file = self.validation_data_dir / f"sensor_validation_realtime_{date_str}_{countries_str}.parquet"
            matched_df.to_parquet(validation_file, index=False)
            
            # Save metrics summary
            metrics_file = self.validation_data_dir / f"validation_metrics_realtime_{date_str}_{countries_str}.json"
            import json
            with open(metrics_file, 'w') as f:
                # Convert numpy types to Python types for JSON serialization
                json_metrics = {}
                for key, value in metrics.items():
                    if isinstance(value, np.floating):
                        json_metrics[key] = float(value)
                    elif isinstance(value, np.integer):
                        json_metrics[key] = int(value)
                    else:
                        json_metrics[key] = value
                
                json.dump(json_metrics, f, indent=2)
            
            logger.info(f"Validation results saved:")
            logger.info(f"  Data: {validation_file}")
            logger.info(f"  Metrics: {metrics_file}")
            
            return str(validation_file)
            
        except Exception as e:
            logger.error(f"Failed to save validation results: {e}")
            return None
    
    def run_validation(
        self,
        prediction_file: str,
        countries: List[str] = None,
        target_date: date = None,
        map_resolution: int = 6,
        generate_map: bool = True,
        mode: str = "realtime"
    ) -> Dict:
        """
        Run complete validation workflow for predictions.
        
        Args:
            prediction_file: Path to prediction parquet file
            countries: List of country codes
            target_date: Target date for processing
            map_resolution: H3 resolution for map generation
            generate_map: Whether to generate enhanced map
            mode: Processing mode ('realtime' or 'historical')
            
        Returns:
            Dictionary with validation results and paths
        """
        logger.info("="*60)
        logger.info("STARTING SENSOR VALIDATION")
        logger.info("="*60)
        
        if not countries:
            countries = self.config_loader.get_countries()
        
        try:
            # Load data
            logger.info("Step 1: Loading prediction data...")
            prediction_df = self.load_prediction_data(prediction_file)
            if prediction_df.empty:
                raise ValueError("No prediction data loaded")
            
            logger.info("Step 2: Loading sensor data...")
            sensor_df = self.load_sensor_data(countries, target_date, mode=mode)
            if sensor_df.empty:
                logger.warning("No sensor data available for validation")
                return {"status": "no_sensor_data", "message": "No sensor data available for validation"}
            
            # Validate predictions
            logger.info("Step 3: Validating predictions against sensors...")
            matched_df, metrics = self.validate_predictions(sensor_df, prediction_df)
            if matched_df.empty:
                logger.warning("No matching locations between predictions and sensors")
                return {"status": "no_matches", "message": "No matching locations found"}
            
            # Save validation results
            logger.info("Step 4: Saving validation results...")
            validation_file = self.save_validation_results(
                matched_df, metrics, countries, target_date
            )
            
            results = {
                "status": "success",
                "validation_file": validation_file,
                "metrics": metrics,
                "n_matched": len(matched_df),
                "n_sensors": len(sensor_df),
                "n_predictions": len(prediction_df)
            }
            
            # Generate enhanced map
            if generate_map:
                logger.info("Step 5: Generating enhanced validation map...")
                map_path = self.create_enhanced_map(
                    prediction_df, sensor_df, matched_df, countries, target_date, map_resolution
                )
                if map_path:
                    results["map_file"] = map_path
            
            # Generate scatter plot
            logger.info("Step 6: Generating validation scatter plot...")
            scatter_path = self.create_scatter_plot(
                matched_df, metrics, countries, target_date
            )
            if scatter_path:
                results["scatter_plot"] = scatter_path
            
            logger.info("="*60)
            logger.info("SENSOR VALIDATION COMPLETED SUCCESSFULLY")
            logger.info(f"Validated {len(matched_df)} locations")
            logger.info(f"MAE: {metrics['mae']:.2f} μg/m³, R²: {metrics['r_squared']:.3f}")
            logger.info(f"Category Agreement: {metrics['category_agreement']:.1%}")
            if scatter_path:
                logger.info(f"Generated scatter plot with key metrics")
            logger.info("="*60)
            
            return results
            
        except Exception as e:
            logger.error("="*60)
            logger.error("SENSOR VALIDATION FAILED")
            logger.error(f"Error: {str(e)}")
            logger.error("="*60)
            import traceback
            logger.error(traceback.format_exc())
            
            return {"status": "error", "message": str(e)}


def main():
    """Main function for command line usage."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Validate air quality predictions against sensor measurements")
    
    parser.add_argument('--prediction-file', type=str, required=True,
                       help='Path to prediction parquet file')
    
    parser.add_argument('--countries', nargs='+', default=None,
                       help='Country codes (default: from config)')
    
    parser.add_argument('--target-date', type=str,
                       help='Target date for realtime processing (YYYY-MM-DD)')
    
    parser.add_argument('--map-resolution', type=int, default=6,
                       help='H3 resolution for map generation (default: 6)')
    
    parser.add_argument('--no-map', action='store_true',
                       help='Skip map generation')
    
    parser.add_argument('--config', type=str,
                       help='Path to configuration file (optional)')
    
    args = parser.parse_args()
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Parse target date
    target_date = None
    if args.target_date:
        target_date = datetime.strptime(args.target_date, '%Y-%m-%d').date()
    
    # Initialize validator
    validator = SensorValidator(args.config)
    
    # Run validation
    results = validator.run_validation(
        prediction_file=args.prediction_file,
        countries=args.countries,
        target_date=target_date,
        map_resolution=args.map_resolution,
        generate_map=not args.no_map
    )
    
    # Print results
    if results["status"] == "success":
        print(f"\n✅ Validation completed successfully!")
        print(f"📊 Metrics:")
        metrics = results["metrics"]
        print(f"   • Matched locations: {results['n_matched']}")
        print(f"   • MAE: {metrics['mae']:.2f} μg/m³")
        print(f"   • RMSE: {metrics['rmse']:.2f} μg/m³")
        print(f"   • Correlation: {metrics['correlation']:.3f}")
        print(f"   • R²: {metrics['r_squared']:.3f}")
        print(f"   • AQI Category Agreement: {metrics['category_agreement']:.1%}")
        print(f"📁 Files:")
        print(f"   • Validation data: {results['validation_file']}")
        if "map_file" in results:
            print(f"   • Enhanced map: {results['map_file']}")
        if "scatter_plot" in results:
            print(f"   • Scatter plot: {results['scatter_plot']}")
    else:
        print(f"\n❌ Validation failed: {results['message']}")
        exit(1)


if __name__ == "__main__":
    main() 