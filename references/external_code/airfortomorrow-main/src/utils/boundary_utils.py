#!/usr/bin/env python3
"""
Centralized boundary utilities for country boundary creation and processing.

This module consolidates all boundary creation functionality that was previously
duplicated across multiple files in the codebase.
"""

import pandas as pd
import geopandas as gpd
from shapely.ops import unary_union
from typing import List
import logging

import io, requests
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def _create_retry_session(retries=3, backoff_factor=1.0):
    """
    Create a requests session with retry logic for handling transient network errors.
    
    Args:
        retries: Number of retry attempts
        backoff_factor: Factor for exponential backoff between retries
        
    Returns:
        requests.Session with retry adapter configured
    """
    session = requests.Session()
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    
    # Mount adapter with retry strategy
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session


def create_country_boundaries(
    country_code_list: List[str], 
    buffer_degrees: float = 0.4
) -> gpd.GeoDataFrame:
    """
    Create a merged boundary for a list of countries with buffer.
    
    This function consolidates all the duplicated boundary creation logic
    from across the codebase into a single, well-tested utility.
    
    Args:
        country_code_list: List of country codes (e.g. ['THA', 'LAO'])
        buffer_degrees: Buffer size in degrees (default: 0.4)
        
    Returns:
        GeoDataFrame with merged boundaries
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Creating boundaries for countries: {country_code_list}")
    
    if not country_code_list:
        raise ValueError("country_code_list cannot be empty")
    
    # Get country boundaries for each country in the list
    country_boundaries_list = []
    session = _create_retry_session(retries=5, backoff_factor=2.0)
    
    for country_code in country_code_list:
        max_retries = 3
        retry_delay = 5  # seconds
        
        for attempt in range(max_retries):
            try:
                url = f'https://github.com/wmgeolab/geoBoundaries/raw/fcccfab7523d4d5e55dfc7f63c166df918119fd1/releaseData/gbOpen/{country_code}/ADM0/geoBoundaries-{country_code}-ADM0.geojson'
                
                logger.debug(f"Loading boundary for {country_code} (attempt {attempt + 1}/{max_retries})")
                
                resp = session.get(
                    url, 
                    headers={"User-Agent": "Mozilla/5.0"}, 
                    timeout=60,
                    verify=True
                )
                resp.raise_for_status()
                boundary = gpd.read_file(io.BytesIO(resp.content))

                country_boundaries_list.append(boundary)
                logger.info(f"Successfully loaded boundary for {country_code}")
                break  # Success, exit retry loop
                
            except (requests.exceptions.SSLError, requests.exceptions.ConnectionError) as e:
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (attempt + 1)
                    logger.warning(f"SSL/Connection error for {country_code} (attempt {attempt + 1}/{max_retries}): {e}")
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to load boundary for {country_code} after {max_retries} attempts: {e}")
                    raise
            except Exception as e:
                logger.error(f"Error loading boundary for {country_code}: {e}")
                raise
    
    if not country_boundaries_list:
        raise ValueError("No country boundaries could be loaded")
    
    # Merge all countries
    geo_df = gpd.GeoDataFrame(pd.concat(country_boundaries_list), geometry='geometry')
    
    # Add buffer to account for transborder effects
    geo_df = geo_df.buffer(buffer_degrees)
    
    # Create one single polygon out of all geometries
    merged_polygon = unary_union(geo_df.geometry)
    boundaries_countries = gpd.GeoDataFrame(geometry=[merged_polygon], crs="EPSG:4326")
    
    logger.info(f"Created boundary polygon for {len(country_code_list)} countries with {buffer_degrees}-degree buffer")
    
    return boundaries_countries


# Backward compatibility aliases for existing code
def create_boundaries_countries(country_code_list: List[str], buffer_degrees: float = 0.4) -> gpd.GeoDataFrame:
    """Alias for backward compatibility with existing code."""
    return create_country_boundaries(country_code_list, buffer_degrees) 