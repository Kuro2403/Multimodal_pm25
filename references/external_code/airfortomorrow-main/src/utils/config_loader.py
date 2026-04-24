#!/usr/bin/env python3
"""
Configuration Loader Utility

This module provides a centralized way to load and access configuration
from config.yaml. All scripts should use this module instead of loading
config directly to ensure consistency.

Usage:
    from src.utils.config_loader import ConfigLoader
    
    config = ConfigLoader()
    countries = config.get_countries()
    paths = config.get_paths()
    api_key = config.get_api_key('openaq')
"""

import os
import yaml
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
import logging


class ConfigLoader:
    """Centralized configuration loader for the air quality prediction system."""
    
    _instance = None
    _config = None
    
    def __new__(cls, config_path: Optional[str] = None):
        """Singleton pattern to ensure config is loaded only once."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize the configuration loader."""
        if self._config is None:
            self.logger = logging.getLogger(__name__)
            self._load_config(config_path)
    
    def _load_config(self, config_path: Optional[str] = None) -> None:
        """Load configuration from YAML file."""
        if config_path is None:
            # Try to find config.yaml relative to project root
            current_dir = Path(__file__).resolve()
            project_root = current_dir.parent.parent.parent  # src/utils/config_loader.py -> project_root
            config_path = project_root / "config" / "config.yaml"
        
        config_path = Path(config_path)
        
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                self._config = yaml.safe_load(f)
            self.logger.info(f"Configuration loaded from: {config_path}")
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in configuration file: {e}")
        except Exception as e:
            raise RuntimeError(f"Failed to load configuration: {e}")
    
    @property
    def config(self) -> Dict[str, Any]:
        """Get the raw configuration dictionary."""
        return self._config
    
    # ========================================
    # Country and Geographic Settings
    # ========================================
    
    def get_countries(self, data_source: Optional[str] = None) -> List[Union[str, int]]:
        """
        Get country list for a specific data source or default.
        
        Args:
            data_source: Specific data source ('openaq', 'airgradient', etc.)
                        If None, returns default country list
                        
        Returns:
            List of country codes (format depends on data source)
        """
        if data_source is None:
            return self._config['system']['countries']['default']
        
        if data_source == 'openaq':
            return self._config['data_collection']['openaq']['countries']
        elif data_source in ['airgradient', 'himawari', 'era5', 'firms']:
            return self._config['data_collection'][data_source]['countries']
        else:
            # Fallback to default
            self.logger.warning(f"Unknown data source '{data_source}', using default countries")
            return self._config['system']['countries']['default']
    
    def get_country_mapping(self, source_format: str, target_format: str) -> Dict[str, Union[str, int]]:
        """
        Get mapping between different country code formats.
        
        Args:
            source_format: Source format ('iso3', 'openaq_numeric')
            target_format: Target format ('iso3', 'openaq_numeric')
            
        Returns:
            Dictionary mapping source codes to target codes
        """
        mappings = self._config['system']['countries']['mappings']
        
        if source_format == target_format:
            # Return identity mapping
            source_codes = list(mappings[f"{source_format}_codes"].values())
            return {code: code for code in source_codes}
        
        # Create mapping between formats
        source_map = mappings[f"{source_format}_codes"]
        target_map = mappings[f"{target_format}_codes"]
        
        # Reverse target map to get code->country mapping
        target_reverse = {v: k for k, v in target_map.items()}
        
        # Create source->target mapping
        result = {}
        for country, source_code in source_map.items():
            if country in target_map:
                result[source_code] = target_map[country]
        
        return result
    
    def get_h3_resolution(self, context: str = 'default') -> int:
        """
        Get H3 resolution for different contexts.
        
        Args:
            context: Context ('default', 'kriging', 'maps')
            
        Returns:
            H3 resolution level
        """
        if context == 'default':
            return self._config['system']['geographic']['h3_resolution']
        elif context == 'kriging':
            return self._config['data_processing']['h3']['kriging_resolution']
        elif context == 'maps':
            return self._config['visualization']['maps']['default_resolution']
        else:
            return self._config['system']['geographic']['h3_resolution']
    
    def get_buffer_degrees(self) -> float:
        """Get geographic buffer in degrees for country boundaries."""
        return self._config['system']['geographic']['buffer_degrees']
    
    # ========================================
    # File Paths
    # ========================================
    
    def get_path(self, path_key: str, create_if_missing: bool = False) -> Path:
        """
        Get a file path from configuration.
        
        Args:
            path_key: Dot-notation path key (e.g., 'raw.openaq.historical')
            create_if_missing: Create directory if it doesn't exist
            
        Returns:
            Path object
        """
        path_parts = path_key.split('.')
        path_config = self._config['paths']
        
        try:
            for part in path_parts:
                path_config = path_config[part]
        except KeyError:
            raise ValueError(f"Path key not found in configuration: {path_key}")
        
        path_obj = Path(path_config)
        
        if create_if_missing and not path_obj.exists():
            path_obj.mkdir(parents=True, exist_ok=True)
            self.logger.info(f"Created directory: {path_obj}")
        
        return path_obj
    
    def get_paths(self) -> Dict[str, Any]:
        """Get all path configurations."""
        return self._config['paths']
    
    # ========================================
    # Data Collection Settings
    # ========================================
    
    def get_data_collection_config(self, data_source: str) -> Dict[str, Any]:
        """
        Get data collection configuration for a specific source.
        
        Args:
            data_source: Data source name ('openaq', 'airgradient', etc.)
            
        Returns:
            Configuration dictionary for the data source
        """
        if data_source not in self._config['data_collection']:
            raise ValueError(f"Data source not found in configuration: {data_source}")
        
        return self._config['data_collection'][data_source]
    
    def get_time_window(self, mode: str) -> int:
        """
        Get default time window for processing mode.
        
        Args:
            mode: Processing mode ('realtime' or 'historical')
            
        Returns:
            Time window in hours (realtime) or days (historical)
        """
        if mode == 'realtime':
            return self._config['system']['time_windows']['realtime_hours']
        elif mode == 'historical':
            return self._config['system']['time_windows']['historical_buffer_days']
        else:
            raise ValueError(f"Unknown mode: {mode}")
    
    # ========================================
    # API Keys and Credentials
    # ========================================
    
    def get_api_key(self, service: str) -> Optional[str]:
        """
        Get API key for a service from environment variables.
        
        Environment variable format: {SERVICE}_API_KEY (e.g., OPENAQ_API_KEY)
        
        Args:
            service: Service name ('openaq', etc.)
            
        Returns:
            API key string or None if not found
        """
        env_var_name = f"{service.upper()}_API_KEY"
        return os.getenv(env_var_name)
    
    def get_api_keys(self) -> Dict[str, str]:
        """Get all API keys."""
        return self._config.get('api_keys', {})
    
    # ========================================
    # Processing Configuration
    # ========================================
    
    def get_processing_config(self, category: str) -> Dict[str, Any]:
        """
        Get processing configuration for a category.
        
        Args:
            category: Configuration category ('h3', 'kriging', 'aggregation')
            
        Returns:
            Configuration dictionary
        """
        return self._config['data_processing'].get(category, {})
    
    def get_kriging_settings(self) -> Dict[str, Any]:
        """Get kriging-specific settings."""
        return self._config['data_processing']['kriging']
    
    def get_variogram_model(self) -> str:
        """Get the variogram model for kriging interpolation."""
        return self._config['data_processing']['kriging'].get('variogram_model', 'spherical')
    
    def get_aggregation_thresholds(self) -> Dict[str, float]:
        """Get aggregation threshold settings."""
        return self._config['data_processing']['aggregation']
    
    # ========================================
    # Model Configuration
    # ========================================
    
    def get_model_config(self, section: str = None) -> Dict[str, Any]:
        """
        Get model configuration.
        
        Args:
            section: Specific section ('features', 'training', 'validation', 'prediction')
                    If None, returns entire model config
                    
        Returns:
            Model configuration dictionary
        """
        model_config = self._config['model']
        
        if section is None:
            return model_config
        
        return model_config.get(section, {})
    
    def get_model_path(self) -> Path:
        """Get the path to the trained model file."""
        return Path(self._config['model']['path'])
    
    # ========================================
    # Execution Settings
    # ========================================
    
    def get_execution_config(self, category: str = None) -> Dict[str, Any]:
        """
        Get execution configuration.
        
        Args:
            category: Specific category ('timeouts', 'parallel', 'error_handling')
                     If None, returns entire execution config
                     
        Returns:
            Execution configuration dictionary
        """
        execution_config = self._config['execution']
        
        if category is None:
            return execution_config
        
        return execution_config.get(category, {})
    
    def get_timeout(self, operation: str = 'pipeline_default') -> int:
        """
        Get timeout value for an operation.
        
        Args:
            operation: Operation type ('pipeline_default', 'download_timeout', etc.)
            
        Returns:
            Timeout in seconds
        """
        return self._config['execution']['timeouts'].get(operation, 3600)
    
    # ========================================
    # Logging Configuration
    # ========================================
    
    def get_logging_config(self) -> Dict[str, str]:
        """Get logging configuration."""
        return self._config['logging']
    
    def setup_logging(self, logger_name: str = None) -> logging.Logger:
        """
        Set up logging with configuration from config file.
        
        Args:
            logger_name: Name for the logger (defaults to calling module)
            
        Returns:
            Configured logger instance
        """
        logging_config = self.get_logging_config()
        
        # Configure logging
        logging.basicConfig(
            level=getattr(logging, logging_config['level']),
            format=logging_config['format']
        )
        
        if logger_name is None:
            logger_name = __name__
        
        return logging.getLogger(logger_name)
    
    # ========================================
    # Utility Methods
    # ========================================
    
    def validate_config(self) -> bool:
        """
        Validate the loaded configuration for required fields.
        
        Returns:
            True if configuration is valid
            
        Raises:
            ValueError: If configuration is invalid
        """
        required_sections = [
            'system', 'data_collection', 'data_processing', 
            'paths', 'api_keys', 'model', 'execution'
        ]
        
        for section in required_sections:
            if section not in self._config:
                raise ValueError(f"Required configuration section missing: {section}")
        
        # Validate specific required fields
        if not self._config['system']['countries']['default']:
            raise ValueError("Default countries list cannot be empty")
        
        # Check for OpenAQ API key in environment
        if not os.getenv('OPENAQ_API_KEY'):
            self.logger.warning("OpenAQ API key not found in environment variables (OPENAQ_API_KEY)")
        
        return True
    
    def get_env_override(self, config_key: str, env_var: str, default: Any = None) -> Any:
        """
        Get configuration value with environment variable override.
        
        Args:
            config_key: Dot-notation config key (e.g., 'system.countries.default')
            env_var: Environment variable name
            default: Default value if neither config nor env var is found
            
        Returns:
            Configuration value, environment override, or default
        """
        # Check environment variable first
        env_value = os.getenv(env_var)
        if env_value is not None:
            return env_value
        
        # Fall back to config
        try:
            keys = config_key.split('.')
            value = self._config
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            return default


# ========================================
# Convenience Functions
# ========================================

def get_config() -> ConfigLoader:
    """Get the global configuration instance."""
    return ConfigLoader()


def get_countries(data_source: str = None) -> List[Union[str, int]]:
    """Get country list for a data source."""
    return get_config().get_countries(data_source)


def get_path(path_key: str, create_if_missing: bool = False) -> Path:
    """Get a path from configuration."""
    return get_config().get_path(path_key, create_if_missing)


def get_api_key(service: str) -> Optional[str]:
    """Get an API key."""
    return get_config().get_api_key(service)


# ========================================
# Migration Helper Functions
# ========================================

def get_legacy_country_codes(legacy_format: str = 'default') -> List[str]:
    """
    Get country codes in legacy format for backwards compatibility.
    
    Args:
        legacy_format: Legacy format identifier
        
    Returns:
        Country codes in legacy format
    """
    config = get_config()
    
    if legacy_format == 'openaq_numeric':
        return config.get_countries('openaq')
    elif legacy_format == 'airgradient':
        return config.get_countries('airgradient')
    else:
        return config.get_countries()


def get_legacy_paths() -> Dict[str, str]:
    """
    Get paths in legacy format for backwards compatibility.
    
    Returns:
        Dictionary of path keys to path strings
    """
    config = get_config()
    paths = config.get_paths()
    
    # Convert nested structure to flat keys for backwards compatibility
    legacy_paths = {}
    
    # Direct mappings
    legacy_paths['raw_data'] = str(paths['raw']['base'])
    legacy_paths['processed_data'] = str(paths['processed']['base'])
    legacy_paths['logs'] = str(paths['logs'])
    legacy_paths['assets'] = str(paths['assets'])
    
    # Specific data source paths
    legacy_paths['raw_data_openaq_historical'] = str(paths['raw']['openaq']['historical'])
    legacy_paths['raw_data_openaq_realtime'] = str(paths['raw']['openaq']['realtime'])
    legacy_paths['raw_data_airgradient_historical'] = str(paths['raw']['airgradient']['historical'])
    legacy_paths['raw_data_airgradient_realtime'] = str(paths['raw']['airgradient']['realtime'])
    
    return legacy_paths 