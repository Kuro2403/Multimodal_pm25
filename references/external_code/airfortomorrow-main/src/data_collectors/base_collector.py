from abc import ABC, abstractmethod
import logging
from typing import Dict, Any, List
import yaml
from pathlib import Path

# Import the new configuration loader
from src.utils.config_loader import ConfigLoader, get_legacy_paths

class BaseCollector(ABC):
    """Base class for data collection."""
    
    def __init__(self, config_path: str = None):
        """
        Initialize the collector with configuration.
        
        Args:
            config_path: Path to config file (optional, uses default if None)
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Use new configuration loader
        self.config_loader = ConfigLoader(config_path)
        
        # Backwards compatibility: provide old config interface
        self.config = self._create_legacy_config()
        
    def _create_legacy_config(self) -> Dict[str, Any]:
        """
        Create a config dictionary that matches the old format for backwards compatibility.
        
        Returns:
            Dictionary with legacy config structure
        """
        # Get the new config
        new_config = self.config_loader.config
        
        # Create legacy structure
        legacy_config = {
            # Legacy data collection settings
            'data_collection': {
                'country_codes': self.config_loader.get_countries(),
                'country_codes_openaq': self.config_loader.get_countries('openaq'),
                'country_codes_airgradient': self.config_loader.get_countries('airgradient'),
                'indicators': new_config['data_collection']['openaq']['indicators']
            },
            
            # Legacy paths with Path objects (as expected by old code)
            'paths': {},
            
            # API keys (now from environment variables)
            'api_keys': {},
            
            # Other legacy fields
            'data_processing': new_config['data_processing'],
            'logging': new_config['logging'],
            'aws': new_config['aws']
        }
        
        # Convert paths to Path objects as expected by old code
        legacy_paths = get_legacy_paths()
        for key, path_str in legacy_paths.items():
            legacy_config['paths'][key] = Path(path_str)
        
        # Add AirGradient specific settings for backwards compatibility
        if 'airgradient' in new_config['data_collection']:
            legacy_config['airgradient'] = new_config['data_collection']['airgradient']
        
        return legacy_config
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """
        Legacy method for loading configuration.
        
        DEPRECATED: Use self.config_loader instead.
        This method is kept for backwards compatibility.
        """
        import warnings
        warnings.warn(
            "The _load_config method is deprecated. Use self.config_loader instead.",
            DeprecationWarning,
            stacklevel=2
        )
        
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            # Convert string paths to Path objects
            for key, value in config['paths'].items():
                config['paths'][key] = Path(value)
            return config
    
    # ========================================
    # New Configuration Interface
    # ========================================
    
    def get_countries(self, data_source: str = None) -> List:
        """
        Get country list for this collector's data source.
        
        Args:
            data_source: Specific data source, or None for default
            
        Returns:
            List of country codes
        """
        return self.config_loader.get_countries(data_source)
    
    def get_output_path(self, path_type: str, mode: str = "historical", create_if_missing: bool = True) -> Path:
        """
        Get output path for this collector.
        
        Args:
            path_type: Type of path ('historical', 'realtime')
            mode: Processing mode
            create_if_missing: Create directory if it doesn't exist
            
        Returns:
            Path object
        """
        # This method should be overridden by specific collectors
        # to use their appropriate data source paths
        path_key = f"raw.{self.__class__.__name__.lower().replace('collector', '')}.{path_type}"

        try:
            return self.config_loader.get_path(path_key, create_if_missing)
        except (KeyError, TypeError, ValueError) as exc:
            self.logger.debug(
                "Falling back to legacy path for '%s' due to config lookup issue: %s",
                path_key,
                exc,
            )

        # Fallback to legacy behavior
        return Path(self.config['paths'][f'raw_data_{path_type}'])
    
    def get_api_key(self, service: str) -> str:
        """
        Get API key for a service.
        
        Args:
            service: Service name
            
        Returns:
            API key string
        """
        return self.config_loader.get_api_key(service)
    
    def get_processing_config(self, category: str = None) -> Dict[str, Any]:
        """
        Get processing configuration.
        
        Args:
            category: Configuration category
            
        Returns:
            Processing configuration
        """
        if category:
            return self.config_loader.get_processing_config(category)
        else:
            return self.config_loader.config['data_processing']
    
    # ========================================
    # Abstract Methods (unchanged)
    # ========================================
    
    @abstractmethod
    def fetch_data(self, start_date: str, end_date: str, **kwargs) -> List[Dict[str, Any]]:
        """Fetch data for the specified date range."""
        pass
        
    @abstractmethod
    def validate_data(self, data: List[Dict[str, Any]]) -> bool:
        """Validate the collected data."""
        pass
        
    @abstractmethod
    def save_data(self, data: List[Dict[str, Any]], filename: str) -> None:
        """Save the collected data."""
        pass
        
    def collect(self, start_date: str, end_date: str, filename: str, **kwargs) -> None:
        """Main method to collect, validate and save data."""
        self.logger.info(f"Starting data collection from {start_date} to {end_date}")
        
        data = self.fetch_data(start_date, end_date, **kwargs)
        
        if self.validate_data(data):
            self.save_data(data, filename)
            self.logger.info("Data collection completed successfully")
        else:
            self.logger.error("Data validation failed") 
