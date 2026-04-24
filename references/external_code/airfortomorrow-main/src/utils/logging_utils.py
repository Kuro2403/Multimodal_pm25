#!/usr/bin/env python3
"""
Centralized logging utilities for the air quality prediction project.

This module consolidates all logging setup functionality that was previously
duplicated across multiple files in the codebase.
"""

import logging
import sys
from pathlib import Path
from typing import Optional, Union
from datetime import datetime


def setup_logging(
    level: Union[str, int] = logging.INFO,
    format_string: Optional[str] = None,
    logger_name: Optional[str] = None,
    log_file: Optional[Union[str, Path]] = None,
    include_timestamp: bool = True
) -> logging.Logger:
    """
    Setup logging configuration with flexible options.
    
    This function consolidates all the duplicated logging setup logic
    from across the codebase into a single, configurable utility.
    
    Args:
        level: Logging level (can be string like 'INFO' or constant like logging.INFO)
        format_string: Custom format string (if None, uses default)
        logger_name: Name for the logger (if None, uses __main__)
        log_file: Optional file to write logs to
        include_timestamp: Whether to include timestamp in log format
        
    Returns:
        logging.Logger: Configured logger instance
        
    Example:
        # Basic usage
        logger = setup_logging()
        
        # With custom level and file output
        logger = setup_logging(level='DEBUG', log_file='app.log')
        
        # With custom format
        logger = setup_logging(
            format_string='%(name)s - %(levelname)s - %(message)s',
            logger_name='my_module'
        )
    """
    # Convert string level to logging constant if needed
    if isinstance(level, str):
        level = getattr(logging, level.upper(), logging.INFO)
    
    # Create default format string if not provided
    if format_string is None:
        if include_timestamp:
            format_string = '%(asctime)s - %(levelname)s - %(message)s'
        else:
            format_string = '%(levelname)s - %(message)s'
    
    # Configure root logger first to avoid conflicts
    logging.basicConfig(
        level=level,
        format=format_string,
        handlers=[]  # Clear any existing handlers
    )
    
    # Get or create logger
    if logger_name is None:
        logger = logging.getLogger(__name__)
    else:
        logger = logging.getLogger(logger_name)
    
    # Clear any existing handlers to avoid duplicates
    logger.handlers.clear()
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_formatter = logging.Formatter(format_string)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # Create file handler if requested
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_path)
        file_handler.setLevel(level)
        file_formatter = logging.Formatter(format_string)
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    
    # Set logger level
    logger.setLevel(level)
    
    # Prevent duplicate logs from propagating to root logger
    logger.propagate = False
    
    return logger


def setup_basic_logging(logger_name: Optional[str] = None) -> logging.Logger:
    """
    Simple logging setup for basic use cases.
    
    This is a convenience function for the most common logging setup pattern
    found across the codebase.
    
    Args:
        logger_name: Optional logger name
        
    Returns:
        logging.Logger: Basic configured logger
    """
    return setup_logging(
        level=logging.INFO,
        logger_name=logger_name,
        include_timestamp=True
    )


def setup_debug_logging(logger_name: Optional[str] = None, log_file: Optional[str] = None) -> logging.Logger:
    """
    Setup logging for debugging with verbose output.
    
    Args:
        logger_name: Optional logger name
        log_file: Optional file to write debug logs to
        
    Returns:
        logging.Logger: Debug-level configured logger
    """
    return setup_logging(
        level=logging.DEBUG,
        logger_name=logger_name,
        log_file=log_file,
        include_timestamp=True
    )


def setup_pipeline_logging(pipeline_name: str, log_dir: Optional[str] = None) -> logging.Logger:
    """
    Setup logging specifically for pipeline processes.
    
    Args:
        pipeline_name: Name of the pipeline for the logger
        log_dir: Optional directory to write log files to
        
    Returns:
        logging.Logger: Pipeline-specific logger
    """
    log_file = None
    if log_dir:
        log_file = Path(log_dir) / f"{pipeline_name}.log"
    
    return setup_logging(
        level=logging.INFO,
        logger_name=f"pipeline.{pipeline_name}",
        log_file=log_file,
        format_string=f'%(asctime)s - {pipeline_name} - %(levelname)s - %(message)s'
    )


# Legacy compatibility functions
def configure_logging(level: Union[str, int] = logging.INFO) -> logging.Logger:
    """Legacy compatibility function."""
    return setup_logging(level=level)


def get_logger(name: str) -> logging.Logger:
    """Get a logger with standard configuration."""
    return setup_logging(logger_name=name)


def setup_with_config(config_loader, logger_name: Optional[str] = None) -> logging.Logger:
    """
    Setup logging using configuration system from ConfigLoader.
    
    This function integrates with the ConfigLoader to get logging configuration
    and sets up file and console logging with appropriate paths.
    
    Args:
        config_loader: ConfigLoader instance with logging configuration
        logger_name: Optional logger name
        
    Returns:
        logging.Logger: Configured logger with file and console handlers
    """
    if config_loader is None:
        # Fallback to basic logging if no config loader provided
        return setup_basic_logging(logger_name)
    
    try:
        # Get logging configuration
        logging_config = config_loader.get_logging_config()
        log_level = logging_config.get('level', 'INFO')
        log_format = logging_config.get('format', '%(asctime)s - %(levelname)s - %(message)s')
        
        # Create logs directory using config
        logs_dir = config_loader.get_path('logs', create_if_missing=True)
        
        # Generate log file name based on the logger name
        if logger_name:
            log_name = logger_name.replace('src.', '').replace('__main__', 'main')
        else:
            log_name = 'application'
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = logs_dir / f"{log_name}_{timestamp}.log"
        
        # Setup logging with config values
        return setup_logging(
            level=log_level,
            format_string=log_format,
            logger_name=logger_name,
            log_file=log_file
        )
        
    except Exception as e:
        # Fallback to basic logging if config loading fails
        print(f"Warning: Could not setup logging with config: {e}")
        return setup_basic_logging(logger_name) 