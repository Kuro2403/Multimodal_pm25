#!/bin/bash

# ERA5 Real-time Data Collection Script
# This script runs the data collection for real-time ERA5 meteorological data

# Set the base directory to the script location
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
cd "$BASE_DIR"

# Set up script directory for configuration access
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Source configuration and utility functions
if [[ -f "$SCRIPT_DIR/utils/config_reader.sh" ]]; then
    source "$SCRIPT_DIR/utils/config_reader.sh"
    CONFIG_AVAILABLE=true
else
    CONFIG_AVAILABLE=false
    echo "⚠️  Configuration system not available, using fallbacks"
fi

# Create logs directory if it doesn't exist
if [[ "$CONFIG_AVAILABLE" == true ]]; then
    LOG_DIR=$(get_config_path "logs" "./logs")
else
    LOG_DIR="$BASE_DIR/logs"
fi
mkdir -p "$LOG_DIR"

# Set log file with timestamp
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="$LOG_DIR/era5_realtime_${TIMESTAMP}.log"

# Configuration-driven defaults with fallbacks
if [[ "$CONFIG_AVAILABLE" == true ]]; then
    DEFAULT_COUNTRIES=($(get_config_countries))
    DEFAULT_HOURS=$(get_config_time_window "realtime")
    DEFAULT_PARAMS=($(get_config_era5_params))
    DEFAULT_RAW_DATA_DIR=$(get_config_path "raw.era5.base" "$BASE_DIR/data/raw/era5")
    DEFAULT_OUTPUT_DIR=$(get_config_path "processed.era5.h3" "$BASE_DIR/data/processed/era5/h3")
    DEFAULT_TIMEOUT=$(get_config_timeout "processing_timeout")
    [[ "$CONFIG_AVAILABLE" == true ]] && echo "✅ Using configuration system for defaults"
else
    # Fallback defaults if config system unavailable
    DEFAULT_COUNTRIES=("THA" "LAO")
    DEFAULT_HOURS=24
    DEFAULT_PARAMS=("2d" "2t" "10u" "10v")
    DEFAULT_RAW_DATA_DIR="$BASE_DIR/data/raw/era5"
    DEFAULT_OUTPUT_DIR="$BASE_DIR/data/processed/era5/h3"
    DEFAULT_TIMEOUT=7200
    echo "⚠️  Configuration system unavailable, using hardcoded defaults"
fi

# Default settings (config-driven with fallbacks)
HOURS="$DEFAULT_HOURS"  # Default to 24 hours
RAW_DATA_DIR="$DEFAULT_RAW_DATA_DIR"
OUTPUT_DIR="$DEFAULT_OUTPUT_DIR"
PARAMS=("${DEFAULT_PARAMS[@]}")
COUNTRIES=("${DEFAULT_COUNTRIES[@]}")
TIMEOUT="$DEFAULT_TIMEOUT"

# Function to log messages
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Help message
usage() {
    echo "ERA5 Real-time Data Collection Script (Configuration-Aware)"
    echo ""
    echo "This script runs the data collection for real-time ERA5 meteorological data"
    echo ""
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  --hours HOURS         Number of hours to collect (default: $DEFAULT_HOURS)"
    echo "  --params PARAMS       ERA5 parameters to collect (default: ${DEFAULT_PARAMS[*]})"
    echo "  --countries CODES     Country codes for boundaries (default: ${DEFAULT_COUNTRIES[*]})"
    echo "  --raw-data-dir DIR    Directory to store raw data metadata (default: $DEFAULT_RAW_DATA_DIR)"
    echo "  --output-dir DIR      Directory to store processed H3 files (default: $DEFAULT_OUTPUT_DIR)"
    echo "  --timeout SECONDS     Processing timeout (default: ${DEFAULT_TIMEOUT}s)"
    echo "  --help                Display this help message"
    echo ""
    echo "Configuration Integration:"
    echo "  This script uses the centralized configuration system when available."
    echo "  Configuration defaults are loaded from config/config.yaml."
    echo "  Command-line arguments override configuration defaults."
    echo ""
    echo "Configuration Status:"
    echo "  Config system: $([[ "$CONFIG_AVAILABLE" == true ]] && echo "✅ Available" || echo "❌ Using fallbacks")"
    echo "  Default countries: ${DEFAULT_COUNTRIES[*]}"
    echo "  Default parameters: ${DEFAULT_PARAMS[*]}"
    echo "  Default hours: $DEFAULT_HOURS"
    echo "  Default timeout: ${DEFAULT_TIMEOUT}s"
    echo ""
    echo "Note: Geographic bounds are automatically calculated from the country list."
    echo ""
    echo "Examples:"
    echo "  # Basic real-time collection"
    echo "  $0 --hours 24"
    echo ""
    echo "  # Collect specific parameters"
    echo "  $0 --hours 24 --params 2t 10u 10v"
    echo ""
    echo "  # Multi-country processing"
    echo "  $0 --hours 24 --countries THA LAO VNM KHM"
    exit 1
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --hours)
            HOURS="$2"
            shift 2
            ;;
        --params)
            PARAMS=()
            shift
            while [[ $# -gt 0 && ! "$1" =~ ^-- ]]; do
                PARAMS+=("$1")
                shift
            done
            ;;
        --countries)
            COUNTRIES=()
            shift
            while [[ $# -gt 0 && ! "$1" =~ ^-- ]]; do
                COUNTRIES+=("$1")
                shift
            done
            ;;
        --raw-data-dir)
            RAW_DATA_DIR="$2"
            shift 2
            ;;
        --output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --timeout)
            TIMEOUT="$2"
            shift 2
            ;;
        --help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Log configuration
log "Starting ERA5 real-time data collection..."
log "Hours to collect: $HOURS"
log "Parameters: ${PARAMS[*]}"
log "Countries: ${COUNTRIES[*]}"
log "Raw data directory: $RAW_DATA_DIR"
log "Output directory: $OUTPUT_DIR"
log "Note: Geographic bounds will be calculated automatically from country list"

# Make output directories if they don't exist
mkdir -p "$RAW_DATA_DIR"
mkdir -p "$OUTPUT_DIR"

# Build command arguments
CMD_ARGS="--mode realtime --hours $HOURS --output-dir \"$OUTPUT_DIR\" --raw-data-dir \"$RAW_DATA_DIR\" --log-dir \"$LOG_DIR\""

# Add parameters
CMD_ARGS="$CMD_ARGS --params ${PARAMS[*]}"

# Add countries
CMD_ARGS="$CMD_ARGS --countries ${COUNTRIES[*]}"

# Execute the real-time data collection script
log "Running real-time data collection..."
eval "python src/data_collectors/era5_meteorological.py $CMD_ARGS"

# Check if processing was successful
if [ $? -eq 0 ]; then
    log "ERA5 real-time data collection completed successfully."
else
    log "ERROR: ERA5 real-time data collection failed."
    exit 1
fi

# Count the number of files processed
if [ -d "$OUTPUT_DIR" ]; then
    # Find recent files (last 48 hours to be safe)
    TWO_DAYS_AGO=$(date -v-2d +"%Y%m%d" 2>/dev/null || date --date="2 days ago" +"%Y%m%d")
    PARQUET_COUNT=$(find "$OUTPUT_DIR" -name "*.parquet" -newermt "$TWO_DAYS_AGO" | wc -l)
    log "Recent processed files: $PARQUET_COUNT"
fi

# Calculate total size
if [ -d "$RAW_DATA_DIR" ]; then
    RAW_SIZE=$(du -sh "$RAW_DATA_DIR" 2>/dev/null | cut -f1)
    log "Raw data size: $RAW_SIZE"
fi

if [ -d "$OUTPUT_DIR" ]; then
    PROCESSED_SIZE=$(du -sh "$OUTPUT_DIR" 2>/dev/null | cut -f1)
    log "Processed data size: $PROCESSED_SIZE"
fi

log "Script execution completed."
exit 0 