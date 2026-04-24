#!/bin/bash

# FIRMS Data Processing Script
# This script processes FIRMS fire data for Thailand and Laos
# It takes fire data (historical or real-time), performs interpolation, and creates an H3 hexagonal grid

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
LOG_FILE="$LOG_DIR/firms_processing_${TIMESTAMP}.log"

# Configuration-driven defaults with fallbacks
if [[ "$CONFIG_AVAILABLE" == true ]]; then
    DEFAULT_COUNTRIES=($(get_config_countries))
    DEFAULT_RESOLUTION=$(get_config_h3_resolution)
    DEFAULT_DENSITY=$(get_config_firms_kde_setting "grid_size" "6400")
    DEFAULT_CHUNKS=$(get_config_firms_kde_setting "number_of_chunks" "8")
    DEFAULT_OUTPUT_DIR=$(get_config_path "processed.firms.base" "$BASE_DIR/data/processed/firms")
    DEFAULT_TIMEOUT=$(get_config_timeout "processing_timeout")
    [[ "$CONFIG_AVAILABLE" == true ]] && echo "✅ Using configuration system for defaults"
else
    # Fallback defaults if config system unavailable
    DEFAULT_COUNTRIES=("THA" "LAO")
    DEFAULT_RESOLUTION=8
    DEFAULT_DENSITY=6400
    DEFAULT_CHUNKS=8
    DEFAULT_OUTPUT_DIR="$BASE_DIR/data/processed/firms"
    DEFAULT_TIMEOUT=7200
    echo "⚠️  Configuration system unavailable, using hardcoded defaults"
fi

# Default settings (config-driven with fallbacks)
DATA_TYPE="realtime"  # historical or realtime
DATA_DIR=""  # Will be set based on data type
OUTPUT_DIR="$DEFAULT_OUTPUT_DIR"
RESOLUTION="$DEFAULT_RESOLUTION"
DENSITY="$DEFAULT_DENSITY"
CHUNKS="$DEFAULT_CHUNKS"
INDEX_FILE=""
COUNTRIES=("${DEFAULT_COUNTRIES[@]}")  # Default countries
PREPARE_ONLY=false
SKIP_DEDUPLICATION=false
START_DATE=""
END_DATE=""
TIMEOUT="$DEFAULT_TIMEOUT"

# Function to log messages
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Help message
usage() {
    echo "FIRMS Data Processing Script (Configuration-Aware)"
    echo ""
    echo "This script processes FIRMS fire data for Thailand and Laos"
    echo "It takes fire data (historical or real-time), performs interpolation, and creates an H3 hexagonal grid"
    echo ""
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  --data-type TYPE      Type of data to process: historical or realtime (default: realtime)"
    echo "  --data-dir DIR        Directory with historical data files (for historical mode only)"
    echo "  --output-dir DIR      Directory to save output files (default: $DEFAULT_OUTPUT_DIR)"
    echo "  --resolution RES      H3 hexagon resolution (default: $DEFAULT_RESOLUTION)"
    echo "  --density DENSITY     Interpolation grid density (default: $DEFAULT_DENSITY)"
    echo "  --chunks CHUNKS       Number of chunks for processing (default: $DEFAULT_CHUNKS)"
    echo "  --countries LIST     Space-separated list of country codes (default: ${DEFAULT_COUNTRIES[*]})"
    echo "  --prepare-only       Only prepare data without interpolation and H3 grid creation"
    echo "  --skip-deduplication Skip the deduplication step (faster processing)"
    echo "  --start-date DATE    Start date for filtering data (YYYY-MM-DD format)"
    echo "  --end-date DATE      End date for filtering data (YYYY-MM-DD format)"
    echo "  --timeout SECONDS    Processing timeout (default: ${DEFAULT_TIMEOUT}s)"
    echo "  --help               Display this help message"
    echo ""
    echo "Configuration Integration:"
    echo "  This script uses the centralized configuration system when available."
    echo "  Configuration defaults are loaded from config/config.yaml."
    echo "  Command-line arguments override configuration defaults."
    echo ""
    echo "Configuration Status:"
    echo "  Config system: $([[ "$CONFIG_AVAILABLE" == true ]] && echo "✅ Available" || echo "❌ Using fallbacks")"
    echo "  Default countries: ${DEFAULT_COUNTRIES[*]}"
    echo "  Default resolution: $DEFAULT_RESOLUTION"
    echo "  Default density: $DEFAULT_DENSITY"
    echo "  Default chunks: $DEFAULT_CHUNKS"
    echo "  Default timeout: ${DEFAULT_TIMEOUT}s"
    echo ""
    exit 1
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --data-type)
            DATA_TYPE="$2"
            shift 2
            ;;
        --index-file)
            INDEX_FILE="$2"
            shift 2
            ;;
        --data-dir)
            DATA_DIR="$2"
            shift 2
            ;;
        --output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --resolution)
            RESOLUTION="$2"
            shift 2
            ;;
        --density)
            DENSITY="$2"
            shift 2
            ;;
        --chunks)
            CHUNKS="$2"
            shift 2
            ;;
        --use-latest)
            # Ignored but kept for backward compatibility
            shift
            ;;
        --countries)
            shift
            COUNTRIES=()
            while [[ $# -gt 0 && ! $1 =~ ^-- ]]; do
                COUNTRIES+=("$1")
                shift
            done
            ;;
        --prepare-only)
            PREPARE_ONLY=true
            shift
            ;;
        --skip-deduplication)
            SKIP_DEDUPLICATION=true
            shift
            ;;
        --start-date)
            START_DATE="$2"
            shift 2
            ;;
        --end-date)
            END_DATE="$2"
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

# Validate data type
if [[ "$DATA_TYPE" != "historical" && "$DATA_TYPE" != "realtime" ]]; then
    log "ERROR: Invalid data type. Must be either 'historical' or 'realtime'"
    exit 1
fi

# Set default data directory based on data type if not provided
if [[ "$DATA_TYPE" == "historical" && -z "$DATA_DIR" ]]; then
    DATA_DIR="$BASE_DIR/data/raw/firms/historical"
    
    # Check if the data directory exists
    if [ ! -d "$DATA_DIR" ]; then
        log "ERROR: Historical data directory does not exist: $DATA_DIR"
        exit 1
    fi
fi

# Log start of execution
log "Starting FIRMS data processing..."
log "Data type: $DATA_TYPE"
if [[ "$DATA_TYPE" == "historical" ]]; then
    log "Data directory: $DATA_DIR"
else
    log "Mode: Direct download from NASA FIRMS servers"
fi
log "Output directory: $OUTPUT_DIR"
if [[ "$PREPARE_ONLY" == true ]]; then
    log "Mode: Data preparation only"
else
    log "Mode: Full processing (preparation + interpolation + H3 grid)"
    log "H3 resolution: $RESOLUTION"
    log "Interpolation density: $DENSITY"
    log "Processing chunks: $CHUNKS"
fi
log "Countries to process: ${COUNTRIES[*]}"
if [[ "$SKIP_DEDUPLICATION" == true ]]; then
    log "Deduplication: SKIPPED (faster processing)"
else
    log "Deduplication: ENABLED (removes overlapping MODIS/VIIRS fires)"
fi
if [[ -n "$START_DATE" || -n "$END_DATE" ]]; then
    log "Date range: ${START_DATE:-earliest} to ${END_DATE:-latest}"
fi

# Make output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Build command arguments
CMD_ARGS=(--data-type "$DATA_TYPE")
if [[ "$DATA_TYPE" == "historical" ]]; then
    CMD_ARGS+=(--data-dir "$DATA_DIR")
fi
CMD_ARGS+=(--output-dir "$OUTPUT_DIR")

# Add countries argument
CMD_ARGS+=(--countries "${COUNTRIES[@]}")

# Add date range arguments if provided
if [[ -n "$START_DATE" ]]; then
    CMD_ARGS+=(--start-date "$START_DATE")
fi
if [[ -n "$END_DATE" ]]; then
    CMD_ARGS+=(--end-date "$END_DATE")
fi

# Add prepare-only flag if set
if [[ "$PREPARE_ONLY" == true ]]; then
    CMD_ARGS+=(--prepare-only)
else
    CMD_ARGS+=(--resolution "$RESOLUTION" --density "$DENSITY" --chunks "$CHUNKS")
fi

# Add skip-deduplication flag if set
if [[ "$SKIP_DEDUPLICATION" == true ]]; then
    CMD_ARGS+=(--skip-deduplication)
fi

# Execute the Python processor
log "Executing command: python3 src/data_processors/firms_data_processor.py ${CMD_ARGS[*]}"
python3 src/data_processors/firms_data_processor.py "${CMD_ARGS[@]}"

# Check exit code
EXIT_CODE=$?
if [ $EXIT_CODE -eq 0 ]; then
    log "FIRMS data processing completed successfully with exit code: $EXIT_CODE"
else
    log "ERROR: FIRMS data processing failed with exit code: $EXIT_CODE"
    exit $EXIT_CODE
fi

exit 0 
