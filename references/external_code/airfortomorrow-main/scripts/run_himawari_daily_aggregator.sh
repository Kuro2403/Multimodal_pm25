#!/bin/bash

# Himawari Daily Aggregator Script
# This script aggregates H3-indexed Himawari data into daily averages

# Set the base directory to the project root (parent of scripts/)
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
LOG_FILE="$LOG_DIR/himawari_daily_aggregator_${TIMESTAMP}.log"

# Configuration-driven defaults with fallbacks
if [[ "$CONFIG_AVAILABLE" == true ]]; then
    DEFAULT_COUNTRIES=($(get_config_countries))
    DEFAULT_H3_DIR=$(get_config_path "processed.himawari.h3" "./data/processed/himawari/h3")
    DEFAULT_OUTPUT_DIR=$(get_config_path "processed.himawari.daily_aggregated.base" "./data/processed/himawari/daily_aggregated")
    DEFAULT_HOURS=$(get_config_time_window "realtime")
    DEFAULT_TIMEOUT=$(get_config_timeout "processing_timeout")
    [[ "$CONFIG_AVAILABLE" == true ]] && echo "✅ Using configuration system for defaults"
else
    # Fallback defaults if config system unavailable
    DEFAULT_COUNTRIES=("LAO" "THA")
    DEFAULT_H3_DIR="./data/processed/himawari/h3"
    DEFAULT_OUTPUT_DIR="./data/processed/himawari/daily_aggregated"
    DEFAULT_HOURS=24
    DEFAULT_TIMEOUT=3600
    echo "⚠️  Using fallback configuration"
fi

# Initialize variables with defaults
MODE="historical"
COUNTRIES=("${DEFAULT_COUNTRIES[@]}")
H3_DIR="$DEFAULT_H3_DIR"
OUTPUT_DIR="$DEFAULT_OUTPUT_DIR"
HOURS_LOOKBACK="$DEFAULT_HOURS"
TIMEOUT="$DEFAULT_TIMEOUT"
BUFFER_DEGREES=0.4

# Extra arguments to pass to Python script
EXTRA_ARGS="--mode $MODE"

# Function to display usage information
usage() {
    cat << EOF
Himawari Daily Aggregator Script

This script aggregates H3-indexed Himawari AOD data into daily averages.
Creates comprehensive datasets with both 1-day and 2-day averages for all boundary hexagons.

Usage: $0 [OPTIONS]

OPTIONS:
    --mode MODE                 Processing mode: historical or realtime (default: historical)
    --countries COUNTRY1 COUNTRY2   List of ISO country codes (default: LAO THA)
    --h3-dir DIR               Directory containing H3 indexed data (default: $DEFAULT_H3_DIR)
    --output-dir DIR           Output directory for aggregated data (default: $DEFAULT_OUTPUT_DIR)
    --start-date DATE          Start date for historical processing (YYYY-MM-DD)
    --end-date DATE            End date for historical processing (YYYY-MM-DD)
    --hours-lookback HOURS     Hours to look back for realtime processing (default: $DEFAULT_HOURS)
    --buffer-degrees DEGREES   Buffer around country boundaries in degrees (default: 0.4)
    --timeout SECONDS          Maximum execution time in seconds (default: $DEFAULT_TIMEOUT)
    --help                     Show this help message

EXAMPLES:
    # Historical processing for a specific date range
    $0 --mode historical --start-date 2024-01-01 --end-date 2024-01-31

    # Realtime processing for the last 48 hours
    $0 --mode realtime --hours-lookback 48

    # Process specific countries
    $0 --mode historical --start-date 2024-02-01 --end-date 2024-02-05 --countries LAO THA

OUTPUT:
    The script creates daily aggregated files in the following structure:
    - historical/: daily_h3_aod_YYYYMMDD_COUNTRIES.parquet
    - realtime/: daily_h3_aod_YYYYMMDD_COUNTRIES.parquet
    
    Each file contains all boundary hexagons with columns:
    - h3_08: H3 hexagon ID
    - aod_1day: 1-day average AOD (can be NaN)
    - aod_2day: 2-day average AOD (can be NaN)

EOF
    exit 1
}

# Function to log messages
log() {
    local message="$1"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $message" | tee -a "$LOG_FILE"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --mode)
            MODE="$2"
            EXTRA_ARGS="--mode $2"
            shift 2
            ;;
        --countries)
            # Read multiple country arguments
            COUNTRIES=()
            shift
            while [[ $# -gt 0 && ! "$1" =~ ^-- ]]; do
                COUNTRIES+=("$1")
                shift
            done
            # Add countries to extra args
            EXTRA_ARGS="$EXTRA_ARGS --countries ${COUNTRIES[*]}"
            ;;
        --h3-dir)
            H3_DIR="$2"
            EXTRA_ARGS="$EXTRA_ARGS --h3-dir $2"
            shift 2
            ;;
        --output-dir)
            OUTPUT_DIR="$2"
            EXTRA_ARGS="$EXTRA_ARGS --output-dir $2"
            shift 2
            ;;
        --start-date)
            EXTRA_ARGS="$EXTRA_ARGS --start-date $2"
            shift 2
            ;;
        --end-date)
            EXTRA_ARGS="$EXTRA_ARGS --end-date $2"
            shift 2
            ;;
        --hours-lookback)
            HOURS_LOOKBACK="$2"
            EXTRA_ARGS="$EXTRA_ARGS --hours-lookback $2"
            shift 2
            ;;
        --buffer-degrees)
            BUFFER_DEGREES="$2"
            EXTRA_ARGS="$EXTRA_ARGS --buffer-degrees $2"
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

# Validate mode
if [[ "$MODE" != "historical" && "$MODE" != "realtime" ]]; then
    echo "Error: Mode must be 'historical' or 'realtime'"
    exit 1
fi

# Log configuration
log "Starting Himawari Daily Aggregator..."
log "Mode: $MODE"
log "Countries: ${COUNTRIES[*]}"
log "H3 directory: $H3_DIR"
log "Output directory: $OUTPUT_DIR"
log "Buffer degrees: $BUFFER_DEGREES"
if [ "$MODE" = "realtime" ]; then
    log "Hours lookback: $HOURS_LOOKBACK"
fi

# Check if H3 directory exists
if [ ! -d "$H3_DIR" ]; then
    log "Error: H3 directory not found: $H3_DIR"
    exit 1
fi

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Execute the daily aggregator with timeout
log "Running daily aggregator..."
log "Command: python3 src/data_processors/himawari_daily_aggregator.py $EXTRA_ARGS"

timeout "$TIMEOUT" python3 src/data_processors/himawari_daily_aggregator.py $EXTRA_ARGS

# Check exit status
EXIT_CODE=$?

if [ $EXIT_CODE -eq 124 ]; then
    log "ERROR: Daily aggregator timed out after $TIMEOUT seconds."
    exit 1
elif [ $EXIT_CODE -eq 0 ]; then
    log "Himawari daily aggregation completed successfully."
    
    # Show output directory contents
    log "Output structure:"
    
    # Check for historical mode files
    if [ -d "$OUTPUT_DIR/historical" ]; then
        HISTORICAL_COUNT=$(ls -1 "$OUTPUT_DIR/historical"/*.parquet 2>/dev/null | wc -l)
        log "  - Historical daily files: $HISTORICAL_COUNT"
    fi
    
    # Check for realtime mode files
    if [ -d "$OUTPUT_DIR/realtime" ]; then
        REALTIME_COUNT=$(ls -1 "$OUTPUT_DIR/realtime"/*.parquet 2>/dev/null | wc -l)
        log "  - Realtime daily files: $REALTIME_COUNT"
    fi
    
    # Show generated boundaries file
    if [ -f "$OUTPUT_DIR/generated_boundaries_grid.parquet" ]; then
        log "  - Generated boundaries grid saved"
    fi
    
else
    log "ERROR: Himawari daily aggregation failed with exit code $EXIT_CODE."
    exit 1
fi

log "Script execution completed."
exit 0 