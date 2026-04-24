#!/bin/bash

# AirGradient Data Collection Script
# This script collects air quality data from AirGradient sensors for Thailand and Laos

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
LOG_FILE="$LOG_DIR/airgradient_collection_${TIMESTAMP}.log"

# Configuration-driven defaults with fallbacks
if [[ "$CONFIG_AVAILABLE" == true ]]; then
    DEFAULT_DAYS=$(get_config_time_window_days "realtime")
    DEFAULT_OUTPUT_DIR=$(get_config_path "raw.airgradient.base" "$BASE_DIR/data/raw/airgradient")
    DEFAULT_TIMEOUT=$(get_config_timeout "download_timeout")
    [[ "$CONFIG_AVAILABLE" == true ]] && echo "✅ Using configuration system for defaults"
else
    # Fallback defaults if config system unavailable
    DEFAULT_DAYS=7
    DEFAULT_OUTPUT_DIR="$BASE_DIR/data/raw/airgradient"
    DEFAULT_TIMEOUT=1800
    echo "⚠️  Configuration system unavailable, using hardcoded defaults"
fi

# Default settings (config-driven with fallbacks)
MODE="realtime"
DAYS="$DEFAULT_DAYS"
START_DATE=""
END_DATE=""
OUTPUT_DIR="$DEFAULT_OUTPUT_DIR"
TIMEOUT="$DEFAULT_TIMEOUT"

# Function to log messages
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Help message
usage() {
    echo "AirGradient Data Collection Script (Configuration-Aware)"
    echo ""
    echo "This script collects air quality data from AirGradient sensors for Thailand and Laos"
    echo ""
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  --mode MODE           Collection mode: historical or realtime (default: realtime)"
    echo "  --days DAYS           Number of days to collect for realtime mode (default: $DEFAULT_DAYS)"
    echo "  --start-date DATE     Start date for historical mode (format: YYYY-MM-DD)"
    echo "  --end-date DATE       End date for historical mode (format: YYYY-MM-DD)"
    echo "  --output-dir DIR      Directory to store collected data (default: $DEFAULT_OUTPUT_DIR)"
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
    echo "  Default days: $DEFAULT_DAYS"
    echo "  Default output dir: $DEFAULT_OUTPUT_DIR"
    echo "  Default timeout: ${DEFAULT_TIMEOUT}s"
    echo ""
    exit 1
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --mode)
            MODE="$2"
            shift 2
            ;;
        --days)
            DAYS="$2"
            shift 2
            ;;
        --start-date)
            START_DATE="$2"
            shift 2
            ;;
        --end-date)
            END_DATE="$2"
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

# Validate parameters
if [[ "$MODE" == "historical" && (-z "$START_DATE" || -z "$END_DATE") ]]; then
    log "ERROR: Historical mode requires both --start-date and --end-date"
    usage
fi

# Log configuration
log "Starting AirGradient data collection..."
log "Mode: $MODE"
if [[ "$MODE" == "realtime" ]]; then
    log "Hours to collect: $DAYS"
else
    log "Date range: $START_DATE to $END_DATE"
fi
log "Output directory: $OUTPUT_DIR"

# Make output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Build command arguments
CMD_ARGS=(--mode "$MODE")

if [[ "$MODE" == "historical" ]]; then
    CMD_ARGS+=(--start-date "$START_DATE" --end-date "$END_DATE")
fi

# Execute the AirGradient data collection script using module syntax
log "Running AirGradient data collection..."
python -m src.collect_airgradient_data "${CMD_ARGS[@]}"

# Check if processing was successful
if [ $? -eq 0 ]; then
    log "AirGradient data collection completed successfully."
else
    log "ERROR: AirGradient data collection failed."
    exit 1
fi

# Count collected data
DATA_COUNT=$(find "$OUTPUT_DIR" -name "*.parquet" | wc -l)
log "Parquet files created: $DATA_COUNT"

# Calculate total size
DATA_SIZE=$(du -sh "$OUTPUT_DIR" 2>/dev/null | cut -f1)
log "Storage used: $DATA_SIZE"

log "Script execution completed."
exit 0 
