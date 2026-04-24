#!/bin/bash

# Integrated Air Quality Pipeline Script
# This script runs the complete air quality data collection and processing pipeline

# Set the base directory to the project root (parent of scripts/)
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
cd "$BASE_DIR"

# Set up script directory for configuration access
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Source configuration and utility functions
if [[ -f "$SCRIPT_DIR/utils/config_reader.sh" ]]; then
    source "$SCRIPT_DIR/utils/config_reader.sh"
    CONFIG_AVAILABLE=true
    echo "✅ Configuration system loaded successfully"
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
LOG_FILE="$LOG_DIR/air_quality_pipeline_${TIMESTAMP}.log"

# Configuration-driven defaults with fallbacks
if [[ "$CONFIG_AVAILABLE" == true ]]; then
    DEFAULT_COUNTRIES=($(get_config_countries))
    DEFAULT_DAYS=$(get_config_time_window_days "realtime")
    echo "✅ Using configuration system for defaults"
else
    # Fallback defaults if config system unavailable
    DEFAULT_COUNTRIES=("THA" "LAO")
    DEFAULT_DAYS=3
    echo "⚠️  Configuration system unavailable, using hardcoded defaults"
fi

# Default settings (config-driven with fallbacks)
MODE="realtime"
DAYS="$DEFAULT_DAYS"
START_DATE=""
END_DATE=""
COUNTRIES=("${DEFAULT_COUNTRIES[@]}")

# Function to log messages
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Help message
usage() {
    echo "Integrated Air Quality Pipeline (Configuration-Aware)"
    echo ""
    echo "This script runs the complete air quality data collection and processing pipeline"
    echo ""
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  --mode MODE           Processing mode: historical or realtime (default: realtime)"
    echo "  --days DAYS          Number of days to process for realtime mode (default: $DEFAULT_DAYS)"
    echo "  --start-date DATE    Start date for historical mode (format: YYYY-MM-DD)"
    echo "  --end-date DATE      End date for historical mode (format: YYYY-MM-DD)"
    echo "  --countries CODES    Country codes to process (space-separated, default: ${DEFAULT_COUNTRIES[*]})"
    echo "  --help              Display this help message"
    echo ""
    echo "Configuration Integration:"
    echo "  This script uses the centralized configuration system when available."
    echo "  Configuration defaults are loaded from config/config.yaml."
    echo "  Command-line arguments override configuration defaults."
    echo ""
    echo "Configuration Status:"
    echo "  Config system: $([[ "$CONFIG_AVAILABLE" == true ]] && echo "✅ Available" || echo "❌ Using fallbacks")"
    echo "  Default countries: ${DEFAULT_COUNTRIES[*]}"
    echo "  Default days: $DEFAULT_DAYS"
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
        --countries)
            shift
            COUNTRIES=()
            while [[ $# -gt 0 && ! $1 =~ ^-- ]]; do
                COUNTRIES+=("$1")
                shift
            done
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
log "Starting integrated air quality pipeline..."
log "Mode: $MODE"
if [[ "$MODE" == "realtime" ]]; then
    log "Days to process: $DAYS"
else
    log "Date range: $START_DATE to $END_DATE"
fi
log "Countries: ${COUNTRIES[*]}"

# Build the command
CMD=(python -m src.air_quality_integrated_pipeline --mode "$MODE" --days "$DAYS")
if [[ "$MODE" == "historical" ]]; then
    CMD+=(--start-date "$START_DATE" --end-date "$END_DATE")
fi
CMD+=(--countries "${COUNTRIES[@]}")

# Execute the pipeline
log "Running command: ${CMD[*]}"
"${CMD[@]}"

# Check if processing was successful
if [ $? -eq 0 ]; then
    log "Air quality pipeline completed successfully."
    
    # Calculate total data size
    RAW_SIZE=$(du -sh "$BASE_DIR/data/raw" 2>/dev/null | cut -f1)
    PROCESSED_SIZE=$(du -sh "$BASE_DIR/data/processed" 2>/dev/null | cut -f1)
    
    log "Summary:"
    log "- Raw data size: $RAW_SIZE"
    log "- Processed data size: $PROCESSED_SIZE"
else
    log "ERROR: Air quality pipeline failed."
    exit 1
fi

log "Script execution completed."
exit 0 
