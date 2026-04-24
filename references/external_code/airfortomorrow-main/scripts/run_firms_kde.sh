#!/bin/bash

# FIRMS KDE Interpolation Script
# This script runs the KDE interpolation for FIRMS data
# It processes both real-time and historical fire data

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
LOG_FILE="$LOG_DIR/firms_kde_${TIMESTAMP}.log"

# Configuration-driven defaults with fallbacks
if [[ "$CONFIG_AVAILABLE" == true ]]; then
    DEFAULT_COUNTRIES=($(get_config_countries))
    DEFAULT_OUTPUT_DIR=$(get_config_path "processed.firms.base" "$BASE_DIR/data/processed/firms")
    DEFAULT_TIMEOUT=$(get_config_timeout "processing_timeout")
    [[ "$CONFIG_AVAILABLE" == true ]] && echo "✅ Using configuration system for defaults"
else
    # Fallback defaults if config system unavailable
    DEFAULT_COUNTRIES=("THA" "LAO")
    DEFAULT_OUTPUT_DIR="$BASE_DIR/data/processed/firms"
    DEFAULT_TIMEOUT=7200
    echo "⚠️  Configuration system unavailable, using hardcoded defaults"
fi

# Default settings (config-driven with fallbacks)
COUNTRIES=("${DEFAULT_COUNTRIES[@]}")  # Default countries
OUTPUT_DIR="$DEFAULT_OUTPUT_DIR"
SAVE_KDE_GRIDS=false  # Default to not saving KDE grids to save space
MODE="realtime"  # Default mode
START_DATE=""
END_DATE=""
INPUT_FILE=""
TIMEOUT="$DEFAULT_TIMEOUT"
BUFFER_DEGREES=0.4

# Function to log messages
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Help message
usage() {
    echo "FIRMS KDE Interpolation Script (Configuration-Aware)"
    echo ""
    echo "This script runs the KDE interpolation for FIRMS data"
    echo "It processes both real-time and historical fire data"
    echo ""
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  --input-file FILE     Path to the prepared FIRMS data file (overrides automatic selection)"
    echo "  --output-dir DIR      Directory to save output files (default: $DEFAULT_OUTPUT_DIR)"
    echo "  --countries LIST      Space-separated list of country codes (default: ${DEFAULT_COUNTRIES[*]})"
    echo "  --start-date DATE     Start date for historical mode (YYYY-MM-DD format)"
    echo "  --end-date DATE       End date for historical mode (YYYY-MM-DD format)"
    echo "  --save-kde-grids      Save the large KDE grid files (not recommended, requires ~240MB storage)"
    echo "  --timeout SECONDS     Processing timeout (default: ${DEFAULT_TIMEOUT}s)"
    echo "  --buffer DEGREES     Buffer degrees for KDE interpolation (default: $BUFFER_DEGREES)"
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
    echo "  Default output dir: $DEFAULT_OUTPUT_DIR"
    echo "  Default timeout: ${DEFAULT_TIMEOUT}s"
    echo ""
    exit 1
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --input-file)
            INPUT_FILE="$2"
            shift 2
            ;;
        --output-dir)
            OUTPUT_DIR="$2"
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
        --start-date)
            START_DATE="$2"
            MODE="historical"
            shift 2
            ;;
        --end-date)
            END_DATE="$2"
            MODE="historical"
            shift 2
            ;;
        --save-kde-grids)
            SAVE_KDE_GRIDS=true
            shift
            ;;
        --timeout)
            TIMEOUT="$2"
            shift 2
            ;;
        --buffer)
            BUFFER_DEGREES="$2"
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

# Set the FIRMS file based on mode
if [[ -z "$INPUT_FILE" ]]; then
    if [[ "$MODE" == "realtime" ]]; then
        # Find the latest prepared FIRMS file for realtime
        INPUT_FILE=$(ls -t "$OUTPUT_DIR/deduplicated/realtime/firms_prepared_realtime_"*.parquet 2>/dev/null | head -1)
    else
        # Find the historical file that matches the date range
        DATE_STRING="${START_DATE//-/}${END_DATE:+_to_${END_DATE//-/}}"
        COUNTRY_STRING=$(printf "%s_" "${COUNTRIES[@]}" | sed 's/_$//')
        INPUT_FILE=$(ls -t "$OUTPUT_DIR/deduplicated/historical/firms_prepared_historical_${COUNTRY_STRING}_${DATE_STRING}.parquet" 2>/dev/null)
        
        # If not found with exact pattern, try finding the most recent historical file
        if [[ -z "$INPUT_FILE" ]]; then
            if [[ -n "$START_DATE" && -n "$END_DATE" ]]; then
                log "No exact match found for date range ${START_DATE} to ${END_DATE}, searching for closest match..."
                INPUT_FILE=$(ls -t "$OUTPUT_DIR/deduplicated/historical/firms_prepared_historical_"*.parquet 2>/dev/null | head -1)
            fi
        fi
    fi
fi

# Check if we have a FIRMS file
if [[ -z "$INPUT_FILE" ]]; then
    log "ERROR: No prepared FIRMS file found. Please run the FIRMS preparation pipeline first."
    exit 1
fi

# Log start of execution
log "Starting FIRMS KDE interpolation..."
log "Mode: $MODE"
log "Input file: $INPUT_FILE"
log "Output directory: $OUTPUT_DIR"
log "Countries to process: ${COUNTRIES[*]}"
if [[ "$MODE" == "historical" && -n "$START_DATE" ]]; then
    log "Date range: ${START_DATE}${END_DATE:+ to $END_DATE}"
fi
log "Save KDE grids: $SAVE_KDE_GRIDS"

# Make output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Ensure new directory structure exists
mkdir -p "data/processed/firms/h3/$MODE"
mkdir -p "data/processed/firms/plots/$MODE"

# Build command arguments
CMD_ARGS="--input-file \"$INPUT_FILE\" --output-dir \"$OUTPUT_DIR\" --buffer $BUFFER_DEGREES"

# Add countries argument
CMD_ARGS="$CMD_ARGS --countries ${COUNTRIES[@]}"

# Add save-kde-grids flag if specified
if [[ "$SAVE_KDE_GRIDS" == "true" ]]; then
    CMD_ARGS="$CMD_ARGS --save-kde-grids"
fi

# Execute the KDE interpolation script
log "Running KDE interpolation..."
eval "python3 src/data_processors/firms_kde_interpolation.py $CMD_ARGS"

# Check if processing was successful
if [ $? -eq 0 ]; then
    log "KDE interpolation completed successfully."
else
    log "ERROR: KDE interpolation failed."
    exit 1
fi

# Find the latest files created (using directory structure)
if [[ "$SAVE_KDE_GRIDS" == "true" ]]; then
    LATEST_KDE=$(ls -t "$OUTPUT_DIR/kde/firms_kde_"*.parquet 2>/dev/null | head -1)
    if [ -n "$LATEST_KDE" ]; then
        KDE_SIZE=$(du -h "$LATEST_KDE" | cut -f1)
        log "Latest KDE file created: $LATEST_KDE (Size: $KDE_SIZE)"
    fi
fi

LATEST_H3=$(ls -t "data/processed/firms/h3/$MODE/firms_kde_h308_"*.parquet 2>/dev/null | head -1)
LATEST_PLOT=$(ls -t "data/processed/firms/plots/$MODE/firms_kde_plot_"*.png 2>/dev/null | head -1)

if [ -n "$LATEST_H3" ]; then
    H3_SIZE=$(du -h "$LATEST_H3" | cut -f1)
    log "Latest H3 grid file created: $LATEST_H3 (Size: $H3_SIZE)"
    
    # Get hexagon count
    COUNT=$(python -c "import pandas as pd; print(len(pd.read_parquet('$LATEST_H3')))" 2>/dev/null)
    if [ -n "$COUNT" ]; then
        log "Total hexagons created: $COUNT"
    fi
fi

if [ -n "$LATEST_PLOT" ]; then
    PLOT_SIZE=$(du -h "$LATEST_PLOT" | cut -f1)
    log "Latest visualization created: $LATEST_PLOT (Size: $PLOT_SIZE)"
fi

log "Script execution completed."

exit 0 