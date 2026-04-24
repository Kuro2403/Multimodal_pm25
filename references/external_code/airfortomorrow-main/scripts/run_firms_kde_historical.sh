#!/bin/bash

# FIRMS KDE Historical Interpolation Script
# This script runs the KDE interpolation for historical FIRMS data
# Processing each day separately

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
LOG_FILE="$LOG_DIR/firms_kde_historical_${TIMESTAMP}.log"

# Configuration-driven defaults with fallbacks
if [[ "$CONFIG_AVAILABLE" == true ]]; then
    DEFAULT_COUNTRIES=($(get_config_countries))
    DEFAULT_OUTPUT_DIR=$(get_config_path "processed.firms.historical" "$BASE_DIR/data/processed/firms/historical")
    DEFAULT_TIMEOUT=$(get_config_timeout "processing_timeout")
    [[ "$CONFIG_AVAILABLE" == true ]] && echo "✅ Using configuration system for defaults"
else
    # Fallback defaults if config system unavailable
    DEFAULT_COUNTRIES=("THA" "LAO")
    DEFAULT_OUTPUT_DIR="$BASE_DIR/data/processed/firms/historical"
    DEFAULT_TIMEOUT=7200
    echo "⚠️  Configuration system unavailable, using hardcoded defaults"
fi

# Build default historical file path
COUNTRY_STRING=$(printf "%s_" "${DEFAULT_COUNTRIES[@]}" | sed 's/_$//')
DEFAULT_HISTORICAL_FILE="$BASE_DIR/data/processed/firms/deduplicated/historical/firms_prepared_historical_${COUNTRY_STRING}_20240101_to_20241231.parquet"

# Default settings (config-driven with fallbacks)
COUNTRIES=("${DEFAULT_COUNTRIES[@]}")  # Default countries
OUTPUT_DIR="$DEFAULT_OUTPUT_DIR"
HISTORICAL_FILE="$DEFAULT_HISTORICAL_FILE"
START_DATE=""
END_DATE=""
GENERATE_PLOTS=true
SAVE_KDE_GRIDS=false  # Default to not saving KDE grids to save space
TIMEOUT="$DEFAULT_TIMEOUT"
BUFFER_DEGREES=0.4

# Function to log messages
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Help message
usage() {
    echo "FIRMS KDE Historical Interpolation Script (Configuration-Aware)"
    echo ""
    echo "This script runs the KDE interpolation for historical FIRMS data"
    echo "Processing each day separately"
    echo ""
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  --input-file FILE     Path to the prepared historical FIRMS data file (default: auto-generated)"
    echo "  --output-dir DIR      Directory to save output files (default: $DEFAULT_OUTPUT_DIR)"
    echo "  --countries LIST      Space-separated list of country codes (default: ${DEFAULT_COUNTRIES[*]})"
    echo "  --start-date DATE     Start date for filtering (YYYY-MM-DD format)"
    echo "  --end-date DATE       End date for filtering (YYYY-MM-DD format)"
    echo "  --no-plots            Skip generating plots to save time"
    echo "  --save-kde-grids      Save the large KDE grid files (not recommended, requires ~95GB storage)"
    echo "  --timeout SECONDS     Processing timeout (default: ${DEFAULT_TIMEOUT}s)"
    echo "  --buffer DEGREES     Buffer degrees for KDE interpolation (default: 0.4)"
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
            HISTORICAL_FILE="$2"
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
            shift 2
            ;;
        --end-date)
            END_DATE="$2"
            shift 2
            ;;
        --no-plots)
            GENERATE_PLOTS=false
            shift
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

# Check if the historical file exists
if [[ ! -f "$HISTORICAL_FILE" ]]; then
    log "ERROR: Historical FIRMS file not found: $HISTORICAL_FILE"
    log "Please run the FIRMS preparation pipeline first or specify a valid file with --input-file"
    exit 1
fi

# Log configuration
log "Starting FIRMS KDE historical interpolation..."
log "Input file: $HISTORICAL_FILE"
log "Output directory: $OUTPUT_DIR"
log "Countries to process: ${COUNTRIES[*]}"
if [[ -n "$START_DATE" ]]; then
    log "Start date: $START_DATE"
fi
if [[ -n "$END_DATE" ]]; then
    log "End date: $END_DATE"
fi
log "Generate plots: $GENERATE_PLOTS"
log "Save KDE grids: $SAVE_KDE_GRIDS"

# Make output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Execute the Python script using the virtual environment's Python
log "Running KDE processing..."

# Build the command with proper plot generation logic
CMD_ARGS="--input-file \"$HISTORICAL_FILE\" --output-dir \"$OUTPUT_DIR\" --countries ${COUNTRIES[@]}"

# Add date arguments if provided
if [[ -n "$START_DATE" ]]; then
    CMD_ARGS="$CMD_ARGS --start-date \"$START_DATE\""
fi
if [[ -n "$END_DATE" ]]; then
    CMD_ARGS="$CMD_ARGS --end-date \"$END_DATE\""
fi

# Add --save-kde-grids if requested
if [[ "$SAVE_KDE_GRIDS" == "true" ]]; then
    CMD_ARGS="$CMD_ARGS --save-kde-grids"
fi

# Add buffer argument
CMD_ARGS="$CMD_ARGS --buffer $BUFFER_DEGREES"

# Execute the command
eval "python src/data_processors/firms_kde_historical.py $CMD_ARGS"

# Check if processing was successful
if [ $? -eq 0 ]; then
    log "Historical KDE interpolation completed successfully."
else
    log "ERROR: Historical KDE interpolation failed."
    exit 1
fi

# Count the number of files created
H3_COUNT=$(find "data/processed/firms/h3/historical" -name "firms_kde_h308_*.parquet" 2>/dev/null | wc -l)
PLOT_COUNT=$(find "data/processed/firms/plots/historical" -name "firms_kde_plot_*.png" 2>/dev/null | wc -l)

log "Files created: $H3_COUNT H3 grids, $PLOT_COUNT plots"

# Calculate total size
H3_SIZE=$(du -sh "data/processed/firms/h3/historical" 2>/dev/null | cut -f1)
PLOT_SIZE=$(du -sh "data/processed/firms/plots/historical" 2>/dev/null | cut -f1)
TOTAL_SIZE=$(du -sh "$OUTPUT_DIR" 2>/dev/null | cut -f1)

# Only check KDE directory if we saved KDE grids
if [[ "$SAVE_KDE_GRIDS" == "true" && -d "$OUTPUT_DIR/kde" ]]; then
    KDE_COUNT=$(find "$OUTPUT_DIR/kde" -name "firms_kde_*.parquet" 2>/dev/null | wc -l)
    KDE_SIZE=$(du -sh "$OUTPUT_DIR/kde" 2>/dev/null | cut -f1)
    log "Storage used: KDE grids: $KDE_SIZE, H3 grids: $H3_SIZE, Plots: $PLOT_SIZE, Total: $TOTAL_SIZE"
else
    log "Storage used: H3 grids: $H3_SIZE, Plots: $PLOT_SIZE, Total: $TOTAL_SIZE"
fi

log "Script execution completed."

exit 0 