#!/bin/bash

# Himawari Aerosol Optical Depth (AOD) Real-time Data Collection Script
# This script runs the data collection for real-time Himawari-8 AOD data

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
LOG_FILE="$LOG_DIR/himawari_aod_realtime_${TIMESTAMP}.log"

# Configuration-driven defaults with fallbacks
if [[ "$CONFIG_AVAILABLE" == true ]]; then
    DEFAULT_COUNTRIES=($(get_config_countries))
    DEFAULT_HOURS=$(get_config_time_window "realtime")
    DEFAULT_RAW_DATA_DIR=$(get_config_path "raw.himawari.base" "$BASE_DIR/data/raw/himawari")
    DEFAULT_TIF_DIR=$(get_config_path "raw.himawari.tif" "$BASE_DIR/data/processed/himawari/tif")
    DEFAULT_H3_DIR=$(get_config_path "processed.himawari.h3" "$BASE_DIR/data/processed/himawari/h3")
    DEFAULT_TIMEOUT=$(get_config_timeout "processing_timeout")
    [[ "$CONFIG_AVAILABLE" == true ]] && echo "✅ Using configuration system for defaults"
else
    # Fallback defaults if config system unavailable
    DEFAULT_COUNTRIES=("LAO" "THA")
    DEFAULT_HOURS=24
    DEFAULT_RAW_DATA_DIR="$BASE_DIR/data/raw/himawari"
    DEFAULT_TIF_DIR="$BASE_DIR/data/processed/himawari/tif"
    DEFAULT_H3_DIR="$BASE_DIR/data/processed/himawari/h3"
    DEFAULT_TIMEOUT=7200
    echo "⚠️  Configuration system unavailable, using hardcoded defaults"
fi

# Default settings (config-driven with fallbacks)
HOURS="$DEFAULT_HOURS"  # Default to 24 hours
RAW_DATA_DIR="$DEFAULT_RAW_DATA_DIR"
TIF_DIR="$DEFAULT_TIF_DIR"
H3_DIR="$DEFAULT_H3_DIR"
FORCE_DOWNLOAD=false
DOWNLOAD_ONLY=false
TRANSFORM_ONLY=false
SKIP_IF_H3_EXISTS=false
COUNTRIES=("${DEFAULT_COUNTRIES[@]}")
TIMEOUT="$DEFAULT_TIMEOUT"

# Function to log messages
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Help message
usage() {
    echo "Himawari Aerosol Optical Depth (AOD) Real-time Data Collection Script (Configuration-Aware)"
    echo ""
    echo "This script runs the data collection for real-time Himawari-8 AOD data"
    echo ""
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  --hours HOURS         Number of hours to collect (default: $DEFAULT_HOURS)"
    echo "  --countries CODES     Country codes for boundaries (default: ${DEFAULT_COUNTRIES[*]})"
    echo "  --raw-data-dir DIR    Directory to store raw NetCDF files (default: $DEFAULT_RAW_DATA_DIR)"
    echo "  --tif-dir DIR         Directory to store GeoTIFF files (default: $DEFAULT_TIF_DIR)"
    echo "  --h3-dir DIR          Directory containing H3 indexed parquet files (default: $DEFAULT_H3_DIR)"
    echo "  --force-download      Force re-download of existing files"
    echo "  --skip-if-h3-exists   Skip downloading files if corresponding H3 parquet files already exist"
    echo "  --download-only       Only download data without conversion to TIF"
    echo "  --transform-only      Only convert existing NetCDF files to TIF"
    echo "  --timeout SECONDS     Processing timeout (default: ${DEFAULT_TIMEOUT}s)"
    echo "  --help                Display this help message"
    echo ""
    echo "Configuration Integration:"
    echo "  This script uses the centralized configuration system when available."
    echo "  Configuration defaults are loaded from config/config.yaml."
    echo "  Command-line arguments override configuration defaults."
    echo "  HIMAWARI_FTP_USER and HIMAWARI_FTP_PASSWORD must be set in the environment."
    echo ""
    echo "Configuration Status:"
    echo "  Config system: $([[ "$CONFIG_AVAILABLE" == true ]] && echo "✅ Available" || echo "❌ Using fallbacks")"
    echo "  Default countries: ${DEFAULT_COUNTRIES[*]}"
    echo "  Default hours: $DEFAULT_HOURS"
    echo "  Default raw data dir: $DEFAULT_RAW_DATA_DIR"
    echo "  Default TIF dir: $DEFAULT_TIF_DIR"
    echo "  Default timeout: ${DEFAULT_TIMEOUT}s"
    echo ""
    echo "Examples:"
    echo "  # Basic real-time collection"
    echo "  $0 --hours 24"
    echo ""
    echo "  # With H3 optimization (skip files that already have H3 data)"
    echo "  $0 --hours 24 --skip-if-h3-exists"
    echo ""
    echo "  # Multi-country processing"
    echo "  $0 --hours 24 --countries THA LAO VNM KHM"
    exit 1
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --user|--password)
            log "ERROR: --user/--password are no longer supported. Set HIMAWARI_FTP_USER and HIMAWARI_FTP_PASSWORD in the environment."
            exit 1
            ;;
        --user=*|--password=*)
            log "ERROR: --user/--password are no longer supported. Set HIMAWARI_FTP_USER and HIMAWARI_FTP_PASSWORD in the environment."
            exit 1
            ;;
        --hours)
            HOURS="$2"
            shift 2
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
        --tif-dir)
            TIF_DIR="$2"
            shift 2
            ;;
        --h3-dir)
            H3_DIR="$2"
            shift 2
            ;;
        --force-download)
            FORCE_DOWNLOAD=true
            shift
            ;;
        --skip-if-h3-exists)
            SKIP_IF_H3_EXISTS=true
            shift
            ;;
        --download-only)
            DOWNLOAD_ONLY=true
            shift
            ;;
        --transform-only)
            TRANSFORM_ONLY=true
            shift
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

# Check if required parameters are provided
if [[ -z "${HIMAWARI_FTP_USER:-}" || -z "${HIMAWARI_FTP_PASSWORD:-}" ]]; then
    log "ERROR: HIMAWARI_FTP_USER and HIMAWARI_FTP_PASSWORD environment variables are required"
    usage
fi

# Log configuration
log "Starting Himawari AOD real-time data collection..."
log "FTP credentials: supplied via environment"
log "Hours to collect: $HOURS"
log "Countries: ${COUNTRIES[*]}"
log "Raw data directory: $RAW_DATA_DIR"
log "TIF directory: $TIF_DIR"
log "H3 directory: $H3_DIR"
log "Force download: $FORCE_DOWNLOAD"
log "Skip if H3 exists: $SKIP_IF_H3_EXISTS"
log "Download only: $DOWNLOAD_ONLY"
log "Transform only: $TRANSFORM_ONLY"
log "Timeout: $TIMEOUT"

# Make output directories if they don't exist
mkdir -p "$RAW_DATA_DIR"
mkdir -p "$TIF_DIR"
mkdir -p "$H3_DIR"

# Build command arguments
CMD_ARGS=(--mode realtime --hours "$HOURS" --raw-data-dir "$RAW_DATA_DIR" --tif-dir "$TIF_DIR" --log-dir "$LOG_DIR")

# Add flags if specified
if [[ "$FORCE_DOWNLOAD" == "true" ]]; then
    CMD_ARGS+=(--force-download)
fi

if [[ "$SKIP_IF_H3_EXISTS" == "true" ]]; then
    CMD_ARGS+=(--skip-if-h3-exists --h3-dir "$H3_DIR")
fi

if [[ "$DOWNLOAD_ONLY" == "true" ]]; then
    CMD_ARGS+=(--download-only)
fi

if [[ "$TRANSFORM_ONLY" == "true" ]]; then
    CMD_ARGS+=(--transform-only)
fi

# Execute the real-time data collection script
log "Running real-time data collection..."
python3 src/data_collectors/himawari_aod.py "${CMD_ARGS[@]}"

# Check if processing was successful
if [ $? -eq 0 ]; then
    log "Himawari AOD real-time data collection completed successfully."
else
    log "ERROR: Himawari AOD real-time data collection failed."
    exit 1
fi

# Count the number of files downloaded and processed
if [[ "$TRANSFORM_ONLY" == "false" ]]; then
    # Find the most recent files (last 48 hours to be safe)
    TWO_DAYS_AGO=$(date -v-2d +"%Y%m%d" 2>/dev/null || date --date="2 days ago" +"%Y%m%d")
    NC_COUNT=$(find "$RAW_DATA_DIR" -name "*.nc" -newermt "$TWO_DAYS_AGO" | wc -l)
    log "Recent NetCDF files: $NC_COUNT"
fi

if [[ "$DOWNLOAD_ONLY" == "false" ]]; then
    # Find the most recent TIF files (last 48 hours to be safe)
    TIF_COUNT=$(find "$TIF_DIR" -name "*.tif" -newermt "$TWO_DAYS_AGO" | wc -l)
    log "Recent GeoTIFF files: $TIF_COUNT"
fi

# Calculate total size
RAW_SIZE=$(du -sh "$RAW_DATA_DIR" 2>/dev/null | cut -f1)
TIF_SIZE=$(du -sh "$TIF_DIR" 2>/dev/null | cut -f1)

log "Storage used: Raw data: $RAW_SIZE, TIF files: $TIF_SIZE"
log "Script execution completed."

exit 0 
