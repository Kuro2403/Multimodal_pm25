#!/bin/bash

# Himawari Complete Pipeline Script
# This script runs the complete Himawari AOD pipeline:
# 1. Download + H3 processing
# 2. Daily aggregation 
# 3. IDW interpolation

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

# Set PYTHONPATH to include the project root directory
export PYTHONPATH="$BASE_DIR:$PYTHONPATH"

# Create logs directory if it doesn't exist
if [[ "$CONFIG_AVAILABLE" == true ]]; then
    LOG_DIR=$(get_config_path "logs" "./logs")
else
    LOG_DIR="$BASE_DIR/logs"
fi
mkdir -p "$LOG_DIR"

# Set log file with timestamp
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="$LOG_DIR/himawari_complete_${TIMESTAMP}.log"

# Configuration-driven defaults with fallbacks
if [[ "$CONFIG_AVAILABLE" == true ]]; then
    DEFAULT_COUNTRIES=($(get_config_countries))
    DEFAULT_HOURS=$(get_config_time_window "realtime")
    DEFAULT_BUFFER=$(get_config_buffer "himawari")
    DEFAULT_H3_RESOLUTION=$(get_config_h3_resolution)
    DEFAULT_TIMEOUT=$(get_config_timeout "processing_timeout")
    echo "✅ Using configuration system for defaults"
else
    # Fallback defaults if config system unavailable
    DEFAULT_COUNTRIES=("LAO" "THA")
    DEFAULT_HOURS=24
    DEFAULT_BUFFER=0.4
    DEFAULT_H3_RESOLUTION=8
    DEFAULT_TIMEOUT=7200
    echo "⚠️  Configuration system unavailable, using hardcoded defaults"
fi

# Default settings (config-driven with fallbacks)
MODE="realtime"
HOURS="$DEFAULT_HOURS"
COUNTRIES=("${DEFAULT_COUNTRIES[@]}")
KEEP_ORIGINALS=false
SKIP_AGGREGATION=false
SKIP_INTERPOLATION=false
TIMEOUT="$DEFAULT_TIMEOUT"
BUFFER_DEGREES="$DEFAULT_BUFFER"
H3_RESOLUTION="$DEFAULT_H3_RESOLUTION"

# Function to log messages
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Function to check if interpolated files exist for date range
check_interpolated_files_exist() {
    local start_date=$1
    local end_date=$2
    local interpolated_dir="./data/processed/himawari/interpolated/historical"
    
    # Build country string (sorted alphabetically to match file naming)
    local country_str=$(printf '%s\n' "${COUNTRIES[@]}" | sort | tr '\n' '_' | sed 's/_$//')
    
    # Convert dates to YYYYMMDD format
    start_yyyymmdd=$(date -j -f "%Y-%m-%d" "$start_date" "+%Y%m%d" 2>/dev/null || date --date="$start_date" "+%Y%m%d")
    end_yyyymmdd=$(date -j -f "%Y-%m-%d" "$end_date" "+%Y%m%d" 2>/dev/null || date --date="$end_date" "+%Y%m%d")
    
    # Check if all expected files exist
    current_date=$start_yyyymmdd
    while [ "$current_date" -le "$end_yyyymmdd" ]; do
        # Check for the interpolated file with the correct pattern (country codes sorted alphabetically)
        if [ ! -f "$interpolated_dir/interpolated_h3_aod_${current_date}_${country_str}.parquet" ]; then
            return 1  # Files don't exist
        fi
        
        # Increment date
        current_date=$(date -j -v+1d -f "%Y%m%d" "$current_date" "+%Y%m%d" 2>/dev/null || date --date="$current_date +1 day" "+%Y%m%d")
    done
    
    return 0  # All files exist
}

# Function to check if interpolated files exist for a single date
check_interpolated_file_exists() {
    local date=$1
    local interpolated_dir="./data/processed/himawari/interpolated/historical"
    
    # Build country string (sorted alphabetically to match file naming)
    local country_str=$(printf '%s\n' "${COUNTRIES[@]}" | sort | tr '\n' '_' | sed 's/_$//')
    
    local interpolated_file="$interpolated_dir/interpolated_h3_aod_${date}_${country_str}.parquet"
    
    if [ -f "$interpolated_file" ]; then
        return 0  # File exists
    else
        return 1  # File doesn't exist
    fi
}

# Function to validate mode-specific parameters
validate_mode_parameters() {
    if [ "$MODE" = "historical" ]; then
        if [ -z "$START_DATE" ] || [ -z "$END_DATE" ]; then
            log "ERROR: Historical mode requires both --start-date and --end-date parameters"
            echo "ERROR: Historical mode requires both --start-date and --end-date parameters"
            echo "Example: $0 --mode historical --start-date 2025-01-01 --end-date 2025-01-31 --countries THA LAO"
            exit 1
        fi
        
        # Validate date format
        if ! date -d "$START_DATE" >/dev/null 2>&1 && ! date -j -f "%Y-%m-%d" "$START_DATE" >/dev/null 2>&1; then
            log "ERROR: Invalid start date format. Use YYYY-MM-DD"
            echo "ERROR: Invalid start date format: $START_DATE. Use YYYY-MM-DD"
            exit 1
        fi
        
        if ! date -d "$END_DATE" >/dev/null 2>&1 && ! date -j -f "%Y-%m-%d" "$END_DATE" >/dev/null 2>&1; then
            log "ERROR: Invalid end date format. Use YYYY-MM-DD"
            echo "ERROR: Invalid end date format: $END_DATE. Use YYYY-MM-DD"
            exit 1
        fi
        
        log "Mode validation: Historical mode with date range $START_DATE to $END_DATE"
    elif [ "$MODE" = "realtime" ]; then
        if [ -n "$START_DATE" ] || [ -n "$END_DATE" ]; then
            log "WARNING: Start/end dates ignored in realtime mode. Using --hours parameter instead."
            echo "WARNING: Start/end dates ignored in realtime mode. Using --hours parameter instead."
        fi
        
        if [ "$HOURS" -lt 1 ] || [ "$HOURS" -gt 168 ]; then
            log "ERROR: Hours parameter must be between 1 and 168 (1 week)"
            echo "ERROR: Hours parameter must be between 1 and 168 (1 week). Current value: $HOURS"
            exit 1
        fi
        
        log "Mode validation: Realtime mode with $HOURS hours lookback"
    else
        log "ERROR: Invalid mode. Must be 'historical' or 'realtime'"
        echo "ERROR: Invalid mode: $MODE. Must be 'historical' or 'realtime'"
        exit 1
    fi
}

# Function to validate countries
validate_countries() {
    local valid_countries=("LAO" "THA" "VNM" "KHM" "IDN" "MYS" "SGP" "BRN")
    
    for country in "${COUNTRIES[@]}"; do
        local found=false
        for valid in "${valid_countries[@]}"; do
            if [ "$country" = "$valid" ]; then
                found=true
                break
            fi
        done
        
        if [ "$found" = false ]; then
            log "WARNING: Country code '$country' may not be supported. Supported codes: ${valid_countries[*]}"
            echo "WARNING: Country code '$country' may not be supported. Supported codes: ${valid_countries[*]}"
        fi
    done
    
    log "Countries validation: Processing ${#COUNTRIES[@]} countries: ${COUNTRIES[*]}"
}

# Help message
usage() {
    echo "Himawari AOD Complete Pipeline (Configuration-Aware)"
    echo ""
    echo "This script runs the complete Himawari AOD pipeline:"
    echo "1. Download + H3 processing"
    echo "2. Daily aggregation"
    echo "3. IDW interpolation"
    echo ""
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  --mode MODE           Operation mode: historical or realtime (default: realtime)"
    echo "  --hours HOURS         Number of hours to collect in realtime mode (default: $DEFAULT_HOURS)"
    echo "  --countries CODES     Country codes for boundaries (default: ${DEFAULT_COUNTRIES[*]})"
    echo "  --start-date DATE     Start date for historical mode (format: YYYY-MM-DD)"
    echo "  --end-date DATE       End date for historical mode (format: YYYY-MM-DD)"
    echo "  --keep-originals      Keep original NetCDF and TIF files in permanent locations"
    echo "  --skip-download       Skip download step"
    echo "  --skip-h3             Skip H3 processing step"
    echo "  --skip-aggregation    Skip daily aggregation step"
    echo "  --skip-interpolation  Skip IDW interpolation step"
    echo "  --force-download      Force re-download of existing files"
    echo "  --timeout SECONDS     Processing timeout (default: ${DEFAULT_TIMEOUT}s)"
    echo "  --buffer DEGREES      Geographic buffer for boundaries (default: $DEFAULT_BUFFER)"
    echo "  --resolution LEVEL    H3 resolution level (default: $DEFAULT_H3_RESOLUTION)"
    echo "  --help                Display this help message"
    echo ""
    echo "Configuration Integration:"
    echo "  This script uses the centralized configuration system when available."
    echo "  Configuration defaults are loaded from config/config.yaml."
    echo "  Command-line arguments override configuration defaults."
    echo ""
    echo "STORAGE MODES:"
    echo "  Default (cache-based)  NetCDF → cache → delete, TIF → cache → delete, only H3 kept"
    echo "  --keep-originals      NetCDF + TIF + H3 all kept in permanent locations"
    echo ""
    echo "STORAGE EFFICIENCY:"
    echo "  Cache-based: ~99% space savings (10GB NetCDF → 100MB H3)"
    echo "  Keep-originals: Full storage but fastest reprocessing"
    echo "  Use cache-based for production, keep-originals for development"
    echo ""
    echo "Configuration Status:"
    echo "  Config system: $([[ "$CONFIG_AVAILABLE" == true ]] && echo "✅ Available" || echo "❌ Using fallbacks")"
    echo "  Default countries: ${DEFAULT_COUNTRIES[*]}"
    echo "  Default hours: $DEFAULT_HOURS"
    echo "  Default timeout: ${DEFAULT_TIMEOUT}s"
    echo "  Default buffer: ${DEFAULT_BUFFER}°"
    echo "  Default H3 resolution: $DEFAULT_H3_RESOLUTION"
    echo ""
    exit 1
}

# Parse command line arguments
INTEGRATED_ARGS=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --mode)
            MODE="$2"
            INTEGRATED_ARGS="$INTEGRATED_ARGS --mode $2"
            shift 2
            ;;
        --hours)
            HOURS="$2"
            INTEGRATED_ARGS="$INTEGRATED_ARGS --hours $2"
            shift 2
            ;;
        --countries)
            COUNTRIES=()
            shift
            while [[ $# -gt 0 && ! "$1" =~ ^-- ]]; do
                COUNTRIES+=("$1")
                shift
            done
            INTEGRATED_ARGS="$INTEGRATED_ARGS --countries ${COUNTRIES[*]}"
            ;;
        --start-date)
            START_DATE="$2"
            INTEGRATED_ARGS="$INTEGRATED_ARGS --start-date $2"
            shift 2
            ;;
        --end-date)
            END_DATE="$2"
            INTEGRATED_ARGS="$INTEGRATED_ARGS --end-date $2"
            shift 2
            ;;
        --keep-originals)
            KEEP_ORIGINALS=true
            INTEGRATED_ARGS="$INTEGRATED_ARGS --keep-originals"
            shift
            ;;
        --skip-download)
            INTEGRATED_ARGS="$INTEGRATED_ARGS --skip-download"
            shift
            ;;
        --skip-h3)
            INTEGRATED_ARGS="$INTEGRATED_ARGS --skip-h3"
            shift
            ;;
        --skip-aggregation)
            SKIP_AGGREGATION=true
            shift
            ;;
        --skip-interpolation)
            SKIP_INTERPOLATION=true
            shift
            ;;
        --force-download)
            INTEGRATED_ARGS="$INTEGRATED_ARGS --force-download"
            shift
            ;;
        --timeout)
            TIMEOUT="$2"
            INTEGRATED_ARGS="$INTEGRATED_ARGS --timeout $2"
            shift 2
            ;;
        --buffer)
            BUFFER_DEGREES="$2"
            INTEGRATED_ARGS="$INTEGRATED_ARGS --buffer $2"
            shift 2
            ;;
        --resolution)
            H3_RESOLUTION="$2"
            INTEGRATED_ARGS="$INTEGRATED_ARGS --resolution $2"
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
validate_mode_parameters
validate_countries

# Apply 7-day buffer for rolling calculations
BUFFER_DAYS=7
ORIGINAL_START_DATE=""
ORIGINAL_END_DATE=""

if [[ "$MODE" == "historical" && -n "$START_DATE" ]]; then
    # Store original dates
    ORIGINAL_START_DATE="$START_DATE"
    ORIGINAL_END_DATE="$END_DATE"
    
    # Calculate buffered start date (7 days before)
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS date command
        BUFFER_START_DATE=$(date -j -v-${BUFFER_DAYS}d -f "%Y-%m-%d" "$START_DATE" "+%Y-%m-%d" 2>/dev/null)
    else
        # Linux date command
        BUFFER_START_DATE=$(date --date="$START_DATE -${BUFFER_DAYS} days" "+%Y-%m-%d" 2>/dev/null)
    fi
    
    if [[ -z "$BUFFER_START_DATE" ]]; then
        log "ERROR: Failed to calculate buffer date. Using original start date."
        BUFFER_START_DATE="$START_DATE"
    else
        log "Original date range requested: $ORIGINAL_START_DATE to $ORIGINAL_END_DATE"
        log "Applying ${BUFFER_DAYS}-day buffer for rolling calculations: $BUFFER_START_DATE to $END_DATE"
        
        # Use buffered date for data collection and processing
        START_DATE="$BUFFER_START_DATE"
        
        # Update integrated args with buffered dates
        INTEGRATED_ARGS=$(echo "$INTEGRATED_ARGS" | sed "s/--start-date $ORIGINAL_START_DATE/--start-date $BUFFER_START_DATE/")
    fi
elif [[ "$MODE" == "realtime" ]]; then
    # For realtime, ensure we collect at least 7 days (168 hours)
    if [[ -n "$HOURS" && "$HOURS" -lt 168 ]]; then
        log "Realtime mode: requested $HOURS hours, extending to 168 hours (7 days) for rolling calculations"
        HOURS=168
        # Update integrated args - check if --hours exists, if not add it
        if echo "$INTEGRATED_ARGS" | grep -q "\--hours"; then
            INTEGRATED_ARGS=$(echo "$INTEGRATED_ARGS" | sed "s/--hours [0-9]*/--hours 168/")
        else
            INTEGRATED_ARGS="$INTEGRATED_ARGS --hours 168"
        fi
    fi
fi

# Log configuration
log "Starting Himawari AOD Complete Pipeline..."
log "Mode: $MODE"
log "Countries: ${COUNTRIES[*]}"
if [ "$MODE" = "realtime" ]; then
    log "Hours (realtime): $HOURS"
fi
log "Keep originals: $KEEP_ORIGINALS"
log "Skip aggregation: $SKIP_AGGREGATION"
log "Skip interpolation: $SKIP_INTERPOLATION"

# STEP 1: Execute the integrated pipeline (download + H3 processing)
log "=== STEP 1: INTEGRATED PIPELINE (Download + H3 Processing) ==="
python src/himawari_integrated_pipeline.py $INTEGRATED_ARGS

# Check if processing was successful
if [ $? -ne 0 ]; then
    log "ERROR: Integrated pipeline failed."
    exit 1
fi
log "Integrated pipeline completed successfully."

# STEP 2: Daily Aggregation
if [ "$SKIP_AGGREGATION" = false ]; then
    # For historical mode, check if kriged files already exist
    if [ "$MODE" = "historical" ] && [ ! -z "$START_DATE" ] && [ ! -z "$END_DATE" ]; then
        if check_interpolated_files_exist "$START_DATE" "$END_DATE"; then
            log "Interpolated files already exist for date range $START_DATE to $END_DATE"
            log "Skipping aggregation and interpolation steps"
            SKIP_AGGREGATION=true
            SKIP_INTERPOLATION=true
        else
            # Check each date individually for daily aggregation
            log "=== STEP 2: DAILY AGGREGATION ==="
            
            # Convert dates to YYYYMMDD format
            start_yyyymmdd=$(date -j -f "%Y-%m-%d" "$START_DATE" "+%Y%m%d" 2>/dev/null || date --date="$START_DATE" "+%Y%m%d")
            end_yyyymmdd=$(date -j -f "%Y-%m-%d" "$END_DATE" "+%Y%m%d" 2>/dev/null || date --date="$END_DATE" "+%Y%m%d")
            
            # Check each date
            current_date=$start_yyyymmdd
            while [ "$current_date" -le "$end_yyyymmdd" ]; do
                if check_interpolated_file_exists "$current_date"; then
                    log "Skipping aggregation for $current_date - interpolated file already exists"
                else
                    log "Running aggregation for $current_date"
                    AGGREGATION_ARGS="--mode $MODE --start-date $(date -j -f "%Y%m%d" "$current_date" "+%Y-%m-%d" 2>/dev/null || date --date="$current_date" "+%Y-%m-%d") --end-date $(date -j -f "%Y%m%d" "$current_date" "+%Y-%m-%d" 2>/dev/null || date --date="$current_date" "+%Y-%m-%d")"
                    python src/data_processors/himawari_daily_aggregator.py $AGGREGATION_ARGS
                    
                    if [ $? -ne 0 ]; then
                        log "ERROR: Daily aggregation failed for $current_date"
                        exit 1
                    fi
                fi
                
                # Increment date
                current_date=$(date -j -v+1d -f "%Y%m%d" "$current_date" "+%Y%m%d" 2>/dev/null || date --date="$current_date +1 day" "+%Y%m%d")
            done
            
            log "Daily aggregation completed successfully."
        fi
    else
        # For realtime mode, run aggregation as before
        log "=== STEP 2: DAILY AGGREGATION ==="
        
        AGGREGATION_ARGS="--mode $MODE --hours-lookback $HOURS"
        
        log "Running daily aggregation..."
        python src/data_processors/himawari_daily_aggregator.py $AGGREGATION_ARGS
        
        if [ $? -eq 0 ]; then
            log "Daily aggregation completed successfully."
        else
            log "ERROR: Daily aggregation failed."
            exit 1
        fi
    fi
else
    log "Skipping daily aggregation step."
fi

# STEP 3: IDW Interpolation
if [ "$SKIP_INTERPOLATION" = false ]; then
    log "=== STEP 3: IDW INTERPOLATION ==="
    
    # Determine the correct daily aggregated directory based on mode
    DAILY_AGG_DIR="./data/processed/himawari/daily_aggregated"
    
    # For historical mode, check each date individually
    if [ "$MODE" = "historical" ] && [ ! -z "$START_DATE" ] && [ ! -z "$END_DATE" ]; then
        # Convert dates to YYYYMMDD format
        start_yyyymmdd=$(date -j -f "%Y-%m-%d" "$START_DATE" "+%Y%m%d" 2>/dev/null || date --date="$START_DATE" "+%Y%m%d")
        end_yyyymmdd=$(date -j -f "%Y-%m-%d" "$END_DATE" "+%Y%m%d" 2>/dev/null || date --date="$END_DATE" "+%Y%m%d")
        
        # Check each date
        current_date=$start_yyyymmdd
        while [ "$current_date" -le "$end_yyyymmdd" ]; do
            if check_interpolated_file_exists "$current_date"; then
                log "Skipping interpolation for $current_date - file already exists"
            else
                log "Running IDW interpolation for $current_date"
                INTERPOLATION_ARGS="--mode $MODE --countries ${COUNTRIES[*]} --start-date $(date -j -f "%Y%m%d" "$current_date" "+%Y-%m-%d" 2>/dev/null || date --date="$current_date" "+%Y-%m-%d") --end-date $(date -j -f "%Y%m%d" "$current_date" "+%Y-%m-%d" 2>/dev/null || date --date="$current_date" "+%Y-%m-%d") --rings 10"
                python src/data_processors/himawari_idw_interpolator.py $INTERPOLATION_ARGS
                
                if [ $? -ne 0 ]; then
                    log "ERROR: IDW interpolation failed for $current_date"
                    exit 1
                fi
            fi
            
            # Increment date
            current_date=$(date -j -v+1d -f "%Y%m%d" "$current_date" "+%Y%m%d" 2>/dev/null || date --date="$current_date +1 day" "+%Y%m%d")
        done
    else
        # For realtime mode, run IDW interpolation
        INTERPOLATION_ARGS="--mode $MODE --countries ${COUNTRIES[*]} --rings 10"
        log "Running IDW interpolation..."
        python src/data_processors/himawari_idw_interpolator.py $INTERPOLATION_ARGS
        
        if [ $? -eq 0 ]; then
            log "IDW interpolation completed successfully."
        else
            log "ERROR: IDW interpolation failed."
            exit 1
        fi
    fi
else
    log "Skipping IDW interpolation step."
fi

log "=== PIPELINE SUMMARY ==="

# Show final directory sizes
if [ -d "./data/raw/himawari" ]; then
    RAW_SIZE=$(du -sh ./data/raw/himawari 2>/dev/null | cut -f1)
    log "Raw data size: $RAW_SIZE"
fi

if [ -d "./data/processed/himawari/h3" ]; then
    H3_SIZE=$(du -sh ./data/processed/himawari/h3 2>/dev/null | cut -f1)
    log "H3 data size: $H3_SIZE"
fi

if [ -d "./data/processed/himawari/daily_aggregated" ]; then
    AGG_SIZE=$(du -sh ./data/processed/himawari/daily_aggregated 2>/dev/null | cut -f1)
    log "Daily aggregated size: $AGG_SIZE"
fi

if [ -d "./data/processed/himawari/interpolated" ]; then
    INTERPOLATED_SIZE=$(du -sh ./data/processed/himawari/interpolated 2>/dev/null | cut -f1)
    log "Interpolated data size: $INTERPOLATED_SIZE"
fi

log "🎉 HIMAWARI AOD PIPELINE COMPLETED SUCCESSFULLY!"
log "Log file: $LOG_FILE"
exit 0 