#!/bin/bash

# FIRMS Data Pipeline Script
# This script runs the complete FIRMS data pipeline (all 3 steps) for either:
# 1. Historical data:
#    - Uses manually downloaded data from data/raw/firms/historical
#    - Processes, interpolates, and creates KDE heat maps
# 2. Real-time data:
#    - Collects fire data directly from NASA FIRMS servers
#    - Processes, interpolates, and creates KDE heat maps

# Set the base directory to the script location
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
LOG_FILE="$LOG_DIR/firms_pipeline_${TIMESTAMP}.log"

# Configuration-driven defaults with fallbacks
if [[ "$CONFIG_AVAILABLE" == true ]]; then
    DEFAULT_COUNTRIES=($(get_config_countries))
    DEFAULT_RESOLUTION=$(get_config_h3_resolution)
    DEFAULT_DENSITY=$(get_config_firms_kde_setting "grid_size" "6400")
    DEFAULT_CHUNKS=$(get_config_firms_kde_setting "number_of_chunks" "8")
    DEFAULT_BANDWIDTH=$(get_config_firms_kde_setting "bandwidth_factor" "0.3")
    DEFAULT_BUFFER=$(get_config_buffer "firms")
    DEFAULT_TIMEOUT=$(get_config_timeout "processing_timeout")
    echo "✅ Using configuration system for defaults"
    echo "$DEFAULT_BUFFER buffer"
else
    # Fallback defaults if config system unavailable
    DEFAULT_COUNTRIES=("THA" "LAO")
    DEFAULT_RESOLUTION=8
    DEFAULT_DENSITY=6400
    DEFAULT_CHUNKS=8
    DEFAULT_BANDWIDTH=0.3
    DEFAULT_BUFFER=0
    DEFAULT_TIMEOUT=7200
    echo "⚠️  Configuration system unavailable, using hardcoded defaults"
fi

# Default settings (config-driven with fallbacks)
DATA_TYPE="realtime"  # historical or realtime
RESOLUTION="$DEFAULT_RESOLUTION"
DENSITY="$DEFAULT_DENSITY"
CHUNKS="$DEFAULT_CHUNKS"
RAW_DIR=""  # Will be set based on data type
PROCESSED_DIR="$BASE_DIR/data/processed/firms"
COUNTRIES=("${DEFAULT_COUNTRIES[@]}")  # Default countries
PREPARE_ONLY=false
SKIP_DEDUPLICATION=false
START_DATE=""
END_DATE=""
TIMEOUT="$DEFAULT_TIMEOUT"
BUFFER_DEGREES="$DEFAULT_BUFFER"
BANDWIDTH_FACTOR="$DEFAULT_BANDWIDTH"

# Function to log messages
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Help message
usage() {
    echo "FIRMS Fire Detection Pipeline (Configuration-Aware)"
    echo ""
    echo "Complete FIRMS pipeline: Collection + Processing + KDE Interpolation"
    echo ""
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  --mode TYPE           Type of data to process: historical or realtime (default: realtime)"
    echo "  --data-type TYPE      Legacy alias for --mode (deprecated)"
    echo "  --resolution RES      H3 hexagon resolution (default: $DEFAULT_RESOLUTION)"
    echo "  --density DENSITY     Interpolation grid density (default: $DEFAULT_DENSITY)"
    echo "  --chunks CHUNKS       Number of chunks for processing (default: $DEFAULT_CHUNKS)"
    echo "  --raw-dir DIR        Directory for raw data (default: ./data/raw/firms/historical for historical mode)"
    echo "  --processed-dir DIR   Directory for processed data (default: ./data/processed/firms)"
    echo "  --countries LIST     Space-separated list of country codes (default: ${DEFAULT_COUNTRIES[*]})"
    echo "  --prepare-only       Only prepare data without H3 grids and KDE interpolation"
    echo "  --skip-deduplication Skip the deduplication step (faster processing)"
    echo "  --start-date DATE    Start date for filtering data (YYYY-MM-DD format)"
    echo "  --end-date DATE      End date for filtering data (YYYY-MM-DD format)"
    echo "  --timeout SECONDS    Processing timeout (default: ${DEFAULT_TIMEOUT}s)"
    echo "  --buffer DEGREES     Geographic buffer for boundaries (default: ${DEFAULT_BUFFER}°)"
    echo "  --bandwidth FACTOR   KDE bandwidth factor (default: $DEFAULT_BANDWIDTH)"
    echo "  --help               Display this help message"
    echo ""
    echo "Configuration Integration:"
    echo "  This script uses the centralized configuration system when available."
    echo "  Configuration defaults are loaded from config/config.yaml."
    echo "  Command-line arguments override configuration defaults."
    echo ""
    echo "Processing Modes:"
    echo "  realtime             Download and process recent fire detection data"
    echo "  historical           Process manually downloaded historical data"
    echo ""
    echo "Configuration Status:"
    echo "  Config system: $([[ "$CONFIG_AVAILABLE" == true ]] && echo "✅ Available" || echo "❌ Using fallbacks")"
    echo "  Default countries: ${DEFAULT_COUNTRIES[*]}"
    echo "  Default resolution: $DEFAULT_RESOLUTION"
    echo "  Default density: $DEFAULT_DENSITY"
    echo "  Default chunks: $DEFAULT_CHUNKS"
    echo "  Default timeout: ${DEFAULT_TIMEOUT}s"
    echo "  Default buffer: ${DEFAULT_BUFFER}°"
    echo "  Default bandwidth: $DEFAULT_BANDWIDTH"
    echo ""
    exit 1
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --mode)
            DATA_TYPE="$2"
            shift 2
            ;;
        --data-type)
            # Legacy alias for --mode (deprecated)
            DATA_TYPE="$2"
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
        --raw-dir)
            RAW_DIR="$2"
            shift 2
            ;;
        --processed-dir)
            PROCESSED_DIR="$2"
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
        --buffer)
            BUFFER_DEGREES="$2"
            shift 2
            ;;
        --bandwidth)
            BANDWIDTH_FACTOR="$2"
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

# Set default raw directory based on data type if not provided
if [[ -z "$RAW_DIR" && "$DATA_TYPE" == "historical" ]]; then
    RAW_DIR="$BASE_DIR/data/raw/firms/historical"
fi

# Log start of pipeline execution
log "Starting complete FIRMS data pipeline (Collection + Processing + KDE)..."
log "Data type: $DATA_TYPE"
if [[ "$DATA_TYPE" == "historical" ]]; then
    log "Raw data directory: $RAW_DIR"
else
    log "Mode: Direct download from NASA FIRMS servers"
fi
log "Processed data directory: $PROCESSED_DIR"
if [[ "$PREPARE_ONLY" == true ]]; then
    log "Mode: Data preparation only (skipping H3 grids and KDE interpolation)"
else
    log "Mode: Full processing (preparation + H3 grids + KDE interpolation)"
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

# Apply 7-day buffer for rolling calculations
BUFFER_DAYS=7
ORIGINAL_START_DATE=""
ORIGINAL_END_DATE=""

if [[ "$DATA_TYPE" == "historical" && -n "$START_DATE" ]]; then
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
        log "Collecting with ${BUFFER_DAYS}-day buffer: $BUFFER_START_DATE to $END_DATE"
        log "Buffer allows calculation of ${BUFFER_DAYS}-day rolling averages from day 1"
        
        # Use buffered date for collection
        START_DATE="$BUFFER_START_DATE"
    fi
elif [[ "$DATA_TYPE" == "realtime" ]]; then
    # For realtime, calculate date range for at least 7 days
    if [[ -z "$END_DATE" ]]; then
        END_DATE=$(date "+%Y-%m-%d")
    fi
    
    if [[ -z "$START_DATE" ]]; then
        # Calculate 7 days back from end date
        if [[ "$OSTYPE" == "darwin"* ]]; then
            START_DATE=$(date -j -v-${BUFFER_DAYS}d -f "%Y-%m-%d" "$END_DATE" "+%Y-%m-%d" 2>/dev/null)
        else
            START_DATE=$(date --date="$END_DATE -${BUFFER_DAYS} days" "+%Y-%m-%d" 2>/dev/null)
        fi
        
        log "Realtime mode: collecting ${BUFFER_DAYS} days of data ($START_DATE to $END_DATE)"
    fi
fi

# Process data based on type
if [[ "$DATA_TYPE" == "historical" ]]; then
    # Process historical data
    log "Processing historical data..."
    
    # Build array of arguments
    ARGS=()
    ARGS+=(--data-type historical)
    ARGS+=(--data-dir "$RAW_DIR")
    ARGS+=(--output-dir "$PROCESSED_DIR")
    ARGS+=(--countries "${COUNTRIES[@]}")
    ARGS+=(--buffer "$BUFFER_DEGREES")
    
    # Add date range arguments (buffered dates for collection)
    if [[ -n "$START_DATE" ]]; then
        ARGS+=(--start-date "$START_DATE")
    fi
    if [[ -n "$END_DATE" ]]; then
        ARGS+=(--end-date "$END_DATE")
    fi
    
    # Add original dates for filtering final output
    if [[ -n "$ORIGINAL_START_DATE" ]]; then
        ARGS+=(--original-start-date "$ORIGINAL_START_DATE")
    fi
    if [[ -n "$ORIGINAL_END_DATE" ]]; then
        ARGS+=(--original-end-date "$ORIGINAL_END_DATE")
    fi
    
    if [[ "$PREPARE_ONLY" == true ]]; then
        ARGS+=(--prepare-only)
    else
        ARGS+=(--resolution "$RESOLUTION")
        ARGS+=(--density "$DENSITY")
        ARGS+=(--chunks "$CHUNKS")
    fi
    
    if [[ "$SKIP_DEDUPLICATION" == true ]]; then
        ARGS+=(--skip-deduplication)
    fi
    
    log "Starting FIRMS data processing..."
    log "Data type: $DATA_TYPE"
    log "Mode: Direct download from NASA FIRMS servers"
    log "Output directory: $PROCESSED_DIR"
    if [[ "$PREPARE_ONLY" == true ]]; then
        log "Mode: Data preparation only (skipping interpolation + H3 grid)"
    else
        log "Mode: Full processing (preparation + interpolation + H3 grid)"
        log "H3 resolution: $RESOLUTION"
        log "Interpolation density: $DENSITY"
        log "Processing chunks: $CHUNKS"
    fi
    log "Countries to process: ${COUNTRIES[@]}"
    if [[ "$SKIP_DEDUPLICATION" == true ]]; then
        log "Deduplication: SKIPPED (faster processing)"
    else
        log "Deduplication: ENABLED (removes overlapping MODIS/VIIRS fires)"
    fi
    
    # Execute the Python script using the virtual environment's Python
    log "Executing command: python src/data_processors/firms_data_processor.py ${ARGS[@]}"
    python src/data_processors/firms_data_processor.py "${ARGS[@]}"
else
    # Process realtime data
    log "Processing realtime data directly from NASA FIRMS servers..."
    
    # Build array of arguments
    ARGS=()
    ARGS+=(--data-type realtime)
    ARGS+=(--output-dir "$PROCESSED_DIR")
    ARGS+=(--countries "${COUNTRIES[@]}")
    ARGS+=(--buffer "$BUFFER_DEGREES")
    
    # Add date range arguments (with 7-day buffer already calculated)
    if [[ -n "$START_DATE" ]]; then
        ARGS+=(--start-date "$START_DATE")
    fi
    if [[ -n "$END_DATE" ]]; then
        ARGS+=(--end-date "$END_DATE")
    fi
    
    # For realtime, original dates are same as buffered dates (buffer applied above)
    # No need to pass original-start-date/original-end-date for realtime
    
    if [[ "$PREPARE_ONLY" == true ]]; then
        ARGS+=(--prepare-only)
    else
        ARGS+=(--resolution "$RESOLUTION")
        ARGS+=(--density "$DENSITY")
        ARGS+=(--chunks "$CHUNKS")
    fi
    
    if [[ "$SKIP_DEDUPLICATION" == true ]]; then
        ARGS+=(--skip-deduplication)
    fi
    
    log "Starting FIRMS data processing..."
    log "Data type: $DATA_TYPE"
    log "Mode: Direct download from NASA FIRMS servers"
    log "Output directory: $PROCESSED_DIR"
    if [[ "$PREPARE_ONLY" == true ]]; then
        log "Mode: Data preparation only (skipping interpolation + H3 grid)"
    else
        log "Mode: Full processing (preparation + interpolation + H3 grid)"
        log "H3 resolution: $RESOLUTION"
        log "Interpolation density: $DENSITY"
        log "Processing chunks: $CHUNKS"
    fi
    log "Countries to process: ${COUNTRIES[@]}"
    if [[ "$SKIP_DEDUPLICATION" == true ]]; then
        log "Deduplication: SKIPPED (faster processing)"
    else
        log "Deduplication: ENABLED (removes overlapping MODIS/VIIRS fires)"
    fi
    
    # Execute the Python script using the virtual environment's Python
    log "Executing command: python src/data_processors/firms_data_processor.py ${ARGS[@]}"
    python src/data_processors/firms_data_processor.py "${ARGS[@]}"
fi

# Check if processing was successful
if [ $? -ne 0 ]; then
    log "ERROR: Data processing failed. Exiting pipeline."
    exit 1
fi

log "Data processing completed successfully."

# Step 3: KDE Interpolation (for both realtime and historical)
if [[ "$PREPARE_ONLY" == false ]]; then
    log "STEP 3: Starting KDE interpolation..."
    
    if [[ "$DATA_TYPE" == "historical" ]]; then
        # Use dedicated historical KDE script
        log "Using historical KDE interpolation script..."
        
        # Build the KDE arguments array properly
        KDE_ARGS_ARRAY=()
        KDE_ARGS_ARRAY+=(--countries "${COUNTRIES[@]}")
        
        # Build the correct input file path based on what was actually created
        COUNTRIES_STR=$(printf "%s_" "${COUNTRIES[@]}" | sed 's/_$//')
        if [[ -n "$START_DATE" && -n "$END_DATE" ]]; then
            START_STR=$(date -d "$START_DATE" +"%Y%m%d" 2>/dev/null || date -j -f "%Y-%m-%d" "$START_DATE" +"%Y%m%d")
            END_STR=$(date -d "$END_DATE" +"%Y%m%d" 2>/dev/null || date -j -f "%Y-%m-%d" "$END_DATE" +"%Y%m%d")
            DATE_RANGE="${START_STR}_to_${END_STR}"
            EXPECTED_FILE="$PROCESSED_DIR/deduplicated/historical/firms_prepared_historical_${COUNTRIES_STR}_${DATE_RANGE}.parquet"
            
            # Check if the expected file exists
            if [[ -f "$EXPECTED_FILE" ]]; then
                KDE_ARGS_ARRAY+=(--input-file "$EXPECTED_FILE")
                log "Using specific input file: $EXPECTED_FILE"
            else
                log "WARNING: Expected file not found: $EXPECTED_FILE"
                log "KDE script will try to find the file automatically"
            fi
        fi
        
        # Add date arguments for historical mode
        if [[ -n "$START_DATE" ]]; then
            KDE_ARGS_ARRAY+=(--start-date "$START_DATE")
        fi
        if [[ -n "$END_DATE" ]]; then
            KDE_ARGS_ARRAY+=(--end-date "$END_DATE")
        fi
        
        KDE_BUFFER_DEGREES=0.4
        KDE_ARGS_ARRAY+=(--buffer "$KDE_BUFFER_DEGREES")
        
        log "Running historical KDE interpolation..."
        scripts/run_firms_kde_historical.sh "${KDE_ARGS_ARRAY[@]}"
    else
        # Use realtime KDE script
        log "Using realtime KDE interpolation script..."
        
        # Build the KDE arguments array properly for realtime mode
        KDE_ARGS_ARRAY=()
        KDE_ARGS_ARRAY+=(--countries "${COUNTRIES[@]}")
        KDE_BUFFER_DEGREES=0.4
        KDE_ARGS_ARRAY+=(--buffer "$KDE_BUFFER_DEGREES")
        
        log "Running realtime KDE interpolation..."
        scripts/run_firms_kde.sh "${KDE_ARGS_ARRAY[@]}"
    fi
    
    # Check if KDE interpolation was successful
    if [ $? -ne 0 ]; then
        log "ERROR: KDE interpolation failed. Pipeline incomplete."
        exit 1
    fi
    
    log "KDE interpolation completed successfully."
fi

# Summarize results
if [[ "$PREPARE_ONLY" == true ]]; then
    log "FIRMS data preparation completed successfully. No H3 grids or KDE interpolation was performed."
else
    log "Complete FIRMS pipeline executed successfully:"
    log "1. Data preparation: ✅"
    log "2. Interpolation and H3 grid creation: ✅"
    log "3. KDE interpolation: ✅"
fi

exit 0 