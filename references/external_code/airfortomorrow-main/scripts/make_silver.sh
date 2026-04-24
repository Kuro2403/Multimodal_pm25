#!/bin/bash

# Silver Dataset Generation Script with Centralized Configuration
# 
# This script processes ERA5, Himawari, and FIRMS data into a clean "silver" dataset.
# Supports both real-time and historical processing modes.
#
# Usage:
#   ./scripts/make_silver.sh --mode realtime --hours 24 --countries THA LAO
#   ./scripts/make_silver.sh --mode historical --start-date 2024-06-01 --end-date 2024-06-03 --countries THA LAO

# Set up error handling and script directory
set -e  # Exit on any error
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Source configuration and utility functions
if [[ -f "$SCRIPT_DIR/utils/config_reader.sh" ]]; then
    source "$SCRIPT_DIR/utils/config_reader.sh"
else
    echo "Warning: Configuration system not available, using fallbacks"
fi

if [[ -f "$SCRIPT_DIR/utils/common.sh" ]]; then
    source "$SCRIPT_DIR/utils/common.sh"
else
    echo "Warning: Common utilities not available"
    # Define basic logging if common.sh not available
    log_info() { echo "[INFO] $1"; }
    log_warn() { echo "[WARN] $1"; }
    log_error() { echo "[ERROR] $1"; }
    log_success() { echo "[SUCCESS] $1"; }
    log_debug() { echo "[DEBUG] $1"; }
    
    # Define missing utility functions
    validate_required_args() {
        for arg in "$@"; do
            local key="${arg%%:*}"
            local value="${arg##*:}"
            if [[ -z "$value" ]]; then
                log_error "Required argument --$key is missing"
                return 1
            fi
        done
        return 0
    }
    
    validate_date() {
        local date_str="$1"
        
        # Use Python date validator for reliable cross-platform validation
        if command -v python3 &> /dev/null; then
            if python3 "$BASE_DIR/scripts/utils/date_validator.py" validate "$date_str" &>/dev/null; then
                return 0
            else
                log_error "Invalid date format: $date_str (expected YYYY-MM-DD)"
                return 1
            fi
        else
            # Fallback: basic format check if Python is not available
            if ! [[ "$date_str" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
                log_error "Invalid date format: $date_str (expected YYYY-MM-DD)"
                return 1
            fi
            return 0
        fi
    }
    
    validate_numeric() {
        local value="$1"
        local min="$2"
        local max="$3"
        local name="$4"
        if ! [[ "$value" =~ ^[0-9]+$ ]] || [[ "$value" -lt "$min" ]] || [[ "$value" -gt "$max" ]]; then
            log_error "Invalid $name: $value (must be between $min and $max)"
            return 1
        fi
        return 0
    }
    
    validate_directory() {
        local dir="$1"
        local create="$2"
        if [[ ! -d "$dir" ]]; then
            if [[ "$create" == "true" ]]; then
                mkdir -p "$dir" 2>/dev/null || return 1
            else
                return 1
            fi
        fi
        return 0
    }
    
    execute_with_timeout() {
        local timeout="$1"
        local description="$2"
        shift 2
        if command -v timeout >/dev/null 2>&1; then
            timeout "$timeout" "$@"
        else
            "$@"
        fi
        return $?
    }
    
    count_files() {
        local dir="$1"
        local pattern="$2"
        find "$dir" -name "$pattern" 2>/dev/null | wc -l
    }
    
    show_file_size() {
        local dir="$1"
        if [[ -d "$dir" ]]; then
            du -sh "$dir" 2>/dev/null || echo "Unable to calculate size"
        fi
    }
fi

# Setup environment and error handling (only if functions are available)
if command -v setup_environment &> /dev/null; then
    setup_environment "make_silver"
fi

if command -v setup_error_handling &> /dev/null; then
    setup_error_handling
fi

# Configuration-driven defaults with fallbacks
if command -v get_config_countries &> /dev/null; then
    DEFAULT_COUNTRIES=$(get_config_countries)
    DEFAULT_RESOLUTION=$(get_config_h3_resolution)
    DEFAULT_CACHE_DIR=$(get_config_path "cache.silver")
    DEFAULT_LOG_LEVEL=$(get_config_log_level)
    log_success "Using configuration system for defaults"
else
    # Fallback defaults if config system unavailable
    DEFAULT_COUNTRIES="THA LAO"
    DEFAULT_RESOLUTION=8
    DEFAULT_CACHE_DIR="./data/cache/silver"
    DEFAULT_LOG_LEVEL="INFO"
    log_warn "Configuration system unavailable, using hardcoded defaults"
fi

# Script variables
PYTHON_SCRIPT="$BASE_DIR/src/make_silver.py"

# Default values (config-driven with fallbacks)
MODE=""
COUNTRIES="$DEFAULT_COUNTRIES"
HOURS=24
START_DATE=""
END_DATE=""
RESOLUTION="$DEFAULT_RESOLUTION"
CACHE_DIR="$DEFAULT_CACHE_DIR"
LOG_LEVEL="$DEFAULT_LOG_LEVEL"

# Function to show usage with configuration-aware defaults
usage() {
    cat << EOF
🌍 Silver Dataset Generation Script (Configuration-Aware)

Process ERA5, Himawari, and FIRMS data into a clean "silver" dataset.
Uses centralized configuration system with backwards-compatible fallbacks.

Usage:
    $0 --mode MODE [OPTIONS]

Required Arguments:
    --mode MODE              Processing mode: 'realtime' or 'historical'

Mode-Specific Arguments:
    For realtime mode:
      --hours HOURS          Hours to look back (default: 24)
    
    For historical mode:
      --start-date DATE      Start date (YYYY-MM-DD)
      --end-date DATE        End date (YYYY-MM-DD)

Optional Arguments:
    --countries CODES        Space-separated country codes (default: $DEFAULT_COUNTRIES)
    --resolution LEVEL       H3 resolution level (default: $DEFAULT_RESOLUTION)
    --cache-dir DIR          Cache directory (default: $DEFAULT_CACHE_DIR)
    --log-level LEVEL        Logging level: DEBUG, INFO, WARNING, ERROR (default: $DEFAULT_LOG_LEVEL)
    --config FILE            Custom configuration file path
    --help                   Show this help message

Configuration Integration:
    This script uses the centralized configuration system when available.
    Configuration defaults are loaded from config/config.yaml.
    Command-line arguments override configuration defaults.

Examples:
    # Real-time processing with config defaults
    $0 --mode realtime

    # Real-time processing for Thailand and Laos (past 24 hours)
    $0 --mode realtime --countries THA LAO

    # Real-time processing for past 6 hours
    $0 --mode realtime --hours 6 --countries THA LAO

    # Historical processing for specific date range
    $0 --mode historical --start-date 2024-06-01 --end-date 2024-06-03 --countries THA LAO

    # Multi-country processing
    $0 --mode realtime --countries THA LAO VNM KHM

    # Debug mode with verbose output
    $0 --mode realtime --countries THA LAO --log-level DEBUG

    # Use custom configuration file
    $0 --mode realtime --config /path/to/custom/config.yaml

Output:
    Silver datasets are saved to:
    - ./data/silver/realtime/    (for realtime mode)
    - ./data/silver/historical/  (for historical mode)

    Cache files are stored in:
    - $DEFAULT_CACHE_DIR (configurable cache directory)

Configuration Status:
    Config system: $(command -v get_config_countries &> /dev/null && echo "✅ Available" || echo "❌ Using fallbacks")
    Default countries: $DEFAULT_COUNTRIES
    Default H3 resolution: $DEFAULT_RESOLUTION
    Default cache directory: $DEFAULT_CACHE_DIR

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --mode)
            MODE="$2"
            shift 2
            ;;
        --countries)
            COUNTRIES=""
            shift
            while [[ $# -gt 0 && ! "$1" =~ ^-- ]]; do
                COUNTRIES="$COUNTRIES $1"
                shift
            done
            COUNTRIES=$(echo "$COUNTRIES" | sed 's/^ *//')
            ;;
        --hours)
            HOURS="$2"
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
        --resolution)
            RESOLUTION="$2"
            shift 2
            ;;
        --cache-dir)
            CACHE_DIR="$2"
            shift 2
            ;;
        --log-level)
            LOG_LEVEL="$2"
            shift 2
            ;;
        --config)
            CONFIG_FILE="$2"
            log_info "Using custom configuration file: $CONFIG_FILE"
            shift 2
            ;;
        --help)
            usage
            exit 0
            ;;
        *)
            log_error "Unknown option $1"
            usage
            exit 1
            ;;
    esac
done



# Validate required arguments
if ! validate_required_args "mode:$MODE"; then
    usage
    exit 1
fi

read -r -a COUNTRY_ARR <<< "$COUNTRIES"

if [[ "$MODE" != "realtime" && "$MODE" != "historical" ]]; then
    log_error "--mode must be 'realtime' or 'historical'"
    usage
    exit 1
fi

# Validate mode-specific arguments
if [[ "$MODE" == "historical" ]]; then
    if ! validate_required_args "start-date:$START_DATE" "end-date:$END_DATE"; then
        log_error "Historical mode requires --start-date and --end-date"
        usage
        exit 1
    fi
    
    # Validate date formats
    if ! validate_date "$START_DATE" || ! validate_date "$END_DATE"; then
        usage
        exit 1
    fi
    
    # Check that start date is not after end date
    if [[ "$START_DATE" > "$END_DATE" ]]; then
        log_error "Start date cannot be after end date"
        exit 1
    fi
    
    # Check if date range requires chunking (>90 days)
    start_timestamp=$(date -d "$START_DATE" +%s 2>/dev/null || python3 -c "from datetime import datetime; print(int(datetime.strptime('$START_DATE', '%Y-%m-%d').timestamp()))")
    end_timestamp=$(date -d "$END_DATE" +%s 2>/dev/null || python3 -c "from datetime import datetime; print(int(datetime.strptime('$END_DATE', '%Y-%m-%d').timestamp()))")
    total_days=$(( (end_timestamp - start_timestamp) / 86400 + 1 ))
    
    if [[ $total_days -gt 90 ]]; then
        log_info "Large date range detected ($total_days days). Using 30-day chunked processing..."

        # Calculate buffer start date for rolling calculations
        if command -v python3 &> /dev/null; then
            BUFFER_START_DATE=$(python3 -c "from datetime import datetime, timedelta; print((datetime.strptime('$START_DATE', '%Y-%m-%d') - timedelta(days=7)).strftime('%Y-%m-%d'))")
        else
            # Fallback if python3 not available
            BUFFER_START_DATE="$START_DATE"
            log_warn "Could not calculate buffer start date, using original start date"
        fi
        
        # Let Python handle all the chunking logic
        CHUNKED_CMD=(
            python -m src.make_silver
            --mode "$MODE"
            --countries "${COUNTRY_ARR[@]}"
            --resolution "$RESOLUTION"
            --cache-dir "$CACHE_DIR"
            --log-level "$LOG_LEVEL"
            --start-date "$BUFFER_START_DATE"
            --end-date "$END_DATE"
            --original-start-date "$START_DATE"
            --original-end-date "$END_DATE"
            --chunked
            --chunk-days 30
        )

        if [[ -n "$CONFIG_FILE" ]]; then
            CHUNKED_CMD+=(--config "$CONFIG_FILE")
        fi
        
        if execute_with_timeout "7200" "Chunked processing" "${CHUNKED_CMD[@]}"; then
            end_time=$(date +%s)
            duration=$((end_time - start_time))
            
            log_success "Chunked processing completed successfully!"
            log_info "Total execution time: ${duration} seconds"
            
            # Show final output
            if command -v get_config_path &> /dev/null; then
                OUTPUT_DIR=$(get_config_path "silver.historical")
            else
                OUTPUT_DIR="./data/silver/historical"
            fi
            
            if [[ -d "$OUTPUT_DIR" ]]; then
                log_info ""
                log_info "📁 Final output in $OUTPUT_DIR:"
                file_count=$(count_files "$OUTPUT_DIR" "*.parquet" 2>/dev/null || echo "0")
                show_file_size "$OUTPUT_DIR" 2>/dev/null || true
                
                if [[ -n "$(ls -A "$OUTPUT_DIR" 2>/dev/null)" ]]; then
                    ls -la "$OUTPUT_DIR" | tail -3 | while read line; do
                        log_info "  $line"
                    done
                fi
            fi
            
            exit 0
        else
            log_error "Chunked processing failed!"
            exit 1
        fi
    fi
fi

# Validate hours for realtime mode
if [[ "$MODE" == "realtime" ]]; then
    if ! validate_numeric "$HOURS" "1" "168" "hours"; then
        usage
        exit 1
    fi
fi

# Check if Python script exists
if [[ ! -f "$PYTHON_SCRIPT" ]]; then
    log_error "Python script not found: $PYTHON_SCRIPT"
    exit 1
fi

# Validate and create cache directory
if ! validate_directory "$CACHE_DIR" "true"; then
    log_error "Failed to create cache directory: $CACHE_DIR"
    exit 1
fi

# Create necessary directories using config-aware paths
if command -v get_config_path &> /dev/null; then
    SILVER_REALTIME_DIR=$(get_config_path "silver.realtime")
    SILVER_HISTORICAL_DIR=$(get_config_path "silver.historical")
    validate_directory "$SILVER_REALTIME_DIR" "true"
    validate_directory "$SILVER_HISTORICAL_DIR" "true"
else
    # Fallback directory creation
    validate_directory "./data/silver/realtime" "true"
    validate_directory "./data/silver/historical" "true"
fi

# Change to base directory
cd "$BASE_DIR"

# Set up environment
export PYTHONPATH="$BASE_DIR/src:$PYTHONPATH"

# Build Python command with configuration integration
PYTHON_CMD=(
    python -m src.make_silver
    --mode "$MODE"
    --countries "${COUNTRY_ARR[@]}"
    --resolution "$RESOLUTION"
    --cache-dir "$CACHE_DIR"
    --log-level "$LOG_LEVEL"
)

# Add custom config file if specified
if [[ -n "$CONFIG_FILE" ]]; then
    PYTHON_CMD+=(--config "$CONFIG_FILE")
fi

if [[ "$MODE" == "realtime" ]]; then
     # For realtime mode, we need to process 7 days for rolling calculations but only output today
    TODAY=$(date +"%Y-%m-%d")
    
    if command -v python3 &> /dev/null; then
        SEVEN_DAYS_AGO=$(python3 -c "from datetime import datetime, timedelta; print((datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'))")
        
        log_info "Real-time mode with rolling calculations buffer:"
        log_info "  Data processing range: $SEVEN_DAYS_AGO to $TODAY (7 days)"
        log_info "  Final output will contain only: $TODAY (today)"
        
        # Pass calculated date range and specify today as the final output date
        PYTHON_CMD+=(--mode realtime --start-date "$SEVEN_DAYS_AGO" --end-date "$TODAY" --original-start-date "$TODAY" --original-end-date "$TODAY")
    else
        # Fallback - use traditional realtime mode with hours
        log_warn "Python3 not available for date calculation, using traditional realtime mode"
        log_warn "Rolling calculations may be incomplete"
        PYTHON_CMD+=(--hours "$HOURS")
    fi
else

    BUFFER_START_DATE=$(python3 -c "from datetime import datetime, timedelta; print((datetime.strptime('$START_DATE', '%Y-%m-%d') - timedelta(days=7)).strftime('%Y-%m-%d'))")
    log_info "Calculated buffer start date: $BUFFER_START_DATE (7 days before $START_DATE)"    
    # Pass buffer start date as --start-date and original dates for final clipping
    PYTHON_CMD+=(--start-date "$BUFFER_START_DATE" --end-date "$END_DATE" --original-start-date "$START_DATE" --original-end-date "$END_DATE")

    
fi

# Log execution details with enhanced information
log_info "🌍 Starting Silver Dataset Generation"
log_info "Mode: $MODE"
log_info "Countries: $COUNTRIES"
if [[ "$MODE" == "realtime" ]]; then
    log_info "Hours lookback: $HOURS"
else
    log_info "Date range: $START_DATE to $END_DATE"
fi
log_info "H3 resolution: $RESOLUTION"
log_info "Cache directory: $CACHE_DIR"
log_info "Log level: $LOG_LEVEL"
log_info "Configuration system: $(command -v get_config_countries &> /dev/null && echo "Available" || echo "Fallback mode")"

# Show configuration status
if command -v print_config_summary &> /dev/null; then
    log_info "Configuration summary:"
    print_config_summary | sed 's/^/  /'
fi

log_debug "Python command: ${PYTHON_CMD[*]}"
log_info ""

# Execute Python script with timeout and enhanced error handling
log_info "📊 Executing silver dataset processing..."
start_time=$(date +%s)

if execute_with_timeout "7200" "Silver dataset processing" "${PYTHON_CMD[@]}"; then
    end_time=$(date +%s)
    duration=$((end_time - start_time))
    
    log_success "Silver dataset processing completed successfully!"
    log_info "Total execution time: ${duration} seconds"
    
    # Show output directory contents using config-aware paths
    if [[ "$MODE" == "realtime" ]]; then
        if command -v get_config_path &> /dev/null; then
            OUTPUT_DIR=$(get_config_path "silver.realtime")
        else
            OUTPUT_DIR="./data/silver/realtime"
        fi
    else
        if command -v get_config_path &> /dev/null; then
            OUTPUT_DIR=$(get_config_path "silver.historical")
        else
            OUTPUT_DIR="./data/silver/historical"
        fi
    fi
    
    if [[ -d "$OUTPUT_DIR" ]]; then
        log_info ""
        log_info "📁 Output files in $OUTPUT_DIR:"
        # Count files and show size
        file_count=$(count_files "$OUTPUT_DIR" "*.parquet" 2>/dev/null || echo "0")
        show_file_size "$OUTPUT_DIR" 2>/dev/null || true
        
        # Show latest files
        if [[ -n "$(ls -A "$OUTPUT_DIR" 2>/dev/null)" ]]; then
            ls -la "$OUTPUT_DIR" | tail -5 | while read line; do
                log_info "  $line"
            done
        else
            log_warn "No files found in output directory"
        fi
    fi
    
    # Show cache directory information
    if [[ -d "$CACHE_DIR" ]]; then
        log_info ""
        show_file_size "$CACHE_DIR"
        cache_files=$(count_files "$CACHE_DIR" "*" 2>/dev/null || echo "0")
        log_info "Cache contains $cache_files files"
    fi
    
    exit 0
else
    end_time=$(date +%s)
    duration=$((end_time - start_time))
    
    log_error "Silver dataset processing failed!"
    log_error "Execution time: ${duration} seconds"
    log_error "Check log file for details: $LOG_FILE"
    exit 1
fi 
