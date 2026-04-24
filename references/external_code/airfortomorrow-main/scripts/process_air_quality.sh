#!/bin/bash

# Air Quality Data Processing Script with Centralized Configuration
#
# This script processes air quality data from OpenAQ and AirGradient sensors
# Supports both real-time and historical processing modes.
#
# Usage:
#   ./scripts/process_air_quality.sh --mode realtime --hours 24 --countries THA LAO
#   ./scripts/process_air_quality.sh --mode historical --start-date 2024-06-01 --end-date 2024-06-03

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
fi

# Setup environment and error handling (only if functions are available)
if command -v setup_environment &> /dev/null; then
    setup_environment "process_air_quality"
fi

if command -v setup_error_handling &> /dev/null; then
    setup_error_handling
fi

# Configuration-driven defaults with fallbacks
if command -v get_config_countries &> /dev/null; then
    DEFAULT_COUNTRIES=$(get_config_countries)
    DEFAULT_MIN_THRESHOLD=$(get_config_or_default "data_processing.air_quality.min_threshold" "0.0")
    DEFAULT_MAX_THRESHOLD=$(get_config_or_default "data_processing.air_quality.max_threshold" "500.0")
    DEFAULT_LOG_LEVEL=$(get_config_log_level)
    log_success "Using configuration system for defaults"
else
    # Fallback defaults if config system unavailable
    DEFAULT_COUNTRIES="THA LAO"
    DEFAULT_MIN_THRESHOLD=0.0
    DEFAULT_MAX_THRESHOLD=500.0
    DEFAULT_LOG_LEVEL="INFO"
    log_warn "Configuration system unavailable, using hardcoded defaults"
fi

# Default values (config-driven with fallbacks)
MODE="realtime"
HOURS=24
COUNTRIES="$DEFAULT_COUNTRIES"
MIN_THRESHOLD="$DEFAULT_MIN_THRESHOLD"
MAX_THRESHOLD="$DEFAULT_MAX_THRESHOLD"
START_DATE=""
END_DATE=""

# Function to display usage with configuration-aware defaults
usage() {
    cat << EOF
🌡️ Air Quality Data Processing Script (Configuration-Aware)

Process air quality data from OpenAQ and AirGradient sensors.
Uses centralized configuration system with backwards-compatible fallbacks.

Usage:
    $0 [OPTIONS]

Mode Arguments:
    --mode MODE              Processing mode: realtime or historical (default: realtime)
    --hours HOURS            Hours to look back in realtime mode (default: 24)
    --start-date DATE        Start date for historical mode (YYYY-MM-DD)
    --end-date DATE          End date for historical mode (YYYY-MM-DD)

Processing Arguments:
    --countries CODES        Space-separated country codes (default: $DEFAULT_COUNTRIES)
    --min-threshold VALUE    Minimum PM2.5 value threshold (default: $DEFAULT_MIN_THRESHOLD μg/m³)
    --max-threshold VALUE    Maximum PM2.5 value threshold (default: $DEFAULT_MAX_THRESHOLD μg/m³)

General Options:
    --config FILE            Custom configuration file path
    --help                   Display this help message

Configuration Integration:
    This script uses the centralized configuration system when available.
    Configuration defaults are loaded from config/config.yaml.
    Command-line arguments override configuration defaults.

Examples:
    # Real-time processing with config defaults
    $0

    # Real-time processing for past 48 hours
    $0 --mode realtime --hours 48

    # Historical processing for specific date range
    $0 --mode historical --start-date 2024-06-01 --end-date 2024-06-03

    # Custom thresholds for data filtering
    $0 --mode realtime --min-threshold 5.0 --max-threshold 300.0

    # Multi-country processing
    $0 --mode realtime --countries THA LAO VNM KHM

Configuration Status:
    Config system: $(command -v get_config_countries &> /dev/null && echo "✅ Available" || echo "❌ Using fallbacks")
    Default countries: $DEFAULT_COUNTRIES
    Default thresholds: $DEFAULT_MIN_THRESHOLD - $DEFAULT_MAX_THRESHOLD μg/m³

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --mode)
            MODE="$2"
            shift 2
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
        --countries)
            COUNTRIES=""
            shift
            while [[ $# -gt 0 && ! "$1" =~ ^-- ]]; do
                COUNTRIES="$COUNTRIES $1"
                shift
            done
            COUNTRIES=$(echo "$COUNTRIES" | sed 's/^ *//')
            ;;
        --min-threshold)
            MIN_THRESHOLD="$2"
            shift 2
            ;;
        --max-threshold)
            MAX_THRESHOLD="$2"
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
            log_error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Validate mode-specific arguments
if [[ "$MODE" == "historical" ]]; then
    if command -v validate_required_args &> /dev/null; then
        if ! validate_required_args "start-date:$START_DATE" "end-date:$END_DATE"; then
            log_error "Historical mode requires --start-date and --end-date"
            usage
            exit 1
        fi
    else
        # Fallback validation
        if [[ -z "$START_DATE" ]] || [[ -z "$END_DATE" ]]; then
            log_error "Historical mode requires --start-date and --end-date"
            usage
            exit 1
        fi
    fi
fi

# Build the Python command with configuration integration
read -r -a COUNTRY_ARR <<< "$COUNTRIES"
CMD=(python -m src.data_processors.process_air_quality --mode "$MODE" --hours "$HOURS")

if [[ "$MODE" == "historical" ]]; then
    CMD+=(--start-date "$START_DATE" --end-date "$END_DATE")
fi

CMD+=(--countries "${COUNTRY_ARR[@]}" --min-threshold "$MIN_THRESHOLD" --max-threshold "$MAX_THRESHOLD")

# Add custom config file if specified
if [[ -n "$CONFIG_FILE" ]]; then
    CMD+=(--config "$CONFIG_FILE")
fi

# Change to the base directory and run the command
cd "$BASE_DIR"

# Log execution details
log_info "🌡️ Starting Air Quality Data Processing"
log_info "Mode: $MODE"
log_info "Countries: $COUNTRIES"
if [[ "$MODE" == "realtime" ]]; then
    log_info "Hours lookback: $HOURS"
else
    log_info "Date range: $START_DATE to $END_DATE"
fi
log_info "Thresholds: $MIN_THRESHOLD - $MAX_THRESHOLD μg/m³"
log_info "Configuration system: $(command -v get_config_countries &> /dev/null && echo "Available" || echo "Fallback mode")"

# Show configuration status
if command -v print_config_summary &> /dev/null; then
    log_info "Configuration summary:"
    print_config_summary | sed 's/^/  /'
fi

log_info "Command: ${CMD[*]}"
log_info ""

run_command_with_timeout() {
    local timeout_seconds="$1"
    shift
    if command -v timeout >/dev/null 2>&1; then
        timeout "$timeout_seconds" "$@"
    else
        "$@"
    fi
}

# Execute the command with enhanced error handling
if run_command_with_timeout 3600 "${CMD[@]}"; then
    log_success "Air quality data processing completed successfully"
    exit 0
else
    log_error "Air quality data processing failed"
    exit 1
fi
