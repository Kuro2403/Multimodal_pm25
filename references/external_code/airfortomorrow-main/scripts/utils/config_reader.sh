#!/bin/bash

# Configuration Reader Utility for Air Quality Prediction System
# This script provides functions to read configuration values from config.yaml

# Set script directory and config file path
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
CONFIG_FILE="$BASE_DIR/config/config.yaml"

# Check if yq is available
if ! command -v yq &> /dev/null; then
    echo "Error: yq is required for configuration parsing. Please install it with: pip install yq"
    exit 1
fi

# Check if config file exists
if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "Error: Configuration file not found: $CONFIG_FILE"
    exit 1
fi

# ========================================
# Core Configuration Functions
# ========================================

# Get a configuration value using dot notation
# Usage: get_config_value "system.geographic.h3_resolution"
get_config_value() {
    local key="$1"
    local default_value="$2"
    
    if [[ -z "$key" ]]; then
        echo "Error: Configuration key is required" >&2
        return 1
    fi
    
    local value
    value=$(yq ".$key" "$CONFIG_FILE" 2>/dev/null | sed 's/^"\(.*\)"$/\1/')
    
    if [[ "$value" == "null" || -z "$value" ]]; then
        if [[ -n "$default_value" ]]; then
            echo "$default_value"
        else
            echo "Error: Configuration key not found: $key" >&2
            return 1
        fi
    else
        echo "$value"
    fi
}

# Get configuration path
# Usage: get_config_path "cache.silver"
get_config_path() {
    local path_key="$1"
    local default_path="$2"
    
    if [[ -z "$path_key" ]]; then
        echo "Error: Path key is required" >&2
        return 1
    fi
    
    local path_value
    path_value=$(get_config_value "paths.$path_key" "$default_path")
    
    # Convert relative paths to absolute paths
    if [[ "$path_value" == ./* ]] || [[ "$path_value" != /* ]]; then
        echo "$BASE_DIR/$path_value"
    else
        echo "$path_value"
    fi
}

# ========================================
# Specialized Configuration Functions
# ========================================

# Get default countries list
# Usage: get_config_countries
get_config_countries() {
    local data_source="$1"
    
    if [[ -n "$data_source" ]]; then
        # Try to get source-specific countries
        local countries
        countries=$(yq ".data_collection.$data_source.countries[]" "$CONFIG_FILE" 2>/dev/null | sed 's/^"\(.*\)"$/\1/' | tr '\n' ' ')
        if [[ -n "$countries" && "$countries" != "null" ]]; then
            echo "$countries" | sed 's/[[:space:]]*$//'
            return 0
        fi
    fi
    
    # Fall back to default countries
    local countries
    countries=$(yq ".system.countries.default[]" "$CONFIG_FILE" 2>/dev/null | sed 's/^"\(.*\)"$/\1/' | tr '\n' ' ')
    if [[ -n "$countries" && "$countries" != "null" ]]; then
        echo "$countries" | sed 's/[[:space:]]*$//'
    else
        echo "LAO THA"  # Fallback default
    fi
}

# Get countries as comma-separated string
# Usage: get_config_countries_csv
get_config_countries_csv() {
    local countries
    countries=$(get_config_countries "$1")
    echo "$countries" | tr ' ' ','
}

# Get H3 resolution
# Usage: get_config_h3_resolution [context]
get_config_h3_resolution() {
    local context="${1:-default}"
    
    case "$context" in
        kriging)
            get_config_value "data_processing.h3.kriging_resolution" "8"
            ;;
        coarse)
            get_config_value "data_collection.era5.h3_resolution_coarse" "4"
            ;;
        fine)
            get_config_value "data_collection.era5.h3_resolution_fine" "8"
            ;;
        *)
            get_config_value "system.geographic.h3_resolution" "8"
            ;;
    esac
}

# Get buffer degrees for geographic processing
# Usage: get_config_buffer_degrees [data_source]
get_config_buffer_degrees() {
    local data_source="${1:-default}"
    
    # Get data source-specific buffer from centralized location
    local source_buffer
    source_buffer=$(get_config_value "system.geographic.buffers.$data_source" 2>/dev/null)
    
    if [[ -n "$source_buffer" && "$source_buffer" != "null" ]]; then
        echo "$source_buffer"
    else
        # Fall back to default buffer
        get_config_value "system.geographic.buffers.default" "0.4"
    fi
}

# Get time window settings
# for hours
get_config_time_window() {
    local mode="$1"
    
    case "$mode" in
        realtime)
            get_config_value "system.time_windows.realtime_hours" "24"
            ;;
        historical)
            get_config_value "system.time_windows.historical_buffer_days" "1"
            ;;
        *)
            echo "Error: Invalid mode. Use 'realtime' or 'historical'" >&2
            return 1
            ;;
    esac
}

# Get time window in days
get_config_time_window_days() {
    local mode="$1"
    
    case "$mode" in
        realtime)
            get_config_value "system.time_windows.realtime_days" "2"
            ;;
        historical)
            get_config_value "system.time_windows.historical_buffer_days" "1"
            ;;
        *)
            echo "Error: Invalid mode. Use 'realtime' or 'historical'" >&2
            return 1
            ;;
    esac
}

# Get processing configuration
# Usage: get_config_processing "kriging.grid_size"
get_config_processing() {
    local setting="$1"
    get_config_value "data_processing.$setting"
}

# Get API key from environment variables
# Usage: get_config_api_key "openaq"
get_config_api_key() {
    local service="$1"
    local env_var_name="${service^^}_API_KEY"  # Convert to uppercase
    echo "${!env_var_name}"
}

# Get logging configuration
# Usage: get_config_log_level
get_config_log_level() {
    get_config_or_default "logging.level" "INFO"
}

# Get model configuration
# Usage: get_config_model_path
get_config_model_path() {
    get_config_path "models.xgboost" "src/models/xgboost_model.json"
}

# ========================================
# Validation and Utility Functions
# ========================================

# Validate that required config sections exist
# Usage: validate_config
validate_config() {
    local required_sections=(
        "system.countries.default"
        "paths"
        "data_processing"
        "logging"
    )
    
    for section in "${required_sections[@]}"; do
        if ! yq ".$section" "$CONFIG_FILE" >/dev/null 2>&1; then
            echo "Error: Required configuration section missing: $section" >&2
            return 1
        fi
    done
    
    echo "Configuration validation passed"
    return 0
}

# Get all countries for a data source with fallbacks
# Usage: get_countries_with_fallback "openaq"
get_countries_with_fallback() {
    local data_source="$1"
    local countries
    
    # Try data source specific countries first
    countries=$(get_config_countries "$data_source")
    
    # If empty or null, use default
    if [[ -z "$countries" || "$countries" == "null" ]]; then
        countries=$(get_config_countries)
    fi
    
    echo "$countries"
}

# Create directory if it doesn't exist (config-aware)
# Usage: ensure_config_path "cache.silver"
ensure_config_path() {
    local path_key="$1"
    local path
    path=$(get_config_path "$path_key")
    
    if [[ -n "$path" ]]; then
        mkdir -p "$path"
        echo "$path"
    else
        echo "Error: Could not resolve path for key: $path_key" >&2
        return 1
    fi
}

# Get execution timeout
# Usage: get_config_timeout "download_timeout"
get_config_timeout() {
    local operation="${1:-pipeline_default}"
    get_config_value "execution.timeouts.$operation" "3600"
}

# Get parallel execution settings
# Usage: get_config_max_workers
get_config_max_workers() {
    get_config_value "execution.parallel.max_workers" "4"
}

# Get kriging settings
# Usage: get_config_kriging_setting "variogram_model"
get_config_kriging_setting() {
    local setting="$1"
    
    if [[ -z "$setting" ]]; then
        echo "Error: Kriging setting name is required" >&2
        return 1
    fi
    
    get_config_value "data_processing.kriging.$setting"
}

# Get variogram model for kriging
# Usage: get_config_variogram_model
get_config_variogram_model() {
    get_config_kriging_setting "variogram_model" "spherical"
}

# Get buffer (alias for get_config_buffer_degrees)
# Usage: get_config_buffer [data_source]
get_config_buffer() {
    get_config_buffer_degrees "$1"
}

# Get FIRMS KDE settings
# Usage: get_config_firms_kde_setting "grid_size"
get_config_firms_kde_setting() {
    local setting="$1"
    local default_value="$2"
    
    if [[ -z "$setting" ]]; then
        echo "Error: FIRMS KDE setting name is required" >&2
        return 1
    fi
    
    get_config_value "data_collection.firms.kde_settings.$setting" "$default_value"
}

# Get ERA5 parameters as array
# Usage: get_config_era5_parameters
get_config_era5_parameters() {
    local params
    params=$(yq ".data_collection.era5.parameters[]" "$CONFIG_FILE" 2>/dev/null | sed 's/^"\(.*\)"$/\1/' | tr '\n' ' ')
    if [[ -n "$params" && "$params" != "null" ]]; then
        echo "$params" | sed 's/[[:space:]]*$//'
    else
        echo "2d 2t 10u 10v"  # Fallback default
    fi
}

# Get ERA5 rate limiting settings
# Usage: get_config_era5_rate_limit_setting "requests_per_minute"
get_config_era5_rate_limit_setting() {
    local setting="$1"
    local default_value="$2"
    
    if [[ -z "$setting" ]]; then
        echo "Error: ERA5 rate limit setting name is required" >&2
        return 1
    fi
    
    get_config_value "data_collection.era5.rate_limiting.$setting" "$default_value"
}

# Get ERA5 rate limiting configuration
# Usage: get_config_era5_rate_limits
get_config_era5_rate_limits() {
    echo "requests_per_minute: $(get_config_era5_rate_limit_setting 'requests_per_minute' '10')"
    echo "delay_between_requests: $(get_config_era5_rate_limit_setting 'delay_between_requests' '6.0')"
    echo "max_retries: $(get_config_era5_rate_limit_setting 'max_retries' '5')"
    echo "backoff_factor: $(get_config_era5_rate_limit_setting 'backoff_factor' '2.0')"
    echo "retry_delay_base: $(get_config_era5_rate_limit_setting 'retry_delay_base' '60')"
}

# Get ERA5 IDW rings based on mode
# Usage: get_config_era5_idw_rings "realtime" or "historical"
get_config_era5_idw_rings() {
    local mode="$1"
    
    if [[ -z "$mode" ]]; then
        echo "Error: Mode is required (realtime or historical)" >&2
        return 1
    fi
    
    local rings
    rings=$(yq ".data_collection.era5.idw.$mode.rings" "$CONFIG_FILE" 2>/dev/null | sed 's/^"\(.*\)"$/\1/')
    
    if [[ -n "$rings" && "$rings" != "null" ]]; then
        echo "$rings"
    else
        echo "Error: ERA5 IDW rings not found for mode: $mode" >&2
        return 1
    fi
}

# Get ERA5 IDW weight power based on mode
# Usage: get_config_era5_idw_weight_power "realtime" or "historical"
get_config_era5_idw_weight_power() {
    local mode="$1"
    
    if [[ -z "$mode" ]]; then
        echo "Error: Mode is required (realtime or historical)" >&2
        return 1
    fi
    
    local weight_power
    weight_power=$(yq ".data_collection.era5.idw.$mode.weight_power" "$CONFIG_FILE" 2>/dev/null | sed 's/^"\(.*\)"$/\1/')
    
    if [[ -n "$weight_power" && "$weight_power" != "null" ]]; then
        echo "$weight_power"
    else
        echo "Error: ERA5 IDW weight power not found for mode: $mode" >&2
        return 1
    fi
}

# ========================================
# Debug and Information Functions
# ========================================

# Print configuration summary
# Usage: print_config_summary
print_config_summary() {
    echo "System Configuration Summary:"
    echo "  Countries: $(get_config_countries 2>/dev/null || echo "THA LAO")"
    echo "  H3 Resolution: $(get_config_h3_resolution 2>/dev/null || echo "8")"
    echo "  Log Level: $(get_config_log_level)"
    echo "  Config File: $CONFIG_FILE"
    echo "  Config Status: $(validate_config >/dev/null 2>&1 && echo "Valid" || echo "Invalid/Missing")"
}

# Test configuration access
# Usage: test_config_access
test_config_access() {
    echo "Testing configuration access..."
    
    # Test basic access
    if countries=$(get_config_countries); then
        echo "✅ Countries: $countries"
    else
        echo "❌ Failed to get countries"
        return 1
    fi
    
    # Test path access
    if cache_path=$(get_config_path "cache.silver"); then
        echo "✅ Cache path: $cache_path"
    else
        echo "❌ Failed to get cache path"
        return 1
    fi
    
    # Test processing config
    if h3_res=$(get_config_h3_resolution); then
        echo "✅ H3 Resolution: $h3_res"
    else
        echo "❌ Failed to get H3 resolution"
        return 1
    fi
    
    echo "Configuration access test completed successfully"
    return 0
}

# ========================================
# Export Functions for Sourcing
# ========================================

# When sourced, make these functions available
if [[ "${BASH_SOURCE[0]}" != "${0}" ]]; then
    # Script is being sourced
    export -f get_config_value
    export -f get_config_path
    export -f get_config_countries
    export -f get_config_countries_csv
    export -f get_config_h3_resolution
    export -f get_config_buffer_degrees
    export -f get_config_buffer
    export -f get_config_firms_kde_setting
    export -f get_config_era5_parameters
    export -f get_config_time_window
    export -f get_config_time_window_days
    export -f get_config_processing
    export -f get_config_kriging_setting
    export -f get_config_variogram_model
    export -f get_config_api_key
    export -f get_config_log_level
    export -f get_config_model_path
    export -f get_config_era5_rate_limit_setting
    export -f get_config_era5_rate_limits
    export -f get_config_era5_idw_rings
    export -f get_config_era5_idw_weight_power
    export -f validate_config
    export -f get_countries_with_fallback
    export -f ensure_config_path
    export -f get_config_timeout
    export -f get_config_max_workers
    export -f print_config_summary
    export -f test_config_access
fi

# ========================================
# Main Function for Direct Execution
# ========================================

# Main function when script is executed directly
main() {
    case "${1:-}" in
        countries)
            get_config_countries
            ;;
        h3_resolution)
            get_config_h3_resolution
            ;;
        buffer)
            if [[ -n "$2" ]]; then
                get_config_buffer "$2"
            else
                get_config_buffer
            fi
            ;;
        path)
            if [[ -n "$2" ]]; then
                get_config_path "$2"
            else
                echo "Error: Path key required" >&2
                return 1
            fi
            ;;
        value)
            if [[ -n "$2" ]]; then
                get_config_value "$2" "$3"
            else
                echo "Error: Configuration key required" >&2
                return 1
            fi
            ;;
        summary)
            print_config_summary
            ;;
        test)
            test_config_access
            ;;
        help|--help)
            show_help
            ;;
        *)
            echo "Unknown command: $1" >&2
            echo "Use '$0 help' for usage information." >&2
            return 1
            ;;
    esac
}

# Execute main function if script is run directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi

# Get configuration value with default fallback
# Usage: get_config_or_default "key.path" "default_value"
get_config_or_default() {
    local key="$1"
    local default_value="$2"
    
    local value
    value=$(get_config_value "$key" 2>/dev/null)
    
    if [[ $? -eq 0 && -n "$value" && "$value" != "null" ]]; then
        echo "$value"
    else
        echo "$default_value"
    fi
}

# Show help message
show_help() {
    cat << EOF
Configuration Reader Utility

Usage: $0 [command] [arguments]

Commands:
  test                        Test configuration access
  summary                     Print configuration summary
  validate                    Validate configuration file
  countries [source]          Get countries list (optionally for specific source)
  buffer [data_source]        Get buffer degrees (optionally for specific data source)
  path <path_key>             Get configured path
  value <key> [default]       Get configuration value
  h3_resolution [context]     Get H3 resolution (optionally for specific context)
  help                        Show this help message

Buffer Data Sources:
  default, firms, openaq, airgradient, himawari, era5
  Note: FIRMS uses 4.0 degrees, others use 0.4 degrees

Examples:
  $0 test
  $0 countries
  $0 buffer firms          # Returns 4.0 for FIRMS-specific buffer
  $0 buffer               # Returns default buffer (0.4)
  $0 path cache.silver
  $0 value system.geographic.h3_resolution

When sourced, provides functions:
  get_config_value, get_config_path, get_config_countries,
  get_config_h3_resolution, get_config_buffer_degrees, 
  get_config_time_window, etc.
EOF
} 