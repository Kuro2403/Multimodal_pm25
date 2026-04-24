#!/bin/bash

# Common Shell Utilities for Air Quality Prediction System
# This script provides shared functions used across all shell scripts

# Set script directory and base directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Source configuration reader if available
if [[ -f "$SCRIPT_DIR/config_reader.sh" ]]; then
    source "$SCRIPT_DIR/config_reader.sh"
fi

# ========================================
# Logging and Output Functions
# ========================================

# Enhanced logging function with timestamps and levels
# Usage: log_message "INFO|WARN|ERROR" "message"
log_message() {
    local level="${1:-INFO}"
    local message="${2:-}"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    # Color codes for different log levels
    local color_reset='\033[0m'
    local color_info='\033[0;36m'    # Cyan
    local color_warn='\033[0;33m'    # Yellow
    local color_error='\033[0;31m'   # Red
    local color_success='\033[0;32m' # Green
    local color_debug='\033[0;90m'   # Gray
    
    local color_code
    case "$level" in
        INFO)     color_code="$color_info" ;;
        WARN)     color_code="$color_warn" ;;
        ERROR)    color_code="$color_error" ;;
        SUCCESS)  color_code="$color_success" ;;
        DEBUG)    color_code="$color_debug" ;;
        *)        color_code="$color_info" ;;
    esac
    
    # Format: [TIMESTAMP] [LEVEL] MESSAGE
    echo -e "${color_code}[$timestamp] [$level]${color_reset} $message"
    
    # Also log to file if LOG_FILE is set
    if [[ -n "$LOG_FILE" ]]; then
        echo "[$timestamp] [$level] $message" >> "$LOG_FILE"
    fi
}

# Convenience logging functions
log_info() {
    log_message "INFO" "$1"
}

log_warn() {
    log_message "WARN" "$1"
}

log_error() {
    log_message "ERROR" "$1"
}

log_success() {
    log_message "SUCCESS" "$1"
}

log_debug() {
    log_message "DEBUG" "$1"
}

# Progress indicator for long-running operations
# Usage: show_progress "Processing..." &; PROGRESS_PID=$!; ... ; kill $PROGRESS_PID
show_progress() {
    local message="${1:-Processing...}"
    local chars="⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
    local delay=0.1
    
    while true; do
        for (( i=0; i<${#chars}; i++ )); do
            echo -ne "\r${chars:$i:1} $message"
            sleep $delay
        done
    done
}

# ========================================
# Environment Setup Functions
# ========================================

# Setup common environment variables and paths
# Usage: setup_environment [script_name]
setup_environment() {
    local script_name="${1:-$(basename "$0")}"
    
    # Set up logging
    local log_dir
    if command -v get_config_path &> /dev/null; then
        log_dir=$(get_config_path "logs")
    else
        log_dir="$BASE_DIR/logs"
    fi
    
    mkdir -p "$log_dir"
    export LOG_FILE="$log_dir/${script_name}_$(date +%Y%m%d_%H%M%S).log"
    
    # Set up Python path
    export PYTHONPATH="$BASE_DIR/src:$PYTHONPATH"
    
    # Change to base directory
    cd "$BASE_DIR"
    
    log_info "Environment setup completed"
    log_info "Base directory: $BASE_DIR"
    log_info "Log file: $LOG_FILE"
    log_info "Python path: $PYTHONPATH"
}

# Activate virtual environment
# Usage: activate_venv
activate_venv() {
    local base_dir
    base_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
    
    if [[ -f "$base_dir/scripts/activate_venv.sh" ]]; then
        if source "$base_dir/scripts/activate_venv.sh"; then
            return 0
        else
            log_error "Failed to activate virtual environment"
            return 1
        fi
    else
        log_error "Virtual environment activation script not found"
        return 1
    fi
}

# Check and install dependencies
# Usage: check_dependencies [dependency1] [dependency2] ...
check_dependencies() {
    local missing_deps=()
    
    for dep in "$@"; do
        if ! command -v "$dep" &> /dev/null; then
            missing_deps+=("$dep")
        fi
    done
    
    if [[ ${#missing_deps[@]} -gt 0 ]]; then
        log_error "Missing dependencies: ${missing_deps[*]}"
        return 1
    fi
    
    log_success "All dependencies available: $*"
    return 0
}

# ========================================
# Argument Validation Functions
# ========================================

# Validate required arguments
# Usage: validate_required_args "arg1_name:$arg1" "arg2_name:$arg2" ...
validate_required_args() {
    local missing_args=()
    
    for arg_pair in "$@"; do
        local arg_name="${arg_pair%%:*}"
        local arg_value="${arg_pair#*:}"
        
        if [[ -z "$arg_value" ]]; then
            missing_args+=("$arg_name")
        fi
    done
    
    if [[ ${#missing_args[@]} -gt 0 ]]; then
        log_error "Missing required arguments: ${missing_args[*]}"
        return 1
    fi
    
    return 0
}

# Validate date format (YYYY-MM-DD)
# Usage: validate_date "2024-01-01"
validate_date() {
    local date_str="$1"
    
    if [[ ! "$date_str" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
        log_error "Invalid date format: $date_str (expected: YYYY-MM-DD)"
        return 1
    fi
    
    # Try to parse the date (handle both GNU date and BSD date)
    if date -d "$date_str" &>/dev/null 2>&1; then
        # GNU date (Linux)
        return 0
    elif date -j -f "%Y-%m-%d" "$date_str" &>/dev/null 2>&1; then
        # BSD date (macOS)
        return 0
    else
        log_error "Invalid date: $date_str"
        return 1
    fi
}

# Validate numeric value within range
# Usage: validate_numeric "value" "min" "max" "name"
validate_numeric() {
    local value="$1"
    local min="$2"
    local max="$3"
    local name="${4:-value}"
    
    if ! [[ "$value" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
        log_error "$name must be a number: $value"
        return 1
    fi
    
    if (( $(echo "$value < $min" | bc -l) )); then
        log_error "$name must be >= $min: $value"
        return 1
    fi
    
    if (( $(echo "$value > $max" | bc -l) )); then
        log_error "$name must be <= $max: $value"
        return 1
    fi
    
    return 0
}

# Validate that directory exists or can be created
# Usage: validate_directory "/path/to/dir" "create_if_missing"
validate_directory() {
    local dir_path="$1"
    local create_if_missing="${2:-false}"
    
    if [[ -z "$dir_path" ]]; then
        log_error "Directory path is required"
        return 1
    fi
    
    if [[ ! -d "$dir_path" ]]; then
        if [[ "$create_if_missing" == "true" ]]; then
            mkdir -p "$dir_path"
            log_info "Created directory: $dir_path"
        else
            log_error "Directory does not exist: $dir_path"
            return 1
        fi
    fi
    
    return 0
}

# ========================================
# Execution and Process Management
# ========================================

# Execute command with timeout and logging
# Preferred usage: execute_with_timeout "timeout_seconds" "description" command arg1 arg2 ...
# Backward-compatible usage: execute_with_timeout "command string" "timeout_seconds" "description"
execute_with_timeout() {
    local timeout_seconds=""
    local description=""
    local -a cmd=()

    if [[ "$1" =~ ^[0-9]+$ ]]; then
        timeout_seconds="$1"
        description="${2:-Command}"
        shift 2
        cmd=("$@")
    else
        local command_string="$1"
        timeout_seconds="$2"
        description="${3:-Command}"
        # Backward-compatible tokenization for legacy callers.
        read -r -a cmd <<< "$command_string"
    fi
    
    log_info "Executing: $description"
    log_info "Timeout: ${timeout_seconds}s"
    
    # Start the command in background and capture its PID
    "${cmd[@]}" &
    local cmd_pid=$!
    
    # Monitor the process
    local elapsed=0
    while kill -0 "$cmd_pid" 2>/dev/null; do
        if [[ $elapsed -ge $timeout_seconds ]]; then
            log_error "$description timed out after ${timeout_seconds}s"
            kill -TERM "$cmd_pid" 2>/dev/null
            sleep 2
            kill -KILL "$cmd_pid" 2>/dev/null
            return 1
        fi
        
        sleep 5
        elapsed=$((elapsed + 5))
        
        # Show progress every 30 seconds
        if [[ $((elapsed % 30)) -eq 0 ]]; then
            log_info "$description running... (${elapsed}s elapsed)"
        fi
    done
    
    # Wait for the process to complete and get exit status
    wait "$cmd_pid"
    return $?
}

# Run command in background with PID tracking
# Usage: run_background "command" "pid_file"
run_background() {
    local command="$1"
    local pid_file="$2"
    
    log_info "Starting background process: $command"
    
    bash -c "$command" &
    local pid=$!
    
    if [[ -n "$pid_file" ]]; then
        echo "$pid" > "$pid_file"
        log_info "Background process PID $pid saved to $pid_file"
    fi
    
    echo "$pid"
}

# Wait for background process to complete
# Usage: wait_for_process "pid_file" "description"
wait_for_process() {
    local pid_file="$1"
    local description="${2:-Process}"
    
    if [[ ! -f "$pid_file" ]]; then
        log_error "PID file not found: $pid_file"
        return 1
    fi
    
    local pid=$(cat "$pid_file")
    log_info "Waiting for $description (PID: $pid)"
    
    if wait "$pid"; then
        log_success "$description completed successfully"
        rm -f "$pid_file"
        return 0
    else
        local exit_code=$?
        log_error "$description failed with exit code $exit_code"
        rm -f "$pid_file"
        return $exit_code
    fi
}

# ========================================
# File and Path Management
# ========================================

# Create timestamped backup of file
# Usage: backup_file "/path/to/file"
backup_file() {
    local file_path="$1"
    
    if [[ ! -f "$file_path" ]]; then
        log_warn "File does not exist for backup: $file_path"
        return 1
    fi
    
    local backup_path="${file_path}.backup.$(date +%Y%m%d_%H%M%S)"
    cp "$file_path" "$backup_path"
    log_info "Created backup: $backup_path"
}

# Calculate and display file size
# Usage: show_file_size "/path/to/file_or_directory"
show_file_size() {
    local path="$1"
    
    if [[ ! -e "$path" ]]; then
        log_error "Path does not exist: $path"
        return 1
    fi
    
    local size
    if command -v du &> /dev/null; then
        size=$(du -sh "$path" 2>/dev/null | cut -f1)
        log_info "Size of $path: $size"
    else
        log_warn "du command not available, cannot show size"
        return 1
    fi
}

# Count files matching pattern
# Usage: count_files "/path/to/directory" "*.parquet"
count_files() {
    local directory="$1"
    local pattern="${2:-*}"
    
    if [[ ! -d "$directory" ]]; then
        log_error "Directory does not exist: $directory"
        return 1
    fi
    
    local count=$(find "$directory" -name "$pattern" -type f | wc -l)
    log_info "Found $count files matching '$pattern' in $directory"
    echo "$count"
}

# ========================================
# Configuration Integration Helpers
# ========================================

# Load configuration with fallbacks
# Usage: load_config_with_fallbacks
load_config_with_fallbacks() {
    if command -v get_config_countries &> /dev/null; then
        # Configuration system available
        log_success "Configuration system available"
        
        # Test basic config access
        if ! get_config_countries >/dev/null 2>&1; then
            log_warn "Configuration validation failed, using fallbacks"
            return 1
        fi
        
        return 0
    else
        log_warn "Configuration system not available, using hardcoded defaults"
        return 1
    fi
}

# Get configuration value with fallback
# Usage: get_config_or_default "config.key" "fallback_value"
get_config_or_default() {
    local config_key="$1"
    local fallback="$2"
    
    if command -v get_config_value &> /dev/null; then
        local value
        if value=$(get_config_value "$config_key" 2>/dev/null); then
            echo "$value"
            return 0
        fi
    fi
    
    echo "$fallback"
}

# ========================================
# Error Handling and Cleanup
# ========================================

# Set up error handling and cleanup
# Usage: setup_error_handling
setup_error_handling() {
    set -e  # Exit on error
    set -u  # Exit on undefined variable
    set -o pipefail  # Exit on pipe failure
    
    # Set up trap for cleanup
    trap cleanup_on_exit EXIT
    trap cleanup_on_error ERR
    trap cleanup_on_interrupt INT TERM
}

# Cleanup function called on exit
cleanup_on_exit() {
    local exit_code=$?
    
    if [[ $exit_code -eq 0 ]]; then
        log_success "Script completed successfully"
    else
        log_error "Script exited with code $exit_code"
    fi
    
    # Clean up any temporary files
    if [[ -n "${TEMP_FILES:-}" ]]; then
        for temp_file in "${TEMP_FILES[@]}"; do
            if [[ -f "$temp_file" ]]; then
                rm -f "$temp_file"
                log_debug "Cleaned up temporary file: $temp_file"
            fi
        done
    fi
}

# Cleanup function called on error
cleanup_on_error() {
    local exit_code=$?
    local line_number=$1
    
    log_error "Error occurred at line $line_number with exit code $exit_code"
    
    # Additional error-specific cleanup can be added here
}

# Cleanup function called on interrupt
cleanup_on_interrupt() {
    log_warn "Script interrupted by user"
    exit 1
}

# ========================================
# Export Functions for Sourcing
# ========================================

# When sourced, make these functions available
if [[ "${BASH_SOURCE[0]}" != "${0}" ]]; then
    # Export all functions
    export -f log_message log_info log_warn log_error log_success log_debug
    export -f show_progress
    export -f setup_environment activate_venv check_dependencies
    export -f validate_required_args validate_date validate_numeric validate_directory
    export -f execute_with_timeout run_background wait_for_process
    export -f backup_file show_file_size count_files
    export -f load_config_with_fallbacks get_config_or_default
    export -f setup_error_handling cleanup_on_exit cleanup_on_error cleanup_on_interrupt
fi

# ========================================
# Main Function for Direct Execution
# ========================================

# Main function when script is executed directly
main() {
    case "${1:-}" in
        test)
            log_info "Testing common utilities..."
            setup_environment "test"
            
            # Test logging
            log_info "This is an info message"
            log_warn "This is a warning"
            log_success "This is a success message"
            
            # Test validation
            if validate_date "2024-01-01"; then
                log_success "Date validation passed"
            fi
            
            # Test config integration
            if load_config_with_fallbacks; then
                log_success "Configuration system available"
            else
                log_warn "Using fallback configuration"
            fi
            
            log_success "Common utilities test completed"
            ;;
        help|--help|-h)
            cat << EOF
Common Shell Utilities

Usage: $0 [command]

Commands:
  test        Test common utilities functionality
  help        Show this help message

When sourced, provides functions:
  Logging: log_info, log_warn, log_error, log_success, log_debug
  Environment: setup_environment, activate_venv, check_dependencies
  Validation: validate_required_args, validate_date, validate_numeric
  Execution: execute_with_timeout, run_background, wait_for_process
  File management: backup_file, show_file_size, count_files
  Config integration: load_config_with_fallbacks, get_config_or_default
  Error handling: setup_error_handling, cleanup functions
EOF
            ;;
        "")
            echo "Common utilities loaded. Use '$0 help' for usage information."
            ;;
        *)
            echo "Unknown command: $1"
            echo "Use '$0 help' for usage information."
            exit 1
            ;;
    esac
}

# Execute main function if script is run directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi 
