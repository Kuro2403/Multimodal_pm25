#!/bin/bash

# ERA5 IDW Pipeline Script
# This script runs the ERA5 meteorological data pipeline with IDW interpolation:
# 1. Data collection from ECMWF Open Data/CDS
# 2. IDW interpolation to fill missing H3 cells
# 3. Direct daily aggregation (no intermediate H3 files)

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
LOG_FILE="$LOG_DIR/era5_idw_pipeline_${TIMESTAMP}.log"

# Configuration-driven defaults with fallbacks
if [[ "$CONFIG_AVAILABLE" == true ]]; then
    DEFAULT_COUNTRIES=($(get_config_countries))
    DEFAULT_HOURS=$(get_config_time_window "realtime")
    DEFAULT_PARAMS=($(get_config_era5_parameters))
    DEFAULT_BUFFER=$(get_config_buffer "era5")
    DEFAULT_H3_RESOLUTION=$(get_config_h3_resolution)
    DEFAULT_TIMEOUT=$(get_config_timeout "processing_timeout")
    DEFAULT_RAW_DATA_DIR=$(get_config_path "raw.era5.base" "$BASE_DIR/data/raw/era5")
    DEFAULT_OUTPUT_DIR=$(get_config_path "processed.era5.base" "$BASE_DIR/data/processed/era5")
    echo "✅ Using configuration system for defaults"
else
    # Fallback defaults if config system unavailable
    DEFAULT_COUNTRIES=("THA" "LAO")
    DEFAULT_HOURS=24
    DEFAULT_PARAMS=("2d" "2t" "10u" "10v")
    DEFAULT_BUFFER=0.4
    DEFAULT_H3_RESOLUTION=8
    DEFAULT_TIMEOUT=7200
    DEFAULT_RAW_DATA_DIR="$BASE_DIR/data/raw/era5"
    DEFAULT_OUTPUT_DIR="$BASE_DIR/data/processed/era5"
    echo "⚠️  Configuration system unavailable, using hardcoded defaults"
fi

# Default settings (config-driven with fallbacks)
MODE="realtime"
HOURS="$DEFAULT_HOURS"
COUNTRIES=("${DEFAULT_COUNTRIES[@]}")
PARAMS=("${DEFAULT_PARAMS[@]}")
RAW_DATA_DIR="$DEFAULT_RAW_DATA_DIR"
OUTPUT_DIR="$DEFAULT_OUTPUT_DIR"
TIMEOUT="$DEFAULT_TIMEOUT"

# IDW-specific parameters - will be set based on mode and config
IDW_RINGS=""
IDW_WEIGHT_POWER=""

# Processing options
FORCE=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --mode)
            MODE="$2"
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
        --params)
            IFS=',' read -ra PARAMS <<< "$2"
            shift 2
            ;;
        --idw-rings)
            IDW_RINGS="$2"
            shift 2
            ;;
        --idw-weight-power)
            IDW_WEIGHT_POWER="$2"
            shift 2
            ;;
        --output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --raw-data-dir)
            RAW_DATA_DIR="$2"
            shift 2
            ;;
        --timeout)
            TIMEOUT="$2"
            shift 2
            ;;
        --log-level)
            LOG_LEVEL="$2"
            shift 2
            ;;
        --force)
            FORCE=true
            shift
            ;;
        -h|--help)
            echo "ERA5 IDW Pipeline Script"
            echo ""
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --mode MODE                    Processing mode: realtime or historical"
            echo "  --start-date DATE             Start date for historical mode (YYYY-MM-DD)"
            echo "  --end-date DATE               End date for historical mode (YYYY-MM-DD)"
            echo "  --hours HOURS                 Hours to look back for realtime mode (default: 24)"
            echo "  --countries COUNTRY1 COUNTRY2  Space-separated list of country codes (default: THA LAO)"
            echo "  --params PARAM1,PARAM2        Comma-separated list of ERA5 parameters (default: 2d,2t,10u,10v)"
            echo "  --idw-rings RINGS             Number of rings for IDW interpolation (default: 10)"
            echo "  --idw-weight-power POWER      Power for distance weighting in IDW (default: 1.5)"
            echo "  --output-dir DIR              Output directory (default: $DEFAULT_OUTPUT_DIR)"
            echo "  --raw-data-dir DIR            Raw data directory (default: $DEFAULT_RAW_DATA_DIR)"
            echo "  --timeout SECONDS             Processing timeout in seconds (default: $DEFAULT_TIMEOUT)"
            echo "  --log-level LEVEL             Logging level: DEBUG, INFO, WARNING, ERROR (default: INFO)"
            echo "  --force                       Force reprocessing even if output files already exist"
            echo "  -h, --help                    Show this help message"
            echo ""
            echo "Examples:"
            echo "  # Real-time mode (past 24 hours)"
            echo "  $0 --mode realtime"
            echo ""
            echo "  # Historical mode for specific date range"
            echo "  $0 --mode historical --start-date 2023-01-01 --end-date 2023-01-31"
            echo ""
            echo "  # Custom IDW parameters"
            echo "  $0 --mode realtime --idw-rings 7 --idw-weight-power 2.0"
            echo ""
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Validate required arguments
if [[ "$MODE" == "historical" ]]; then
    if [[ -z "$START_DATE" || -z "$END_DATE" ]]; then
        echo "❌ ERROR: Historical mode requires --start-date and --end-date"
        exit 1
    fi
fi

# Set default log level if not specified
LOG_LEVEL="${LOG_LEVEL:-INFO}"

# Display configuration
echo "🚀 ERA5 IDW Pipeline Configuration"
echo "=================================="
echo "Mode: $MODE"
echo "Countries: ${COUNTRIES[*]}"
echo "Parameters: ${PARAMS[*]}"
echo "IDW rings: $IDW_RINGS"
echo "IDW weight power: $IDW_WEIGHT_POWER"
echo "Output directory: $OUTPUT_DIR"
echo "Raw data directory: $RAW_DATA_DIR"
echo "Log level: $LOG_LEVEL"
echo "Log file: $LOG_FILE"

if [[ "$MODE" == "historical" ]]; then
    echo "Date range: $START_DATE to $END_DATE"
else
    echo "Hours lookback: $HOURS"
fi

echo ""

# Check for required dependencies
echo "🔍 Checking dependencies..."
if ! command -v python3 &> /dev/null; then
    echo "❌ ERROR: python3 is required but not installed"
    exit 1
fi

# Check for required Python packages
python3 -c "import earthkit, polars, polars_h3, geopandas, h3ronpy" 2>/dev/null
if [[ $? -ne 0 ]]; then
    echo "❌ ERROR: Required Python packages not found"
    echo "Please install: earthkit, polars, polars-h3, geopandas, h3ronpy"
    exit 1
fi

echo "✅ Dependencies check passed"

# Check for CDS API credentials (for historical mode)
if [[ "$MODE" == "historical" ]]; then
    echo "🔑 Checking CDS API credentials..."
    if [[ ( -z "${CDSAPI_KEY:-}" || -z "${CDSAPI_URL:-}" ) && ! -f ~/.cdsapirc ]]; then
        echo "⚠️  WARNING: CDS API credentials not found"
        echo "   Set CDSAPI_KEY and CDSAPI_URL environment variables or create ~/.cdsapirc file"
        echo "   Get credentials from: https://cds.climate.copernicus.eu/user"
        echo "   Continuing anyway - pipeline will fail if credentials are needed..."
    else
        echo "✅ CDS API credentials found"
    fi
fi

# Create output directories
echo "📁 Creating output directories..."
mkdir -p "$OUTPUT_DIR/daily_aggregated/historical"
mkdir -p "$OUTPUT_DIR/daily_aggregated/realtime"
mkdir -p "$RAW_DATA_DIR"

# Set IDW parameters based on mode and configuration
if [[ "$CONFIG_AVAILABLE" == true ]]; then
    echo "🔧 Setting IDW parameters from configuration..."
    if IDW_RINGS_CONFIG=$(get_config_era5_idw_rings "$MODE" 2>/dev/null); then
        IDW_RINGS="$IDW_RINGS_CONFIG"
        echo "✅ IDW rings set to: $IDW_RINGS (from config for $MODE mode)"
    else
        echo "⚠️  Failed to get IDW rings from config, using fallback"
        if [[ "$MODE" == "realtime" ]]; then
            IDW_RINGS=32
        else
            IDW_RINGS=10
        fi
    fi
    
    if IDW_WEIGHT_POWER_CONFIG=$(get_config_era5_idw_weight_power "$MODE" 2>/dev/null); then
        IDW_WEIGHT_POWER="$IDW_WEIGHT_POWER_CONFIG"
        echo "✅ IDW weight power set to: $IDW_WEIGHT_POWER (from config for $MODE mode)"
    else
        echo "⚠️  Failed to get IDW weight power from config, using fallback"
        if [[ "$MODE" == "realtime" ]]; then
            IDW_WEIGHT_POWER=1.75
        else
            IDW_WEIGHT_POWER=1.5
        fi
    fi
else
    echo "⚠️  Configuration system unavailable, using hardcoded defaults"
    if [[ "$MODE" == "realtime" ]]; then
        IDW_RINGS=32
        IDW_WEIGHT_POWER=1.75
    else
        IDW_RINGS=10
        IDW_WEIGHT_POWER=1.5
    fi
fi

echo "🔧 Final IDW configuration:"
echo "   Mode: $MODE"
echo "   Rings: $IDW_RINGS"
echo "   Weight Power: $IDW_WEIGHT_POWER"

# Build command arguments
CMD_ARGS=(
    "--mode" "$MODE"
    "--countries" "${COUNTRIES[@]}"
    "--params" "${PARAMS[@]}"
    "--idw-rings" "$IDW_RINGS"
    "--idw-weight-power" "$IDW_WEIGHT_POWER"
    "--output-dir" "$OUTPUT_DIR"
    "--raw-data-dir" "$RAW_DATA_DIR"
    "--log-level" "$LOG_LEVEL"
)

if [[ "$MODE" == "historical" ]]; then
    CMD_ARGS+=("--start-date" "$START_DATE" "--end-date" "$END_DATE")
else
    CMD_ARGS+=("--hours" "$HOURS")
fi

if [[ "$FORCE" == "true" ]]; then
    CMD_ARGS+=("--force")
fi

# Run the pipeline with timeout
echo ""
echo "🚀 Starting ERA5 IDW Pipeline..."
echo "Command: python3 -m src.era5_integrated_pipeline_idw ${CMD_ARGS[*]}"
echo ""

# Run the pipeline
timeout "$TIMEOUT" python3 -m src.era5_integrated_pipeline_idw "${CMD_ARGS[@]}" 2>&1 | tee "$LOG_FILE"

# Check exit status
EXIT_CODE=${PIPESTATUS[0]}

echo ""
if [[ $EXIT_CODE -eq 0 ]]; then
    echo "✅ ERA5 IDW Pipeline completed successfully!"
    echo "📁 Check output directory: $OUTPUT_DIR/daily_aggregated"
    echo "📋 Check log file: $LOG_FILE"
else
    echo "❌ ERA5 IDW Pipeline failed with exit code $EXIT_CODE"
    echo "📋 Check log file: $LOG_FILE for details"
    
    if [[ $EXIT_CODE -eq 124 ]]; then
        echo "⏰ Pipeline timed out after $TIMEOUT seconds"
        echo "   Consider increasing timeout or reducing date range"
    fi
fi

echo ""
echo "🏁 Pipeline execution completed"
exit $EXIT_CODE
