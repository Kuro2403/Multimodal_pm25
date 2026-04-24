#!/bin/bash

# Himawari IDW Interpolator Script
# This script performs spatial interpolation of daily aggregated Himawari AOD data using Inverse Distance Weighting

# Set the base directory to the project root (parent of scripts/)
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
LOG_DIR="$BASE_DIR/logs"
mkdir -p "$LOG_DIR"

# Set log file with timestamp
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="$LOG_DIR/himawari_idw_${TIMESTAMP}.log"

# Default settings
INPUT_DIR="./data/processed/himawari/daily_aggregated"
OUTPUT_DIR="./data/processed/himawari/interpolated"
MODE="historical"
COUNTRIES=("LAO" "THA")
RINGS=10
WEIGHT_POWER=1.5
START_DATE=""
END_DATE=""
FORCE_OVERWRITE=false

# Function to log messages
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Help message
usage() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Himawari IDW Interpolator - Replace Kriging with Inverse Distance Weighting"
    echo ""
    echo "Options:"
    echo "  --mode MODE                 Processing mode: historical or realtime (default: historical)"
    echo "  --input-dir DIR             Input directory with daily aggregated data (default: ./data/processed/himawari/daily_aggregated)"
    echo "  --output-dir DIR            Output directory for interpolated data (default: ./data/processed/himawari/interpolated)"
    echo "  --countries CODES           Country codes for boundaries (default: LAO THA)"
    echo "  --rings N                   Number of H3 rings for IDW interpolation (default: 10)"
    echo "  --weight-power X            Power for inverse distance weighting (default: 1.5)"
    echo "  --start-date DATE           Start date for processing (YYYY-MM-DD)"
    echo "  --end-date DATE             End date for processing (YYYY-MM-DD)"
    echo "  --force-overwrite           Force reprocessing of existing files"
    echo "  --help                      Display this help message"
    echo ""
    echo "Description:"
    echo "  This script replaces the Kriging interpolation with a faster, validated IDW approach."
    echo "  It processes daily aggregated Himawari AOD data and creates interpolated datasets"
    echo "  with 6 columns: h3_08, date, aod_1day, aod_2day, aod_1day_interpolated, aod_2day_interpolated"
    echo ""
    echo "Input Structure:"
    echo "  data/processed/himawari/daily_aggregated/{mode}/daily_h3_aod_YYYYMMDD_COUNTRIES.parquet"
    echo ""
    echo "Output Structure:"
    echo "  data/processed/himawari/interpolated/{mode}/interpolated_h3_aod_YYYYMMDD_COUNTRIES.parquet"
    echo ""
    echo "Examples:"
    echo "  # Process historical data for specific date range"
    echo "  $0 --mode historical --start-date 2024-02-01 --end-date 2024-02-28 --countries LAO THA"
    echo ""
    echo "  # Process real-time data with custom IDW parameters"
    echo "  $0 --mode realtime --rings 15 --weight-power 2.0 --countries LAO THA"
    echo ""
    echo "  # Process single day with force overwrite"
    echo "  $0 --mode historical --start-date 2024-02-02 --end-date 2024-02-02 --force-overwrite"
    exit 1
}

# Parse command line arguments
EXTRA_ARGS=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --mode)
            MODE="$2"
            EXTRA_ARGS="$EXTRA_ARGS --mode $2"
            shift 2
            ;;
        --input-dir)
            INPUT_DIR="$2"
            EXTRA_ARGS="$EXTRA_ARGS --input-dir $2"
            shift 2
            ;;
        --output-dir)
            OUTPUT_DIR="$2"
            EXTRA_ARGS="$EXTRA_ARGS --output-dir $2"
            shift 2
            ;;
        --countries)
            COUNTRIES=()
            shift
            while [[ $# -gt 0 && ! "$1" =~ ^-- ]]; do
                COUNTRIES+=("$1")
                shift
            done
            EXTRA_ARGS="$EXTRA_ARGS --countries ${COUNTRIES[*]}"
            ;;
        --rings)
            RINGS="$2"
            EXTRA_ARGS="$EXTRA_ARGS --rings $2"
            shift 2
            ;;
        --weight-power)
            WEIGHT_POWER="$2"
            EXTRA_ARGS="$EXTRA_ARGS --weight-power $2"
            shift 2
            ;;
        --start-date)
            START_DATE="$2"
            EXTRA_ARGS="$EXTRA_ARGS --start-date $2"
            shift 2
            ;;
        --end-date)
            END_DATE="$2"
            EXTRA_ARGS="$EXTRA_ARGS --end-date $2"
            shift 2
            ;;
        --force-overwrite)
            FORCE_OVERWRITE=true
            shift 1
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

# Validate arguments
if [[ ! "$MODE" =~ ^(historical|realtime)$ ]]; then
    echo "Error: Mode must be 'historical' or 'realtime'"
    exit 1
fi

if [[ ${#COUNTRIES[@]} -eq 0 ]]; then
    echo "Error: At least one country code must be specified"
    exit 1
fi

if [[ ! -d "$INPUT_DIR" ]]; then
    echo "Error: Input directory not found: $INPUT_DIR"
    exit 1
fi

# Check for input files
MODE_INPUT_DIR="$INPUT_DIR/$MODE"
if [[ ! -d "$MODE_INPUT_DIR" ]]; then
    echo "Error: Mode-specific input directory not found: $MODE_INPUT_DIR"
    exit 1
fi

COUNTRIES_STR=$(IFS="_"; echo "${COUNTRIES[*]}" | tr ' ' '\n' | sort | tr '\n' '_' | sed 's/_$//')
INPUT_PATTERN="$MODE_INPUT_DIR/daily_h3_aod_*_${COUNTRIES_STR}.parquet"
INPUT_FILES=($(ls $INPUT_PATTERN 2>/dev/null))

if [[ ${#INPUT_FILES[@]} -eq 0 ]]; then
    echo "Error: No input files found matching pattern: $INPUT_PATTERN"
    echo "Available files in $MODE_INPUT_DIR:"
    ls -la "$MODE_INPUT_DIR"/*.parquet 2>/dev/null || echo "  No parquet files found"
    exit 1
fi

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Log configuration
log "Starting Himawari IDW Interpolation..."
log "Mode: $MODE"
log "Input directory: $INPUT_DIR"
log "Output directory: $OUTPUT_DIR"
log "Countries: ${COUNTRIES[*]}"
log "IDW Parameters: rings=$RINGS, weight_power=$WEIGHT_POWER"
if [[ -n "$START_DATE" ]]; then
    log "Start date: $START_DATE"
fi
if [[ -n "$END_DATE" ]]; then
    log "End date: $END_DATE"
fi
log "Force overwrite: $FORCE_OVERWRITE"
log "Found ${#INPUT_FILES[@]} input files to process"

# Check Python dependencies
log "Checking Python dependencies..."
python -c "
try:
    import polars, polars_h3, geopandas, h3ronpy
    print('✅ All required packages available')
except ImportError as e:
    print(f'❌ Missing packages: {e}')
    print('Please install: pip install polars polars-h3 geopandas h3ronpy')
    exit(1)
" || exit 1

# Run the IDW interpolator
log "Running IDW interpolation..."
log "Command: python src/data_processors/himawari_idw_interpolator.py $EXTRA_ARGS"

# Add timeout for safety (30 minutes)
timeout 1800 python src/data_processors/himawari_idw_interpolator.py $EXTRA_ARGS

# Check execution result
EXIT_CODE=$?
if [[ $EXIT_CODE -eq 124 ]]; then
    log "ERROR: IDW interpolation timed out after 30 minutes"
    exit 1
elif [[ $EXIT_CODE -eq 0 ]]; then
    log "✅ IDW interpolation completed successfully!"
    log "Results saved to: $OUTPUT_DIR"
    
    # Show output summary
    MODE_OUTPUT_DIR="$OUTPUT_DIR/$MODE"
    if [[ -d "$MODE_OUTPUT_DIR" ]]; then
        PARQUET_COUNT=$(find "$MODE_OUTPUT_DIR" -name "*.parquet" | wc -l)
        log "Generated $PARQUET_COUNT interpolated H3 Parquet files"
        
        # Show sample of latest files
        log "Latest output files:"
        ls -lat "$MODE_OUTPUT_DIR"/*.parquet 2>/dev/null | head -5 | while read line; do
            log "  $line"
        done
    fi
    
    # Validate output structure
    log "Validating output structure..."
    SAMPLE_FILE=$(find "$MODE_OUTPUT_DIR" -name "*.parquet" | head -1)
    if [[ -n "$SAMPLE_FILE" ]]; then
        python -c "
import polars as pl
try:
    df = pl.read_parquet('$SAMPLE_FILE')
    required_cols = ['h3_08', 'date', 'aod_1day', 'aod_2day', 'aod_1day_interpolated', 'aod_2day_interpolated']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f'❌ Missing required columns: {missing_cols}')
        exit(1)
    else:
        print(f'✅ Output structure validated: {len(df)} rows, {len(df.columns)} columns')
        print(f'✅ Columns: {list(df.columns)}')
        
        # Check data coverage
        orig_1day = df.filter(pl.col('aod_1day').is_not_null()).height
        interp_1day = df.filter(pl.col('aod_1day_interpolated').is_not_null()).height
        orig_2day = df.filter(pl.col('aod_2day').is_not_null()).height
        interp_2day = df.filter(pl.col('aod_2day_interpolated').is_not_null()).height
        
        print(f'✅ Coverage - Original 1-day: {orig_1day}, Interpolated 1-day: {interp_1day}')
        print(f'✅ Coverage - Original 2-day: {orig_2day}, Interpolated 2-day: {interp_2day}')
except Exception as e:
    print(f'❌ Error validating output: {e}')
    exit(1)
" || log "⚠️ Output validation failed"
    fi
else
    log "❌ ERROR: IDW interpolation failed with exit code $EXIT_CODE"
    exit 1
fi

log "🎉 Himawari IDW interpolation pipeline completed successfully!" 