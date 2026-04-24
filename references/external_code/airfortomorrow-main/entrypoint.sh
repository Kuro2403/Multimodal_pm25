#!/bin/bash
set -e



# Air Quality Prediction Pipeline - Docker Entry Point
echo "🌤️  Air Quality Prediction Pipeline"
echo "=================================="

# Run the runtime setup check
echo "🔧 Running system verification..."
if [ -f "./scripts/setup.sh" ]; then
    ./scripts/setup.sh   # will stop container if fails
else
    echo "❌ scripts/setup.sh not found"
    exit 1
fi


echo ""
echo "📋 Available commands:"
echo "   Individual data source pipelines:"
echo "     ./scripts/run_air_quality_integrated_pipeline.sh     - Download and processes Open AQ and Airgradient data"
echo "     ./scripts/run_era5_idw_pipeline.sh                   - Download and processes ERA5 meteo data"
echo "     ./scripts/run_firms_pipeline.sh                      - Download and processes FIRMS fire hotspots data"
echo "     ./scripts/run_himawari_integrated_pipeline.sh        - Download and processes Himawari Aerosol Optical Depth data"

echo
echo ""
echo "   Data cleaning:"
echo "     ./scripts/make_silver.sh        - Cleans all processed datasets and aggregates them into a silver DB for prediction"

echo
echo ""
echo "   Model:"
echo "     Pre-trained model bundle: ./src/models/xgboost_model.json"

echo
echo ""
echo "   Prediction:"
echo "     ./scripts/predict_air_quality.sh          - Predicts air quality using the silver database"

echo
echo ""
echo "   Run the complete pipeline:"
echo "     ./scripts/run_complete_pipeline.sh     - Download, process data and predicts air quality"

echo
echo ""
echo "   Set up the environment"
echo "     ./scripts/setup.sh     - Ensure the environment is properly set up - automatically run at container start"


echo
echo ""
echo "💡 Usage examples:"
echo "   ./scripts/run_complete_pipeline.sh --mode realtime "
echo "   ./scripts/run_himawari_integrated_pipeline.sh --mode historical --start-date 2024-01-01 --end-date 2024-01-07"
echo ""

# If arguments provided, execute them; otherwise start interactive shell
# if [ $# -eq 0 ]; then
#     echo "🚀 Starting interactive shell..."
#     exec /bin/bash
# else
#     echo "🏃 Executing: $@"
#     exec "$@"
# fi


# If args provided, run them
if [ "$#" -gt 0 ]; then
  echo "🏃 Executing (args): $*"
  exec "$@"
fi

# Behavior depends on where we are running
if [ "${LOCAL_DEV:-}" = "1" ]; then
  # Local dev: start an interactive shell
  echo "💻 Local development mode (LOCAL_DEV=1). Starting bash..."
  exec /bin/bash
else
  # Default: CapRover / production → idle so you can exec in later
  echo "🛌 No startup command. Idling… (set LOCAL_DEV=1 for local shell)"
  trap "echo '👋 Stopping...'; exit 0" TERM INT
  while :; do
    sleep 86400
  done
fi
