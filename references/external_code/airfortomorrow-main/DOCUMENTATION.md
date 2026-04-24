# 📚 Air for Tomorrow - Technical Documentation

**A product of UNICEF EAPRO's Frontier Data Lab.**

Technical reference for the Air for Tomorrow pipeline, including workflows and configuration options.

---

## 📑 Table of Contents

1. [Complete Pipeline Workflow](#complete-pipeline-workflow)
2. [Individual Data Source Workflows](#individual-data-source-workflows)
   - [1. Air Quality Data](#1-air-quality-data)
   - [2. FIRMS Fire Detection Data](#2-firms-fire-detection-data)
   - [3. Himawari Satellite AOD Data](#3-himawari-satellite-aod-data)
   - [4. ERA5 Meteorological Data](#4-era5-meteorological-data)
   - [5. Silver Dataset Generation](#5-silver-dataset-generation)
   - [6. Air for Tomorrow](#6-air-for-tomorrow)
3. [Model Information](#model-information)
4. [Docker Advanced](#docker-advanced)
5. [Project Structure](#project-structure)
6. [Configuration Reference](#configuration-reference)
7. [External Datasets and Licenses](EXTERNAL_DATASETS_AND_LICENSES.md)

---

## External Datasets and Licenses

The canonical external data inventory for this repository is maintained in:
**[EXTERNAL_DATASETS_AND_LICENSES.md](EXTERNAL_DATASETS_AND_LICENSES.md)**.

---

## Complete Pipeline Workflow

The complete pipeline provides a fully automated workflow from data collection to air quality predictions.

### Pipeline Phases

The complete pipeline executes in these phases:

1. **Phase 1: Data Collection & Processing** - Runs integrated pipelines for all data sources:
   - **Himawari**: Download → Conversion NetCDF to GeoTIFF →  H3 processing → Daily aggregation → IDW interpolation
   - **FIRMS**: Collection → Deduplication → KDE interpolation → Heat maps
   - **ERA5**: Collection → Daily aggregation → IDW interpolation
   - **OpenAQ/AirGradient**: Collection → Deduplication  
2. **Phase 2: Silver Dataset Generation** - Combines all processed data sources into unified H3-indexed dataset
3. **Phase 3: Air Quality Prediction** - Generates PM2.5 predictions with validation and maps

### Sensor Validation

**🎯 Enabled by Default for Historical Mode:**
- Sensor validation, enhanced maps, and validation data saving are **enabled by default** for historical runs. However the feature is not available for realtime runs as we dont know the AQ for the next 24 hours yet.
- Use `--no-sensor-validation` and `--no-enhanced-maps` to disable

### Basic Usage

```bash
# Complete real-time pipeline (recommended for daily operations)
./scripts/run_complete_pipeline.sh --mode realtime --countries THA LAO --generate-maps --parallel

# Historical analysis pipeline
./scripts/run_complete_pipeline.sh --mode historical --start-date 2024-06-01 --end-date 2024-06-03 --countries THA LAO --generate-maps

# Historical without sensor validation
./scripts/run_complete_pipeline.sh --mode historical --start-date 2024-06-01 --end-date 2024-06-03 --countries THA LAO --no-sensor-validation --no-enhanced-maps
```

### Advanced Options

#### Individual Data Source Control

Skip specific data sources to save time or resources:

```bash
# Skip specific data sources
./scripts/run_complete_pipeline.sh --mode realtime --skip-himawari --skip-firms --countries THA LAO

# Available skip flags:
# --skip-himawari      Skip satellite AOD collection
# --skip-firms         Skip fire detection data
# --skip-era5          Skip meteorological data
# --skip-openaq        Skip OpenAQ air quality data
# --skip-airgradient   Skip AirGradient sensor data
```

#### Pipeline Phase Control

Control which phases of the pipeline execute:

```bash
# Run only data collection (skip silver and prediction)
./scripts/run_complete_pipeline.sh --mode realtime --skip-silver --skip-prediction --countries THA LAO

# Run data collection and silver dataset (skip prediction)
./scripts/run_complete_pipeline.sh --mode realtime --skip-prediction --countries THA LAO


# Available skip flags:
# --skip-silver            Skip the generation of the aggregated dataset containing all features
# --skip-prediction        Skip the AQ prediction step

```

#### Validation & Map Control

```bash
# Complete pipeline with all validation features (enabled by default)
./scripts/run_complete_pipeline.sh --mode historical --start-date 2024-06-01 --end-date 2024-06-03 --countries THA LAO --generate-maps

# Explicitly enable all validation features (same as default behavior)
./scripts/run_complete_pipeline.sh --mode historical --start-date 2024-06-01 --end-date 2024-06-03 --countries THA LAO \
  --generate-maps \
  --validate-sensors \
  --enhanced-maps \
  --save-validation

# Available flags: 
#   --generate-maps         Generate a map with the prediction but without the sensors's ground truth into data/predictions/map/
#   --validate-sensors      Activates the validation flow that compares predictions with ground truth and produces relevant charts into data/predictions/scatter/ - only for historical mode where next 24h are known
#   --enhanced-maps         Generates a map overlapping the predictions and the ground truth sensors into data/predictions/validation_map/ - only for historical mode 
#   --save-validation       Saves the validation data used to generate the charts and maps into data/predictions/validation_data/
```

#### Performance Options

```bash
# Parallel execution (faster, uses more resources) - recommended for realtime, not for historical
./scripts/run_complete_pipeline.sh --mode realtime --parallel --countries THA LAO
```

### Recommended Approach by Use Case

#### For Real-Time Operations (Daily Updates)

```bash
# Use complete pipeline with parallel execution
./scripts/run_complete_pipeline.sh --mode realtime --countries THA LAO --generate-maps --parallel
```

#### For Long Historical Analysis (Months/Years)

Run each pipeline individually to better monitor progress and handle errors:

```bash
# 1. Collect air quality data
./scripts/run_air_quality_integrated_pipeline.sh --mode historical --start-date 2023-01-01 --end-date 2023-06-30 --countries THA LAO

# 2. Collect meteorological data
./scripts/run_era5_idw_pipeline.sh --mode historical --start-date 2023-01-01 --end-date 2023-06-30 --countries THA LAO

# 3. Process fire detection data
./scripts/run_firms_pipeline.sh --mode historical --start-date 2023-01-01 --end-date 2023-06-30 --countries THA LAO

# 4. Process satellite AOD data - for period of times > 6months, it's recommended to run each step of this pipeline separately - check the himawari section for more details
./scripts/run_himawari_integrated_pipeline.sh --mode historical --start-date 2023-01-01 --end-date 2023-06-30 --countries THA LAO

# 5. Generate silver dataset
./scripts/make_silver.sh --mode historical --start-date 2023-01-01 --end-date 2023-06-30 --countries THA LAO

# 6. Generate predictions and maps
./scripts/predict_air_quality.sh --mode historical --start-date 2023-01-01 --end-date 2023-06-30 --countries THA LAO --generate-map
```

**Why separate for historical?**
- Better progress monitoring for long-running processes
- Easier to restart from failed step without reprocessing everything
- More efficient resource management for large date ranges
- Can run different date ranges per data source if needed

---

## Individual Data Source Workflows

### 1. Air Quality Data

**Integrated pipeline** - Collects and processes data from both OpenAQ and AirGradient sensors in one command.

#### Basic Usage

```bash
# Real-time mode
./scripts/run_air_quality_integrated_pipeline.sh --mode realtime --countries THA LAO

# Historical mode
./scripts/run_air_quality_integrated_pipeline.sh --mode historical --start-date 2023-01-01 --end-date 2023-12-31 --countries THA LAO
```

#### What it does

1. Collects OpenAQ data (ground monitoring stations)
2. Collects AirGradient data (low-cost sensor network)
3. Processes and deduplicates both sources
4. Adds H3 spatial indexing (resolution 8)
5. Applies quality filtering (PM2.5: 0-500 μg/m³)

#### Output

H3-indexed parquet files ready for silver dataset generation:
- `data/processed/airquality/realtime/` or `data/processed/airquality/historical/`

#### Key Features

- **Deduplication**: Prevents double-counting when sensors overlap (within 500m radius)
- **Multi-source Integration**: Combines two complementary networks
- **Quality Control**: Configurable thresholds and validation
- **Source Tracking**: Maintains `pm25_source` field (openaq/airgradient)


#### Data Sources Used

The Air Quality pipeline uses different data sources depending on the mode and time period:

**OpenAQ Data:**
- **Realtime (< 90 days)**: [OpenAQ v3 API](https://docs.openaq.org/) - Direct API access to latest measurements
  - Updates every few hours with fresh data
  - No S3 access needed, just API key in `.env`
  - Best for operational monitoring
- **Historical (> 90 days)**: [OpenAQ AWS S3 Archive](https://registry.opendata.aws/openaq/) - Public S3 bucket with historical data
  - Free public access (no credentials needed)
  - Complete archive back to 2015
  - Data organized by location and year

**AirGradient Data:**
- **Both Realtime & Historical**: [AirGradient API](https://api.airgradient.com/)
  - Direct API access for both modes
  - Historical data available for past 90 days through API
  - Low-cost sensor network with high spatial coverage in Thailand and Laos

#### Separate processing steps 

You can also run each step of the Air Quality pipeline independently:
```bash
- collect_openaq_realtime.sh - to collect the realtime data from OpenAQ API 
- collect_openaq_historical.sh - to collect the OpenAQ historical data from an AWS S3  
- collect_airgradient_data.sh - to collect the Air Gradient data from the API - both historical and real time 
- process_air_quality.sh - to process and deduplicate the OpenAQ and Air Gradient data collected in previous steps 
```

---

### 2. FIRMS Fire Detection Data

**Integrated Pipeline** - Collects, processes, and applies KDE interpolation for fire hotspot data.

#### Basic Usage

```bash
# Real-time mode (automatic download from NASA FIRMS, past 7 days)
./scripts/run_firms_pipeline.sh --mode realtime --countries THA LAO

# Historical mode (uses pre-downloaded data)
./scripts/run_firms_pipeline.sh --mode historical --start-date 2024-01-01 --end-date 2024-12-31 --countries THA LAO
```

#### Historical Data Availability

Historical fire data is included in the repository for the period **01/01/2022 to 30/10/2025**.

After cloning the repository, pull LFS-managed datasets before running historical mode:

```bash
git lfs install
git lfs pull
```

**For dates after 30/10/2025:**

1. Visit [NASA FIRMS Archive Download](https://firms.modaps.eosdis.nasa.gov/download/)
2. Request and download the following CSV files for your region/date range:
   - **MODIS Collection 6.1** (1km resolution) - `fire_archive_M-C61_*.csv`
   - **VIIRS S-NPP 375m** - `fire_archive_SV-C2_*.csv`
   - **VIIRS NOAA-20 375m** - `fire_nrt_J1V-C2_*.csv` or `fire_archive_J1V-C2_*.csv`
   - **VIIRS NOAA-21 375m** (from Jan 2024) - `fire_nrt_J2V-C2_*.csv` or `fire_archive_J2V-C2_*.csv`
3. Place downloaded files in: `data/raw/firms/historical/`
4. Keep original naming convention from NASA

#### What it does

1. Collects fire hotspot data from VIIRS and MODIS satellites
2. Deduplicates overlapping detections across satellites (within 500m / 30-minute windows)
3. Filters fires within country boundaries (with configurable buffer)
4. Applies KDE (Kernel Density Estimation) interpolation
5. Generates H3-indexed fire intensity grids (resolution 8)
6. Creates fire density heatmaps for visualization

#### KDE (Kernel Density Estimation) Processing

The FIRMS pipeline applies KDE to convert discrete fire points into smooth continuous fire intensity surfaces.

**Basic KDE Usage:**
```bash
# Use default KDE parameters
./scripts/run_firms_pipeline.sh --mode historical --start-date 2024-01-01 --end-date 2024-12-31 --countries THA LAO
```

**Custom KDE Parameters:**
```bash
# KDE with custom parameters
./scripts/run_firms_pipeline.sh --mode historical \
  --start-date 2024-01-01 --end-date 2024-12-31 \
  --countries THA LAO \
  --density 6400 \
  --chunks 8 \
  --bandwidth 0.3 \
  --buffer 0.4
```

**KDE Parameters:**
- **`--density`** (default: 6400): Grid size for KDE interpolation. Higher = finer resolution but slower processing
- **`--chunks`** (default: 8): Number of parallel chunks for processing. Adjust based on available CPU cores
- **`--bandwidth`** (default: 0.3): KDE bandwidth factor. Lower = sharper peaks, Higher = smoother distribution
- **`--buffer`** (default: 0.4°): Geographic buffer around country boundaries for edge smoothing

**Why KDE?**
- Converts point data to smooth continuous surfaces
- Reveals spatial patterns and hotspot clusters
- Handles edges properly with buffer zones
- Integrates seamlessly with H3 grid system

#### Output

- **H3 Data**: `data/processed/firms/h3/` - H3-indexed fire intensity
- **Heatmaps**: `data/processed/firms/plots/` - Visualization PNGs
- **KDE Grids**: `data/processed/firms/kde/` - Intermediate KDE outputs (optional)

#### Key Features

- **Multi-Satellite Fusion**: Combines VIIRS (375m) and MODIS (1km) for better coverage
- **Smart Deduplication**: Removes duplicate detections within 500m / 30-minute windows
- **KDE Interpolation**: Smooth fire density estimation with 0.4° buffer
- **Quality Filtering**: Removes low-confidence and cloud-contaminated detections
- **Automatic Downloads**: Real-time mode automatically downloads latest data

#### Data Sources Used

The FIRMS pipeline uses different data sources depending on the time period:

**NASA FIRMS Active Fire Data:**
- **Realtime (< 10 days)**: [NASA FIRMS NRT (Near Real-Time) API](https://firms.modaps.eosdis.nasa.gov/api/)
  - Automatically downloads last 7-10 days from NASA servers
  - Updates every 3-4 hours with new satellite passes
  - No credentials required (uses MAP_KEY from NASA FIRMS)
  - Includes multiple satellites:
    - **VIIRS S-NPP** (375m resolution)
    - **VIIRS NOAA-20** (375m resolution)
    - **VIIRS NOAA-21** (375m resolution, from Jan 2024)
    - **MODIS Aqua & Terra** (1km resolution)

- **Historical (> 10 days)**: [NASA FIRMS Archive Download Tool](https://firms.modaps.eosdis.nasa.gov/download/)
  - Pre-downloaded CSV archives stored in `data/raw/firms/historical/`
  - **Included in repository**: Historical data from 01/01/2022 to 30/10/2025
  - **For newer dates**: Manual download required from FIRMS website
  - Covers same satellite sources as realtime

#### Separate processing steps 

You can also run each step of the FIRMS pipeline independently:
```bash
- process_firms_data.sh - to collect and process the FIRMS data
- run_firms_kde.sh - to run the KDE interpolation in realtime mode
- run_firms_kde_historical.sh - to run the KDE interpolation in historical mode
```

---

### 3. Himawari Satellite AOD Data

**Integrated Pipeline** - Downloads, processes, and interpolates Aerosol Optical Depth (AOD) from Himawari-8 satellite.
This pipeline is very heavy, it is recommended to do the following: 
- for realtime runs: use run_himawari_integrated_pipeline.sh
- for historical runs under 6 months: use run_himawari_integrated_pipeline.sh 
- for historical runs over 6 months: run each each sub scripts: run_himawari_aod_historical.sh / run_himawari_daily_aggregator.sh / run_himawari_idw.sh (see below for details)


#### Basic Usage

```bash
# Real-time mode (collects and process past 7 days)
./scripts/run_himawari_integrated_pipeline.sh --mode realtime --hours 24 --countries THA LAO

# Historical mode
./scripts/run_himawari_integrated_pipeline.sh --mode historical --start-date 2025-01-01 --end-date 2025-01-31 --countries THA LAO

# Keep original NetCDF files (for debugging/research)
./scripts/run_himawari_integrated_pipeline.sh --mode realtime --hours 24 --keep-originals --countries THA LAO
```

#### What it does

1. Downloads NetCDF files from JAXA FTP server
2. Converts NetCDF → GeoTIFF → H3 hexagons (hourly data)
3. Aggregates hourly data to daily averages
4. Applies IDW interpolation to fill gaps from cloud cover
5. Outputs H3-indexed AOD ready for silver dataset

#### IDW (Inverse Distance Weighting) Interpolation

IDW fills spatial gaps caused by cloud cover and satellite gaps.

**Basic IDW Usage:**
```bash
# Use default IDW parameters
./scripts/run_himawari_integrated_pipeline.sh --mode historical --start-date 2025-01-01 --end-date 2025-01-31 --countries THA LAO
```

**Custom IDW Parameters:**
```bash
# IDW with custom parameters (via integrated pipeline)
./scripts/run_himawari_integrated_pipeline.sh --mode historical \
  --start-date 2025-01-01 --end-date 2025-01-31 \
  --countries THA LAO \
  --buffer 0.4
  
# Adjust IDW parameters directly (advanced)
python src/data_processors/himawari_idw_interpolator.py \
  --mode historical --countries THA LAO \
  --rings 15 --weight-power 2.0 \
  --start-date 2025-01-01 --end-date 2025-01-31
```

**IDW Parameters:**
- **`--rings`** (default: 10): Number of H3 rings for neighbor search. Higher = smoother but slower
- **`--weight-power`** (default: 1.5): Distance weight exponent. Higher = more weight to closer points
- **`--buffer`** (default: 0.4°): Geographic buffer around boundaries for edge smoothing
- **AOD Thresholds**: Filters values between 0 and 2.0 (removes invalid readings)

#### Output

- **Interpolated Data**: `data/processed/himawari/interpolated/` - Daily H3-indexed AOD
- **Daily Aggregated**: `data/processed/himawari/daily_aggregated/` - Pre-interpolation daily data
- **H3 Hourly**: `data/processed/himawari/h3_hourly/` - Hourly H3 data
- **TIF Files**: `data/cache/himawari/` - Temporary GeoTIFF files (auto-deleted)

#### Key Features

- **Cloud Gap Filling**: IDW interpolates missing values from cloud cover
- **7-Day Buffer**: Automatically collects 7 extra days for rolling average calculations
- **Space Efficient**: Auto-deletes NetCDF files after processing (99%+ space savings: 6.6GB → 81MB)
- **Smart Caching**: Skips existing interpolated files to avoid reprocessing
- **Automatic Cleanup**: Removes temporary TIF files after processing

#### Data Sources Used

The Himawari pipeline uses the following data source for both realtime and historical modes:

**JAXA Himawari-8 AOD Product:**
- **Both Realtime & Historical**: [JAXA P-Tree FTP Server](ftp://ftp.ptree.jaxa.jp)
  - Credentials required (stored in `.env` as `HIMAWARI_FTP_USER` and `HIMAWARI_FTP_PASSWORD`)
  - Registration: [https://www.eorc.jaxa.jp/ptree/registration_top.html](https://www.eorc.jaxa.jp/ptree/registration_top.html)
  - **Data Product**: Aerosol Optical Depth (AOD) at 500nm wavelength
  - **Temporal Resolution**: Hourly observations (24 files per day)
  - **Spatial Coverage**: Full Asia-Pacific region
  - **Spatial Resolution**: ~5km at nadir
  - **Format**: NetCDF4 files (~270 MB per hour)
  - **Availability**: Realtime and complete historical archive back to July 2015
  - **Latency**: ~2-3 hours after observation time

**Note on Data Volume:**
- Raw NetCDF files are very large (~6.6 GB per day)
- Pipeline auto-converts to GeoTIFF and H3 format, then deletes NetCDF files
- Final H3-indexed data is ~99% smaller (~81 MB per day)
- Use `--keep-originals` flag only if you need NetCDF files for research

#### Separate processing steps

You can also run each step of the Himawari pipeline independently:

```bash
# Download and convert realtime data
./scripts/run_himawari_aod_realtime.sh --hours 24 --countries THA LAO

# Download and convert historical data
./scripts/run_himawari_aod_historical.sh --start-date 2024-01-01 --end-date 2024-01-31 --countries THA LAO

# Aggregate hourly data to daily means
./scripts/run_himawari_daily_aggregator.sh --mode historical --start-date 2024-01-01 --end-date 2024-01-31

# Apply IDW interpolation to fill cloud gaps
./scripts/run_himawari_idw.sh --mode historical --start-date 2024-01-01 --end-date 2024-01-31
```

---

### 4. ERA5 Meteorological Data

**Complete Integrated Pipeline** - Collects and interpolates weather data from ERA5.

#### Basic Usage

```bash
# Real-time processing (past 24 hours)
./scripts/run_era5_idw_pipeline.sh --mode realtime --hours 24 --countries LAO THA

# Historical processing
./scripts/run_era5_idw_pipeline.sh --mode historical --start-date 2025-01-01 --end-date 2025-01-31 --countries LAO THA
```

#### Data Parameters

The pipeline collects the following meteorological variables:
- **`2t`**: 2-meter temperature (°C)
- **`10u`**: 10-meter u-component of wind (m/s)
- **`10v`**: 10-meter v-component of wind (m/s)
- **`2d`**: 2-meter dewpoint temperature (°C)

#### IDW Interpolation

ERA5 data comes on a coarse grid (0.25°) and is interpolated to the finer H3 grid:

**Basic IDW Usage:**
```bash
# Use default IDW parameters
./scripts/run_era5_idw_pipeline.sh --mode realtime --hours 24 --countries LAO THA
```

**Custom IDW Parameters:**
```bash
# Custom IDW parameters
./scripts/run_era5_idw_pipeline.sh --mode realtime --hours 24 --countries LAO THA \
    --idw-rings 15 --idw-weight-power 2.0
```

**IDW Parameters:**
- **Algorithm**: Inverse Distance Weighting for spatial interpolation
- **Rings**: 10 rings for interpolation (configurable with `--idw-rings`)
- **Weight Power**: 1.5 for distance weighting (configurable with `--idw-weight-power`)
- **Output**: H3 resolution 8 hexagons (~0.74 km² per cell)

#### Processing Modes

1. **Real-time Mode**: Uses ECMWF Open Data API (no credentials required)
   - Faster, simpler
   - Limited to recent data (past few days)
   
2. **Historical Mode**: Uses Climate Data Store (CDS) API (requires CDS API credentials)
   - Full historical archive
   - Requires registration at [CDS](https://cds.climate.copernicus.eu/)

#### Output

- **Interpolated Data**: `data/processed/era5/interpolated/` - Daily H3-indexed weather variables
- **Daily Aggregated**: `data/processed/era5/daily_aggregated/` - Pre-interpolation daily data
- **Raw Data**: `data/raw/era5/` - Original ERA5 GRIB/NetCDF files

#### Data Sources Used

The ERA5 pipeline uses **three different data sources** depending on the time period:

**1. Today (Day 0) - ECMWF Open Data Live API:**
- **Source**: [ECMWF Open Data API](https://www.ecmwf.int/en/forecasts/datasets/open-data) (Live forecasts)
- **No credentials required** - Public access
- **Use case**: Latest forecast for today
- **Updates**: Every 6 hours with new forecast runs
- **Latency**: ~4 hours after observation time
- **Data**: Latest operational forecast (steps: 0, 6, 12, 18, 24 hours)
- **Best for**: Real-time operational monitoring

**2. Recent Past (Days 1-6) - ECMWF AWS Mirror:**
- **Source**: [ECMWF AWS S3 Mirror](https://registry.opendata.aws/ecmwf-forecasts/) (Historical forecasts)
- **No credentials required** - Public S3 bucket
- **Use case**: Recent past days (typically 1-6 days ago)
- **Updates**: Daily archive of operational forecasts
- **Latency**: 1-3 day lag for data availability
- **Data**: Archived operational forecasts from AWS mirror
- **Note**: Some very recent dates may not be available yet (404 errors expected)

**3. Historical (> 7 days) - CDS API:**
- **Source**: [CDS API (Climate Data Store)](https://cds.climate.copernicus.eu/) - ERA5 Reanalysis
- **Credentials required** - Free registration needed at [CDS Registration](https://cds.climate.copernicus.eu/#!/home)
- **Setup**: Set `CDSAPI_KEY` and `CDSAPI_URL` in `.env` file (or create `~/.cdsapirc`)
- **Use case**: Long-term historical analysis and model training
- **Data**: ERA5 reanalysis (quality-controlled, homogeneous dataset)
- **Latency**: ~5 days to 3 months for final product
- **Archive**: Complete back to 1940
- **Best for**: Research, model training, long-term analysis

**Why Three Sources?**
- **Live API**: Provides latest forecast immediately (no lag)
- **AWS Mirror**: Bridges the gap between live API and reanalysis (1-6 days ago)
- **CDS Reanalysis**: Provides quality-controlled historical data for research

**Variables Collected (all sources):**
- `2t` - 2-meter temperature (Kelvin, converted to Celsius)
- `10u` - 10-meter u-component of wind (m/s, eastward)
- `10v` - 10-meter v-component of wind (m/s, northward)  
- `2d` - 2-meter dewpoint temperature (Kelvin, converted to Celsius)

**Spatial Resolution:** 0.25° (~25 km) for all sources

#### Separate processing steps 

You can also run each step of the ERA5 pipeline independently:

```bash
- run_era5_realtime.sh
- run_era5_idw_pipeline.sh
```

---

### 5. Silver Dataset Generation

Combines all processed data sources into a unified ML-ready dataset with feature engineering.

#### Basic Usage

```bash
# Real-time mode (generates today's data with 7-day rolling features)
./scripts/make_silver.sh --mode realtime --countries THA LAO

# Historical mode (single day)
./scripts/make_silver.sh --mode historical --start-date 2024-06-01 --end-date 2024-06-01 --countries THA LAO

# Historical mode (date range)
./scripts/make_silver.sh --mode historical --start-date 2024-06-01 --end-date 2024-06-30 --countries THA LAO
```

#### What it does

1. **Creates H3 base grid** for specified countries (H3 resolution 8)
2. **Joins all data sources**: Himawari AOD, FIRMS fire, ERA5 weather, air quality, elevation, landcover, WorldPop
3. **Feature engineering**:
   - 3-day and 7-day rolling averages for all time-series features
   - Next-day PM2.5 targets for ML training
   - Parent H3-04 aggregate PM2.5 (coarser spatial context)
4. **7-day buffer**: Automatically collects 7 extra days before requested date for complete rolling calculations
5. **Saves daily files**: One parquet file per day

#### Data Sources Integrated

| Source | Columns | Type |
|--------|---------|------|
| **Himawari** | `aod_1day_interpolated` + rolling 3/7-day | Time-series |
| **FIRMS** | `fire_hotspot_strength` + rolling 3/7-day | Time-series |
| **ERA5** | `temperature_2m`, `wind_u_10m`, `wind_v_10m`, `dewpoint_2m` + rolling 3/7-day | Time-series |
| **Air Quality** | `pm25_value`, `pm25_source` | Current day |
| **Elevation** | `elevation` | Static |
| **Landcover** | `trees`, `grass`, `crops`, `built`, `flooded_vegetation`, `water`, `snow_ice`, `bare`, `shrubland` | Static |
| **WorldPop** | `worldpop_population` | Static |

#### Feature Engineering Details

**Rolling Features (all time-series variables):**
```
{variable}_roll3  # 3-day rolling average (mean of days t-2, t-1, t)
{variable}_roll7  # 7-day rolling average (mean of days t-6, t-5, ..., t)
```

Examples:
- `aod_1day_interpolated_roll3`, `aod_1day_interpolated_roll7`
- `fire_hotspot_strength_roll3`, `fire_hotspot_strength_roll7`
- `temperature_2m_roll3`, `temperature_2m_roll7`

**Target Variables for ML:**
- `pm25_value` - Next day PM2.5 (what we want to predict)
- `current_day_pm25` - Current day PM2.5 (for training validation)

**Spatial Context:**
- `parent_h3_04_pm25` - Average PM2.5 in parent H3-04 cell (current day)
- `yesterday_parent_h3_04_pm25` - Yesterday's parent H3-04 average

#### Large Date Ranges

For date ranges >90 days, the system automatically uses chunked processing (30-day chunks):

```bash
# Large historical range (auto-chunked)
./scripts/make_silver.sh --mode historical --start-date 2024-01-01 --end-date 2024-12-31 --countries THA LAO
# -> Automatically splits into 12 chunks, processes each separately
```

This prevents memory issues and allows for incremental progress.

#### Output Structure

```
data/silver/
├── realtime/
│   └── silver_realtime_LAO_THA_YYYYMMDD.parquet    # One file per day
└── historical/
    ├── silver_historical_LAO_THA_20240101.parquet   # One file per day
    ├── silver_historical_LAO_THA_20240102.parquet
    └── ...
```

#### Key Features

- **7-Day Buffer**: Ensures complete rolling averages from day 1
- **Daily Files**: Easy to process incrementally, no giant files
- **Smart Chunking**: Handles years of data without memory issues
- **Complete Feature Set**: 40+ features ready for ML training
- **Auto-skip**: Skips existing files to avoid reprocessing

---

### 6. Air for Tomorrow

Uses pre-trained XGBoost model to predict PM2.5 concentrations from silver datasets.

#### Basic Usage

```bash
# Real-time predictions (basic - no maps)
./scripts/predict_air_quality.sh --mode realtime --countries THA LAO

# Historical predictions with maps
./scripts/predict_air_quality.sh --mode historical --start-date 2024-06-01 --end-date 2024-06-03 --countries THA LAO --generate-map

# Historical with sensor validation and enhanced maps
./scripts/predict_air_quality.sh --mode historical --start-date 2024-06-01 --end-date 2024-06-03 --countries THA LAO --validate-sensors --enhanced-maps
```

#### What it does

1. Loads pre-trained XGBoost model (`src/models/xgboost_model.json`)
2. Loads daily silver dataset files for specified date range
3. Prepares 21 features (8 base + 1 spatial context + 12 rolling averages)
4. Generates PM2.5 predictions (log-transformed then exponentiated)
5. Saves daily prediction files (one parquet per day)
6. Optionally generates maps and validates against sensors

#### Model Features (21 total)

**Base Features (8):**
- `month` (1-12) - Seasonal patterns
- `worldpop_population` - Population density
- `aod_1day_interpolated` - Himawari AOD (satellite aerosol measurement)
- `fire_hotspot_strength` - FIRMS fire intensity
- `temperature_2m`, `wind_u_10m`, `wind_v_10m`, `dewpoint_2m` - ERA5 weather

**Spatial Context (1):**
- `yesterday_parent_h3_04_pm25` - Coarser spatial aggregate from previous day

**Rolling Features (12):**
- `aod_1day_interpolated_roll3`, `aod_1day_interpolated_roll7`
- `fire_hotspot_strength_roll3`, `fire_hotspot_strength_roll7`
- `temperature_2m_roll3`, `temperature_2m_roll7`
- `dewpoint_2m_roll3`, `dewpoint_2m_roll7`
- `wind_u_10m_roll3`, `wind_u_10m_roll7`
- `wind_v_10m_roll3`, `wind_v_10m_roll7`

#### Missing Value Handling

Missing values are handled as follows:
- **Fire features**: Fill with 0.0 (no fires)
- **Weather features**: Fill with column mean
- **Population**: Fill with 0.0
- **AOD**: Can handle missing values (model trained with missing AOD)
- **Rolling features**: Require 7-day buffer (automatically handled by silver dataset generation)

#### Output Files

- **Predictions**: `data/predictions/data/{mode}/aq_predictions_{YYYYMMDD}_{countries}.parquet` (one file per day)
- **Standard Maps**: `data/predictions/map/{mode}/aqi_map_{YYYYMMDD}_{countries}.png` (with `--generate-map`)
- **Enhanced Maps(with ground truth sensors)**: `data/predictions/validation_map/enhanced_aqi_map_{YYYYMMDD}_{countries}.png` (with `--enhanced-maps`)
- **Distribution Charts**: `data/predictions/distribution/pm25_distribution_{YYYYMMDD}_{countries}.png` (always generated)
- **Scatter Plots**: `data/predictions/scatter/validation_scatter_{YYYYMMDD}_{countries}.png` (with `--validate-sensors`)

#### Map Generation

**Map Resolution:**
```bash
# Generate maps with custom resolution (shell default: 6, python default: 8)
./scripts/predict_air_quality.sh --mode historical --start-date 2024-06-01 --end-date 2024-06-03 --generate-map --map-resolution 7
```

**Map Features:**
- **H3 Hexagonal Visualization**: Consistent spatial representation
- **WHO Air Quality Categories**: Color-coded PM2.5 levels
- **Multi-resolution Support**: Configurable H3 resolution (6-8 recommended)
- **Date-specific Maps**: Historical mode generates separate maps for each date

#### Sensor Validation - **only for historical mode**

Validate predictions against ground truth sensor measurements:

```bash
# Historical mode with complete validation
./scripts/predict_air_quality.sh --mode historical \
  --start-date 2024-06-01 --end-date 2024-06-30 \
  --countries THA LAO \
  --validate-sensors \
  --enhanced-maps \
  --save-validation
```

**Validation Features:**
- **Historical Validation**: Validates predictions against available sensor data for each date
- **Enhanced Maps**: Overlays predictions (hexagons) with sensor measurements (circles)
- **Validation Metrics**: MAE, RMSE, R², category agreement
- **Scatter Plots**: Predicted vs actual PM2.5 with regression line and metrics (one per date)
- **Matched Locations**: Sensors matched to nearest H3 cell

**Validation Outputs:**
- **Validation Data**: `data/predictions/validation_data/` - Matched sensor-prediction pairs
- **Scatter Plots**: `data/predictions/scatter/validation_scatter_{date}_{countries}.png`
- **Enhanced Maps**: `data/predictions/validation_map/enhanced_aqi_map_{date}_{countries}.png`

#### Key Features

- **Daily predictions**: One output file per day (not one file per range)
- **Distribution charts**: Always generated (even without maps)
- **Enhanced maps**: Overlays predictions (hexagons) with sensor measurements (circles)
- **Validation metrics**: MAE, RMSE, R², category agreement
- **Historical scatter plots**: Generated per date with available sensor data
- **Prediction clipping**: PM2.5 values clipped to 0-500 μg/m³ range

---

## Model Information

### XGBoost Prediction System

Air for Tomorrow uses an XGBoost regression model to predict PM2.5 concentrations.

#### Model Details

- **Algorithm**: XGBoost (Gradient Boosting)
- **Target**: PM2.5 concentration (μg/m³) - log-transformed during training
- **Features**: 21 features (8 base + 1 spatial context + 12 rolling averages)
- **Model File**: `src/models/xgboost_model.json`
- **Training Period**: 2021-12-31 to 2024-12-31
- **Prediction Output**: Exponentiated log predictions, clipped to 0-500 μg/m³

### Air Quality Categories

The system uses WHO air quality guidelines for map visualization:

| Category | PM2.5 Range (μg/m³) | Color | Description |
|----------|---------------------|-------|-------------|
| Good | 0 - 15 | 🟢 #a8e05f | Satisfactory air quality |
| Moderate | 15 - 35 | 🟡 #fdd64b | Acceptable for most people |
| Unhealthy for Sensitive Groups | 35 - 55 | 🟠 #ff9b57 | Sensitive individuals may experience problems |
| Unhealthy | 55 - 150 | 🔴 #fe6a69 | Everyone may experience health effects |
| Very Unhealthy | 150+ | 🟣 #a97abc | Health warnings for entire population |

---

## Docker Advanced

### System Dependencies

The Docker container includes all required system dependencies:
- **GDAL 3.x**: Geospatial data processing (raster and vector)
- **ecCodes**: Meteorological data handling (GRIB/NetCDF)
- **OpenMP**: Runtime library for XGBoost parallel processing
- **jq**: JSON processor for configuration parsing
- **yq**: YAML processor for reading configuration files
- **Python 3.10**: Runtime environment with all packages from requirements.txt

### Container Entrypoint

The container starts with an interactive menu showing available commands:
- Individual data source pipelines
- Data cleaning and silver dataset generation
- Air quality prediction with maps
- Complete end-to-end pipeline execution

The entrypoint script (`entrypoint.sh`) also:
- Sets up the environment
- Runs initial setup script (`scripts/setup.sh`)
- Validates required local bootstrap files (no runtime Azure bootstrap download)
- Displays usage instructions

### Volume Mounts

Critical volume mounts for data persistence:
- `./data:/app/data` - All processed data and outputs
- `./assets:/app/assets` - Static datasets and sensor lists
- `./config:/app/config` - Configuration files
- `./logs:/app/logs` - Application and processing logs

For development:
- `./src:/app/src` - Source code
- `./scripts:/app/scripts` - Shell scripts
- `./.env:/app/.env` - Environment variables

---

## Project Structure

Use the high-level project map in `README.md` for the current repository layout.

### Key Notes

- **Auto-generated directories**: Many directories under `data/` are created automatically on first run
- **Python cache**: `__pycache__/` directories are auto-generated and can be ignored
- **On-demand fetching**: DEM (Digital Elevation Model) and landcover data are fetched during processing
- **Automatic cleanup**: NetCDF files in `data/raw/himawari/` are auto-deleted after processing (unless `--keep-originals` is used)

### Docker Volume Mounts

For data persistence and development:

- `./data:/app/data` - All processed data and outputs
- `./assets:/app/assets` - Static datasets and sensor lists  
- `./config:/app/config` - Configuration files
- `./logs:/app/logs` - Application and processing logs
- `./src:/app/src` - Source code (for development)
- `./scripts:/app/scripts` - Shell scripts (for development)

---

## Configuration Reference

### Environment Variables

The system uses environment variables defined in `.env` file (copied from `env_template`):

**Required:**
- `HIMAWARI_FTP_USER` - Himawari FTP username
- `HIMAWARI_FTP_PASSWORD` - Himawari FTP password
- `CDSAPI_KEY` - Copernicus CDS API key (for historical ERA5)
- `CDSAPI_URL` - Copernicus CDS API URL
- `OPENAQ_API_KEY` - OpenAQ API key

**Optional:**
- `LOCAL_DEV` - Set to 1 for development mode and added logging

### Configuration File

Main configuration file: `config/config.yaml`

**Key sections:**
- `countries`: Default country codes
- `data_processing`:
  - `h3_resolution`: Default H3 resolution (8)
  - `temporal_window`: Time windows for processing
- `models`: Model paths and settings
- `paths`: Directory structure configuration
- `logging`: Logging configuration

### Command-Line Options

Each script supports common options:
- `--mode`: Processing mode (realtime/historical)
- `--countries`: Space-separated country codes
- `--start-date`: Start date (YYYY-MM-DD) for historical
- `--end-date`: End date (YYYY-MM-DD) for historical
- `--help`: Display usage information

---

**📖 For more information, see the main [README.md](README.md)**
