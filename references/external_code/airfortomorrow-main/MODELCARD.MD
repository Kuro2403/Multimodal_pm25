# Model Card for Air Quality PM2.5 Prediction Model (Laos and Thailand)

A machine learning model (XGBoost regressor) to nearcast fine particulate matter (PM2.5) across Laos and Thailand for the next 24 hours using near-realtime satellite and auxiliary data sources.

This model integrates multi-source data including:

- Near-realtime satellite-derived aerosol optical depth (AOD) from Himawari-8
- Near-realtime fire hotspot intensity from NASA FIRMS
- Realtime meteorological data (temperature, dewpoint, wind speed and direction) from ERA5
- Low-cost and reference-grade ground-based PM2.5 sensors (AirGradient, OpenAQ)

The model is designed for operational use, producing daily PM2.5 nearcasts at approximately 0.74 km² resolution (H3 hexagonal grid) across Thailand and Lao PDR. It is intended to support air-quality estimation in areas with few or no ground monitoring stations, enabling governments, UNICEF country offices, and partners to anticipate pollution events and inform preventative public-health measures.

---

## Model Details

### Model Description

The Air Quality PM2.5 Prediction Model is an extreme gradient-boosted decision tree regression model (XGBoost) that forms the core of an end-to-end air-quality estimation pipeline. The model predicts daily average surface-level PM2.5 concentrations (µg/m³) using features derived from satellite remote sensing, meteorology, fire activity, population density, and temporal/spatial context features.

Predictions are produced on an H3 resolution-8 grid (approximately 0.74 km² hexagons) covering all of Lao PDR and Thailand, with the design extensible to neighboring Southeast Asian countries given similar data availability.

Key characteristics include:

- **Data fusion:** Integrates satellite AOD, fire detections, meteorological variables, population density, and engineered lag/rolling context features into a unified daily representation per grid cell.
- **Near-real-time operation:** Automatically ingests data from the previous 24 hours and generates daily PM2.5 estimates for the subsequent 24-hour period.
- **Historical analysis:** Can be run retrospectively on archived datasets to analyze past air-quality episodes.
- **High resolution and full coverage:** Produces wall-to-wall estimates across Laos and Thailand, including areas without sensors.
- **Pre-trained regional model:** Delivered as a pre-trained XGBoost model with no additional fine-tuning required for Laos and Thailand.

**Developed by:** UNICEF East Asia & Pacific Region Frontier Data Lab and UNICEF Lao PDR Country Office, in collaboration with Lao PDR government partners (Ministry of Natural Resources and Environment; Ministry of Education and Sports)

**Funded by:** UNICEF Office of Innovation, with support from Arm Ltd.

**Shared by:** UNICEF as open-source software for public benefit and local adaptation

**Model type:** Extreme gradient-boosted decision tree regression (XGBoost)

**License:** GPL-3.0

## Uses

This model is intended to support environmental monitoring and public health decision-making in regions with limited air quality monitoring infrastructure. It provides real-time PM2.5 estimates for all populated areas of Laos and Thailand by leveraging satellite and meteorological data. Intended users include government agencies, health organizations, NGOs, and researchers. The model's predictions can inform actions such as issuing health advisories, closing schools during severe pollution episodes, or guiding resource allocation for air pollution mitigation.

### Direct Use

Direct use involves running the model within the provided pipeline to generate air quality estimates and maps. A user can input the latest 24 hours of data (from satellites and sensors) and get an air quality map of current conditions, or run the pipeline on historical data to analyze pollution trends. Out of the box, no additional training is required; the model can be used as provided for Laos and Thailand once the necessary inputs are supplied.

Typical direct use cases:

- Daily air quality forecasting: Running the pipeline every day (via a scheduled job) to produce a current PM2.5 map for Laos and Thailand. The outputs can be integrated into public dashboards or early warning systems.
- Historical analysis: Researchers can back-calculate air quality for specific past events or periods by feeding in archived data, helping understand pollution patterns where measurements were not available.
- Educational tools: NGOs and educators can use the model outputs to raise awareness by visualizing pollution in areas where children live, highlighting the impact of fires or weather on air quality.

### Downstream Use

Downstream use includes fine-tuning or extending the model to new regions or incorporating the model's output into larger systems:

- Transfer to other regions: The methodology can be adapted to neighboring countries or regions with similar climates but requires retraining or calibration with local data.
- Integrating with health models: Predicted PM2.5 levels can serve as inputs to epidemiological models or decision-support tools that estimate health risks given pollution levels.
- Improving coverage of government networks: Authorities can use model estimates to augment sparse sensor networks, combining outputs with interpolation or data assimilation systems as part of broader air quality monitoring platforms.

### Out-of-Scope Use

This model should not be used for purposes or settings for which it was not designed:

- Outside Southeast Asia: The model is trained specifically on Laos and Thailand data and is likely not suitable for other regions without retraining.
- Other pollutants or indoor air: The model only predicts outdoor PM2.5 concentrations and does not estimate other pollutants or indoor air quality.
- Long-term forecasting: The model provides daily nearcasts and short-range forecasts; it is not a time-series forecaster for weeks or months ahead.
- Regulatory enforcement: Estimates are not legally authoritative and should not be used as the sole basis for regulatory compliance or punitive measures.
- Medical or individual advice: Predictions are community-level estimates and should not drive individual medical decisions without supporting local data or official advisories.

## Bias, Risks, and Limitations

Like any data-driven model, this system has biases and limitations, both technical and sociotechnical:

- Data bias (spatial): Training data from OpenAQ and AirGradient sensors is unevenly distributed, so accuracy may be lower in sensor-sparse regions.
- Data bias (temporal): The training period may not cover all seasonal or unusual events, leading to underestimation of extreme pollution episodes.
- Satellite limitations: AOD data is missing under cloud cover and represents column averages, which may not perfectly reflect ground-level PM2.5.
- Weather and seasonal effects: Relationships between weather and PM2.5 may shift under climate variability, which could reduce accuracy if conditions deviate from the training data.
- Fire detection limitations: NASA FIRMS hotspot data can miss or smooth very localized fires, introducing uncertainty into the fire features.
- Model uncertainty: The model does not provide prediction intervals; outputs should not be interpreted as exact values.
- Potential for misuse: Users could misinterpret or overstate accuracy, especially in areas without validation sensors or if inputs are manipulated.

In summary, the model's predictions are approximations that complement but do not replace ground truth data. Awareness of these limitations is crucial when making decisions.

### Recommendations

To mitigate the above biases and risks, we recommend:

- Use with ground truth when possible, comparing predictions against sensor readings and investigating large discrepancies.
- Communicate uncertainty to stakeholders, preferring qualitative categories or confidence ranges over exact values.
- Retrain regularly as new data becomes available to capture emerging patterns or extreme events.
- Extend to new domains cautiously, retraining or calibrating with local data before deployment elsewhere.
- Monitor bias continuously and apply corrections or add features if systematic errors appear in specific regions.
- Maintain transparency by documenting updates, known issues, and data sources in future versions of the model card or repository.

Users should be fully aware of the model's scope, limitations, and assumptions. Following these recommendations helps manage risk while leveraging the model effectively.

## How to Get Started with the Model

For operational use, run the repository pipeline (`./scripts/run_complete_pipeline.sh`) so feature engineering and schema alignment are handled automatically.

If you need direct programmatic inference, the deployed model expects **21 features** in a strict schema. A minimal example:

```python
import numpy as np
import pandas as pd
import xgboost as xgb

feature_names = [
    "month",
    "worldpop_population",
    "aod_1day_interpolated",
    "fire_hotspot_strength",
    "temperature_2m",
    "wind_u_10m",
    "wind_v_10m",
    "dewpoint_2m",
    "yesterday_parent_h3_04_pm25",
    "aod_1day_interpolated_roll3",
    "aod_1day_interpolated_roll7",
    "fire_hotspot_strength_roll3",
    "fire_hotspot_strength_roll7",
    "temperature_2m_roll3",
    "temperature_2m_roll7",
    "dewpoint_2m_roll3",
    "dewpoint_2m_roll7",
    "wind_u_10m_roll3",
    "wind_u_10m_roll7",
    "wind_v_10m_roll3",
    "wind_v_10m_roll7",
]

row = {name: 0.0 for name in feature_names}
row["month"] = 3
row["worldpop_population"] = 500.0
row["temperature_2m"] = 29.0

X = pd.DataFrame([row], columns=feature_names)

model = xgb.XGBRegressor()
model.load_model("src/models/xgboost_model.json")

# The model output is in log1p(PM2.5) space.
pred_log = model.predict(X)[0]
pred_pm25 = float(np.clip(np.expm1(pred_log), 0, 500))
print(f"Predicted PM2.5 (µg/m³): {pred_pm25:.2f}")
```

For custom integrations, replicate repository feature engineering exactly (especially lag/rolling features and date logic) via `src/make_silver.py` and `src/predict_air_quality.py`.

## Training Details

### Training Data

The model was trained on a combined dataset that merges all relevant data sources for Laos and Thailand over a multi-year historical period. Each training example corresponds to a specific location (H3 cell at resolution 8) and date with the following inputs and target:

The canonical inventory for external datasets, source links, and licensing terms is maintained in [EXTERNAL_DATASETS_AND_LICENSES.md](EXTERNAL_DATASETS_AND_LICENSES.md).

- Target variable: Daily average PM2.5 concentration (µg/m³) from ground monitors (OpenAQ and AirGradient).
- Satellite AOD (Himawari-8): Hourly daylight aerosol optical depth aggregated to daily means per H3 cell, with interpolation for cloud-related gaps.
- Fire hotspot density (NASA FIRMS): Daily fire detections converted into `fire_hotspot_strength`.
- Meteorological data (ERA5 reanalysis): Daily 2m air temperature, 2m dewpoint, and 10m wind components interpolated to the H3 grid.
- Static demographic context: WorldPop population (`worldpop_population`).
- Temporal and contextual features: `month`, previous-day parent-cell PM2.5 context, and rolling 3-day/7-day features for AOD, fire, and weather variables.

### Deployed Feature Schema (Exact)

The deployed model file (`src/models/xgboost_model.json`) expects the following 21 numeric features:

1. `month`
2. `worldpop_population`
3. `aod_1day_interpolated`
4. `fire_hotspot_strength`
5. `temperature_2m`
6. `wind_u_10m`
7. `wind_v_10m`
8. `dewpoint_2m`
9. `yesterday_parent_h3_04_pm25`
10. `aod_1day_interpolated_roll3`
11. `aod_1day_interpolated_roll7`
12. `fire_hotspot_strength_roll3`
13. `fire_hotspot_strength_roll7`
14. `temperature_2m_roll3`
15. `temperature_2m_roll7`
16. `dewpoint_2m_roll3`
17. `dewpoint_2m_roll7`
18. `wind_u_10m_roll3`
19. `wind_u_10m_roll7`
20. `wind_v_10m_roll3`
21. `wind_v_10m_roll7`

### Training Procedure

Baseline training followed a date-based split:

- Train window: 2021-12-31 to 2024-12-31 (`date < 2025-01-01`)
- Validation window: 2025-01-01 to 2025-10-30 (`date >= 2025-01-01`)
- Validation filtering: `pm25_value < 250` (to reduce sensor/noise extremes in this baseline evaluation setup)

Model setup used in the model exploration notebook:

- Algorithm: `XGBRegressor(random_state=42, objective='reg:squarederror')`
- Target transform: `log1p(pm25_value)` for training; inverse via `expm1` at prediction time
- Sample weighting: Higher training weights for high-pollution samples by US EPA PM2.5 bands
- Numeric precision: Float32 matrices for model fitting

### Preprocessing

Preprocessing steps included:

- Fill missing AOD data using inverse distance weighting
- Upsample ERA5 data to H3 resolution 8
- Generate 3-day and 7-day rolling averages for AOD, fire, and ERA5 variables
- Aggregate/interpolate all sources to daily resolution on the H3 grid
- Fill `fire_hotspot_strength` nulls as 0.0 (interpreted as no detected nearby fires)
- Build the lagged contextual feature `yesterday_parent_h3_04_pm25`

### Speeds, Sizes, Times

- Model artifact size: a few MB (`src/models/xgboost_model.json`)
- Inference throughput: model scoring is fast (typically not the main pipeline bottleneck)
- End-to-end runtime: data collection and preprocessing dominate daily pipeline execution time

## Evaluation

Evaluation values below are from the model exploration workflow in `notebooks/air_quality_pipeline_walkthrough.ipynb`, re-run on **March 6, 2026**, using `data/training/silver_dataset.parquet` and the date split above.

### Testing Data, Factors, and Metrics

#### Testing Data

The hold-out set consisted of daily PM2.5 rows from 2025-01-01 through 2025-10-30 (93,572 validation rows after the `<250 µg/m³` filter), spanning Lao PDR and Thailand across multiple seasons and concentration ranges.

#### Factors

Performance diagnostics considered:

- Geographic variation by country
- Temporal variation by meteorological season (DJF, MAM, JJA, SON)
- Concentration-range variation (US EPA-style PM2.5 bins)
- Category agreement under US EPA PM2.5 banding

#### Metrics

The main evaluation metrics were:

- Mean absolute error (MAE, µg/m³)
- Root mean squared error (RMSE, µg/m³)
- R²
- Pearson correlation
- Mean bias (predicted minus observed, µg/m³)
- Category agreement using US EPA PM2.5 bands

### Results

#### Overall Hold-Out Performance

| Segment | n | MAE (µg/m³) | RMSE (µg/m³) | R² | Correlation | Mean Bias (µg/m³) |
|---|---:|---:|---:|---:|---:|---:|
| All validation rows | 93,572 | 9.59 | 16.16 | 0.638 | 0.805 | -0.10 |

#### By Country

| Country | n | MAE (µg/m³) | RMSE (µg/m³) | R² | Correlation | Mean Bias (µg/m³) |
|---|---:|---:|---:|---:|---:|---:|
| LAO | 22,987 | 13.06 | 21.37 | 0.586 | 0.773 | -1.86 |
| THA | 70,585 | 8.46 | 14.06 | 0.669 | 0.824 | 0.47 |

#### By Pollution Bin (US EPA PM2.5 Ranges)

| Bin | n | MAE (µg/m³) | RMSE (µg/m³) | Mean Bias (µg/m³) |
|---|---:|---:|---:|---:|
| Good (<=12) | 33,105 | 5.96 | 9.00 | 5.37 |
| Moderate (12.1-35.4) | 37,611 | 7.24 | 10.32 | -0.10 |
| USG (35.5-55.4) | 13,765 | 12.08 | 16.60 | -3.09 |
| Unhealthy (55.5-150.4) | 8,444 | 25.71 | 33.71 | -12.15 |
| Very Unhealthy (>150.4) | 647 | 68.61 | 82.18 | -59.53 |

#### Category Agreement (US EPA)

| True Category | n | Correct (%) | One-Bucket Error (%) | >1 Bucket Error (%) |
|---|---:|---:|---:|---:|
| Good | 33,105 | 62.18 | 36.67 | 1.14 |
| Moderate | 37,611 | 71.93 | 24.42 | 3.65 |
| Unhealthy for Sensitive Groups | 13,765 | 30.08 | 66.69 | 3.23 |
| Unhealthy | 8,444 | 64.72 | 26.31 | 8.96 |
| Very Unhealthy | 645 | 63.41 | 31.94 | 4.65 |

#### Summary

- Strengths: Good overall tracking of observed variability (correlation 0.805), stronger performance in Thailand, and usable category agreement in lower-to-mid concentration bands.
- Weaknesses: Systematic underprediction in higher PM2.5 ranges and weaker performance in sensor-sparser contexts.
- Practical implication: The model is useful for regional situational awareness and screening, but should be paired with local measurements for high-stakes decisions.

## Model Examination

Model examination and SHAP analysis in the exploration workflow showed patterns consistent with domain expectations:

- AOD (`aod_1day_interpolated`), fire intensity (`fire_hotspot_strength`), and meteorology are major contributors.
- Contextual features (`yesterday_parent_h3_04_pm25` and rolling means) materially improve temporal and spatial stability.
- Month contributes seasonal baseline information.
- WorldPop population helps capture structural spatial differences.

Interpretability methods such as SHAP may be useful in future work, particularly if the approach is extended to additional regions.

## Technical Specifications

### Model Architecture and Objective

- Architecture: Gradient boosted decision trees (XGBoost).
- Input format: Numeric vector of 21 engineered features (listed above).
- Output format: Continuous daily PM2.5 concentration (µg/m³) per location.
- Objective: `reg:squarederror`.
- Implementation: Classical ML approach without neural networks, optimized for tabular geospatial data.

### Inference Post-Processing (Operational)

- The model predicts in `log1p(PM2.5)` space.
- Inference converts outputs with `expm1`.
- Predictions are clipped to a bounded operational range of 0 to 500 µg/m³.

### Compute Infrastructure

During training and inference:

- Dockerized environment for dependency consistency.
- CPU-based execution (no GPU required for baseline workflow).
- Repository environment currently pins Python 3.10 and XGBoost 3.0.4.

Recommended for production inference:

- CPU with at least 4 cores and 16 GB RAM to accommodate data downloads and preprocessing.
- Approximately 20 GB storage for intermediate satellite/weather caches.
- Internet connectivity for external data retrieval (satellite feeds, weather services, sensor networks).

#### Software

- Core libraries: XGBoost, NumPy, Polars, h3-py, GDAL, rasterio, SciPy.
- Environment management: Docker setup with YAML-driven configuration via `config/config.yaml`.
- Cross-platform note: The maintained runtime environment is Linux via Docker. macOS and Windows may work if dependency versions are matched.

## Citation

If you use this model or pipeline, please cite:

**BibTeX:**

```
@software{UNICEF_AirQuality_Laos_Thailand_2025,
  author = {UNICEF East Asia-Pacific Frontier Data Lab and UNICEF Lao PDR Office and collaborators},
  title = {Air for Tomorrow for Laos and Thailand (PM2.5 Machine Learning Model)},
  year = {2025},
  howpublished = {\url{https://github.com/unicef/airfortomorrow}},
  note = {Predictive model for fine particulate matter (PM2.5) using multisource data, implemented in XGBoost}
}
```

**APA:**

UNICEF East Asia and Pacific Frontier Data Lab, & UNICEF Lao PDR. (2025). *Air for Tomorrow for Laos and Thailand: PM2.5 Machine Learning Model* [Computer software]. https://github.com/unicef/airfortomorrow

(No academic paper is available yet; these citations reference the project and code repository.)

## Glossary

- PM2.5: Particulate matter with diameter less than or equal to 2.5 micrometers, measured in µg/m³.
- AOD (Aerosol Optical Depth): A dimensionless measure of how much light airborne particles block through the atmosphere; correlated with surface PM2.5.
- H3 grid: Hexagonal geospatial indexing system developed by Uber; each resolution-8 cell covers roughly 0.74 km².
- Inverse distance weighting (IDW): Interpolation method weighting nearby observations inversely to their distance; used to fill satellite and weather data gaps.
- Kernel density estimation (KDE): Technique to convert discrete events (for example, fire detections) into continuous density surfaces.
- OpenAQ: Open data platform aggregating government and research air quality measurements worldwide.
- AirGradient: Provider of low-cost air quality sensors and associated community network.
- ERA5: ECMWF climate reanalysis dataset providing hourly meteorological variables.
- XGBoost: Gradient boosting library for decision trees, widely used for tabular data.

## More Information

For additional context and resources:

- Project repository (GitHub): `unicef/airfortomorrow`, containing code, documentation, and example outputs.
- UNICEF article: "Protecting Children from Air Pollution: Open-Source Air Quality Machine Learning in SE Asia" (2025).
- UNICEF press release (February 2025): "The silent killer: over 100 daily deaths of children under five linked to air pollution in East Asia and the Pacific."
- Related projects: UNICEF Innovation Fund initiatives such as AQAI by Formative (India).
- WHO Air Quality Guidelines (2021 update) for PM2.5 breakpoints used in output categorization.

Future updates (for example, new data sources or architecture changes) will be documented in the repository and reflected in this model card.

## Model Card Authors

- Anthony Mockler, Data Scientist, UNICEF East Asia & Pacific Frontier Data Lab
- Hugo Ruiz Verastegui, UNICEF East Asia & Pacific Frontier Data Lab

This model card was prepared by the UNICEF East Asia-Pacific Frontier Data Lab team with contributions from the UNICEF Lao PDR office and partners.

## Model Card Contact

- Anthony Mockler, Data Scientist, UNICEF East Asia & Pacific Frontier Data Lab: amockler@unicef.org

For questions or feedback, contact the project maintainers via GitHub issues.
