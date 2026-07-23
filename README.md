# Multimodal PM2.5

Project structure for PM2.5 multimodal experiments using ground observations and satellite features.

## Folder structure

```text
.
├── data/
│   ├── raw/
│   │   ├── DataSample/          # Original OpenAQ / HealthyAir source files
│   │   └── stations/            # Station metadata
│   ├── interim/
│   │   └── modis/               # Intermediate MODIS MCD19A2 AOD extracts
│   └── processed/
│       ├── modis/               # Feature-engineered MODIS tables
│       └── s5p/                 # Sentinel-5P joined and diagnostic tables
├── docs/                        # Notes and text references
├── models/                      # Trained model artifacts
├── notebooks/                   # Exploratory and training notebooks
├── outputs/
│   └── figures/                 # Generated plots/images
├── references/
│   └── external_code/           # Third-party/example repositories
├── scripts/                     # Small runnable checks/utilities
├── src/                         # Place for reusable project modules
└── requirements.txt
```

Pipeline data science:

data/raw
    ↓
Data Cleaning
    ↓
data/interim
    ↓
Feature Engineering
    ↓
data/processed
    ↓
Model Training

Pipeline Training AI:

Raw Data
    ↓
Preprocessing
    ↓
EDA
    ↓
Feature Engineering
    ↓
Feature Selection
    ↓
Train/Val/Test Split
    ↓
Baseline Model
    ↓
Model Development
    ↓
Hyperparameter Tuning
    ↓
Evaluation
    ↓
Error Analysis
    ↓
Conclusion
## Notes

- Run notebooks from the project root so relative paths such as `data/raw/...` resolve correctly.
- `notebooks/extract_mcd19a2_aod.ipynb` was renamed from `extract_mcd19a2_aod.py` because the file content is notebook JSON.
- Use `data/raw` only for original inputs. Put temporary/generated extraction outputs in `data/interim`, and final modeling tables in `data/processed`.

## Spatio-Temporal Methodology

This project utilizes an advanced **Feature Engineering** approach to achieve high PM2.5 estimation accuracy ($R^2 > 0.8$), prioritizing physical interactions and geospatial context over simple gap-filling and smoothing techniques.

**Core Techniques:**
1. **Multimodal Data Integration**: Fusing ground station PM2.5 with Satellite data (Aerosol Optical Depth - AOD, TROPOMI NO2/CO/SO2), ECMWF Meteorology, and Wildfire Radiative Power (FRP).
2. **Physical Interactions (Ventilation Coefficient)**: Capturing dispersion mechanics using Boundary Layer Height (BLH) and Wind Speed.
3. **Spatio-Temporal Context**: Using Spatial KNN Means, temporal moving averages (Lags), and Cyclic Time encoding to capture pollution transport and seasonal dynamics.
