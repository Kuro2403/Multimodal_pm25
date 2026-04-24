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

## Notes

- Run notebooks from the project root so relative paths such as `data/raw/...` resolve correctly.
- `notebooks/extract_mcd19a2_aod.ipynb` was renamed from `extract_mcd19a2_aod.py` because the file content is notebook JSON.
- Use `data/raw` only for original inputs. Put temporary/generated extraction outputs in `data/interim`, and final modeling tables in `data/processed`.
