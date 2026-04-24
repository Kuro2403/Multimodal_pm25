# External Datasets and Licenses

This is the canonical inventory for third-party datasets used by this repository.

Last verified: **March 6, 2026**.

Scope:
- Data collected at runtime (realtime and historical).
- External static assets used in feature engineering and prediction inputs.
- External boundary datasets used for spatial filtering/H3 coverage.

This document is descriptive and operational, not legal advice.

## Dataset Inventory

| Dataset/source name | How it is used in this project | Official source URL | License or governing terms | License/terms URL | Attribution/redistribution constraints for this repo |
|---|---|---|---|---|---|
| OpenAQ v3 API (realtime) | Ground PM2.5 sensor collection in realtime mode (`openaq_collector`) | [docs.openaq.org](https://docs.openaq.org/) | OpenAQ Terms of Use; per-record upstream licenses may vary | [OpenAQ Terms of Use](https://openaq.org/terms-of-use/) and [OpenAQ licenses resource](https://docs.openaq.org/resources/licenses) | Keep attribution and honor source-specific licenses provided in OpenAQ metadata. |
| OpenAQ AWS archive (historical) | Ground PM2.5 historical backfill from S3 (`openaq-data-archive`) | [registry.opendata.aws/openaq](https://registry.opendata.aws/openaq/) | Same governing terms as OpenAQ data platform; per-source licenses apply | [OpenAQ Terms of Use](https://openaq.org/terms-of-use/) and [OpenAQ licenses resource](https://docs.openaq.org/resources/licenses) | Do not assume a single uniform license for all records; preserve provider/license metadata when possible. |
| AirGradient Public API | Ground PM2.5 sensor collection in realtime/historical modes (`airgradient_collector`) | [api.airgradient.com/public/api/v1](https://api.airgradient.com/public/api/v1) | Publicly shared data is published under CC BY-SA 4.0 (per AirGradient policy) | [AirGradient data ownership policy](https://www.airgradient.com/data-ownership/) | Attribute AirGradient and comply with CC BY-SA 4.0 share-alike requirements for adaptations. |
| NASA FIRMS NRT (MODIS + VIIRS) | Realtime fire hotspot ingestion (past ~7-10 days) | [FIRMS API](https://firms.modaps.eosdis.nasa.gov/api/) | NASA Earth science open data policy + LANCE/FIRMS disclaimer/citation guidance | [NASA data information policy](https://www.earthdata.nasa.gov/engage/open-data-services-and-software/data-information-policy) and [FIRMS data page](https://www.earthdata.nasa.gov/data/near-real-time-data/firms/c6-mcd14dl) | NASA does not place restrictions on use, but proper acknowledgment/citation is expected and no-warranty disclaimers apply. |
| NASA FIRMS archives | Historical fire hotspot CSV archives manually downloaded or LFS-managed | [FIRMS archive download](https://firms.modaps.eosdis.nasa.gov/download/) | Same as above (NASA open data policy + LANCE/FIRMS guidance) | [NASA data information policy](https://www.earthdata.nasa.gov/engage/open-data-services-and-software/data-information-policy) and [FIRMS data page](https://www.earthdata.nasa.gov/data/near-real-time-data/firms/c6-mcd14dl) | Same attribution/disclaimer expectations as FIRMS NRT data. |
| Himawari-8 AOD (JAXA P-Tree) | Satellite AOD ingestion for realtime/historical pipelines (`himawari_aod.py`) | [ftp://ftp.ptree.jaxa.jp](ftp://ftp.ptree.jaxa.jp) and [P-Tree registration](https://www.eorc.jaxa.jp/ptree/registration_top.html) | JAXA P-Tree terms/usage rules | [JAXA P-Tree FAQ](https://www.eorc.jaxa.jp/ptree/faq.html) | Follow P-Tree attribution requirements. JAXA states a policy update effective **February 1, 2026** changed prior non-commercial/non-profit limits; verify current wording before redistribution. |
| ERA5 via ECMWF Open Data (today/live) | Realtime meteorological ingestion for day-0 forecasts | [ECMWF Open Data](https://www.ecmwf.int/en/forecasts/datasets/open-data) | ECMWF Open Data Terms of Use (CC BY 4.0) | [ECMWF Open Data terms](https://www.ecmwf.int/en/forecasts/datasets/open-data) | Attribute ECMWF as required; do not imply ECMWF endorsement. |
| ERA5 via ECMWF AWS mirror (recent past) | Realtime meteorological ingestion for recent days (AWS mirror fallback) | [registry.opendata.aws/ecmwf-forecasts](https://registry.opendata.aws/ecmwf-forecasts/) | Mirrors ECMWF forecast data under ECMWF Open Data terms | [ECMWF Open Data terms](https://www.ecmwf.int/en/forecasts/datasets/open-data) | Treat attribution obligations the same as ECMWF Open Data. |
| ERA5 reanalysis via Copernicus CDS (historical) | Historical meteorological ingestion via CDS API | [cds.climate.copernicus.eu](https://cds.climate.copernicus.eu/) | License to Use Copernicus Products (C3S/ECMWF) | [Copernicus licence PDF](https://cds.climate.copernicus.eu/api/v2/terms/static/licence-to-use-copernicus-products.pdf) | Follow C3S/ECMWF attribution terms when redistributing derived outputs. |
| WorldPop population (LAO/THA 2025 static assets) | Static population feature in silver dataset (`worldpop_population`) | [WorldPop Laos dataset](https://hub.worldpop.org/geodata/summary?id=74079) and [WorldPop Thailand dataset](https://hub.worldpop.org/geodata/summary?id=75663) | CC BY 4.0 (per WorldPop terms) | [WorldPop terms and conditions](https://hub.worldpop.org/terms-and-conditions) and [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) | Attribution and citation required for redistribution/derivative products. |
| geoBoundaries (gbOpen ADM0) | Country boundary polygons used for filtering and H3 boundary grids | [geoBoundaries](https://www.geoboundaries.org/) and [gbOpen API docs](https://www.geoboundaries.org/api.html) | gbOpen metadata indicates CC BY 4.0 licensing | [geoBoundaries API docs](https://www.geoboundaries.org/api.html) | Retain attribution and source metadata from geoBoundaries records. |
| Landcover static asset (`assets/landcover/LAO_THA_landcover.csv`) | Static landcover class fractions in silver dataset | [Dynamic World V1 (Earth Engine)](https://developers.google.com/earth-engine/datasets/catalog/GOOGLE_DYNAMICWORLD_V1) | CC BY 4.0, with required Dynamic World attribution text; includes modified Copernicus Sentinel data notice | [Dynamic World Terms of Use](https://developers.google.com/earth-engine/datasets/catalog/GOOGLE_DYNAMICWORLD_V1), [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/), and [Sentinel Data Legal Notice](https://sentinels.copernicus.eu) | Include required attribution text from Dynamic World and keep Sentinel legal notice obligations in downstream redistribution. |
| DEM static asset (`assets/dem/LAO_THA_elevation.csv`) | Static elevation feature in silver dataset (`elevation`) | [NASA SRTMGL1N.003](https://www.earthdata.nasa.gov/data/catalog/lpcloud-srtmgl1n-003) | EOSDIS Data Use and Citation Guidance (dataset is openly shared, without restriction) | [SRTMGL1N.003 dataset page](https://www.earthdata.nasa.gov/data/catalog/lpcloud-srtmgl1n-003) and [EOSDIS Data Use and Citation Guidance](https://www.earthdata.nasa.gov/engage/open-data-services-and-software/data-information-policy) | Redistribution is allowed; include NASA/LP DAAC citation and follow EOSDIS citation guidance. |

## Unresolved Items and Follow-up Actions

No unresolved license/provenance items are currently open in this inventory (as of **March 6, 2026**).

## Maintenance Rule

When any new external dataset is added (or an existing one is replaced), update this file in the same PR:
- Add source and terms URLs.
- Add attribution/redistribution notes.
- Mark unresolved items explicitly instead of guessing.
