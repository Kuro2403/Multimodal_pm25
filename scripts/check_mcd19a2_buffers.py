import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MODIS_DIR = PROJECT_ROOT / "data" / "interim" / "modis"

df1 = pd.read_csv(MODIS_DIR / "mcd19a2_daily_aod_buffer1km.csv")
df2 = pd.read_csv(MODIS_DIR / "mcd19a2_daily_aod_buffer3km.csv")

print("File buffer 1km:")
print(df1["mcd19a2_aod_055"].notna().sum())

print("File buffer 3km:")
print(df2["mcd19a2_aod_055"].notna().sum())
