import subprocess
import os

notebooks_to_run = [
    'era5_rh850_crawling.ipynb',
    'era5_t_inversion_crawling.ipynb',
    'sentinelhub_satellite_crawling.ipynb',
    'cams_aod_crawling.ipynb',
    'maiac_aod_crawling.ipynb',
    'hcho_crawling.ipynb',
    'gee_urban_fraction_worldcover.ipynb',
    'gee_ndvi_buffer.ipynb'
]

folder = 'notebooks/01_Crawling_data'

for nb in notebooks_to_run:
    path = os.path.join(folder, nb)
    print(f'========== RUNNING {nb} ==========')
    try:
        subprocess.run([
            '.venv/Scripts/jupyter.exe', 'nbconvert', '--to', 'notebook', '--execute', '--inplace', path
        ], check=True)
        print(f'SUCCESS: {nb}')
    except subprocess.CalledProcessError as e:
        print(f'FAILED: {nb} with exit code {e.returncode}')
