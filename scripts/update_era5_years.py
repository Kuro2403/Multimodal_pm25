import json
import os

folder = 'notebooks/01_Crawling_data'
target_files = ['era5_data_crawling.ipynb', 'era5_rh850_crawling.ipynb', 'era5_t_inversion_crawling.ipynb']
for f in target_files:
    path = os.path.join(folder, f)
    if not os.path.exists(path): continue
    with open(path, 'r', encoding='utf-8') as file:
        nb = json.load(file)
            
    modified = False
    for cell in nb.get('cells', []):
        if cell.get('cell_type') == 'code':
            full_text = ''.join(cell.get('source', []))
            if 'year_configs = [' in full_text and '"year": "2021"' not in full_text:
                new_text = full_text.replace('year_configs = [', 'year_configs = [\n    {"year": "2021", "months": [f"{m:02d}" for m in range(1, 13)]},\n    {"year": "2022", "months": [f"{m:02d}" for m in range(1, 13)]},\n    {"year": "2023", "months": [f"{m:02d}" for m in range(1, 13)]},')
                cell['source'] = [line + '\n' for line in new_text.split('\n')[:-1]]
                modified = True
                    
    if modified:
        with open(path, 'w', encoding='utf-8') as file:
            json.dump(nb, file, indent=1)
        print(f'Updated {f}')
