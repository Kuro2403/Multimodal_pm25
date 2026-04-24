import os
import warnings

warnings.filterwarnings("ignore")


test_group_list = [8, 5, 9, 0, 1, 2, 3, 4, 6, 7]

for used_group in test_group_list:
    cmd = f"python daily_dataPrepare_universalCNNembed.py --save_dirs preTrain.ElevaLand.embedResults --case_label ElevaLand --used_group {used_group}"
    print(f"\nRunning cmd is {cmd}\n")
    os.system(cmd)
