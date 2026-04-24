"""
Merge the transformed data containing Elevation and LandCover encodings
with the original daily CSV data and perform preliminary processing.
Relies on data from SituData.
"""

import argparse
import json
import os
import warnings

import pandas as pd
import torch

warnings.filterwarnings("ignore")

dataPath = './data/DeepAir/'

os.environ["CUDA_VISIBLE_DEVICES"] = "3"

use_cuda = torch.cuda.is_available()
DEVICE = torch.device('cuda' if use_cuda else 'cpu')   # 'cpu' in this case
print("Using device : ", DEVICE)

parser = argparse.ArgumentParser()

parser.add_argument('--save_dirs', type = str, default = None)
parser.add_argument('--CNNembed_dirs', type = str, default = None)
# log file name
parser.add_argument('--mergedSuffix', type=str, default=None)
parser.add_argument('--year', type = str, default = '2016')
parser.add_argument('--yearDay', type = str, default = '2016000')
parser.add_argument('--used_group', type = str, help="used test group model") 

args = parser.parse_args()

args.raw_filePath = dataPath + f"SituData/{args.year}_nb/gridsData_nb_{args.yearDay}_raw.csv"

config = json.load(open('config.json', 'r'))

def run():
    if args.mergedSuffix == None:
        args.mergedSuffix = args.save_dirs.split('.')[-1]

    transferDataPath = f"./{args.CNNembed_dirs}/universal_ElevaLandEmbed_G{config['model_group']}.csv"

    transfered_df = pd.read_csv(transferDataPath)
    transfered_df["mergeIndex"] = transfered_df["Lon"].apply(lambda x:"%.2f" %x).astype(str)+ \
                                "_" +transfered_df["Lat"].apply(lambda x:"%.2f" %x).astype(str)

    transfered_df_dropCols = ["Lon", "Lat"]
    transfered_df.drop(transfered_df_dropCols, axis=1, inplace=True)

    situData_path = args.raw_filePath
    situData_df = pd.read_csv(situData_path)
    situData_df['yearDay'] = situData_df['yearDay'].astype('str')
    situData_df["mergeIndex"] = situData_df["Lon"].apply(lambda x:"%.2f" %x).astype(str)+ \
                                "_" +situData_df["Lat"].apply(lambda x:"%.2f" %x).astype(str)
    situ_drop_cols = [f"LandCover_{str(i).zfill(2)}" for i in range(20)]
    situ_drop_cols.extend(["Elevation"])
    situData_df.drop(situ_drop_cols, axis=1, inplace=True)

    newData = pd.merge(situData_df, transfered_df, how='left', on = 'mergeIndex')
    # newData.drop(newData_dropCols,axis=1, inplace=True)
    newData.drop(["mergeIndex"], axis=1, inplace=True)
    newData['yearDay'] = newData['yearDay'].astype('int')
    # print(newData.dtypes)
    print("newData shape is ",newData.shape)
    newData.dropna(how="any", axis=0, inplace=True)

    merge_csv_saveDir = f"./CA_{args.save_dirs}/G{args.used_group}/{args.year}/{args.yearDay}/mergedData"
    if not os.path.exists(f"{merge_csv_saveDir}"):
        os.makedirs(f"{merge_csv_saveDir}")
    newData.to_csv(f"{merge_csv_saveDir}/merged_CA_data_{args.mergedSuffix}.csv", index=0)
    

if __name__ == '__main__':
    config['model_group'] = args.used_group
    print(f"\nmodel group is {config['model_group'] }")

    run()

