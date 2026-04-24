"""
File    :   daily_CA_PM25_GBMPred.py
Desc    :   This should be the code for grouped training of different GBM models and storing them.
            Directly encodes LandCover and Elevation data in the code and merges them with the original SituData.
            Loads the pre-trained model to make predictions and infer PM2.5 data for the entire California region.

"""

"""Train and test GBM model on Situ data"""
import argparse
import datetime
import gc
import os
import warnings

import lightgbm as lgb
import numpy as np
import pandas as pd
from GBM_situDataPreprocess import *
from sklearn.utils import shuffle

from ..utils import *

warnings.filterwarnings("ignore")

if os.path.exists("./data/DeepAir/"):
    dataPath = "./data/DeepAir/"

CA_boundary = [-124.48, -114.13, 32.53, 42.01]
CA_boundary_10km = [-124.5, -114.1, 32.5, 42.1]
gridWidth = 0.01

metFeatures = [
    "TEMP",
    "Humidity",
    "PRESS",
    "uWIND",
    "vWIND",
    "Evaporation",
    "Precipitation",
]

parser = argparse.ArgumentParser()
parser.add_argument("--data_dir", type=str, default="ElevaLand.preTrain.shapeNoDrop")
parser.add_argument("--suffix", type=str, help="dirs that save different kind data")
parser.add_argument("--testGroup", type=str)
parser.add_argument("--kind", type=str, default="raw", help="Use data prerocess or not")
parser.add_argument("--y_trans", type=str, default="raw", help="log trans to y or not")
parser.add_argument("--outlier_detection", default="False", action="store_true")
parser.add_argument("--year", type=str)
parser.add_argument(
    "--appendix", default=None, type=str, help="To distinguish different version Code"
)
parser.add_argument("--used_group", type=str)  # "MSE"
parser.add_argument("--CNNembed_dirs", type=str, default=None)


"""
python daily_CA_PM25_GBMPred.py --data_dir ElevaLand.preTrain.shapeNoDrop --kind preProcess --year 2016 --yearDay 2016362
"""
args = parser.parse_args()
temp_suffix = args.data_dir.split(".")[-1]


def log_trans(data):
    ret = np.log(data + 1)
    return ret


def unLog_trans(data):
    ret = np.exp(data) - 1
    return ret


def eval_on_batch_mape(pred, label):
    label_index = label > 0
    # loss = np.abs(pred - label) / label
    loss = np.abs(pred[label_index] - label[label_index]) / label[label_index]
    return np.mean(loss)


def eval_on_batch_mae(pred, label):
    loss = np.abs(pred - label)
    return np.mean(loss)


def eval_on_batch_rmse(pred, label):
    loss = np.square(pred - label)
    loss = np.mean(loss)
    return np.sqrt(loss)


from pyod.models.copod import COPOD


def removeOutlier(df, contamination=0.1):
    clf = COPOD(contamination=contamination)
    clf.fit(df)
    # get outlier scores
    y_scores = clf.decision_scores_  # raw outlier scores
    y_label = clf.labels_
    normal_index = np.argwhere(y_label == 0).reshape(-1)
    normal_df = df.loc[normal_index, :].reset_index(drop=True)
    return normal_df


def pred_CA_PM25(transfered_df, gbm, bar=10.0):
    """
    Merge pre-trained data with data for the specified date and perform predictions.
    Some data preprocessing operations are conducted before prediction.
    Predict data for the specific date and output the results (previously output as CSV files, later changed to ZIP files).
    """
    allData = getMergedData(transfered_df)
    print(f"pre data shape is {allData.shape}")
    allData = shuffle(allData)  # shuffledata
    allData.reset_index(drop=True, inplace=True)
    print(f"shuffled data shape is {allData.shape}")

    # allData = dropOutlierRecords(allData) # drop outlier observations
    # data preprocess
    if args.kind == "preProcess":
        allData = featureTransform(allData, args)
        allData = dropColumns(allData, args)
    print(f"preProessed data shape is {allData.shape}")

    X_drop_cols = ["PM2.5", "yearDay", "Lon", "Lat"]
    df_X = allData.drop(X_drop_cols, axis=1)
    print(f"train data shape is {df_X.shape}")

    all_y_pred = gbm.predict(df_X, num_iteration=gbm.best_iteration)
    all_y_pred = all_y_pred.astype("float")

    save_csv = allData[["yearDay", "month", "weekDay", "Lon", "Lat"]]
    assert save_csv.shape[0] == len(all_y_pred)
    save_csv.loc[:, "pred_PM2.5"] = all_y_pred
    save_csv.loc[:, "yearDay"] = save_csv["yearDay"].astype("int")
    save_csv.loc[:, "month"] = save_csv["month"].astype("int")
    save_csv.loc[:, "weekDay"] = save_csv["weekDay"].astype("int")

    print("Successfully predict CA data!")

    valued_grid_daily_df = pd.read_csv(args.valued_grid_path)
    valued_grid_daily_df = valued_grid_daily_df[
        ["yearDay", "month", "weekDay", "Lon", "Lat", "PM2.5"]
    ]
    valued_grid_daily_df.rename(columns={"PM2.5": "pred_PM2.5"}, inplace=True)
    CA_grid_PM = pd.concat([save_csv, valued_grid_daily_df], axis=0, ignore_index=True)
    print(f"CA_grid_PM shape is {CA_grid_PM.shape}")
    # CA_grid_PM.to_csv(f"{args.saveDir}/CA_allGrid_PM25_pred.csv", index=0)
    # save predict_PM2.5 zip file
    compression_opts = dict(method="zip", archive_name=f"CA_allGrid_PM25_pred.csv")
    CA_grid_PM.to_csv(
        f"{args.saveDir}/CA_allGrid_PM25_pred.zip",
        index=0,
        compression=compression_opts,
    )
    print(f"Successfully save all CA grid data of yearDay {args.yearDay}")

    del allData, CA_grid_PM
    gc.collect()


def getMergedData(transfered_df):
    """
    Merge the raw data with the embedding data for Elevation and LandCover generated by the pre-trained model.
    Obtain the model used for prediction.
    """
    print(f"Reading raw data from {args.raw_filePath}")
    situData_path = args.raw_filePath
    situData_df = pd.read_csv(situData_path)
    situData_df["yearDay"] = situData_df["yearDay"].astype("str")
    situData_df["mergeIndex"] = (
        situData_df["Lon"].apply(lambda x: "%.2f" % x).astype(str)
        + "_"
        + situData_df["Lat"].apply(lambda x: "%.2f" % x).astype(str)
    )
    situ_drop_cols = [f"LandCover_{str(i).zfill(2)}" for i in range(20)]
    situ_drop_cols.extend(["Elevation"])
    situData_df.drop(situ_drop_cols, axis=1, inplace=True)
    # merge data
    newData = pd.merge(situData_df, transfered_df, how="left", on="mergeIndex")
    newData.drop(["mergeIndex"], axis=1, inplace=True)
    newData["yearDay"] = newData["yearDay"].astype("int")
    print(f"### Merged newData shape is {newData.shape} ###")
    newData.dropna(how="any", axis=0, inplace=True)
    return newData


def main():
    transferDataPath = (
        f"./{args.CNNembed_dirs}/universal_ElevaLandEmbed_G{config['model_group']}.csv"
    )
    transfered_df = pd.read_csv(transferDataPath)
    transfered_df["mergeIndex"] = (
        transfered_df["Lon"].apply(lambda x: "%.2f" % x).astype(str)
        + "_"
        + transfered_df["Lat"].apply(lambda x: "%.2f" % x).astype(str)
    )
    transfered_df_dropCols = ["Lon", "Lat"]
    transfered_df.drop(transfered_df_dropCols, axis=1, inplace=True)

    gbmModelFile = config["GBM_model_load_path"]
    gbm = lgb.Booster(model_file=gbmModelFile)
    print("#######  GBM model loaded !  #######")

    # pred all data of years
    firstDayInYear = datetime.datetime.strptime(str(args.year) + "-01-01", "%Y-%m-%d")
    lastDayInYear = datetime.datetime.strptime(str(args.year) + "-12-31", "%Y-%m-%d")
    numDaysInYear = (lastDayInYear - firstDayInYear).days + 1

    # pred all data of year
    for d in range(0, numDaysInYear):
        yearDay = str(args.year) + str(d).zfill(3)
        args.yearDay = yearDay
        print(f"\nPredicting PM2.5 from yearDay {yearDay}...")
        args.valued_grid_path = (
            dataPath + f"SituData/{args.year}_nb/gridsData_nb_{args.yearDay}_valued.csv"
        )

        args.raw_filePath = (
            dataPath + f"SituData/{args.year}_nb/gridsData_nb_{args.yearDay}_raw.csv"
        )  # data read path
        args.saveDir = f"./CA_{args.data_dir}/G{args.used_group}/{args.year}/{args.yearDay}/GBMPredResults_{args.kind}"
        if args.appendix:
            args.saveDir = f"./CA_{args.data_dir}/G{args.used_group}/{args.year}/{args.yearDay}/GBMPredResults_{args.kind}_{args.appendix}"
        if not os.path.exists(args.saveDir):
            os.makedirs(args.saveDir)
        # pred PM2.5
        pred_CA_PM25(transfered_df, gbm)


if __name__ == "__main__":
    config["model_group"] = args.used_group
    config["GBM_model_load_path"] = (
        f"../DeepAir/Benchmark/GBM/preTrain_shapeNoDrop_allGroups_preProcess_0/{args.used_group}/model/GBM_model_G{args.used_group}.txt"
    )
    print(f"\nmodel group is {config['model_group'] }")
    print(f"GBM model load path is {config['GBM_model_load_path']} \n")
    main()


"""
python model_GBM_STData.py --suffix raw --appendix 0
"""
