"""
The code for the RF model in the baseline, including the data preprocessing part.
"""

import argparse
import datetime
import os
import random

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import r2_score

from baseline_models.situDataPreprocess import *

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
parser.add_argument("--suffix", type=str, default="raw")
parser.add_argument("--kind", type=str, default="raw", help="Use data prerocess or not")
parser.add_argument("--y_trans", type=str, default="raw", help="log trans to y or not")
parser.add_argument("--outlier_detection", default=False, action="store_true")
parser.add_argument(
    "--appendix", default=None, type=str, help="To distinguish different version Code"
)
parser.add_argument(
    "--situ_appendix", type=str, default=None, help="select different situData"
)

"""
python model_RF_STData.py --suffix baseline_raw --kind raw --y_trans raw
python model_RF_STData.py --suffix baseline_preProcess --kind preProcess --y_trans raw
"""

args = parser.parse_args()


def eval_on_batch_mape(pred, label):
    loss = np.abs(pred - label) / label
    return np.mean(loss)


def eval_on_batch_mae(pred, label):
    loss = np.abs(pred - label)
    return np.mean(loss)


def eval_on_batch_rmse(pred, label):
    loss = np.square(pred - label)
    loss = np.mean(loss)
    return np.sqrt(loss)


def trainRF(testGroup, df_train, df_test, saveFilePath, bar=10.0):
    print(f"\n\n############ Running in testGroup {testGroup}: ##############")

    test_monitors = df_test

    y_train = df_train["PM2.5"]
    y_test = df_test["PM2.5"]
    X_train = df_train.drop(["PM2.5"], axis=1)
    X_test = df_test.drop(["PM2.5"], axis=1)

    y_train_large_index = y_train >= bar
    y_test_large_index = y_test >= bar

    # build random forest model
    regr = RandomForestRegressor(max_depth=256, random_state=0, n_estimators=256)

    print("Starting training...")
    # train
    regr.fit(X_train, y_train)

    print("Saving model...")
    # save model to file
    curTime = "%s" % datetime.datetime.now()

    print("Starting predicting on training data...")
    # predict
    y_pred = regr.predict(X_train)
    # eval
    train_MAE = "%.4f" % eval_on_batch_mae(y_pred, y_train)
    train_RMSE = "%.4f" % eval_on_batch_rmse(y_pred, y_train)
    train_R2 = "%.4f" % r2_score(y_train, y_pred)
    train_MAPE = "%.4f" % eval_on_batch_mape(y_pred, y_train)
    train_large_MAPE = "%.4f" % eval_on_batch_mape(
        y_pred[y_train_large_index], y_train[y_train_large_index]
    )
    print("Train: The MAE of prediction is:", train_MAE)
    print("Train: The RMSE of prediction is:", train_RMSE)
    print("Train: The R2 of prediction is:", train_R2)
    print("Train: The MAPE of prediction is:", train_MAPE)
    print("Train: The Large MAPE of prediction is:", train_large_MAPE)

    print("Starting predicting...")
    # predict
    y_pred = regr.predict(X_test)
    # eval
    test_MAE = "%.4f" % eval_on_batch_mae(y_pred, y_test)
    test_RMSE = "%.4f" % eval_on_batch_rmse(y_pred, y_test)
    test_R2 = "%.4f" % r2_score(y_test, y_pred)
    test_MAPE = "%.4f" % eval_on_batch_mape(y_pred, y_test)
    test_large_MAPE = "%.4f" % eval_on_batch_mape(
        y_pred[y_test_large_index], y_test[y_test_large_index]
    )
    print("Test: The MAE of prediction is:", test_MAE)
    print("Test: The rmse of prediction is:", test_RMSE)
    print("Test: The r2 of prediction is:", test_R2)
    print("Test: The mape of prediction is:", test_MAPE)
    print("Test: The Large Mape of prediction is:", test_large_MAPE)
    # print(allData.columns)

    # draw scatter plot to show R2
    test_monitors.loc[:, "pred_PM2.5"] = y_pred
    test_monitors.loc[:, "monitors"] = (
        test_monitors["Lon"].apply(lambda x: "%.2f" % x).astype("str")
        + "_"
        + test_monitors["Lat"].apply(lambda x: "%.2f" % x).astype("str")
    )
    test_monitors.loc[:, "recordID"] = (
        test_monitors["yearDay"].astype("str") + "_" + test_monitors["monitors"]
    )
    plotR2Scatters(test_monitors, testGroup=testGroup)

    # save predictions
    if not os.path.exists(f"./RF/{args.suffix}/pred_results/"):
        os.makedirs(f"./RF/{args.suffix}/pred_results/")
    test_monitors.to_csv(
        f"./RF/{args.suffix}/pred_results/prediction_G{testGroup}.csv", index=0
    )

    result_list = [
        train_R2,
        train_MAE,
        train_RMSE,
        train_MAPE,
        train_large_MAPE,
        test_R2,
        test_MAE,
        test_RMSE,
        test_MAPE,
        test_large_MAPE,
    ]
    return result_list, y_test, y_pred


def trainRF_CV(year_list=["years"], bar=10.0):
    # create dirs to save model and visualzed plots
    if not os.path.exists(f"./RF/{args.suffix}"):
        os.makedirs(f"./RF/{args.suffix}")

    numGroups = 10  # group to test
    # numGroups = 1
    allData = []
    for year in year_list:
        print(f"Reading data from year {year}")
        if args.situ_appendix:
            df_path = dataPath + f"STData/situData_{year}_{args.situ_appendix}.csv"
        else:
            df_path = dataPath + f"STData/situData_{year}.csv"
        print(f"Reading data from {df_path}")
        situData = open(df_path, "r")
        header = situData.readline().rstrip()
        header = header.split(",")
        for row in situData:
            row = row.rstrip().split(",")
            row = [float(r) for r in row]
            allData.append(row)
        random.shuffle(allData)

    allData = pd.DataFrame(allData, columns=header)
    print(f"\noriginal data shape is {allData.shape}")
    allData = dropOutlierRecords(allData)  # drop outlier observations

    # data preprocess
    if args.kind == "preProcess":
        allData = featureTransform(allData, args)
        allData = dropColumns(allData, args)

    try:
        print(f"\n### Try to delete nb information ###\n")
        nb_cols = (
            [f"nb_{i}_PM2.5" for i in range(5)]
            + [f"nb_{i}_Lon" for i in range(5)]
            + [f"nb_{i}_Lat" for i in range(5)]
            + [f"nb_{i}_Dist" for i in range(5)]
        )
        allData.drop(nb_cols, axis=1, inplace=True)
    except:
        print(f"Don't delete RAW nb info in main function")

    try:
        nb_cols = ["nb_Fire"] + [f"nb_{i}_PM2.5/Dis" for i in range(5)]
        allData.drop(nb_cols, axis=1, inplace=True)
    except:
        print(f"Don't delete ADDED nb info in main function")

    if "raw_Fire" in allData.columns:
        allData.drop("raw_Fire", axis=1, inplace=True)

    print("Data columns are:")
    print(allData.columns)
    print(f"preProessed data shape is {allData.shape}")

    appendix = "-".join([str(i) for i in year_list])
    RF_save_path = f"./RF/{args.suffix}/situ_RF_{args.suffix}.csv"
    outData = open(RF_save_path, "w")
    outData.writelines(
        "testGroup,train_R2,train_MAE,train_RMSE,train_MAPE,train_large_MAPE,test_R2,test_MAE,test_RMSE,test_MAPE,test_large_MAPE\n"
    )
    all_y_label = []
    all_y_pred = []
    for testGroup in range(numGroups):
        print(f"Training testGroup {testGroup}")
        df_train = allData[allData["group"] != testGroup]
        df_train.reset_index(drop=True, inplace=True)
        df_test = allData[allData["group"] == testGroup]
        df_test.reset_index(drop=True, inplace=True)

        print("df_train shape is: ", df_train.shape)
        print("df_test shape is: ", df_test.shape)

        res, temp_y_label, temp_y_pred = trainRF(
            testGroup, df_train, df_test, RF_save_path, bar=10.0
        )
        all_y_label.extend(list(temp_y_label))
        all_y_pred.extend(list(temp_y_pred))
        row = [str(testGroup)] + [str(i) for i in res]
        row = ",".join(row)
        outData.writelines(row + "\n")

    all_y_label = np.array(all_y_label)
    all_y_pred = np.array(all_y_pred)
    all_y_label_large_index = all_y_label >= bar
    all_test_MAE = "%.4f" % eval_on_batch_mae(all_y_pred, all_y_label)
    all_test_RMSE = "%.4f" % eval_on_batch_rmse(all_y_pred, all_y_label)
    all_test_R2 = "%.4f" % r2_score(all_y_label, all_y_pred)
    all_test_MAPE = "%.4f" % eval_on_batch_mape(all_y_pred, all_y_label)
    all_test_large_MAPE = "%.4f" % eval_on_batch_mape(
        all_y_pred[all_y_label_large_index], all_y_label[all_y_label_large_index]
    )
    row = [
        "allGroup",
        "-",
        "-",
        "-",
        "-",
        "-",
        str(all_test_R2),
        str(all_test_MAE),
        str(all_test_RMSE),
        str(all_test_MAPE),
        str(all_test_large_MAPE),
    ]
    row = ",".join(row)
    outData.writelines(row + "\n")

    outData.close()


def getMonitorR2(df, ordered_monitors, monitor_col="monitors"):
    R2_dict = {}
    for monitor in ordered_monitors:
        temp_df = df[df[monitor_col] == monitor]
        R2_dict[monitor] = r2_score(temp_df["PM2.5"], temp_df["pred_PM2.5"])
    return R2_dict


def plotR2Scatters(moniter_df, testGroup):
    # plot all data
    y_label = moniter_df["PM2.5"]
    y_pred = moniter_df["pred_PM2.5"]
    all_r2 = r2_score(y_label, y_pred)
    plt.figure(figsize=(8, 7))
    ax = plt.subplot(1, 1, 1)
    min_val = min(y_label.min(), y_pred.min())
    if min_val < 0:
        print("There are pred value < 0 !!!!")
        min_val = 0
    max_val = max(y_label.max(), y_pred.max())
    max_val = max(max_val, 85)
    plt.scatter(y_label, y_pred, s=4, alpha=0.25, cmap=plt.get_cmap("jet"))
    plt.plot(y_label, y_label, color="red", alpha=0.2)
    ax.text(20, 80, "R2:$%.3f$" % all_r2, size=16, ha="center", va="center")
    plt.xlim(min_val, max_val)
    plt.ylim(min_val, max_val)
    ax.set_xlabel("Observation", fontsize=20)
    ax.set_ylabel("Prediction", fontsize=20)
    plt.tight_layout()
    if not os.path.exists(f"./RF/{args.suffix}/R2_plots"):
        os.makedirs(f"./RF/{args.suffix}/R2_plots")
    plt.savefig(
        f"./RF/{args.suffix}/R2_plots/prediction_G{testGroup}_test.png", dpi=150
    )
    plt.close()
    fig = plt.figure(figsize=(14, 14))
    sup_ax = plt.subplot(1, 1, 1)
    ordered_monitors = sorted(set(moniter_df["monitors"]))
    g = sns.FacetGrid(
        moniter_df, col="monitors", col_order=ordered_monitors, col_wrap=4
    )
    kws = dict(s=20, linewidth=0.5, edgecolor=None, alpha=0.3)
    map_g = (
        g.map(plt.scatter, "PM2.5", "pred_PM2.5", color="g", **kws)
        .set(xlim=(0, 100), ylim=(0, 100))
        .fig.subplots_adjust(wspace=0.25, hspace=0.25)
    )

    R2_dict = getMonitorR2(moniter_df, ordered_monitors, monitor_col="monitors")
    for index, ax in g.axes_dict.items():
        ax.text(
            25, 50, "R2: $%.3f$" % R2_dict[index], size=12, ha="center", va="center"
        )

    fig.set_figheight(3)
    fig.set_figwidth(3)
    sup_ax.set_xlabel("Observation", fontsize=25)
    sup_ax.set_ylabel("Prediction", fontsize=25)
    plt.savefig(
        f"./RF/{args.suffix}/R2_plots/prediction_G{testGroup}_test_grid.png", dpi=150
    )
    plt.close()


def main():
    trainRF_CV()


if __name__ == "__main__":
    main()
