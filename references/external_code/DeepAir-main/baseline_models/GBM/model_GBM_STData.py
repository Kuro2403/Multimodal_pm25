"""
predicting PM2.5 concentration using GBM model
"""

"""Train and test GBM model on Situ data"""
import argparse
import json
import os
import pickle
import random

import lightgbm as lgb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.metrics import r2_score
from utils import *

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
python model_GBM_STData.py --suffix raw_preProcess --kind preProcess --y_trans raw
python model_GBM_STData.py --suffix baseline_raw --kind raw --y_trans raw
python model_GBM_STData.py --suffix baseline_preProcess --kind preProcess --y_trans raw

python model_GBM_STData.py --suffix baseline_raw_withoutDayInfo --kind raw --y_trans raw
"""


args = parser.parse_args()


def log_trans(data):
    ret = np.log(data + 1)
    return ret


def unLog_trans(data):
    ret = np.exp(data) - 1
    return ret


def eval_on_batch_mape(pred, label):
    label_index = label > 0
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


def trainGBM(df_train, df_test, testGroup, bar=10.0):
    print(df_train.size)
    print(df_test.size)

    test_monitors = df_test

    if args.kind == "raw":
        X_drop_cols = ["PM2.5"]
    elif args.kind == "preProcess":
        X_drop_cols = ["PM2.5"]

    y_train = df_train["PM2.5"]
    y_test = df_test["PM2.5"]
    X_train = df_train.drop(X_drop_cols, axis=1)
    X_test = df_test.drop(X_drop_cols, axis=1)

    y_train = y_train.astype("float")
    y_test = y_test.astype("float")

    y_train_large_index = y_train >= bar
    y_test_large_index = y_test >= bar

    if args.kind == "raw":
        categorical_feature = ["month", "weekday", "basinID"]
    elif args.kind == "preProcess":
        categorical_feature = [
            "month",
            "weekDay",
            "season",
            "raw_WIND_rank",
            "Fire_label",
        ]

    if args.y_trans == "raw":
        lgb_train = lgb.Dataset(
            X_train, y_train, categorical_feature=categorical_feature
        )
        lgb_eval = lgb.Dataset(
            X_test, y_test, categorical_feature=categorical_feature, reference=lgb_train
        )
    elif args.y_trans == "log":
        lgb_train = lgb.Dataset(
            X_train, log_trans(y_train), categorical_feature=categorical_feature
        )
        lgb_eval = lgb.Dataset(
            X_test,
            log_trans(y_test),
            categorical_feature=categorical_feature,
            reference=lgb_train,
        )

    params = {
        "boosting_type": "gbdt",
        "objective": "regression",
        "metric": ["l1", "huber"],
        "num_iterations": 100000,
        "num_leaves": 512,
        "max_bin": 512,
        "max_depth": 256,
        "learning_rate": 0.01,
        "feature_fraction": 0.8,
        "bagging_fraction": 0.8,
        "bagging_freq": 20,
        "early_stopping_rounds": 100,
        "verbose": 0,
        "num_threads": 8,
        "seed": 123,
    }

    print("Starting training...")
    evals_result_dict = {}
    # train
    gbm = lgb.train(
        params,
        lgb_train,
        valid_sets=[lgb_train, lgb_eval],
        evals_result=evals_result_dict,
        verbose_eval=False,
    )

    plotFigs(lgb, gbm, evals_result_dict, testGroup)

    print("Saving model...")
    if not os.path.exists(f"./GBM/{args.suffix}/model"):
        os.makedirs(f"./GBM/{args.suffix}/model")
    gbm.save_model(f"./GBM/{args.suffix}/model/GBM_model_G{testGroup}.txt")

    print(f"\ntrain data columns are:")
    print(X_train.columns)

    print("Starting predicting on training data...")
    # predict
    y_pred = gbm.predict(X_train, num_iteration=gbm.best_iteration)
    y_pred = y_pred.astype("float")
    if args.y_trans == "log":
        y_pred = unLog_trans(y_pred)
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
    y_pred = gbm.predict(X_test, num_iteration=gbm.best_iteration)
    y_pred = y_pred.astype("float")
    if args.y_trans == "log":
        y_pred = unLog_trans(y_pred)
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
    if not os.path.exists(f"./GBM/{args.suffix}/pred_results/"):
        os.makedirs(f"./GBM/{args.suffix}/pred_results/")
    test_monitors.to_csv(
        f"./GBM/{args.suffix}/pred_results/prediction_G{testGroup}.csv", index=0
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


# train GBM from different years' data
def trainGBM_CV_groupYears(year_list=["years"], bar=10.0):
    # create dirs to save model and visualzed plots
    if not os.path.exists(f"./GBM/{args.suffix}"):
        os.makedirs(f"./GBM/{args.suffix}")

    numGroups = 10  # group to test
    # numGroups = 1
    # load data from different years
    allData = []
    for year in year_list:
        print(f"Reading data from year {year}")
        if args.situ_appendix:
            df_path = dataPath + f"STData/situData_{year}_{args.situ_appendix}.csv"
        else:
            df_path = dataPath + f"STData/situData_{year}.csv"
        print(f"Reading data from {df_path}")
        situData = open(df_path, "r")
        # situData = open(dataPath + "STData/situData_" + str(year) + ".csv", "r")
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

    # remove raw Fire feature
    if "raw_Fire" in allData.columns:
        allData.drop("raw_Fire", axis=1, inplace=True)

    print("Data columns are:")
    print(allData.columns)
    print(f"preProessed data shape is {allData.shape}")

    appendix = "-".join([str(i) for i in year_list])
    GBM_save_path = f"./GBM/{args.suffix}/situ_GBM_{args.suffix}.csv"
    outData = open(GBM_save_path, "w")
    outData.writelines(
        "testGroup,train_R2,train_MAE,train_RMSE,train_MAPE,train_large_MAPE,test_R2,test_MAE,test_RMSE,test_MAPE,test_large_MAPE\n"
    )
    all_y_label = []
    all_y_pred = []
    for n in range(numGroups):
        print(f"test group is {n}")
        df_test = allData[allData["group"] == n]
        df_train = allData[allData["group"] != n]
        df_test = df_test.drop(["group"], axis=1)
        df_train = df_train.drop(["group"], axis=1)

        # get results
        res, temp_y_label, temp_y_pred = trainGBM(
            df_train, df_test, testGroup=n, bar=bar
        )
        all_y_label.extend(list(temp_y_label))
        all_y_pred.extend(list(temp_y_pred))
        # rmse_train, mape_train, r2_train, rmse_test, mape_test, r2_test, filename = res
        row = [str(n)] + [str(i) for i in res]
        row = ",".join(row)
        outData.writelines(row + "\n")

    # record all group predict results
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


def plotFigs(lgb, gbm, evals_result_dict, testGroup):
    if not os.path.exists(f"./GBM/{args.suffix}/Loss_plots"):
        os.makedirs(f"./GBM/{args.suffix}/Loss_plots")
    # plot figures
    print("Plot eval results during training process...")
    ax = lgb.plot_metric(evals_result_dict, metric="l1")
    plt.title("L1 loss")
    plt.savefig(f"./GBM/{args.suffix}/Loss_plots/train_l1_G{testGroup}.png")
    plt.close()
    ax = lgb.plot_metric(evals_result_dict, metric="huber")
    plt.title("Huber Loss")
    plt.savefig(f"./GBM/{args.suffix}/Loss_plots/train_huber_G{testGroup}.png")
    plt.close()
    print("Plot feature importance...")
    ax = lgb.plot_importance(gbm, figsize=(10, 25))
    plt.title("Feature importance")
    plt.savefig(f"./GBM/{args.suffix}/Loss_plots/feature_importances_G{testGroup}.png")
    plt.close()


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
    if not os.path.exists(f"./GBM/{args.suffix}/R2_plots"):
        os.makedirs(f"./GBM/{args.suffix}/R2_plots")
    plt.savefig(
        f"./GBM/{args.suffix}/R2_plots/prediction_G{testGroup}_test.png", dpi=150
    )
    plt.close()
    # plot grid data
    # scatter plot
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
        f"./GBM/{args.suffix}/R2_plots/prediction_G{testGroup}_test_grid.png", dpi=150
    )
    plt.close()


def main():
    trainGBM_CV_groupYears(year_list=["years"])


if __name__ == "__main__":
    print(args)
    main()


"""
python model_GBM_STData.py --suffix raw
"""


def data_loader(year=2016):
    numGroups = 10

    situData = open(dataPath + "STData/situData_" + str(year) + ".csv", "w")
    header = (
        "group,yearDay,dayIdx,month,weekday,Lon,Lat,basinID,"
        + "TEMP,Humidity,PRESS,uWIND,vWIND,Evaporation,Precipitation,"
        + "AOD,Elevation,EVI,SR_b0,SR_b1,SR_b2,SR_b3,SR_b4,SR_b5,SR_b6,"
    )
    header += ",".join(["LandCover_" + str(l).zfill(2) for l in range(20)])
    header += ",Fire,PM2.5\n"
    situData.writelines(header)

    # location to monitor ID (state, county, site)
    monitorLocToID_CA = pickle.load(open(dataPath + "Geo/monitorLocToID_CA.pkl", "rb"))

    # load elevation data
    inData = open(dataPath + "STData/monitorElevation.json", "r")
    for row in inData:
        elevationDict = json.loads(row)
    inData.close()

    # load emission data, yearly data per row
    inData = open(dataPath + "STData/monitorEmission.json", "r")
    emisionDict = {}
    for row in inData:
        row_dict = json.loads(row)
        emisionDict[row_dict["year"]] = row_dict
    inData.close()

    # load the land cover histgram
    gridLandCover = pickle.load(open(dataPath + "NLCD/gridLandCovers_hist.pkl", "rb"))

    """
    # load network embedding 
    inData = open(dataPath + "../NetworkEmbedding/gridIds.csv", 'r')
    girdLocToId = {}
    for row in inData:
        row = row.rstrip().split(",")
        gid = int(row[0])
        lon = float(row[1])
        lat = float(row[2])
        girdLocToId[(lon, lat)] = str(gid)
    inData.close()
    inData = open(dataPath + "../NetworkEmbedding/embedding_line.json", 'r')
    for row in inData:
        gridsEmbedding = json.loads(row)
    inData.close()
    """

    # load the data
    count = 0
    count_kept = 0
    for g in range(numGroups):
        inData = open(
            dataPath + "STData/STData_" + str(year) + "_group_" + str(g) + ".json", "r"
        )

        for row in inData:
            count += 1
            row_dict = json.loads(row)
            if row_dict["PM2.5"] <= 0:
                continue
            yearDay = row_dict["yearDay"]
            year = int(yearDay[:4])
            dayIdx = int(yearDay[4:])
            lon = row_dict["Lon"]
            lat = row_dict["Lat"]
            loc_str = str(lon) + "_" + str(lat)
            monitorID = monitorLocToID_CA[(lon, lat)]
            lon_int = int(100 * (lon - CA_boundary[0]))
            lat_int = int(100 * (lat - CA_boundary[2]))
            row_dict["Year"] = year - 2006
            row_dict["dayIdx"] = dayIdx
            row_dict["LonInt"] = lon_int
            row_dict["LatInt"] = lat_int
            row_dict["LandCover"] = gridLandCover[(lon, lat)]

            # row_dict["gridLocVector"] = gridsEmbedding[girdLocToId[(lon, lat)]]
            PRESS = np.asarray(row_dict["PRESS"])
            PRESS[PRESS > 1e10] = 0
            row_dict["PRESS"] = PRESS.tolist()

            # normalize meteorological data
            """
            met_vec = []
            for d in range(config["temporalWindowSize"]):
                tmp_vec = []
                for met in metFeatures: 
                    tmp = np.array(row_dict[met][d][12])  # situ
                    tmp = np.nan_to_num(tmp)
                    tmp = normalize(tmp, met)
                    tmp_vec.append(tmp)
                met_vec.append(tmp_vec)
            row_dict["Met"] = met_vec
            for met in metFeatures:
                row_dict.pop(met)
            """
            # situ met data
            metData = []
            for met in metFeatures:
                tmp = row_dict[met][-1][12]  # situ
                tmp = np.nan_to_num(tmp)
                # tmp = normalize(tmp, met)
                metData.append(tmp)

            row_dict["Elevation"] = elevationDict[loc_str][1300]  # situ
            row_dict["Emission"] = (
                emisionDict[year][loc_str][1300] / 365.0 / 24.0
            )  # situ
            row_dict["EVI"] = row_dict["EVI"][1300]

            SR_vec = []
            for b in range(7):
                srKey = "SR_b" + str(b)
                SR_vec.append(row_dict[srKey][1300])
                row_dict.pop(srKey)
            row_dict["SR"] = SR_vec

            """
            tmp_vec = []
            for d in range(config["temporalWindowSize"]):
                tmp = row_dict["AOD"][d][1330]  # situ
                tmp_vec.append(tmp)
            row_dict["AOD"] = tmp_vec
            """
            aod = row_dict["AOD"][-1][1300]

            """
            tmp_vec = []
            for d in range(config["temporalWindowSize"]):
                try:
                    tmp = row_dict["Fire"][d][1300]  # situ
                except:
                    tmp = 0  # no fire
                tmp_vec.append(tmp)
            """
            try:
                fire = row_dict["Fire"][-1][1300]
            except:
                fire = 0

            # save
            row_save = [
                str(g),
                str(yearDay),
                str(dayIdx),
                str(row_dict["month"]),
                str(row_dict["weekday"]),
                str(lon),
                str(lat),
                str(row_dict["basinID"]),
            ]
            row_save += [str(m) for m in metData]
            row_save += [str(aod), str(row_dict["Elevation"]), str(row_dict["EVI"])]
            row_save += [str(s) for s in SR_vec]
            row_save += [str(lc) for lc in row_dict["LandCover"]]
            row_save += [str(fire), str(row_dict["PM2.5"])]
            situData.writelines(",".join(row_save) + "\n")

            count_kept += 1
        inData.close()
    situData.close()

    print("# samples loaded: %d / %d" % (count_kept, count))


def trainGBM_CV(year=2016):
    numGroups = 10
    # load data
    situData = open(dataPath + "STData/situData_" + str(year) + ".csv", "r")
    header = situData.readline().rstrip()
    header = header.split(",")

    allData = []

    for row in situData:
        row = row.rstrip().split(",")
        row = [float(r) for r in row]
        allData.append(row)
    random.shuffle(allData)

    allData = pd.DataFrame(allData, columns=header)
    print(allData.columns)

    GBM_save_path = f"./GBM/situ_GBM_{str(year)}.csv"
    if not os.path.exists("./GBM"):
        os.mkdir("./GBM")
    outData = open(GBM_save_path, "w")
    outData.writelines(
        "testGroup,train_R2,train_RMSE,train_MAPE,train_large_MAPE,test_R2,test_RMSE,test_MAPE,test_large_MAPE\n"
    )
    for n in range(numGroups):
        df_test = allData[allData["group"] == n]
        df_train = allData[allData["group"] != n]
        df_test = df_test.drop(["group"], axis=1)
        df_train = df_train.drop(["group"], axis=1)
        res = trainGBM(df_train, df_test, testGroup=n)
        row = [str(n)] + [str(i) for i in res]
        row = ",".join(row)
        outData.writelines(row + "\n")
    outData.close()


def multipleRuns_random(year=2016, numR=50):
    allData = []
    allLocations = set()
    for year in range(2016, 2017):
        inData = open(dataPath + "SituData/monitorData_" + str(year) + ".csv", "r")
        header = inData.readline().rstrip().split(",")
        for row in inData:
            row = row.rstrip().split(",")
            yearDay = str(row[0])
            row = [float(r) for r in row]
            lon = row[3]
            lat = row[4]
            allLocations.add((lon, lat))
            allData.append(row)

    random.shuffle(allData)
    allLocations = list(allLocations)
    # split allData into training and testing by locations
    print("# of monitors : %d" % len(allLocations))

    outData = open(dataPath + "STData/situ_GBM_" + str(year) + "_random.csv", "w")
    outData.writelines(
        "rmse_train,mape_train,r2_train,rmse_test,mape_test,r2_test,modelFile\n"
    )
    for r in range(numR):
        print("Running %d ..." % r)
        random.shuffle(allLocations)

        trainingSize = int(len(allLocations) * 0.9)
        trainingMonitors = set(allLocations[:trainingSize])
        testingMonitors = set(allLocations[trainingSize:])
        print(
            "# of monitors for training and testing : %d, %d"
            % (len(trainingMonitors), len(testingMonitors))
        )

        allData_df = pd.DataFrame(allData, columns=header)
        allData_df = allData_df.drop(["weekday", "month", "basinID"], axis=1)
        print(allData_df.columns)

        df_train = []
        df_test = []
        for index, row in allData_df.iterrows():
            lon = row["Lon"]
            lat = row["Lat"]
            if (lon, lat) in trainingMonitors:
                df_train.append(row)
            else:
                df_test.append(row)

        df_train = pd.DataFrame(df_train, columns=allData_df.columns)
        df_test = pd.DataFrame(df_test, columns=allData_df.columns)
        print(df_train.columns)
        print(df_train.size)
        print(df_test.size)
        res = trainGBM(df_train, df_test)
        row = [str(i) for i in res]
        row = ",".join(row)
        outData.writelines(row + "\n")
    outData.close()
