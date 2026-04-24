import numpy as np


def dropOutlierRecords(df):
    print("\nDrop records with PM2.5<0.1 or PM2.5>400...\n")
    df = df[df["PM2.5"] >= 0.5]
    df = df[df["PM2.5"] < 400]
    print("\nDrop records with EVI<0\n")
    df = df[df["EVI"] >= 0]
    df.reset_index(drop=True, inplace=True)
    return df


def featureTransform(df, args):
    print("\n### Some feature transform process... ###\n")
    """season """
    season_dict = {}
    for i in range(1, 13):
        if i >= 3 and i <= 5:
            season_dict[i] = 0
        elif i >= 6 and i <= 8:
            season_dict[i] = 1
        elif i >= 9 and i <= 11:
            season_dict[i] = 2
        elif i == 12 or i <= 2:
            season_dict[i] = 3
    print(season_dict)
    # Spring, Summer, Autumn, Winter => 0, 1, 2, 3
    df["season"] = df["month"].apply(lambda x: season_dict[x])
    df["raw_WIND"] = np.sqrt(df["uWIND"] ** 2 + df["vWIND"] ** 2)
    df["raw_WIND"] = df["raw_WIND"] * 1.609344
    df["raw_WIND_rank"] = df["raw_WIND"].apply(lambda x: windMap(x))
    df["PRESS_ratio"] = df["PRESS"] / 101325
    df["Fire_label"] = df["Fire"].apply(lambda x: 1 if x > 0 else 0)
    """nearby widfire (Fire/Distance)"""
    monitorIDs = (
        df["yearDay"].astype("int").astype("str")
        + "_"
        + df["Lon"].apply(lambda x: "%.2f" % x).astype("str")
        + "_"
        + df["Lat"].apply(lambda x: "%.2f" % x).astype("str")
    )
    monitorFire_value = df["Fire"]
    checkFire_dict = dict(zip(monitorIDs, monitorFire_value))
    for i in range(5):
        df[f"nb_{i}_Fire"] = (
            df["yearDay"].astype("int").astype("str")
            + "_"
            + df[f"nb_{i}_Lon"].apply(lambda x: "%.2f" % x).astype("str")
            + "_"
            + df[f"nb_{i}_Lat"].apply(lambda x: "%.2f" % x).astype("str")
        )
        df[f"nb_{i}_Fire"] = df[f"nb_{i}_Fire"].apply(
            lambda x: checkFire_dict[x] if x in checkFire_dict.keys() else 0
        )
        df[f"nb_{i}_Fire"] = df[f"nb_{i}_Fire"] / (1 + df[f"nb_{i}_Dist"])
    df["nb_Fire"] = (
        df["nb_0_Fire"]
        + df["nb_1_Fire"]
        + df["nb_2_Fire"]
        + df["nb_3_Fire"]
        + df["nb_4_Fire"]
    )
    """nearby PM.5(PM2.5/Distance)"""
    for i in range(5):
        df[f"nb_{i}_PM2.5/Dis"] = df[f"nb_{i}_PM2.5"] / (1 + df[f"nb_{i}_Dist"])

    """ drop some columns """
    df_dropCols = (
        [f"nb_{i}_PM2.5" for i in range(5)]
        + [f"nb_{i}_Lon" for i in range(5)]
        + [f"nb_{i}_Lat" for i in range(5)]
        + [f"nb_{i}_Dist" for i in range(5)]
        + [f"nb_{i}_Fire" for i in range(5)]
    )
    df.drop(df_dropCols, axis=1, inplace=True)

    # new features
    if args.appendix == "1":
        df["Emission*AOD"] = df["Emission"] * df["AOD"]
        df["Evaporation*TEMP"] = (1 + df["Evaporation"]) * df["TEMP"]
        df["Evaporation*AOD"] = (df["Evaporation"]) * (df["AOD"])

    if args.appendix == "normFire":
        maxFire = np.max(df["Fire"])
        assert not np.isnan(maxFire) and maxFire >= 0
        if maxFire > 0:
            df["Fire"] = df["Fire"] / np.max(df["Fire"])
            assert np.max(df["Fire"]) == 1.0
            print("\n### Fire Feature is Normlized! ###\n")
        else:
            print(f"\n### No Fire records! ###\n")

    return df


def dropColumns(df, args):
    print("\n### Drop some columns... ###\n")

    drop_cols = ["uWIND", "vWIND", "PRESS", "basinID"] + [f"SR_b{i}" for i in range(6)]

    print(f"Wanna drop columns: {drop_cols}")
    df.drop(drop_cols, axis=1, inplace=True)

    keep_cols = (
        [
            "group",
            "yearDay",
            "dayIdx",
            "month",
            "weekday",
            "Lon",
            "Lat",
            "TEMP",
            "Humidity",
            "Evaporation",
            "Precipitation",
            "AOD",
            "EVI",
            "SR_b6",
            "Fire",
            "Emission",
            "DistanceToRoads",
            "PM2.5",
        ]
        + [f"Elevation_{i}" for i in range(3)]
        + [f"LandCover_{i}" for i in range(3)]
        + [
            "season",
            "raw_WIND",
            "raw_WIND_rank",
            "PRESS_ratio",
            "Fire_label",
            "nb_Fire",
        ]
        + [f"nb_{i}_PM2.5/Dis" for i in range(5)]
    )
    if "baseline" in args.merged_data_dir:
        keep_cols = (
            [
                "group",
                "yearDay",
                "dayIdx",
                "month",
                "weekday",
                "Lon",
                "Lat",
                "TEMP",
                "Humidity",
                "Evaporation",
                "Precipitation",
                "AOD",
                "EVI",
                "SR_b6",
                "Fire",
                "Emission",
                "DistanceToRoads",
                "PM2.5",
                "Elevation",
            ]
            + [f"LandCover_{str(i).zfill(2)}" for i in range(20)]
            + [
                "season",
                "raw_WIND",
                "raw_WIND_rank",
                "PRESS_ratio",
                "Fire_label",
                "nb_Fire",
            ]
            + [f"nb_{i}_PM2.5/Dis" for i in range(5)]
        )

    if args.appendix == "delAOD":
        keep_cols.remove("AOD")

    if args.appendix == "delFea":
        print("\n Delete some features with low importance \n")
        keep_cols.remove("nb_Fire")
        keep_cols.remove("Fire_label")
        keep_cols.remove("season")
        keep_cols.remove("raw_WIND_rank")

    df = df[keep_cols]
    df.rename(columns={"weekday": "weekDay"}, inplace=True)
    keep_int_cols = ["yearDay", "dayIdx", "month", "weekDay"]
    for col in keep_int_cols:
        df[col] = df[col].astype(int)
    return df


"""
Actual train data columns are:
 Index(['dayIdx', 'month', 'weekday', 'TEMP', 'Humidity', 'Evaporation',
       'Precipitation', 'AOD', 'EVI', 'SR_b6', 'Fire', 'Emission',
       'DistanceToRoads', 'Elevation_0', 'Elevation_1', 'Elevation_2',
       'LandCover_0', 'LandCover_1', 'LandCover_2', 'season', 'raw_WIND',
       'raw_WIND_rank', 'PRESS_ratio', 'Fire_label', 'nb_Fire',
       'nb_0_PM2.5/Dis', 'nb_1_PM2.5/Dis', 'nb_2_PM2.5/Dis', 'nb_3_PM2.5/Dis',
       'nb_4_PM2.5/Dis']
"""


def windMap(x):
    if x <= 2:
        return 0
    elif x <= 6:
        return 1
    elif x <= 12:
        return 2
    elif x <= 19:
        return 3
    elif x <= 30:
        return 4
    elif x <= 40:
        return 5
    else:
        return 6
