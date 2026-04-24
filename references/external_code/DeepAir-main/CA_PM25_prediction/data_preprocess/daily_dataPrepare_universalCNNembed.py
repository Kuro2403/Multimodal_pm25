"""use CNN to deal with Elevation data and LandCover data"""

"""
When selecting different model structures, the imported model needs to be modified.
Based on the pre-trained model, generate the Elevation and LandCover embedding representations and save them to CA_{args.save_dirs}.
Only one group's embedding data is generated at a time, to be used in subsequent merging operations.

The generated information is static, with each grid having a fixed encoding.

Data is output to the CA_preTrain.ElevaLand.embedResults folder.
"""

import argparse
import gc
import inspect
import json
import os

import geojson
import pandas as pd
import torch
from matplotlib.path import Path

print("pytorch version : ", torch.__version__)

import data_loader_group_CA
import utils

import CA_PM25_prediction.data_preprocess.load_static_data_CA as load_static_data_CA

os.environ["CUDA_VISIBLE_DEVICES"] = "3"

use_cuda = torch.cuda.is_available()
DEVICE = torch.device("cuda" if use_cuda else "cpu")  # 'cpu' in this case

print("Using device : ", DEVICE)


dataPath = ".data/DeepAir/"

CA_boundry_geojson = "./data/DeepAir/Geo/CA_boundary/CA_boundary.geojson"

parser = argparse.ArgumentParser()
parser.add_argument("--batch_size", type=int, default=256)
parser.add_argument("--save_dirs", type=str, default=None)
parser.add_argument("--case_label", type=str, default=None)  # "met3DEEESA"
parser.add_argument("--loss_func", type=str, default="Huber")  # "MSE"
parser.add_argument("--used_group", type=str)  # "MSE"
parser.add_argument("--suffix", type=str, default="")


# log file name
parser.add_argument("--log_file", type=str, default="PM25")

args = parser.parse_args()

config = json.load(open("config.json", "r"))

"""
import model file based on different dirs
"""
if args.save_dirs == "ElevaLand.preTrain.shapeNoDrop":
    from situ_DNN_preTrain_shapeNoDrop import *


def get_model(args, kwargs):
    if args.case_label == "ElevaLand":
        model = situNet_ElevaLand(args, **kwargs)
    return model


def get_calseKwargs(caseLabel):
    if args.case_label == "ElevaLand":
        kwargs = get_kwargs(situNet_ElevaLand)
    return kwargs


def inPolygon(stopLon, stopLat, polygon):
    if polygon.contains_point([stopLon, stopLat]):
        return True
    else:
        return False


def get_CA_grids():
    with open(CA_boundry_geojson, "rb") as f:
        CA_geoData = geojson.load(f)

    boundryPoints = CA_geoData["features"][0]["geometry"]["coordinates"][0][0]
    CA_polygon = Path(boundryPoints)

    CA_grids_df = pd.read_csv("./data/DeepAir/Geo/CA_grids.csv")
    print(CA_grids_df.shape)
    inCA = []
    for row in CA_grids_df.itertuples():
        lon = getattr(row, "minLon")
        lat = getattr(row, "minLat")
        inCA.append(inPolygon(lon, lat, CA_polygon))
    CA_grids_df["in_CA"] = inCA
    print(f"raw shape {CA_grids_df.shape}")
    CA_grids_df = CA_grids_df[CA_grids_df["in_CA"] == True]
    print(f"CA grids shape {CA_grids_df.shape}")

    gridLocs = set()
    for row in CA_grids_df.itertuples():
        lon = getattr(row, "minLon")
        lat = getattr(row, "minLat")
        gridLocs.add((lon, lat))
    gridLocs = list(gridLocs)
    len(gridLocs)

    print(f"\nCA_grids_df shape {CA_grids_df.shape}")
    for col in CA_grids_df.columns:
        print(
            f"{col} maxValue {CA_grids_df[col].max()} minValue {CA_grids_df[col].min()}"
        )

    all_CA_grids_df = CA_grids_df[["minLon", "minLat"]]
    all_CA_grids_df.rename(columns={"minLon": "Lon", "minLat": "Lat"}, inplace=True)

    return all_CA_grids_df, gridLocs


# we keep uncertainty and the final prediction for each sample
# for each testing file, we load 20 models and predict the testing samples
def dataTransfer(
    args, all_CA_grids_df, gridLocs, elevationDict, landCoverImagesDict, model
):
    """
    Based on the pre-trained model, generate the embedding representations for Elevation and LandCover
    and save them to CA_{args.save_dirs}.
    Only one group's embedding data is generated at a time.
    """
    print(" --------- ")
    trainTestData = []

    partData = data_loader_group_CA.universal_MySet(all_CA_grids_df, gridLocs)
    gridLocs = partData.getGridLocs()
    trainTestData = partData.content
    del partData
    gc.collect()
    print("# data number : %d" % len(trainTestData))
    print(f"grid locs num is {len(gridLocs)}")

    data_iter = data_loader_group_CA.get_loader_fromData(
        trainTestData, args.batch_size, shufData=False
    )

    model.eval()
    transferedData = torch.Tensor()
    for idx, attr in enumerate(data_iter):
        attr["Elevation"] = load_static_data_CA.addElevationToBatchData(
            attr, elevationDict
        )
        attr["LandCoverImg"] = load_static_data_CA.addLandCoverToBatchData(
            attr, landCoverImagesDict
        )
        attr = utils.to_var(attr)

        temp_transferData = model.universal_data_transfer(attr, config)

        if idx == 0:
            transferedData = temp_transferData
        else:
            transferedData = torch.cat([transferedData, temp_transferData], dim=0)

    transferedData = transferedData.detach().cpu().numpy()
    print(f"transferedData shape is {transferedData.shape}")
    outColumns = ["Lon", "Lat"]

    print(f"CNN embedding finished")

    if args.save_dirs == "ElevaLand.preTrain.shapeNoDrop":
        Elevation_embed_dim = 3
        LandCover_embed_dim = 3

    Elevation_cols = [f"Elevation_{i}" for i in range(Elevation_embed_dim)]
    landCover_cols = [f"LandCover_{i}" for i in range(LandCover_embed_dim)]
    outColumns.extend(Elevation_cols)
    outColumns.extend(landCover_cols)

    outFrame = pd.DataFrame(columns=outColumns, data=transferedData)

    outFrame["Lon"] = outFrame["Lon"].apply(lambda x: utils.unnormalize(x, "Lon"))
    outFrame["Lat"] = outFrame["Lat"].apply(lambda x: utils.unnormalize(x, "Lat"))
    for col in ["Lon", "Lat"]:
        outFrame[col] = outFrame[col].apply(lambda x: "%.2f" % x)

    csv_savePath = f"./CA_{args.save_dirs}"
    if not os.path.exists(csv_savePath):
        os.makedirs(csv_savePath)
    csv_savePath = (
        f"{csv_savePath}/universal_ElevaLandEmbed_G{config['model_group']}.csv"
    )

    print(f"outFrame data dtypes is: {outFrame.dtypes}")
    outFrame.to_csv(csv_savePath, index=0)
    print(f"\nSuccessfully save CSV file to {csv_savePath}")
    print(f"CSV shape is {outFrame.shape}")


def get_kwargs(model_class):
    model_args = list(inspect.signature(model_class.__init__).parameters)
    shell_args = args._get_kwargs()
    kwargs = dict(shell_args)
    for arg, val in shell_args:
        if not arg in model_args:
            kwargs.pop(arg)
    return kwargs


def run():
    csv_savePath = f"./CA_{args.save_dirs}/"
    if not os.path.exists(csv_savePath):
        os.makedirs(csv_savePath)

    # get the model arguments
    kwargs = get_calseKwargs(args.case_label)

    print(f"args are {args}")
    print(f"kwargs are {kwargs}")

    all_CA_grids_df, gridLocs = get_CA_grids()

    # load all basemap images empty
    baseMapDict = {}
    print("%d Basemap Images loaded!" % len(baseMapDict))

    # load all elevation data
    elevationDict = load_static_data_CA.loadElevation()
    print(f"{len(elevationDict)} Elevation data loaded!")

    # load all emission data empty
    emissionDict = {}
    print("Emission loaded!")

    # load all landcover images
    landCoverImagesDict = load_static_data_CA.loadLandCoverImgs(gridLocs)
    print("%d LandCover Images loaded!" % len(landCoverImagesDict))

    weight_filePath = config["CNN_model_load_path"]
    kwargs = get_calseKwargs(args.case_label)
    model = get_model(args, kwargs)
    if torch.cuda.is_available():
        model.cuda()

    modelFile = weight_filePath
    print(f"weight file is {modelFile}")
    model.load_state_dict(torch.load(modelFile, map_location=DEVICE))
    print("Model loaded!")

    dataTransfer(
        args, all_CA_grids_df, gridLocs, elevationDict, landCoverImagesDict, model
    )


if __name__ == "__main__":
    config["model_group"] = args.used_group
    # load pretrained CNN model
    config["CNN_model_load_path"] = (
        f"../DeepAir/situPreCNN_GBM/ElevaLand.preTrain.shapeNoDrop/saved_earlyStop_weights_yearsG{args.used_group}/PM25_G{args.used_group}_3E3L_earlyStopping"
    )

    print(f"\nmodel group is {config['model_group'] }")
    print(f"CNN model save path is {config['CNN_model_load_path']}\n")

    run()

"""
python main_pretrainedCNN2data.py --save_dirs ElevaLand.preTrain.tempModel --weight_save_dir saved_earlyStop_weights_yearsG5 --case_label ElevaLand --loss_func Huber
python daily_dataPrepare_universalCNNembed.py --save_dirs preTrain.ElevaLand.embedResults --case_label ElevaLand --used_group 0
"""
