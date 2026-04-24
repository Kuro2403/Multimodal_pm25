"""Update: change temporal meteorological data to spatio-tempral"""

import random

import numpy as np
import pandas as pd
import torch
import utils
from torch.utils.data import DataLoader, Dataset
from utils import *

spatialWindowSize = config["spatialWindowSize"]
temporalWindowSize = config["temporalWindowSize"]

dataPath = "./data/DeepAir/"

CA_boundary = [-124.48, -114.13, 32.53, 42.01]
metFeatures = [
    "TEMP",
    "Humidity",
    "PRESS",
    "uWIND",
    "vWIND",
    "Evaporation",
    "Precipitation",
]

normalizedFeatures = []
normalizedFeatures += ["Lon", "Lat"]


def get_situFlag(case_label):
    """if flag is True, then only use situData"""
    if case_label == "ElevaLand":
        situFlag = {
            "AOD": True,
            "Emission": True,
            "Elevation": False,
            "EVI": True,
            "Met": True,
            "SR": True,
            "Fire": True,
        }

    else:
        print("\n\nIllegal case_label!\n")
        exit(0)
    return situFlag


class MySet(Dataset):
    def __init__(self, args, input_file):
        # we select the first part for testing
        self.content = []
        self.gridLocs = []
        # inData.readline()
        inData_df = pd.read_csv(input_file)
        print(inData_df.columns)
        inData_df.rename(columns={"PM2.5": "PM25"}, inplace=True)

        count = 0
        count_kept = 0

        situFlag = get_situFlag(args.case_label)

        for row in inData_df.itertuples():
            count += 1
            yearDay = str(int(getattr(row, "yearDay")))
            year = int(yearDay[:4])
            yearID = year - 2006
            dayIdx = int(yearDay[4:])
            month = getattr(row, "month")
            weekday = getattr(row, "weekDay")
            Lon = getattr(row, "Lon")
            Lat = getattr(row, "Lat")
            loc_str = str(Lon) + "_" + str(Lat)
            self.gridLocs.append((Lon, Lat))

            PM25 = getattr(row, "PM25")

            # attibutes
            row_dict = {}
            row_dict["yearID"] = yearID
            row_dict["dayIdx"] = dayIdx
            row_dict["month"] = month
            row_dict["weekday"] = weekday
            row_dict["Lon"] = Lon
            row_dict["Lat"] = Lat
            row_dict["PM2.5"] = PM25
            row_dict["basinID"] = 0

            # normalizedFeatures = ["Lon", "Lat", "Elevation", "EVI", "AOD", "Emission", "DistanceToRoads"]
            for fea in normalizedFeatures:
                row_dict[fea] = utils.normalize(row_dict[fea], fea)

            self.content.append(row_dict)
            count_kept += 1

        # disorder the content
        random.shuffle(self.content)
        print("# samples loaded: %d / %d" % (count_kept, count))
        self.lengths = list(map(lambda x: 1, self.content))

    def getGridLocs(self):
        return self.gridLocs

    def __getitem__(self, idx):
        return self.content[idx]

    def __len__(self):
        return len(list(self.content))


class universal_MySet(Dataset):
    def __init__(self, all_CA_grids_df, gridLocs):
        # we select the first part for testing
        self.content = []
        self.gridLocs = gridLocs
        count = 0
        count_kept = 0

        for row in all_CA_grids_df.itertuples():
            count += 1
            Lon = getattr(row, "Lon")
            Lat = getattr(row, "Lat")
            loc_str = str(Lon) + "_" + str(Lat)
            # attibutes
            row_dict = {}
            row_dict["Lon"] = Lon
            row_dict["Lat"] = Lat

            # normalizedFeatures = ["Lon", "Lat", "Elevation", "EVI", "AOD", "Emission", "DistanceToRoads"]
            for fea in ["Lon", "Lat"]:
                row_dict[fea] = utils.normalize(row_dict[fea], fea)

            self.content.append(row_dict)
            count_kept += 1

        # disorder the content
        random.shuffle(self.content)
        print("# samples loaded: %d / %d" % (count_kept, count))
        self.lengths = list(map(lambda x: 1, self.content))

    def getGridLocs(self):
        return self.gridLocs

    def __getitem__(self, idx):
        return self.content[idx]

    def __len__(self):
        return len(list(self.content))




def collate_fn(data):
    info_attrs = ["Lon", "Lat"]
    attr = {}
    for key in info_attrs:
        attr[key] = torch.FloatTensor([item[key] for item in data])
    return attr


class BatchSampler:
    def __init__(self, dataset, batch_size):
        self.count = len(dataset)
        self.batch_size = batch_size
        self.lengths = dataset.lengths
        self.indices = list(range(self.count))

    def __iter__(self):
        """
        Divide the data into chunks with size = batch_size * 100
        sort by the length in one chunk
        """
        np.random.shuffle(self.indices)


        chunk_size = self.batch_size * 100

        chunks = (self.count + chunk_size - 1) // chunk_size

        # re-arrange indices to minimize the padding
        for i in range(chunks):
            partial_indices = self.indices[i * chunk_size : (i + 1) * chunk_size]
            partial_indices.sort(key=lambda x: self.lengths[x], reverse=True)
            self.indices[i * chunk_size : (i + 1) * chunk_size] = partial_indices

        # yield batch
        batches = (self.count - 1 + self.batch_size) // self.batch_size

        for i in range(batches):
            yield self.indices[i * self.batch_size : (i + 1) * self.batch_size]

    def __len__(self):
        return (self.count + self.batch_size - 1) // self.batch_size


class BatchSampler_fromData:
    def __init__(self, dataset_numSamples, dataset_lengths, batch_size, shufData=True):
        self.count = dataset_numSamples  # data sample number
        self.batch_size = batch_size
        self.lengths = dataset_lengths  # [1, 1, 1, ... , 1]
        self.indices = list(range(self.count))
        self.shufData = shufData  # shuffle or not

    def __iter__(self):
        """
        Divide the data into chunks with size = batch_size * 100
        sort by the length in one chunk
        """
        if self.shufData == True:
            np.random.shuffle(self.indices)

        chunk_size = self.batch_size * 100

        chunks = (self.count + chunk_size - 1) // chunk_size

        # re-arrange indices to minimize the padding
        for i in range(chunks):
            partial_indices = self.indices[i * chunk_size : (i + 1) * chunk_size]
            partial_indices.sort(key=lambda x: self.lengths[x], reverse=True)
            self.indices[i * chunk_size : (i + 1) * chunk_size] = partial_indices

        # yield batch
        batches = (self.count - 1 + self.batch_size) // self.batch_size

        for i in range(batches):
            yield self.indices[i * self.batch_size : (i + 1) * self.batch_size]

    def __len__(self):
        return (self.count + self.batch_size - 1) // self.batch_size


def get_loader(input_file, batch_size):
    dataset = MySet(input_file=input_file)

    batch_sampler = BatchSampler(dataset, batch_size)

    data_loader = DataLoader(
        dataset=dataset,
        batch_size=1,
        collate_fn=lambda x: collate_fn(x),
        num_workers=4,
        batch_sampler=batch_sampler,
        pin_memory=True,
    )

    return data_loader


def get_loader_fromData(input_data, batch_size, shufData=True):
    dataset_lengths = list(map(lambda x: 1, input_data))
    dataset_numSamples = len(input_data)

    batch_sampler = BatchSampler_fromData(
        dataset_numSamples, dataset_lengths, batch_size, shufData=shufData
    )

    data_loader = DataLoader(
        dataset=input_data,
        batch_size=1,
        collate_fn=lambda x: collate_fn(x),  #  num_workers=4,
        num_workers=0,
        batch_sampler=batch_sampler,
        pin_memory=True,
    )

    return data_loader
