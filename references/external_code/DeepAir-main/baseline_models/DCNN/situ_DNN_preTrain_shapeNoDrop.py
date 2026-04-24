""" DNN model with situ data as input, pm2.5 as output
We check if this model could surpass the GBM """

import torch
import torch.nn as nn
import urbanNet
from utils import *

metFeatures = {
    "TEMP",
    "Humidity",
    "PRESS",
    "uWIND",
    "vWIND",
    "Evaporation",
    "Precipitation",
}


# select loss function
def get_lossFunction(loss_func="MSE"):
    if loss_func == "MSE":
        return torch.nn.MSELoss()
    elif loss_func == "L1":
        return torch.nn.L1Loss()
    elif loss_func == "Huber":
        return torch.nn.SmoothL1Loss()
    else:
        print("None loss function! ")
        exit(-1)


class SpatialEmbedding(nn.Module):
    def __init__(self, out_dim):
        super(SpatialEmbedding, self).__init__()
        self.layer1 = nn.Sequential(
            nn.Conv2d(1, 16, kernel_size=5, stride=1, padding=2),
            # nn.BatchNorm2d(num_features=16, eps=1e-05, momentum=0.1, affine=True, track_running_stats=True),
            nn.LeakyReLU(),
            # nn.Dropout(p=0.5, inplace=True),
            nn.MaxPool2d(kernel_size=2, stride=2),
        )
        self.layer2 = nn.Sequential(
            nn.Conv2d(16, 32, kernel_size=5, stride=1, padding=2),
            nn.LeakyReLU(),
            nn.MaxPool2d(kernel_size=2, stride=2),
        )
        self.layer3 = nn.Sequential(
            nn.Conv2d(32, 16, kernel_size=5, stride=1, padding=2),
            nn.LeakyReLU(),
            nn.MaxPool2d(kernel_size=2, stride=2),
        )
        self.layer4 = nn.Sequential(
            nn.Conv2d(16, 8, kernel_size=5, stride=1, padding=2),
            nn.LeakyReLU(),
            nn.MaxPool2d(kernel_size=2, stride=2),
        )
        self.fc1 = nn.Sequential(
            nn.Linear(3 * 3 * 8, 32),
            nn.LeakyReLU(),
            nn.Linear(32, out_dim),
        ) 

    def forward(self, x):
        x = self.layer1(x)  # [bs, 32, 50, 50]
        x = self.layer2(x)  # [bs, 64, 25, 25]
        x = self.layer3(x)
        x = self.layer4(x)
        x = x.reshape(x.size(0), -1)  # [bs, 64*25*25] = [bs, 40000]
        x = self.fc1(x)  # [bs, 1000]
        return x


# CancelOut Layer
# https://github.com/unnir/CancelOut
class CancelOut(nn.Module):
    """
    CancelOut Layer

    x - an input data (vector, matrix, tensor)
    """

    def __init__(self, inp, *kargs, **kwargs):
        super(CancelOut, self).__init__()
        self.weights = nn.Parameter(torch.zeros(inp, requires_grad=True) + 4)

    def forward(self, x):
        return x * torch.sigmoid(self.weights.float())


## Define the NN architecture
class MLPNet_large(nn.Module):
    def __init__(self, input_size):
        super(MLPNet_large, self).__init__()
        # Inputs to hidden layer linear transformation
        self.cancelout = CancelOut(input_size)
        self.layer1 = nn.Sequential(
            nn.Linear(input_size, 512),
            nn.LeakyReLU(),
            nn.Dropout(p=0.5, inplace=True),
        )
        self.layer2 = nn.Sequential(
            nn.Linear(512, 256),
            nn.LeakyReLU(),
            nn.Dropout(p=0.5, inplace=True),
        )
        self.layer3 = nn.Sequential(
            nn.Linear(256, 64),
            nn.LeakyReLU(),
            nn.Dropout(p=0.5, inplace=True),
        )
        self.layer4 = nn.Sequential(
            nn.Linear(64, 16),
            nn.LeakyReLU(),
            nn.Dropout(p=0.5, inplace=True),
        )

        self.layer5 = nn.Linear(16, 1)

    def forward(self, x):
        # Pass the input tensor through each of our operations
        x = self.cancelout(x)
        x = self.layer1(x)
        x = self.layer2(x)
        x = self.layer3(x)
        x = self.layer4(x)
        x = self.layer5(x)
        return x


class situNet_ElevaLand(nn.Module):
    def __init__(self, args):
        super(situNet_ElevaLand, self).__init__()

        # attibute
        self.year_embedding = nn.Embedding(20, 4)
        self.day_embedding = nn.Embedding(366, 8)
        self.weekday_embedding = nn.Embedding(8, 4)
        self.month_embedding = nn.Embedding(13, 4)
        self.basin_embedding = nn.Embedding(35, 8)

        self.Elevation_embedding = SpatialEmbedding(out_dim=3)  # origin 20
        self.LandCover_embedding = urbanNet.BasemapNet(out_dim=3)
        # prediction module
        self.mlp = MLPNet_large(54)  # Elevation 3, LandCover 3

        self.loss_function = get_lossFunction(loss_func=args.loss_func)

    def forward(self, attr, config):
        # embedding of month, weekday, basinID
        year_vec = self.year_embedding(attr["yearID"])
        day_vec = self.day_embedding(attr["dayIdx"])
        weekday_vec = self.weekday_embedding(attr["weekday"])
        month_vec = self.month_embedding(attr["month"])
        basin_vec = self.basin_embedding(attr["basinID"])
        attr_t = torch.cat(
            [year_vec, day_vec, weekday_vec, month_vec, basin_vec], 1
        )  # [bs, 28]

        Elevation_vec = self.Elevation_embedding(attr["Elevation"].unsqueeze(1))
        LandCover_vec = self.LandCover_embedding(
            attr["LandCoverImg"]
        )  # out is [bs, 20]

        # connect numerical predictors
        numeric_vector = torch.cat(
            [
                # lon_vec, lat_vec, \
                Elevation_vec,
                LandCover_vec,  # attr["LandCover"], \
                attr["EVI"].unsqueeze(1),
                attr["Fire"].unsqueeze(1),  # attr["Elevation"].unsqueeze(1), \
                attr["AOD"].unsqueeze(1),
                attr["Met"],
                attr["SR"],
                attr["Emission"].unsqueeze(1),
                attr["DistanceToRoads"].unsqueeze(1),
                attr["intercept"].unsqueeze(1),
            ],
            1,
        )  # [bs, 60]

        batchsize = attr["PM2.5"].size()[0]

        full_vector = torch.cat([numeric_vector, attr_t], 1)  # (bs, 74)

        # MLP
        entire_out = self.mlp(full_vector)  # [bs, 1]

        if torch.sum(entire_out != entire_out) > 0:
            print(entire_out.cpu().detach().numpy())

        return entire_out

    def eval_on_batch(self, attr, config):
        if self.training:
            entire_out = self(attr, config)
        else:
            entire_out = self(attr, config)

        label = attr["PM2.5"]  # (16, 7)
        label = label.view(-1, 1)  # [bs] --> [bs, 1]

        loss_ = self.loss_function
        entire_loss = loss_(label, entire_out)

        pred_dict = {"label": label, "pred": entire_out}

        if self.training:
            return pred_dict, entire_loss
        else:
            return pred_dict, entire_loss

    def data_transfer(self, attr, config):
        Elevation_vec = self.Elevation_embedding(attr["Elevation"].unsqueeze(1))
        LandCover_vec = self.LandCover_embedding(
            attr["LandCoverImg"]
        )  # out is [bs, 20]
        ret_vector = torch.cat(
            [
                attr["yearID"].unsqueeze(1),
                attr["dayIdx"].unsqueeze(1),
                attr["weekday"].unsqueeze(1),
                attr["month"].unsqueeze(1),
                attr["Lon"].unsqueeze(1),
                attr["Lat"].unsqueeze(1),
                attr["basinID"].unsqueeze(1),
                Elevation_vec,
                LandCover_vec,
            ],
            1,
        )  # [bs, 60]
        return ret_vector.detach().cpu()

    def universal_data_transfer(self, attr, config):
        Elevation_vec = self.Elevation_embedding(attr["Elevation"].unsqueeze(1))
        LandCover_vec = self.LandCover_embedding(
            attr["LandCoverImg"]
        )  # out is [bs, 20]
        ret_vector = torch.cat(
            [
                attr["Lon"].unsqueeze(1),
                attr["Lat"].unsqueeze(1),
                Elevation_vec,
                LandCover_vec,
            ],
            1,
        )  # [bs, 2+3+3]
        return ret_vector.detach().cpu()
