import json
from math import asin, cos, radians, sin, sqrt

config = json.load(open("config.json", "r"))


def geo_distance(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    lon1, lat1, lon2, lat2 = map(radians, map(float, [lon1, lat1, lon2, lat2]))
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371
    return c * r


def normalize(x, key):
    mean = config[key + "_mean"]
    std = config[key + "_std"]
    return (x - mean) / std


def unnormalize(x, key):
    mean = config[key + "_mean"]
    std = config[key + "_std"]
    return x * std + mean
