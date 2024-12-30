import numpy as np


def compute_distance(p1, p2):
    from math import sin, cos, sqrt, atan2, radians

    # approximate radius of earth in m
    R = 6373000
    lat1, lon1 = p1
    lat2, lon2 = p2

    lat1 = radians(lat1)
    lon1 = radians(lon1)
    lat2 = radians(lat2)
    lon2 = radians(lon2)

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    return R * c


def compute_precision(meters):
    from math import sin, cos, sqrt, atan2, radians

    # approximate radius of earth in m
    R = 6373000
    P = 2 * np.pi * R

    return 360 / P * meters
