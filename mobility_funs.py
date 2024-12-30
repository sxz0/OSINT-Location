import pandas as pd
import numpy as np
from scipy.stats import gaussian_kde

from utils import compute_distance, compute_precision


def select_first_gateway_ts(json_data):
    all_ts = pd.to_datetime([x["gateways"][0]["rx_time"]["time"] for x in json_data])
    all_gid = [x["gateways"][0]["id"] for x in json_data]
    return all_ts, all_gid


def select_gateway_median_ts(json_data):
    # All ts are converted to utc time
    all_ts = []
    for x in json_data:
        ts = [pd.to_datetime(g["rx_time"]["time"]).value for g in x["gateways"]]
        ts = np.median(ts)
        all_ts.append(ts)
    all_ts = pd.to_datetime(all_ts)
    return all_ts


def parse_lorawan_dataset(json_data, all_ts, all_gid):
    return pd.DataFrame(
        {
            "latitude": [x["latitude"] for x in json_data],
            "longitude": [x["longitude"] for x in json_data],
            "dev_eui": [x["dev_eui"] for x in json_data],
            "sf": [x["sf"] for x in json_data],
            "dev_addr": [x["dev_addr"] for x in json_data],
            "timestamp": all_ts,
            "gateway_id": all_gid,
        }
    )


def parse_gateway_dataset(jd):
    json_data = []
    for x in jd:
        b = True
        x2 = x
        for val in ['data', 'message', 'rx_metadata']:
            if val not in x2:
                b = False
                break
            x2 = x2[val]
        if not b:
            continue
        x2 = x2[0]
        for val in ['location']:
            if val not in x2:
                b = False
                break
        if not b:
            continue
        json_data.append(x)
    return pd.DataFrame(
        {
            "latitude": [x["data"]["message"]["rx_metadata"][0]["location"]["latitude"] for x in json_data],
            "longitude": [x["data"]["message"]["rx_metadata"][0]["location"]["longitude"] for x in json_data],
            #"dev_eui": [x["dev_eui"] for x in json_data],
            "dev_addr": [x["data"]["message"]["payload"]["mac_payload"]["f_hdr"]["dev_addr"] for x in json_data],
            #"dev_addr": [x["identifiers"][0]["gateway_ids"]["eui"] for x in json_data],
            "timestamp": pd.to_datetime([x["time"] for x in json_data]),
            "gateway_id": [x["identifiers"][0]["gateway_ids"]["gateway_id"] for x in json_data],
        }
    )


def get_by_measurement_dataset(df, key_device="dev_addr"):
    df = df.sort_values([key_device, "timestamp"])
    df["day"] = df["timestamp"].dt.date
    df["dow"] = df["timestamp"].dt.day_name()

    # Get start and end measures
    df["start_time"] = df.groupby([key_device, "day"])[["timestamp"]].transform(min)
    df["end_time"] = df.groupby([key_device, "day"])[["timestamp"]].transform(max)

    df_lls = df.loc[(df.start_time == df.timestamp)]
    df_lle = df.loc[(df.end_time == df.timestamp)]
    df = df.merge(
        df_lls[[key_device, "day", "latitude", "longitude"]],
        how="left",
        on=[key_device, "day"],
        suffixes=["", "_start"],
    )
    df = df.merge(
        df_lle[[key_device, "day", "latitude", "longitude"]],
        how="left",
        on=[key_device, "day"],
        suffixes=["", "_end"],
    )

    df["start_time"] = df["start_time"].dt.time
    df["end_time"] = df["end_time"].dt.time

    df["idx"] = df.groupby([key_device, "day"]).cumcount()
    df["idx2"] = df["idx"] - 1

    dist = df[["idx", key_device, "day", "latitude", "longitude", "timestamp"]].merge(
        df[["idx2", key_device, "day", "latitude", "longitude", "timestamp"]],
        how="inner",
        left_on=[key_device, "day", "idx"],
        right_on=[key_device, "day", "idx2"],
    )
    dist["distance"] = [
        compute_distance((la1, lo1), (la2, lo2))
        for la1, lo1, la2, lo2 in zip(
            dist["latitude_x"],
            dist["longitude_x"],
            dist["latitude_y"],
            dist["longitude_y"],
        )
    ]

    df = df.merge(
        dist[[key_device, "day", "idx", "distance"]],
        how="left",
        on=[key_device, "day", "idx"],
    )
    del df["idx2"]
    del df["idx"]
    return df


def get_mean_std_time_by_day(df_day, key_device='dev_addr'):
    df_day['start_time'] = pd.to_timedelta(df_day['start_time'].astype(str)).dt.total_seconds() * 1e9
    df_day['end_time'] = pd.to_timedelta(df_day['end_time'].astype(str)).dt.total_seconds() * 1e9
    df_time = df_day.groupby(key_device).agg({x: ['mean', 'std'] for x in ['start_time', 'end_time']})
    df_time.columns = ["_".join(col_name).rstrip('_') for col_name in df_time.columns]
    df_time = df_time.reset_index()
    for col in ['start_time_mean', 'end_time_mean', 'start_time_std', 'end_time_std']:
        df_time[col] = pd.to_datetime(df_time[col]).dt.time
    return df_time


def get_qtime_by_day(df_day, key_device='dev_addr', quantiles=None):
    if quantiles is None:
        quantiles = [0.5]
    df_temp = df_day.copy()
    df_temp['start_time'] = pd.to_timedelta(df_temp['start_time'].astype(str)).dt.total_seconds() * 1e9
    df_temp['end_time'] = pd.to_timedelta(df_temp['end_time'].astype(str)).dt.total_seconds() * 1e9
    df_time = df_temp.groupby(key_device)[['start_time', 'end_time']].quantile(quantiles)
    df_time = df_time.reset_index()
    df_time.columns = [key_device, 'quantile', 'start_time', 'end_time']
    df_count = df_temp.groupby(key_device, as_index=False)['start_time'].count()
    df_count['N'] = df_count['start_time']
    df_time = df_count[[key_device, 'N']].merge(df_time, how='left', on=key_device)
    for col in ['start_time', 'end_time']:
        df_time[col] = pd.to_datetime(df_time[col]).dt.time
    return df_time


def get_qll_by_day(df_day, key_device='dev_addr', quantiles=None):
    if quantiles is None:
        quantiles = [0.25, 0.5, 0.75]
    df_ll = df_day.groupby(key_device)[['latitude_start', 'longitude_start', 'latitude_end', 'longitude_end']].quantile(quantiles)
    df_ll = df_ll.reset_index()
    df_ll.columns = [key_device, 'quantile', 'latitude_start', 'longitude_start', 'latitude_end', 'longitude_end']
    df_count = df_day.groupby(key_device, as_index=False)['latitude_start'].count()
    df_count['N'] = df_count['latitude_start']
    df_ll = df_count[[key_device, 'N']].merge(df_ll, how='left', on=key_device)
    return df_ll


def get_by_day_dataset(df, key_device='dev_addr'):
    df_day = df.groupby(
        [
            key_device,
            "day",
            "dow",
            "start_time",
            "end_time",
            "latitude_start",
            "longitude_start",
            "latitude_end",
            "longitude_end",
        ],
        as_index=False,
    ).agg({"distance": "sum"})
    df_day['time'] = (pd.to_timedelta(df_day['end_time'].astype(str)) - pd.to_timedelta(df_day['start_time'].astype(str))).astype('int') / 1.0e9
    df_day['speed'] = 0
    df_day.loc[df_day.time > 0, 'speed'] = df_day.loc[df_day.time > 0, 'distance'] * 3.6 / (df_day.loc[df_day.time > 0, 'time'])
    return df_day

def get_speed_by_day(df_day, key_device='dev_addr'):
    df_speed = df_day.copy()
    df_speed = df_speed.groupby(key_device)[['speed']].quantile([1.0])
    df_speed = df_speed.reset_index()
    df_speed.columns = [key_device, 'quantile', 'speed']
    df_count = df_day.groupby(key_device, as_index=False)['latitude_start'].count()
    df_count['N'] = df_count['latitude_start']
    df_speed = df_count[[key_device, 'N']].merge(df_speed, how='left', on=key_device)
    return df_speed


def get_dist_by_day(df_day, key_device='dev_addr'):
    df_dist = df_day.copy()
    df_dist = df_dist.groupby(key_device)[['distance']].quantile([1.0])
    df_dist = df_dist.reset_index()
    df_dist.columns = [key_device, 'quantile', 'distance']
    df_count = df_day.groupby(key_device, as_index=False)['latitude_start'].count()
    df_count['N'] = df_count['latitude_start']
    df_dist = df_count[[key_device, 'N']].merge(df_dist, how='left', on=key_device)
    return df_dist


def get_weekday_perc(df_day, key_device='dev_addr'):
    df_temp = df_day.copy()
    df_temp['weekend'] = (df_temp['dow'].isin(['Saturday', 'Sunday']))
    df_wd = df_temp.groupby([key_device,'weekend'], as_index=False)[['day']].count()
    df_count = df_wd.groupby([key_device], as_index=False)['day'].sum()
    df_count['N'] = df_count['day']
    df_wd = df_wd.merge(df_count[[key_device, 'N']], on='dev_addr')
    df_wd = df_wd.loc[df_wd.weekend == False]
    df_wd['perc_weekday'] = df_wd['day'] / df_wd['N'] * 100
    del df_wd['day']
    del df_wd['weekend']
    return df_wd


def get_processed_list(df_day, key_device, key_value, precision=5):
    df_list = df_day.copy().loc[df_day[key_device] == key_value]
    df_list['N'] = 1
    lprec = compute_precision(precision)
    try:
        kde_start = gaussian_kde(df_list[['latitude_start', 'longitude_start']].values.T)
        for col in ['latitude_start', 'longitude_start']:
            df_list[col] = np.round(df_list[col] / lprec) * lprec
        df_list_start = df_list.groupby([key_device, 'latitude_start', 'longitude_start'], as_index=False)['N'].sum()
        df_list_start['pdf'] = kde_start(df_list_start[['latitude_start', 'longitude_start']].values.T)
        df_list_start = df_list_start.sort_values('pdf', ascending=False)
    except Exception:
        df_list_start = df_list[[key_device, 'latitude_start', 'longitude_start']]
        df_list_start['pdf'] = 1
    try:
        kde_end = gaussian_kde(df_list[['latitude_end', 'longitude_end']].values.T)
        for col in ['latitude_end', 'longitude_end']:
            df_list[col] = np.round(df_list[col] / lprec) * lprec
        df_list_end = df_list.groupby([key_device, 'latitude_end', 'longitude_end'], as_index=False)['N'].sum()
        df_list_end['pdf'] = kde_end(df_list_end[['latitude_end', 'longitude_end']].values.T)
        df_list_end = df_list_end.sort_values('pdf', ascending=False)
    except Exception:
        df_list_end = df_list[[key_device, 'latitude_end', 'longitude_end']]
        df_list_end['pdf'] = 1
    return df_list_start, df_list_end