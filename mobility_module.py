"""Reads the mobility datasets and generates the following JSONs:

* Metrics by device
{
    '[device_id]': {
        'start_time': ...  # Median value
        'end_time': ...
        'weekday_perc': ...
        'speed': ...  # Max value (daily)
        'distance': ...  # Max value (daily)
    }
}
* Metrics by device and day
{
    '[device_id]': {
        '[date]': {
            'start_time': ...
            'end_time': ...
            'dow': ...
            'speed': ...
            'distance': ...  # Total
        }
    }
}
* List of possible locations for start and end points
{
    '[device_id]': {
        'start': {
            'lat': [],
            'lon': [],
            'confidence_scores': []
        },
        'end': {
            ...
        }
        'first_ts': ...
        'last_ts': ...
    }
}
* Trajectory for a particular day and device
{
    '[device_id]': {
        '[date]': {
            'lat': [],
            'lon': [],
            'ts': []
        }
    }
}
"""
import argparse
import json
import os
import pandas as pd

import mobility_funs as mf


def mobility(json_data, device=None, date=None):
    device_json = {}
    dev_day_json = {}
    list_json = {}
    trajectory_json = {}

    key_device = "dev_addr"

    mobility=""

    if "longitude" in json_data[0].keys():
        mobility="lorawan"
    else:
        mobility="gateway"

    # Parse dataset
    if mobility == 'gateway':
        df = mf.parse_gateway_dataset(json_data)
    elif mobility == 'lorawan':
        all_ts, all_gid = mf.select_first_gateway_ts(json_data)
        df = mf.parse_lorawan_dataset(json_data, all_ts, all_gid)

    # Construct dataframe with all timestamp instances
    df = mf.get_by_measurement_dataset(df, key_device)

    # Filter data
    if device is not None:
        df = df.loc[df[key_device] == device]
    if date is not None:
        df = df.loc[(df.timestamp > date) & (df.timestamp < str(pd.to_datetime(date) + pd.DateOffset(1)))]

    # Construct dataframe with metrics aggregated by day
    df_day = mf.get_by_day_dataset(df, key_device)

    # Construct dataframe with global metrics of the devices
    df_time = mf.get_qtime_by_day(df_day, key_device)
    df_dist = mf.get_dist_by_day(df_day, key_device)
    df_speed = mf.get_speed_by_day(df_day, key_device)
    df_wd = mf.get_weekday_perc(df_day, key_device)
    df_device = df_time[[key_device, 'N', 'start_time', 'end_time']].merge(
        df_dist[[key_device, 'distance']], on=key_device
    ).merge(
        df_speed[[key_device, 'speed']], on=key_device
    ).merge(
        df_wd[[key_device, 'perc_weekday']], on=key_device
    )

    # Create JSON file for global metrics of the devices
    for i in range(len(df_device)):
        row = df_device.iloc[i]
        device_json[row[key_device]] = {
            'N': int(row['N']),
            'start_time': str(row['start_time']),
            'end_time': str(row['end_time']),
            'speed': row['speed'],
            'perc_weekday': row['perc_weekday']
        }
        device_json[row[key_device]]['N'] = int(device_json[row[key_device]]['N'])
    
    # Create JSON file for day metrics of the devices
    for i in range(len(df_day)):
        row = df_day.iloc[i]
        dev = row[key_device]
        if dev not in dev_day_json:
            dev_day_json[dev] = {}
        day = str(row['day'])
        dev_day_json[dev][day] = {
            'start_time': str(row['start_time']),
            'end_time': str(row['end_time']),
            'dow': row['dow'],
            'speed': row['speed'],
            'distance': row['distance']
        }
    
    # For every device:
    #  - construct start/end list of points (and JSON)
    #  - construct trajectories per day (and JSON)
    for dev_addr_ex in df_device[key_device].unique():
        # List of points
        df_list_start, df_list_end = mf.get_processed_list(df_day, key_device, dev_addr_ex, 100)
        list_json[dev_addr_ex] = {
            'start': {
                'lat': df_list_start['latitude_start'].values.tolist(),
                'lon': df_list_start['longitude_start'].values.tolist(),
                'confidence_scores': df_list_start['pdf'].values.tolist()
            },
            'end': {
                'lat': df_list_end['latitude_end'].values.tolist(),
                'lon': df_list_end['longitude_end'].values.tolist(),
                'confidence_scores': df_list_end['pdf'].values.tolist()
            },
            'first_ts': str(df.loc[df[key_device] == dev_addr_ex, 'timestamp'].min()),
            'last_ts': str(df.loc[df[key_device] == dev_addr_ex, 'timestamp'].max())
        }
        # Trajectories
        trajectory_json[dev_addr_ex] = {}
        for day in df_day['day'].unique():
            dft = df.loc[(df[key_device] == dev_addr_ex) & (df.timestamp > str(day)) & (df.timestamp < str(day + pd.DateOffset(1)))]
            trajectory_json[dev_addr_ex][str(day)] = {
                'lat': dft['latitude'].values.tolist(),
                'lon': dft['longitude'].values.tolist(),
                'ts': dft['timestamp'].astype(str).values.tolist(),
            }

    # Save all the JSONs
    os.makedirs(f'{mobility}_outputs', exist_ok=True)
    with open(f'{mobility}_outputs/device.json', 'w', encoding='utf-8') as f:
        json.dump(device_json, f, ensure_ascii=False, indent=4)
    with open(f'{mobility}_outputs/device_day.json', 'w', encoding='utf-8') as f:
        json.dump(dev_day_json, f, ensure_ascii=False, indent=4)
    with open(f'{mobility}_outputs/trajectories.json', 'w', encoding='utf-8') as f:
        json.dump(trajectory_json, f, ensure_ascii=False, indent=4)
    with open(f'{mobility}_outputs/list_start_end_locs.json', 'w', encoding='utf-8') as f:
        json.dump(list_json, f, ensure_ascii=False, indent=4)
    
    return device_json, dev_day_json, trajectory_json, list_json


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', required=True, default='gateway', choices=['lorawan','gateway'])
    parser.add_argument('--device', type=str)
    parser.add_argument('--date', type=str)
    args = parser.parse_args()
    mobility(**args.__dict__)