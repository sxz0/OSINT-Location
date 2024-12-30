import requests
import argparse
import pandas as pd
import json

# headers = {
#     'Authorization': 'Bearer [YOUR_ACCESS_TOKEN]',
#     'Accept': 'application/json',
# }

# curl \
#   'https://youtube.googleapis.com/youtube/v3/search?part=snippet&location=38.26337%2C-0.73723&locationRadius=1km&publishedAfter=2022-10-03T00%3A00%3A00Z&publishedBefore=2022-10-06T00%3A00%3A00Z&q=lora&type=video&key=[YOUR_API_KEY]' \
#   --header 'Authorization: Bearer [YOUR_ACCESS_TOKEN]' \
#   --header 'Accept: application/json' \
#   --compressed


def parse_youtube(latitude=None, longitude=None, date=None):
    api_key = ''
    
    if date is None:
        date = '2022-10-05'
    if latitude is None:
        latitude = str(38.263375)
    if longitude is None:
        longitude = str(-0.737125)

    date_next = str((pd.to_datetime(date) + pd.DateOffset(1)).date())
    
    response = requests.get(f'https://youtube.googleapis.com/youtube/v3/search?part=snippet&location={latitude}%2C{longitude}&locationRadius=1km&publishedAfter={date}T00%3A00%3A00Z&publishedBefore={date_next}T00%3A00%3A00Z&q=lora&type=video&key={api_key}')
    return response.json()

def process_yb_json(json_data):
    proc_json = [{
        'link': f"https://www.youtube.com/watch?v={item['id']['videoId']}",
        'title': item['snippet']['title'],
        'description': item['snippet']['description'],
        'thumb_img': item['snippet']['thumbnails']['high']['url'],
        'channel_owner': item['snippet']['channelTitle'],
        'publish_time': item['snippet']['publishTime'],
    } for item in json_data['items']]
    with open(f'gateway_outputs/youtube.json', 'w', encoding='utf-8') as f:
        json.dump(proc_json, f, ensure_ascii=False, indent=4)
    return proc_json


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    args = parser.parse_args()
    json_data = parse_youtube()
    proc_json = process_yb_json(json_data)
