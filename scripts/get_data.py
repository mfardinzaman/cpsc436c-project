# get_data.py

import os
import requests
from google.transit import gtfs_realtime_pb2

def get_data():
    api_key = os.getenv("ApiKey")
    trip_url = "https://gtfsapi.translink.ca/v3/gtfsrealtime?apikey=" + api_key
    position_url = "https://gtfsapi.translink.ca/v3/gtfsposition?apikey=" + api_key
    alerts_url = "https://gtfsapi.translink.ca/v3/gtfsalerts?apikey=" + api_key
    
    feed = gtfs_realtime_pb2.FeedMessage()
    response = requests.get(trip_url)
    if (response.ok):
        feed.ParseFromString(response.content)
        print("---------------- TRIP UPDATE ----------------")
        print(feed.entity[0])
        
    response = requests.get(position_url)
    if (response.ok):
        feed.ParseFromString(response.content)
        print("---------------- POSITION UPDATE ----------------")
        print(feed.entity[0])
        
    response = requests.get(alerts_url)
    if (response.ok):
        feed.ParseFromString(response.content)
        print("---------------- ALERTS ----------------")
        print(feed.entity[0])
        
    return


if __name__ == "__main__":
    get_data()