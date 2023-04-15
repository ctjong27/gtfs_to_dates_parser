import pandas as pd
from datetime import datetime, timedelta
import os
import re

extract_path = "./files/extracted/"
transfers_path = "/transfers.txt"
def stops_to_stations():
    """
    This function creates a mapping between stops and stations from the transfer information in the GTFS files.

    Reads from ./files/extracted/*
    Generates ./mappings/stops_to_stations.csv and ./mappings/stations_to_stops.csv to allow for bidirectional indexing

    This consolidates stops into stations that can be used as the base unit for graph analysis
    This is because different lines may use different tracks (or stops) but still share the same station at a location
    """
    
    # Index dates based on existing dates folder in ./files/extracted/ path
    gtfs_generation_dates = [item for item in os.listdir(extract_path) if os.path.isdir(os.path.join(extract_path, item))]

    total_transfers = pd.DataFrame()
    # Set the path to the GTFS files
    for gtfs_date in gtfs_generation_dates:
        # calendar_file = folder_path + filename # example: calendar_20181221.txt'
        total_transfers = pd.concat([total_transfers, process_transfers_dates_file(gtfs_date)])
        
    print(total_transfers[total_transfers['transfer_type'] == 2].shape)
    print(total_transfers[total_transfers['transfer_type'] != 2].shape)

    # Retrieve unique calendar dates from aggregation of calendar_dates.txt
    total_transfers = total_transfers[['from_stop_id','to_stop_id']]
    transfers_df = total_transfers.drop_duplicates()
    transfers_df = transfers_df.sort_values(by=['from_stop_id', 'to_stop_id'])

    # create a dictionary to store the distinct stations and their corresponding stops
    stations = {}

    # loop through each row of the transfers dataframe
    for index, row in transfers_df.iterrows():
        from_stop_id = row['from_stop_id']
        to_stop_id = row['to_stop_id']
        
        # add from_stop_id to stations dictionary if it doesn't already exist
        if from_stop_id not in stations:
            stations[from_stop_id] = [from_stop_id]
        
        # add to_stop_id to the corresponding station in the stations dictionary if it doesn't already exist
        if to_stop_id not in stations[from_stop_id]:
            stations[from_stop_id].append(to_stop_id)
        
        # if to_stop_id already exists in stations dictionary, add any stops in the from_stop_id station to the to_stop_id station
        for station, stops in stations.items():
            if to_stop_id in stops and from_stop_id not in stops:
                stops.append(from_stop_id)
            elif from_stop_id in stops and to_stop_id not in stops:
                stops.append(to_stop_id)

    # create a list to store the station and stop data
    station_stop_list = []

    # loop through the stations dictionary and add the data to the station_stop_list
    for station in stations:
        station_stops = sorted(stations[station])
        if not any(d['stop_id'] == station_stops for d in station_stop_list):
            station_stop_list.append({'station_id': station, 'stop_id': station_stops})

    # create a pandas dataframe from the station_stop_list
    station_stop_df = pd.DataFrame(station_stop_list)

    # create a list to store the stop and station data
    stop_station_list = []

    # loop through the station_stop_df dataframe and add the data to the stop_station_list
    for index, row in station_stop_df.iterrows():
        station = row['station_id']
        stops = row['stop_id']
        for stop in stops:
            stop_station_list.append([stop, station])

    # create a pandas dataframe from the stop_station_list
    stop_station_df = pd.DataFrame(stop_station_list, columns=['stop_id', 'station_id'])

    # Generate a mappings folder if it doesn't exist
    if not os.path.exists('mappings'):
        os.makedirs('mappings')

    # Generate a mapping file for future reference
    station_stop_df.to_csv('mappings/stations_to_stops.csv', index=False)

    # Generate a mapping file for future reference
    stop_station_df.to_csv('mappings/stops_to_stations.csv', index=False)


def process_transfers_dates_file(gtfs_generation_date):
    """
    Reads the transfers.txt file for a specific GTFS generation date and returns it as a pandas DataFrame.
    
    Parameters:
    gtfs_generation_date (str): The GTFS generation date for which to read the transfers.txt file.
    
    Returns:
    pandas.DataFrame: The transfers.txt file as a pandas DataFrame.
    """

    # Read the calendar_dates.txt file into a DataFrame
    transfers = pd.read_csv(extract_path + gtfs_generation_date + transfers_path)

    return(transfers)