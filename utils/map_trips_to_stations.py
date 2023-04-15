import pandas as pd
from datetime import datetime, timedelta
import os
import numpy as np


# retrieve stop_times.txt files for each gtfs generation dates
extract_path = "./files/extracted/"
stop_times_path = "/stop_times.txt"
stops_path = "/stops.txt"
gtfs_generation_dates = [item for item in os.listdir(extract_path) if os.path.isdir(os.path.join(extract_path, item))]

def trips_to_stations():
    """
    Creates a trip to stops mapping table from all gtfs files

    Reads from ./files/extracted/*
    Also reads from ./mappings/services_to_trips.csv and ./mappings/stops_to_stations.csv
    Generates trips_to_station_nodes_edges.csv which maps trips-to-stations by graph theory layout

    Returns:
    station_graph_df (pandas.DataFrame): Dataframe containing trip to stations mapping table
    """

    # Retrieve data
    mappings_path = "./mappings/"
    services_to_trips_df = pd.read_csv(mappings_path + 'services_to_trips.csv')
    stops_to_stations_df = pd.read_csv(mappings_path + 'stops_to_stations.csv')
    stop_times_df = process_stop_times_file()
    stops_df = process_stops_file()

    # Merge the stops dataframe into the stop_times dataframe on the stop_id column
    parent_stop_times_df = pd.merge(stop_times_df, stops_df[['stop_id', 'parent_station']], on='stop_id')

    # Replace the stop_id column with the parent_station column
    parent_stop_times_df['stop_id'] = parent_stop_times_df['parent_station']

    # Drop the parent_station column, which is no longer needed
    parent_stop_times_df = parent_stop_times_df.drop('parent_station', axis=1)

    # Merge the stop_times_df dataframe into the parent_stop_times_df dataframe on the stop_id column
    station_times_df = pd.merge(parent_stop_times_df, stops_to_stations_df[['stop_id', 'station_id']], on='stop_id')

    # Drop the stop_id column, which is no longer needed
    station_times_df = station_times_df.drop('stop_id', axis=1) \
        .sort_values(['trip_id','stop_sequence']) \
        .reset_index(drop=True)

    # Convert time to timedelta object
    station_times_df['departure_time'] = pd.to_timedelta(station_times_df['departure_time'])

    # Add 1 day to the time with hour component > 23
    station_times_df.loc[station_times_df['departure_time'] > pd.Timedelta(hours=23), 'departure_time'] += pd.Timedelta(days=1)

    # Convert timedelta to total number of seconds
    station_times_df['departure_time_seconds'] = station_times_df['departure_time'].dt.total_seconds()
    station_graph_df = station_times_df.copy()

    # Create a new column for station_to by shifting station_id by 1
    station_graph_df['station_to'] = station_graph_df['station_id'].shift(-1)

    # Set station_to to NaN where trip_id changes
    station_graph_df.loc[station_graph_df['trip_id'] != station_graph_df['trip_id'].shift(-1), 'station_to'] = np.nan

    # Calculate transit_duration as the difference between departure_time at station_to and station_from
    station_graph_df['transit_duration'] = station_graph_df['departure_time_seconds'].shift(-1) - station_graph_df['departure_time_seconds']

    station_graph_df['station_from'] = station_graph_df['station_id']

    # Select only the columns we want in the final output
    station_graph_df = station_graph_df[['trip_id', 'station_from', 'station_to', 'transit_duration']]
    station_graph_df.dropna(subset=['station_to'], inplace=True)
    station_graph_df['transit_duration'] = station_graph_df['transit_duration'].round(0).astype(int)

    # Generate a mappings folder if it doesn't exist
    if not os.path.exists('mappings'):
        os.makedirs('mappings')

    # Generate a mapping file for future reference
    station_graph_df.to_csv("mappings/trips_to_station_nodes_edges.csv", index=False)

def process_stop_times_file():
    """
    Processes the stop_times.txt files for each gtfs generation dates and concatenates them into a single dataframe
    
    Returns:
    agg_stop_times (pandas.DataFrame): Dataframe containing trip_id, departure_time, stop_id, and stop_sequence columns
    """

    agg_stop_times = pd.DataFrame()
    for gtfs_date in gtfs_generation_dates:

        # Read the stop_times.txt file into a DataFrame
        stop_times = pd.read_csv(extract_path + gtfs_date + stop_times_path)
        agg_stop_times = pd.concat([agg_stop_times, stop_times])

    agg_stop_times = agg_stop_times[['trip_id','departure_time','stop_id','stop_sequence']] \
        .drop_duplicates()
    
    return(agg_stop_times)

# reduces total number from 31593819 to 10850498 

def process_stops_file():
    """
    Processes the stops.txt files for each gtfs generation dates and concatenates them into a single dataframe
    
    Returns:
    agg_stops (pandas.DataFrame): Dataframe containing stop_id and parent_station columns
    """
    
    agg_stops = pd.DataFrame()
    for gtfs_date in gtfs_generation_dates:

        # Read the stop_times.txt file into a DataFrame
        stops = pd.read_csv(extract_path + gtfs_date + stops_path)
        agg_stops = pd.concat([agg_stops, stops])

    agg_stops = agg_stops[['stop_id','parent_station']] \
        .drop_duplicates()
    return(agg_stops)