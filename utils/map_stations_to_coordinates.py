import pandas as pd
from datetime import datetime, timedelta
import os

# retrieve stop_times.txt files for each gtfs generation dates
extract_path = "./files/extracted/"
stops_path = "/stops.txt"
gtfs_generation_dates = [item for item in os.listdir(extract_path) if os.path.isdir(os.path.join(extract_path, item))]

def stations_to_coordinates():
    # Read in the stations_to_stops.csv file to a DataFrame
    stations_to_stops_df = pd.read_csv('mappings/stations_to_stops.csv')

    stops_df = process_stops_file()

    # Merge the stops_df and stations_to_stops_df DataFrames on the stop_id column
    # merged_df = pd.merge(stops_df, stations_to_stops_df, on='stop_id')
    merged_df = pd.merge(stops_df, stations_to_stops_df, left_on=['stop_id'], right_on=['station_id'])

    merged_df = merged_df[['stop_lat','stop_lon','station_id']]

    # Group the merged_df DataFrame by the station_id column and aggregate the latitude and longitude columns by taking the mean
    coordinates_df = merged_df.groupby('station_id')[['stop_lat', 'stop_lon']].mean()

    # Reset the index to make station_id a column instead of an index
    coordinates_df = coordinates_df.reset_index()

    # Generate a mappings folder if it doesn't exist
    if not os.path.exists('mappings'):
        os.makedirs('mappings')

    # Generate a mapping file for future reference
    coordinates_df.to_csv("mappings/stations_to_coordinates.csv", index=False)


def process_stops_file():

    agg_stops = pd.DataFrame()
    for gtfs_date in gtfs_generation_dates:

        # Read the stop_times.txt file into a DataFrame
        stops = pd.read_csv(extract_path + gtfs_date + stops_path)
        agg_stops = pd.concat([agg_stops, stops])

    agg_stops = agg_stops[['stop_id','parent_station','stop_lat','stop_lon']] \
        .drop_duplicates()
    return(agg_stops)