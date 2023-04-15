import pandas as pd
from datetime import datetime, timedelta
import os
import re

# Combine data into a new table that combines the data as station_from. station_to, total_count, aggregate_seconds
# The data is to be retrieved for each date and stored into their appropriate csv file under year/month/ path

# The dates to process are to be selected within set range

# This code will do the following, generate daily graph data for every day
# from the daily graph data, consolidate them based on a provided range

mappings_path = "./mappings/"

def trips_to_daily_nodes_edges():
    """
    Generates daily node and edge files for each date in the GTFS dataset

    Reads from ./mappings/all_dates_services_schedule.csv, ./mappings/services_to_trips.csv,
           and ./mappings/trips_to_station_nodes_edges.csv
    Generates ./results/daily_files/{date}.csv for every date
    """

    all_dates_services_file_df = pd.read_csv(mappings_path + 'all_dates_services_schedule.csv')
    services_to_trips_file_df = pd.read_csv(mappings_path + 'services_to_trips.csv')
    trips_to_station_graph_file_df = pd.read_csv(mappings_path + 'trips_to_station_nodes_edges.csv')

    # For each date retrieve date -> service -> appropriate trip in timeframe -> nodes_edges
    # retrieve date -> service : 10010101_to_99991231_services_schedule.txt
    # service date -> appropriate trip in timeframe : services_to_trips.csv
    # trips -> graph node & edges : trips_to_station_nodes_edges.csv
    # combine data into a new table that combines the data as station_from. station_to, total_count, aggregate_seconds

    # Generate a mappings folder if it doesn't exist
    if not os.path.exists('results/daily_files'):
        os.makedirs('results/daily_files')

    start_date = pd.to_datetime(all_dates_services_file_df['date'].iloc[0], format='%Y%m%d')
    end_date = pd.to_datetime(all_dates_services_file_df['date'].iloc[-1], format='%Y%m%d')

    date_range = pd.date_range(start=start_date, end=end_date)
    date_range_df = pd.DataFrame({'date': date_range.strftime('%Y%m%d').astype(int)})

    # Loop through each date in the DataFrame and apply the same steps as before
    result_list = []
    for index, row in date_range_df.iterrows():
        date = row['date']
        # Generate a mapping file for future reference
        if not os.path.exists(f'results/daily_files/{date}.csv'):
            
            df_services = all_dates_services_file_df.copy()
            df_services = df_services[df_services['date'] == date]

            df_services_to_trips = services_to_trips_file_df.copy()
            df_services_to_trips = df_services_to_trips[(df_services_to_trips['current_gtfs_date'] <= date) & (df_services_to_trips['next_gtfs_date'] >= date)]
            df_trips = pd.merge(df_services, df_services_to_trips, on="service_id")

            df_trips_to_node_edges = trips_to_station_graph_file_df.copy()
            df_trips_to_node_edges = pd.merge(df_trips, df_trips_to_node_edges, on="trip_id")

            df_trips_to_node_edges = df_trips_to_node_edges.groupby(['station_from', 'station_to']).agg(total_count=('transit_duration', 'count'), aggregate_seconds=('transit_duration', 'sum')).reset_index()
            df_trips_to_node_edges['date'] = date
                
            df_trips_to_node_edges.to_csv(f'results/daily_files/{date}.csv', index=False)
            print(f'results/daily_files/{date}.csv written')
        else:
            print(f'results/daily_files/{date}.csv already exists')