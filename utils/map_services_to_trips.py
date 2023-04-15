import pandas as pd
from datetime import datetime, timedelta
import os

# Retrieve trips.txt files for each gtfs generation dates
extract_path = "./files/extracted/"
trips_path = "/trips.txt"

def services_to_trips():
    """
    Reads and processes the trips.txt files for all GTFS dates in the extract_path directory.
    Generates a mapping file 'services_to_trips.csv' that maps service_ids to corresponding trip_ids.

    Reads from ./files/extracted/*
    Generates ./mappings/services_to_trips.csv which maps services-to-trips
    """

    # Index dates based on existing dates folder in ./files/extracted/ path
    gtfs_generation_dates = [item for item in os.listdir(extract_path) if os.path.isdir(os.path.join(extract_path, item))]

    # For all valid dates, extract trips taken
    total_trips = pd.DataFrame()
    for i, item in enumerate(gtfs_generation_dates):
        
        # Set the last date for 'next_item' to be highdate
        next_item = gtfs_generation_dates[i+1] if i < len(gtfs_generation_dates)-1 else 99991231
        total_trips = pd.concat([total_trips, process_trips_dates_file(item, next_item)])

    # Clean total_transfers down to columns that are applicable for the logic going forward
    total_transfers = total_trips[['service_id','trip_id','current_gtfs_date','next_gtfs_date']]
    unique_transfers = total_transfers.drop_duplicates()

    # Generate a mappings folder if it doesn't exist
    if not os.path.exists('mappings'):
        os.makedirs('mappings')

    # Generate a mapping file for future reference
    unique_transfers.to_csv('mappings/services_to_trips.csv', index=False)

def process_trips_dates_file(current_gtfs_date, next_gtfs_date):
    """
    Reads the 'trips.txt' file for a given GTFS date, adds the 'current_gtfs_date' and 'next_gtfs_date' columns
    to the resulting DataFrame, and returns it.

    Parameters:
        current_gtfs_date (str): The current GTFS date being processed.
        next_gtfs_date (str): The next GTFS date in the list. If the current date is the last date, this is set to 99991231.

    Returns:
    transfers (pandas.DataFrame): A DataFrame containing the 'service_id', 'trip_id', 'current_gtfs_date', and 'next_gtfs_date' columns.
    """

    # Read the trips.txt file into a DataFrame
    transfers = pd.read_csv(extract_path + current_gtfs_date + trips_path)
    transfers['current_gtfs_date'] = current_gtfs_date
    transfers['next_gtfs_date'] = next_gtfs_date

    return(transfers)