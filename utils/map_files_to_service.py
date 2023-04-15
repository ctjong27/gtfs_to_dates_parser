import pandas as pd
from datetime import datetime, timedelta
import os
import re

extract_path = "./files/extracted/"
calendar_path = "/calendar.txt"
calendar_dates_path = "/calendar_dates.txt"

def files_to_service_schedule():
    """
    Function to extract data from ./files/extracted/[date]/calendar.txt and ./files/extracted/[date]/calendar_dates.txt files
    and combines them to create a service schedule.

    It saves the resulting schedule to a CSV file named 'all_dates_services_schedule.csv' in the 'mappings' folder.
    
    Reads files from ./files/extracted/*
    Generates ./mappings/all_dates_service_schedule.csv which maps dates-to-services, for every day
    The code goes through every single date as to ensure that no gtfs feed data is overlooked
    """
    
    # Index dates based on existing dates folder in ./files/extracted/ path
    gtfs_generation_dates = [item for item in os.listdir(extract_path) if os.path.isdir(os.path.join(extract_path, item))]

    df_calendar = pd.DataFrame()
    df_calendar_dates = pd.DataFrame()

    # Set the path to the GTFS files
    for gtfs_date in gtfs_generation_dates:
        # calendar_file = folder_path + filename # example: calendar_20181221.txt'
        df_calendar = pd.concat([df_calendar, process_calendar_file(gtfs_date)])
        df_calendar_dates = pd.concat([df_calendar_dates, process_calendar_dates_file(gtfs_date)])

    # Group the DataFrame by date, service_id
    calendar_grouped = df_calendar.groupby(['date', 'service_id'])

    # Use the apply method to filter the DataFrame and select the rows with the maximum gtfs_generation_date for each group
    calendar_result = calendar_grouped.apply(lambda x: x[x['gtfs_generation_date'] == x['gtfs_generation_date'].max()])

    # Reset the index of the result DataFrame
    calendar_result = calendar_result.reset_index(drop=True)

    # Select the columns of interest
    calendar_result = calendar_result[['date', 'service_id', 'gtfs_generation_date']]

    # Retrieve unique calendar dates from aggregation of calendar_dates.txt
    calendar_dates_unique_rows = df_calendar_dates.drop_duplicates(subset=['date', 'service_id', 'exception_type'])

    # Calendar dates exception types decodes
    # 1 - Service has been added for the specified date.
    # 2 - Service has been removed for the specified date.

    # Iterate through calendar_dates dataframe and make appropriate chnge to calendar_result
    for index, row in calendar_dates_unique_rows.iterrows():
        service_id = row['service_id']
        exception_type = row['exception_type']
        
        
        # If exception type is 1, add service for specified date
        if exception_type == 1:
            date = row['date']
            new_row = {'service_id': service_id, 'date': date, 'exception_type': exception_type}
            print(new_row)
            # calendar_result = calendar_result.append(new_row, ignore_index=True)
            calendar_result = pd.concat([calendar_result, pd.DataFrame(new_row, index=[0])])

        # If exception type is 2, remove service for specified date
        elif exception_type == 2:
            date = row['date']
            calendar_result = calendar_result[~((calendar_result['service_id'] == service_id) & (calendar_result['date'] == date))]

    # Clean calendar_result
    calendar_result.reset_index(drop=True)
    calendar_result = calendar_result[['date','service_id']]

    # sort the DataFrame by the date column in ascending order
    calendar_result = calendar_result.sort_values(by=['date','service_id'])

    # All dates' values will be stored in all_dates_services_schedule.csv
    if not os.path.exists('mappings'):
        os.makedirs('mappings')
    calendar_result.to_csv(f'mappings/all_dates_services_schedule.csv', index=False)


def process_calendar_file(gtfs_generation_date):
    """
    This function reads the calendar.txt file for a given GTFS generation date and generates a DataFrame with the service ID for each date.

    Parameter:
        gtfs_generation_date (str): The GTFS generation date (in YYYYMMDD format) for which to process the calendar.txt file.
    
    Returns:
        dates (DataFrame): A DataFrame with columns 'date', 'service_id', and 'gtfs_generation_date' that lists the service ID for each date in the calendar.txt file.
    """
    
    # Read the calendar.txt file into a DataFrame
    calendar = pd.read_csv(extract_path + gtfs_generation_date + calendar_path)

    # Convert the start_date and end_date columns to datetime objects
    calendar['start_date'] = pd.to_datetime(calendar['start_date'], format='%Y%m%d')
    calendar['end_date'] = pd.to_datetime(calendar['end_date'], format='%Y%m%d')

    # Generate a DataFrame with the service ID for each date
    dates = pd.DataFrame(columns=['date', 'service_id', 'gtfs_generation_date'])
    for _, row in calendar.iterrows():
        start_date = row['start_date']
        end_date = row['end_date']
        service_days = [int(x) for x in row[1:8]]
        service_id = row['service_id']
        for i in range((end_date - start_date).days + 1):
            date = start_date + timedelta(days=i)
            if service_days[date.weekday()] == 1:
                new_row = {'date': int(date.strftime('%Y%m%d')), 'service_id': service_id, 'gtfs_generation_date': gtfs_generation_date}
                # date will be retrieved as a int64
                # new_row = {'date': date, 'service_id': service_id, 'gtfs_generation_date': calendar_file[17:25]}
                dates = pd.concat([dates, pd.DataFrame(new_row, index=[0])])

    return(dates)

def process_calendar_dates_file(gtfs_generation_date):
    """
    This function reads the calendar_dates.txt file for a given GTFS generation date.

    Args:
        gtfs_generation_date (str): The GTFS generation date (in YYYYMMDD format) for which to process the calendar_dates.txt file.
    
    Returns:
        calendar_dates (DataFrame): A DataFrame with the contents of the calendar_dates.txt file for the given GTFS generation date.
    """
    # Read the calendar_dates.txt file into a DataFrame
    calendar_dates = pd.read_csv(extract_path + gtfs_generation_date + calendar_dates_path)

    return(calendar_dates)