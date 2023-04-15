import pandas as pd
from datetime import datetime, timedelta
import os

def aggregate_from_daterange(search_start_year, search_start_month, search_start_day,
                             search_end_year, search_end_month, search_end_day):
    """
    This function aggregates data from a range of dates specified by the input parameters.
    
    Parameters:
        search_start_year (int): Year of the start date for the search.
        search_start_month (int): Month of the start date for the search.
        search_start_day (int): Day of the start date for the search.
        search_end_year (int): Year of the end date for the search.
        search_end_month (int): Month of the end date for the search.
        search_end_day (int): Day of the end date for the search.
    """

    # Configurations
    CALCULATE_DATE_RANGE = True

    search_start_date = search_start_year*10000 + search_start_month*100 + search_start_day
    search_end_date   = search_end_year*10000 + search_end_month*100 + search_end_day

    queried_start_date = pd.to_datetime(search_start_date, format='%Y%m%d')
    queried_end_date = pd.to_datetime(search_end_date, format='%Y%m%d')

    queried_date_range = pd.date_range(start=queried_start_date, end=queried_end_date)
    queried_date_range_df = pd.DataFrame({'date': queried_date_range.strftime('%Y%m%d').astype(int)})
    queried_date_range_df

    ################################################################
    mappings_path = "./mappings/"
    # services_schedule_file_name = f"{SEARCH_START_DATE}_to_{SEARCH_END_DATE}_services_schedule.csv"
    all_dates_services_file_df = pd.read_csv(mappings_path + 'all_dates_services_schedule.csv')
    services_to_trips_file_df = pd.read_csv(mappings_path + 'services_to_trips.csv')
    trips_to_station_graph_file_df = pd.read_csv(mappings_path + 'trips_to_station_nodes_edges.csv')

    largest_date_value = pd.to_datetime(all_dates_services_file_df['date'].iloc[-1], format='%Y%m%d')
    ################################################################
    if CALCULATE_DATE_RANGE:
        aggregate_graph_values_df = pd.DataFrame(columns=['station_from', 'station_to', 'total_count', 'aggregate_seconds'])

        # Loop through each date in the date range and read the corresponding CSV file
        for index, row in queried_date_range_df.iterrows():
            date = row['date']
            if not os.path.exists(f'results/daily_files/{date}.csv'):
                print(f'results/daily_files/{date}.csv does not exist. As a reminder, the largest date value is {largest_date_value}')
                break

            with open(f'results/daily_files/{date}.csv') as f:
                aggregate_graph_values_df = pd.concat([aggregate_graph_values_df, pd.read_csv(f)]). \
                    groupby(['station_from', 'station_to'], as_index=False).\
                    agg({'total_count': 'sum', 'aggregate_seconds': 'sum'})

        # Generate a mappings folder if it doesn't exist
        if not os.path.exists('results/queried_dates'):
            os.makedirs('results/queried_dates')
        aggregate_graph_values_df.to_csv(f'results/queried_dates/{search_start_date}-{search_end_date}-aggregated_nodes_edges.csv', index=False)


