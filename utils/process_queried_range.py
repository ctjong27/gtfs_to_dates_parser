import pandas as pd
from datetime import datetime, timedelta
import os

def aggregate_from_daterange():
    # Configuration
    CALCULATE_DATE_RANGE = True

    SEARCH_START_YEAR  = 2019
    SEARCH_START_MONTH = 1
    SEARCH_START_DAY   = 1

    SEARCH_END_YEAR  = 2019
    SEARCH_END_MONTH = 12
    SEARCH_END_DAY   = 31

    SEARCH_START_DATE = SEARCH_START_YEAR*10000 + SEARCH_START_MONTH*100 + SEARCH_START_DAY
    SEARCH_END_DATE   = SEARCH_END_YEAR*10000 + SEARCH_END_MONTH*100 + SEARCH_END_DAY

    # date_range = pd.date_range(start=SEARCH_START_DATE, end=SEARCH_END_DATE, closed='left')
    # date_range


    queried_start_date = pd.to_datetime(SEARCH_START_DATE, format='%Y%m%d')
    queried_end_date = pd.to_datetime(SEARCH_END_DATE, format='%Y%m%d')

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
        aggregate_graph_values_df.to_csv(f'results/queried_dates/{SEARCH_START_DATE}-{SEARCH_END_DATE}-aggregated_nodes_edges.csv', index=False)


