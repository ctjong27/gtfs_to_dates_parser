import matplotlib.pyplot as plt
import pandas as pd
import os

# Configuration
SEARCH_START_YEAR  = 2020
SEARCH_START_MONTH = 1
SEARCH_START_DAY   = 1

SEARCH_END_YEAR  = 2023
SEARCH_END_MONTH = 5
SEARCH_END_DAY   = 31

SEARCH_START_DATE = SEARCH_START_YEAR*10000 + SEARCH_START_MONTH*100 + SEARCH_START_DAY
SEARCH_END_DATE   = SEARCH_END_YEAR*10000 + SEARCH_END_MONTH*100 + SEARCH_END_DAY

def aggregate_daily_total_stops():
    queried_start_date = pd.to_datetime(SEARCH_START_DATE, format='%Y%m%d')
    queried_end_date = pd.to_datetime(SEARCH_END_DATE, format='%Y%m%d')

    queried_date_range = pd.date_range(start=queried_start_date, end=queried_end_date)
    queried_date_range_df = pd.DataFrame({'date': queried_date_range.strftime('%Y%m%d').astype(int)})

    # for all of the files in ./results/daily_files/, open the ones in the range
    daily_results_path = "./results/daily_files/"

    aggregated_daily_df = pd.DataFrame(columns=['date','total_stops'])

    # for each file opened, add a data frame row entry for the date and the corresponding aggregate of total_count
    for index, row in queried_date_range_df.iterrows():
        date = row['date']
        if not os.path.exists(f'{daily_results_path}{date}.csv'):
            print(f'{daily_results_path}{date}.csv does not exist')
            break
        else:
            # aggregated_daily_df = pd.concat([aggregated_daily_df, 
            #                          pd.DataFrame({'date':date,
            #                                        'total_stops':int(pd.read_csv(f'{daily_results_path}{date}.csv')['total_count'].sum())
            #                                       })])
            aggregated_daily_df = pd.concat([aggregated_daily_df, 
                                    pd.DataFrame({'date':[date],
                                                'total_stops':[int(pd.read_csv(f'{daily_results_path}{date}.csv')['total_count'].sum())]
                                                })])
            
    aggregated_daily_df.to_csv('./results/aggregated_daily.csv', index=False)