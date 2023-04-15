import matplotlib.pyplot as plt
import pandas as pd
import os


def aggregate_daily_total_stops(search_start_year, search_start_month, search_start_day,
                                search_end_year, search_end_month, search_end_day):
    """
    The function Aggregates the total number of stops per day for a given date range.
    
    Parameters:
        search_start_year (int): Year of the start date for the search.
        search_start_month (int): Month of the start date for the search.
        search_start_day (int): Day of the start date for the search.
        search_end_year (int): Year of the end date for the search.
        search_end_month (int): Month of the end date for the search.
        search_end_day (int): Day of the end date for the search.
    """
    
    # Configuration
    search_start_date = search_start_year*10000 + search_start_month*100 + search_start_day
    search_end_date   = search_end_year*10000 + search_end_month*100 + search_end_day

    queried_start_date = pd.to_datetime(search_start_date, format='%Y%m%d')
    queried_end_date = pd.to_datetime(search_end_date, format='%Y%m%d')

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