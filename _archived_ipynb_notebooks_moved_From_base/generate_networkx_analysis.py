import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd
import scipy as sc

# less time is good
# more total is good
# lower weight is good
# time / total = weight

def run_networkx_analysis():
    # Load data into a Pandas DataFrame
    mappings_path = "./mappings/"
    results_path = "./results/queried_dates/"
    df_ne = pd.read_csv(results_path+"20190101-20191231-aggregated_nodes_edges.csv")
    df_s = pd.read_csv(mappings_path+"stations_to_coordinates.csv")

    # drop rows where station_from is the same as station_to
    df_ne = df_ne[df_ne['station_from'] != df_ne['station_to']]

    # Create a directed graph using NetworkX
    G = nx.DiGraph()

    # Add nodes to the graph
    for station in df_s["station_id"].unique():
        G.add_node(station, pos=(df_s[df_s["station_id"] == station]["stop_lon"].values[0], df_s[df_s["station_id"] == station]["stop_lat"].values[0]))

    # Add nodes to the graph using the unique source and target nodes from the dataframe
    G.add_nodes_from(df_ne['station_from'].unique())
    G.add_nodes_from(df_ne['station_to'].unique())

    # Iterate through the dataframe and add edges to the graph, with weights in order to calculate average time per trip
    for index, row in df_ne.iterrows():
        G.add_edge(row['station_from'], row['station_to'], weight=round(row['aggregate_seconds']/row['total_count'], 2))

    nx.get_edge_attributes(G,'weight')

    # nx.draw(G)

    # Draw the graph using matplotlib
    pos = nx.get_node_attributes(G, 'pos')
    plt.figure(figsize=(100,100)) 
    nx.draw(G, pos, with_labels=True, node_color="skyblue", edge_color="gray", node_size=1000, alpha=0.7)
    nx.draw_networkx_edge_labels(G, pos,
                            edge_labels=nx.get_edge_attributes(G,'weight'))
    plt.show()

    # degrees = nx.degree_centrality(G, weight='weight')
    # closeness = nx.closeness_centrality(G, weight='weight')
    # betweenness = nx.betweenness_centrality(G, weight='weight')
    # df = pd.DataFrame([degrees,closeness,betweenness]).transpose()
    # df.columns = ['degrees','closeness','betweenness']

    betweenness = nx.betweenness_centrality(G, weight='weight')
    closeness = nx.closeness_centrality(G, distance='weight')
    df = pd.DataFrame([betweenness, closeness]).transpose()
    df.columns = ['betweenness','closeness']

    import os
    extract_path = "./files/extracted/"
    stops_path = "/stops.txt"
    gtfs_generation_dates = [item for item in os.listdir(extract_path) if os.path.isdir(os.path.join(extract_path, item))]

    def process_stops_file():

        agg_stops = pd.DataFrame()
        for gtfs_date in gtfs_generation_dates:

            # Read the stop_times.txt file into a DataFrame
            stops = pd.read_csv(extract_path + gtfs_date + stops_path)
            agg_stops = pd.concat([agg_stops, stops])

        agg_stops = agg_stops[['stop_id','parent_station', 'stop_name']] \
            .drop_duplicates()
        return(agg_stops)
    
    # Retrieve stops file information to populate station detail from 'stop_name'
    stops_df = process_stops_file()
    stops_df = stops_df[stops_df['parent_station'].isna()]

    # sort stops_df by stop_id and stop_name length in descending order
    stops_df_sorted = stops_df.sort_values(['stop_id', 'stop_name'], ascending=[True, False])

    # keep only the rows with no duplicates and the rows with the maximum stop_name for each stop_id
    stops_df_filtered = stops_df_sorted.loc[~stops_df_sorted.duplicated(subset=['stop_id'], keep='first')].reset_index(drop=True)
    # stops_df_filtered = stops_df_sorted.loc[~stops_df_sorted.duplicated(subset=['stop_id'], keep=False) | stops_df_sorted.groupby('stop_id')['stop_name'].apply(lambda x: x == x.max())].reset_index(drop=True)

    # Merge retrieved station names
    df_s = pd.merge(df_s, stops_df_filtered, left_on='station_id', right_on='stop_id')
    df = pd.merge(df, df_s, left_index=True, right_on='station_id')

    # df[['station_id','stop_lat','stop_lon','degrees','closeness','betweenness']].to_csv('./results/networkx_analysis.csv', index=False)
    # df[['station_id','stop_lat','stop_lon','closeness','betweenness']].to_csv('./results/networkx_analysis.csv', index=False)
    df = df.rename(columns={'closeness': 'clse', 'betweenness': 'btwn'})
    df[['station_id','stop_lat','stop_lon','btwn','clse']].to_csv('./results/networkx_analysis.csv', index=False)


def run_networkx_analysis_with_pop():
    # Load data into a Pandas DataFrame
    mappings_path = "./mappings/"
    results_path = "./results/queried_dates/"
    external_data_path = "./external_data/"
    df_ne = pd.read_csv(results_path+"20190101-20191231-aggregated_nodes_edges.csv")
    df_s = pd.read_csv(mappings_path+"stations_to_coordinates.csv")
    df_pop = pd.read_csv(external_data_path+"station_populations.csv")

    # Create a dictionary that maps station IDs to their population weights
    pop_dict = dict(zip(df_pop['station_id'], df_pop['ACS19_5yr_B01001001']))
    area_dict = dict(zip(df_pop['station_id'], df_pop['Geo_AREALAND']))

    # drop rows where station_from is the same as station_to
    df_ne = df_ne[df_ne['station_from'] != df_ne['station_to']]

    # Create a directed graph using NetworkX
    G = nx.DiGraph()

    # Add nodes to the graph
    for station in df_s["station_id"].unique():
        G.add_node(station, pos=(df_s[df_s["station_id"] == station]["stop_lon"].values[0], df_s[df_s["station_id"] == station]["stop_lat"].values[0]))

    # Add nodes to the graph using the unique source and target nodes from the dataframe
    G.add_nodes_from(df_ne['station_from'].unique())
    G.add_nodes_from(df_ne['station_to'].unique())

    # Iterate through the dataframe and add edges to the graph, with weights in order to calculate average time per trip
    for index, row in df_ne.iterrows():
        G.add_edge(row['station_from'], row['station_to'], weight=(pop_dict[row['station_from']] + pop_dict[row['station_to']]) / (area_dict[row['station_from']] + area_dict[row['station_to']]) * (round(row['aggregate_seconds']/row['total_count'], 2)))
        # G.add_edge(row['station_from'], row['station_to'], weight=round(row['aggregate_seconds']/row['total_count'], 2))

    nx.get_edge_attributes(G,'weight')

    betweenness = nx.betweenness_centrality(G, weight='weight')
    closeness = nx.closeness_centrality(G, distance='weight')
    pagerank = nx.pagerank(G, weight='weight')
    df = pd.DataFrame([betweenness, closeness, pagerank]).transpose()
    df.columns = ['betweenness','closeness', 'pagerank']

    import os
    extract_path = "./files/extracted/"
    stops_path = "/stops.txt"
    gtfs_generation_dates = [item for item in os.listdir(extract_path) if os.path.isdir(os.path.join(extract_path, item))]

    def process_stops_file():

        agg_stops = pd.DataFrame()
        for gtfs_date in gtfs_generation_dates:

            # Read the stop_times.txt file into a DataFrame
            stops = pd.read_csv(extract_path + gtfs_date + stops_path)
            agg_stops = pd.concat([agg_stops, stops])

        agg_stops = agg_stops[['stop_id','parent_station', 'stop_name']] \
            .drop_duplicates()
        return(agg_stops)
    
    # Retrieve stops file information to populate station detail from 'stop_name'
    stops_df = process_stops_file()
    stops_df = stops_df[stops_df['parent_station'].isna()]

    # sort stops_df by stop_id and stop_name length in descending order
    stops_df_sorted = stops_df.sort_values(['stop_id', 'stop_name'], ascending=[True, False])

    # keep only the rows with no duplicates and the rows with the maximum stop_name for each stop_id
    stops_df_filtered = stops_df_sorted.loc[~stops_df_sorted.duplicated(subset=['stop_id'], keep='first')].reset_index(drop=True)
    # stops_df_filtered = stops_df_sorted.loc[~stops_df_sorted.duplicated(subset=['stop_id'], keep=False) | stops_df_sorted.groupby('stop_id')['stop_name'].apply(lambda x: x == x.max())].reset_index(drop=True)

    # Merge retrieved station names
    df_s = pd.merge(df_s, stops_df_filtered, left_on='station_id', right_on='stop_id')
    df = pd.merge(df, df_s, left_index=True, right_on='station_id')

    # df[['station_id','stop_lat','stop_lon','degrees','closeness','betweenness']].to_csv('./results/networkx_analysis.csv', index=False)
    # df[['station_id','stop_lat','stop_lon','closeness','betweenness']].to_csv('./results/networkx_analysis.csv', index=False)
    df = df.rename(columns={'closeness': 'clse', 'betweenness': 'btwn', 'pagerank': 'pgrk'})
    df = df[['station_id','stop_lat','stop_lon','btwn','clse', 'pgrk']]
    df.to_csv('./results/networkx_analysis_with_pop_density.csv', index=False)
    return df

def run_networkx_analysis_with_pop_weighted_nodes(add_weights_to_nodes=False, use_multiplied_weight=False, use_populatioN_density=False):
    # Load data into a Pandas DataFrame
    mappings_path = "./mappings/"
    results_path = "./results/queried_dates/"
    external_data_path = "./external_data/"
    df_ne = pd.read_csv(results_path+"20190101-20191231-aggregated_nodes_edges.csv")
    df_s = pd.read_csv(mappings_path+"stations_to_coordinates.csv")
    df_pop = pd.read_csv(external_data_path+"station_populations.csv")

    # Create a dictionary that maps station IDs to their population weights
    pop_dict = dict(zip(df_pop['station_id'], df_pop['ACS19_5yr_B01001001']))
    area_dict = dict(zip(df_pop['station_id'], df_pop['Geo_AREALAND']))

    # drop rows where station_from is the same as station_to
    df_ne = df_ne[df_ne['station_from'] != df_ne['station_to']]

    # Create a directed graph using NetworkX
    G = nx.DiGraph()

    # Add nodes to the graph
    for station in df_s["station_id"].unique():
        if add_weights_to_nodes:
            G.add_node(station, pos=(df_s[df_s["station_id"] == station]["stop_lon"].values[0], df_s[df_s["station_id"] == station]["stop_lat"].values[0]), weight=pop_dict[station])
        else:
            G.add_node(station, pos=(df_s[df_s["station_id"] == station]["stop_lon"].values[0], df_s[df_s["station_id"] == station]["stop_lat"].values[0]))

    # Add nodes to the graph using the unique source and target nodes from the dataframe
    G.add_nodes_from(df_ne['station_from'].unique())
    G.add_nodes_from(df_ne['station_to'].unique())

    # Iterate through the dataframe and add edges to the graph, with weights in order to calculate average time per trip
    for index, row in df_ne.iterrows():
        if use_multiplied_weight:
            if use_populatioN_density:
                G.add_edge(row['station_from'], row['station_to'], weight=(pop_dict[row['station_from']] + pop_dict[row['station_to']]) / (area_dict[row['station_from']] + area_dict[row['station_to']]) * (round(row['aggregate_seconds']/row['total_count'], 2)))
            else:
                G.add_edge(row['station_from'], row['station_to'], weight=(pop_dict[row['station_from']] + pop_dict[row['station_to']]) * (round(row['aggregate_seconds']/row['total_count'], 2)))
        else:
            G.add_edge(row['station_from'], row['station_to'], weight=round(row['aggregate_seconds']/row['total_count'], 2))

    nx.get_edge_attributes(G,'weight')

    betweenness = nx.betweenness_centrality(G, weight='weight')
    closeness = nx.closeness_centrality(G, distance='weight')
    pagerank = nx.pagerank(G, weight='weight')
    df = pd.DataFrame([betweenness, closeness, pagerank]).transpose()
    df.columns = ['betweenness','closeness', 'pagerank']

    import os
    extract_path = "./files/extracted/"
    stops_path = "/stops.txt"
    gtfs_generation_dates = [item for item in os.listdir(extract_path) if os.path.isdir(os.path.join(extract_path, item))]

    def process_stops_file():

        agg_stops = pd.DataFrame()
        for gtfs_date in gtfs_generation_dates:

            # Read the stop_times.txt file into a DataFrame
            stops = pd.read_csv(extract_path + gtfs_date + stops_path)
            agg_stops = pd.concat([agg_stops, stops])

        agg_stops = agg_stops[['stop_id','parent_station', 'stop_name']] \
            .drop_duplicates()
        return(agg_stops)
    
    # Retrieve stops file information to populate station detail from 'stop_name'
    stops_df = process_stops_file()
    stops_df = stops_df[stops_df['parent_station'].isna()]

    # sort stops_df by stop_id and stop_name length in descending order
    stops_df_sorted = stops_df.sort_values(['stop_id', 'stop_name'], ascending=[True, False])

    # keep only the rows with no duplicates and the rows with the maximum stop_name for each stop_id
    stops_df_filtered = stops_df_sorted.loc[~stops_df_sorted.duplicated(subset=['stop_id'], keep='first')].reset_index(drop=True)
    # stops_df_filtered = stops_df_sorted.loc[~stops_df_sorted.duplicated(subset=['stop_id'], keep=False) | stops_df_sorted.groupby('stop_id')['stop_name'].apply(lambda x: x == x.max())].reset_index(drop=True)

    # Merge retrieved station names
    df_s = pd.merge(df_s, stops_df_filtered, left_on='station_id', right_on='stop_id')
    df = pd.merge(df, df_s, left_index=True, right_on='station_id')

    # df[['station_id','stop_lat','stop_lon','degrees','closeness','betweenness']].to_csv('./results/networkx_analysis.csv', index=False)
    # df[['station_id','stop_lat','stop_lon','closeness','betweenness']].to_csv('./results/networkx_analysis.csv', index=False)
    df = df.rename(columns={'closeness': 'clse', 'betweenness': 'btwn', 'pagerank': 'pgrk'})
    df = df[['station_id','stop_lat','stop_lon','btwn','clse', 'pgrk']]
    if use_multiplied_weight:
        if use_populatioN_density:
            df.to_csv('./results/networkx_analysis_with_pop_density_weighted_nodes.csv', index=False)
        else:
            df.to_csv('./results/networkx_analysis_with_pop_weighted_nodes.csv', index=False)
    else:
        df.to_csv('./results/networkx_analysis_without_pop_weighted_nodes.csv', index=False)

    return df

def run_full_network_analysis_without_pop():
    # Load data into a Pandas DataFrame
    mappings_path = "./mappings/"
    results_path = "./results/queried_dates/"
    external_data_path = "./external_data/"
    df_ne = pd.read_csv(results_path+"20190101-20191231-aggregated_nodes_edges.csv")
    df_s = pd.read_csv(mappings_path+"stations_to_coordinates.csv")

    # drop rows where station_from is the same as station_to
    df_ne = df_ne[df_ne['station_from'] != df_ne['station_to']]

    # Create a directed graph using NetworkX
    G = nx.DiGraph()

    # Add nodes to the graph
    for station in df_s["station_id"].unique():
        G.add_node(station, pos=(df_s[df_s["station_id"] == station]["stop_lon"].values[0], df_s[df_s["station_id"] == station]["stop_lat"].values[0]))

    # Add nodes to the graph using the unique source and target nodes from the dataframe
    G.add_nodes_from(df_ne['station_from'].unique())
    G.add_nodes_from(df_ne['station_to'].unique())

    # Iterate through the dataframe and add edges to the graph, with weights in order to calculate average time per trip
    for index, row in df_ne.iterrows():
        G.add_edge(row['station_from'], row['station_to'], weight=round(row['aggregate_seconds']/row['total_count'], 2))

    nx.get_edge_attributes(G,'weight')

    betweenness = nx.betweenness_centrality(G, weight='weight')
    closeness = nx.closeness_centrality(G, distance='weight')
    pagerank = nx.pagerank(G, weight='weight')
    eigenvector = nx.eigenvector_centrality(G, weight='weight')
    df = pd.DataFrame([betweenness, closeness, pagerank, eigenvector]).transpose()
    df.columns = ['betweenness','closeness', 'pagerank', 'eigenvector']

    import os
    extract_path = "./files/extracted/"
    stops_path = "/stops.txt"
    gtfs_generation_dates = [item for item in os.listdir(extract_path) if os.path.isdir(os.path.join(extract_path, item))]

    def process_stops_file():

        agg_stops = pd.DataFrame()
        for gtfs_date in gtfs_generation_dates:

            # Read the stop_times.txt file into a DataFrame
            stops = pd.read_csv(extract_path + gtfs_date + stops_path)
            agg_stops = pd.concat([agg_stops, stops])

        agg_stops = agg_stops[['stop_id','parent_station', 'stop_name']] \
            .drop_duplicates()
        return(agg_stops)
    
    # Retrieve stops file information to populate station detail from 'stop_name'
    stops_df = process_stops_file()
    stops_df = stops_df[stops_df['parent_station'].isna()]

    # sort stops_df by stop_id and stop_name length in descending order
    stops_df_sorted = stops_df.sort_values(['stop_id', 'stop_name'], ascending=[True, False])

    # keep only the rows with no duplicates and the rows with the maximum stop_name for each stop_id
    stops_df_filtered = stops_df_sorted.loc[~stops_df_sorted.duplicated(subset=['stop_id'], keep='first')].reset_index(drop=True)
    # stops_df_filtered = stops_df_sorted.loc[~stops_df_sorted.duplicated(subset=['stop_id'], keep=False) | stops_df_sorted.groupby('stop_id')['stop_name'].apply(lambda x: x == x.max())].reset_index(drop=True)

    # Merge retrieved station names
    df_s = pd.merge(df_s, stops_df_filtered, left_on='station_id', right_on='stop_id')
    df = pd.merge(df, df_s, left_index=True, right_on='station_id')

    # df[['station_id','stop_lat','stop_lon','degrees','closeness','betweenness']].to_csv('./results/networkx_analysis.csv', index=False)
    # df[['station_id','stop_lat','stop_lon','closeness','betweenness']].to_csv('./results/networkx_analysis.csv', index=False)
    df = df.rename(columns={'closeness': 'clse', 'betweenness': 'btwn', 'pagerank': 'pgrk', 'eigenvector': 'egvt'})
    df = df[['station_id','stop_lat','stop_lon','btwn','clse', 'pgrk', 'egvt']]
    df.to_csv('./results/networkx_analysis_full_without_pop.csv', index=False)
    return df