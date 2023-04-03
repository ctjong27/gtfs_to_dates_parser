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

    degrees = nx.degree_centrality(G)
    closeness = nx.closeness_centrality(G)
    betweenness = nx.betweenness_centrality(G)
    df = pd.DataFrame([degrees,closeness,betweenness]).transpose()
    df.columns = ['degrees','closeness','betweenness']


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
    stops_df_filtered = stops_df_sorted.loc[~stops_df_sorted.duplicated(subset=['stop_id'], keep=False) | stops_df_sorted.groupby('stop_id')['stop_name'].apply(lambda x: x == x.max())].reset_index(drop=True)
    stops_df_filtered

    # Merge retrieved station names
    df_s = pd.merge(df_s, stops_df_filtered, left_on='station_id', right_on='stop_id')
    df = pd.merge(df, df_s, left_index=True, right_on='station_id')

    df[['station_id','stop_lat','stop_lon','degrees','closeness','betweenness']].to_csv('./results/networkx_analysis.csv', index=False)