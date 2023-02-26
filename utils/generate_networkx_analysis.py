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

    df.to_csv('results/networkx_analysis.csv')