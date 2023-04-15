# GTFS to Nodes and Edges in a daterange
Parses GTFS files and generates appropriate routes information for each dates in a range


## To Run
The base workbook "gtfs_process_workspace.ipynb" can be run fully. The ./utils/ python code will handle the download & processing of files.

If one prefers to use the notebooks intead they can be found under ./_ipynb_notebooks_moved_From_base/. They will need to be placed back into the parent folder in them to run properly.

## Logic
The point of the program is to compile total number of station stops in a node-and-edge method.

In order to limit the scope of data extrapolation, the data will be organized as follows

1) Total number of node and edges in context of directional Station-To-Station data parsed per trip_id
    a) Requires Stop-To-Stop analysis via stop_times.txt (from & to & transit_duration (weight) info via trip_id)
    b) Requires Stop-To-Parent_Stop analysis via stops.txt (parent stop)
    c) Requires Parent_Stop-To-Station analysis via transfers.txt (clustered parent stops)

2) When each directional edge & their weights are calculated per each trip_id, a mapping can consolidate all trip node and edges

3) From the mapping tables I should be able to calculate the station-to-station, total count, and transit duration for every day

4) From the daily data, I can query data for sum based on each node-edge or else I can generate a table that displays total stops per date

## Example output text
### ./results/networkx_analysis.csv
station_id,stop_lat,stop_lon,btwn,clse,pgrk,egvt\
101,40.889248,-73.898583,0.0,0.00018961228255773245,0.002279664338591625,2.4596241036719183e-15\
103,40.884667,-73.90087,0.0048661800486618,0.00019289586403670755,0.0045178980371041256,6.498672230058542e-14\
104,40.878856,-73.904834,0.009708622633671591,0.0002189071990705639,0.002977500951886173,2.4982874343804385e-13\
106,40.874561,-73.909831,0.014527327755029374,0.00023931341714283279,0.001770668452629024,1.5003063557276926e-12\
107,40.869444,-73.915279,0.019322295412735148,0.0002444524243390752,0.0020982989554330653,3.816467996252402e-11\
108,40.864621,-73.918822,0.024093525606788912,0.0002498191106015396,0.0022983254710839127,1.0067242162887193e-09\
109,40.860531,-73.925536,0.02884101833719067,0.00025979838292763307,0.0029073220191790784,1.5228525679492402e-08

### ./results/aggregated_daily.csv
date,total_stops\
20200101,136532\
20200102,216241\
20200103,216241\
20200104,149635\
20200105,136532\
20200106,216241\
20200107,216241\
20200108,216241\
20200109,432482
