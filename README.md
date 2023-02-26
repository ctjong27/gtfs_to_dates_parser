# gtfs_to_dates_parser
Parses GTFS files and generates appropriate routes information for each dates in a range


## To run
The base workbook "gtfs_process_workspace.ipynb" can be run fully. The ./utils/ python code will handle the download & processing of files.

If one prefers to use the notebooks intead they can be found under ./_ipynb_notebooks_moved_From_base/

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