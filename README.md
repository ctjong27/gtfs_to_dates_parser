# gtfs_to_dates_parser
Parses GTFS files and generates appropriate routes information for each dates in a range


## Base logic
The point of the program is to compile total number of station stops in a node-and-edge method.

In order to limit the scope of data extrapolation, the data will be organized as follows


1) Total number of node and edges in context of directional Station-To-Station data parsed per trip_id
    a) Requires Stop-To-Stop analysis via stop_times.txt (from & to & transit_duration (weight) info via trip_id)
    b) Requires Stop-To-Parent_Stop analysis via stops.txt (parent stop)
    c) Requires Parent_Stop-To-Station analysis via transfers.txt (clustered parent stops)

2) When each directional edge & their weights are calculated per each trip_id, a mapping can consolidate all trip node and edges for any given time period