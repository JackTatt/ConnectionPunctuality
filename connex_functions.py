# -*- coding: utf-8 -*-
"""
Created on Thu May 25 20:27:17 2023

@author: Jack Tattersall

CONNECTION PUNCTUALITY CALCULATOR
"""

# Required  modules to support calculations
import pandas as pd
import datetime as dt
import osmnx
import taxicab as tc
import simpy
import matplotlib as mp
import itertools

'''
# GTFS Format:
GTFS files must be already unzipped. 
Constituent files routes.txt and agencies.txt must include
column titled agency_id with correct AGENCYID in each entry.

Failure to adhere to these specs may lead to code issues.
'''
# Collect GTFS
Date = str("2022DEC14")     # Date of analysis as string YYYYMMMDD
# Specify list of agencies in analysis
scope = ['TTC', 'GO', 'YRT', 'DRT', 'HSR',
         'BT', 'MilT', 'OakT', 'BurT', 'UPExpress']
# Initialalize dataframes
routes = pd.DataFrame()
stops = pd.DataFrame()
trips = pd.DataFrame()
times = pd.DataFrame()
dates = pd.DataFrame()
calendar = pd.DataFrame()

for Agency in scope:
    # Import as dataframes and add agency_id
    Routes = pd.read_csv(f'Static GTFS\\{Agency}_{Date}_gtfs\\routes.txt',
                         usecols=['agency_id', 'route_id', 'route_short_name',
                                  'route_long_name', 'route_type'],
                         sep=',', header=0)
    Stops = pd.read_csv(f'Static GTFS\\{Agency}_{Date}_gtfs\\stops.txt',
                        usecols=['stop_id', 'stop_name', 'stop_lat', 'stop_lon'], header=0, sep=',')
    Stops['agency_id'] = Agency
    Times = pd.read_csv(f'Static GTFS\\{Agency}_{Date}_gtfs\\stop_times.txt',
                        usecols=['trip_id','stop_id','arrival_time',
                                 'departure_time'],header=0,sep=',')

    if (Agency == 'GO')or(Agency == 'UPExpress'):
        Trip = pd.read_csv(f'Static GTFS\\{Agency}_{Date}_gtfs\\trips.txt',
                                usecols=['trip_id','service_id','route_id'],header=0,sep=',')
        Trips = Trip[Trip.service_id == '20221214']
    else:   
        Trips = pd.read_csv(f'Static GTFS\\{Agency}_{Date}_gtfs\\trips.txt',
                            usecols=['trip_id','service_id','route_id'],header=0,sep=',')
    Times['agency_id'] = Agency
    # Concat to single region-wide GTFS dataframes
    routes = pd.concat([routes, Routes], axis=0, ignore_index=True, sort=True)
    stops = pd.concat([stops, Stops], axis=0, ignore_index=True, sort=True)
    trips = pd.concat([trips, Trips], axis=0, ignore_index=True, sort=True)
    times = pd.concat([times, Times], axis=0, ignore_index=True, sort=True)

# convert to geodataframe:
# Convert to Datetimes
stops['locate'] = stops[['stop_lat', 'stop_lon']].apply(tuple, axis=1)
times.arrival_time = times.arrival_time.str.zfill(8)
times.departure_time = times.departure_time.str.zfill(8)
times.arrival_time = pd.to_timedelta(times.arrival_time)
times.departure_time = pd.to_timedelta(times.departure_time)
# Interchange Identification
'''
Search for main terminals/stations with common names
Check list of stop names including interchange names contained in interchange 'hub list'
Collect stops corresponding to the same interchange to create to/from selection
'''
interchange = pd.DataFrame()
interchanges = pd.DataFrame()
hub_list = pd.read_csv('GTHA_ConnectionHubs.csv')

for i in range(len(hub_list)):
    platforms = stops.loc[stops['stop_name'].str.contains(
        hub_list.hub[i],case=False)][['stop_name', 'locate']]
    if hub_list.althub1[i] != '-':
        platforms = stops.loc[stops['stop_name'].str.contains(
            hub_list.althub1[i],case=False)][['stop_name', 'locate']]
        if hub_list.althub2[i] != '-':
            platforms = stops.loc[stops['stop_name'].str.contains(
                hub_list.althub2[i],case=False)][['stop_name', 'locate']]
    interchange = pd.DataFrame(itertools.permutations(platforms['locate'], 2),columns=['from_loc','to_loc'])
    platforms.reset_index(inplace=True,drop=True)
    interchange['from_stop'] = platforms[platforms.locate.isin(
                interchange['from_loc'])]['stop_name']
    interchange['to_stop'] = platforms[platforms['locate'].isin(
                interchange['to_loc'])]['stop_name']
    interchanges = pd.concat([interchanges, interchange], axis=0)
interchanges.drop_duplicates()
interchange.reset_index(inplace=True)
print(interchanges.head())
interchanges.to_csv('GTHA_connections.csv')
'''
# CONNECTION WINDOW #
Defined by Minimum and Maximum Connection Times
Rounded to nearest minute. 
Minimum Connection Time must be at least 2 minutes.

Using taxicab and OSMnx module
Taxicab is optimized for short distances
https://github.com/nathanrooy/taxicab
'''

# Minimum Connection Time (Walking Distance Between Stop Points)
# Create the graph of the area from OSM
graph_area = ("Golden Horseshoe, Ontario, Canada")
#G = osmnx.graph_from_place(graph_area, network_type='all', simplify=False)
# Save graph to disk
#osmnx.save_graphml(G, "GoldenHorseshoe.graphml")
G = osmnx.load_graphml("GoldenHorseshoe.graphml")
# Get the shortest route by distance
interchanges.dist = tc.distance.shortest_path(G, interchanges.origin, interchanges.dest)[0]
# Travel time in minutes
walk_speed = 60 #metres/minute (1m/s)
interchanges['Min_Connection'] = round(interchanges.dist/walk_speed)
interchanges['Min_Connection'].replace(to_replace=0, value=2)
interchanges['Min_Connection'].replace(to_replace=1, value=2)

# Maximum Connection Time
# Define based on a fixed value, beyond which a connection is not counted
maxCT = 10      # Maximum connection time value for connection window

# Perform analysis
services = pd.merge(times, trips, on='trip_id')
services.drop(columns=['trip_id', 'route_short_name'], inplace=True)
services.sort(['arrival_time', 'departure time'],
              axis=0, inplace=True, ascending=True)
services['stop_name'][services['stop_name'].isin(
    interchanges['from_stop'])].dropna()
#Quick access MinCT DataFram
transfers = interchanges.pivot(index='to_stop', columns='from_stop', values='minCT')
# Mergge interchange Connection Taker Stops and Trip Times
connections = pd.merge(left=interchanges, right=services,left_on='to_stop', right_on='stop_name')
connections.drop(columns=['trip_id', 'route_short_name','arrival_time', 'departure_time'])
connections.reindex()

# Simpy Environment Simulation
env = simpy.Environment()
tick = 0    # Simulation time ticker
connex = pd.DataFrame(columns=['arrstop','arrtime','depstop','mindep','maxdep'])  # Processing DF
while tick <= 1440:
    toc = dt.timedelta(minutes=tick) + dt.timedelta('4:00:00')
    print('Simulation time is ', toc)
    for idx, stn in enumerate(services['stop_name']): 
        # Connection Giver
        if services.arrival_time[idx] == toc:
            connex.arrstop = stn
            connex.arrtime = toc
            for it in connections[connections['from_stop'] == stn]['to_stop']:
                connex.depstop = connections.to_stop[it]
                connex.deproute = connections.deproute[connections.to_stop == connex.depstop]
                connex.mindep = connex.arrtime + transfers[connex.arrstop,connex.depstop]
                connex.maxdep = connex.mindep + maxCT
                connex.drop_duplicates(inplace=True)
        # Connection Taker
        if services.departure_time[idx] == toc:
            # Test for Open Connection
            if (connex.depstop == stn) and (connex.deproute == services.route_id[idx]):
                connex.deptime[connex.depstop == stn] = toc
                # Iterate up total potential connections for stop pair
                connections.calls[interchanges.to_stop == stn] += 1
                # Test for Successful Connection
                if (connex.mindep <= toc)and(connex.maxdep >= toc):
                    connections.successful[connex.depstop == stn] += 1
                    # Close out connection option by deleting row
                    connex.drop(connex[(connex.depstop == stn)and(connex.deptime == toc)].index, axis=0)
    tick += 1 #Advance timestep by 1 minute      
 
# Final Performance
connections.punctuality = connections.successful / connections.calls
# Output results
connections.to_csv('GTHA_connections.csv')

# Plot Results Cartographically
