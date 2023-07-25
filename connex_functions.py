# -*- coding: utf-8 -*-
"""
Created on Thu May 25 20:27:17 2023

@author: Jack Tattersall

CONNECTION PUNCTUALITY CALCULATOR
"""

# Required  modules to support calculations
import pandas as pd
import datetime as dt
import json
import requests
import simpy
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
    Routes=Routes[['agency_id', 'route_id', 'route_short_name',
             'route_long_name', 'route_type']]
    Stops = pd.read_csv(f'Static GTFS\\{Agency}_{Date}_gtfs\\stops.txt',
                        usecols=['stop_id', 'stop_name', 'stop_lat', 'stop_lon'], header=0, sep=',')
    Stops['agency_id'] = Agency
    Stops = Stops[['stop_id', 'stop_name', 'stop_lat', 'stop_lon']]
    Times = pd.read_csv(f'Static GTFS\\{Agency}_{Date}_gtfs\\stop_times.txt',
                        usecols=['trip_id','stop_id','arrival_time',
                                 'departure_time'],header=0,sep=',')
    Times['agency_id'] = Agency
    Times = Times[['trip_id','stop_id','arrival_time',
             'departure_time']]

    if (Agency == 'GO')or(Agency == 'UPExpress'):
        Trip = pd.read_csv(f'Static GTFS\\{Agency}_{Date}_gtfs\\trips.txt',
                                usecols=['trip_id','service_id','route_id'],header=0,sep=',')
        Trips = Trip[Trip.service_id == '20221214']
        Trips = Trips[['trip_id','service_id','route_id']]
    else:   
        Trips = pd.read_csv(f'Static GTFS\\{Agency}_{Date}_gtfs\\trips.txt',
                            usecols=['trip_id','service_id','route_id'],header=0,sep=',')
    # Concat to single region-wide GTFS dataframes
    routes = pd.concat([Routes,routes])
    routes.reset_index(drop=True,inplace=True)
    stops = pd.concat([Stops,stops])
    stops.reset_index(drop=True,inplace=True)
    trips = pd.concat([Trips,trips])
    trips.reset_index(drop=True,inplace=True)
    times = pd.concat([Times,times])
    times.reset_index(drop=True,inplace=True)

# convert to tuple coordinates
# Convert to Datetimes
stops.drop_duplicates(subset='stop_name',inplace=True)
times.arrival_time = times.arrival_time.str.zfill(8)
times.departure_time = times.departure_time.str.zfill(8)
times.arrival_time = pd.to_timedelta(times.arrival_time)
times.departure_time = pd.to_timedelta(times.departure_time)
stops.stop_name = stops.stop_name.str.lower()

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
    platforms = pd.DataFrame() # Empty processing DF for each iteration
    platforms = stops.loc[stops['stop_name'].str.contains(
        hub_list.hub[i],case=False)][['stop_name','stop_lon','stop_lat']]
    if hub_list.althub1[i] != '-':
        platforms = stops.loc[stops['stop_name'].str.contains(
            hub_list.althub1[i],case=False)][['stop_name','stop_lon','stop_lat']]
        if hub_list.althub2[i] != '-':
            platforms = stops.loc[stops['stop_name'].str.contains(
                hub_list.althub2[i],case=False)][['stop_name','stop_lon','stop_lat']]
    platforms = platforms[platforms['stop_name'].str.contains("golf")==False]
    platforms.to_csv('GTHA_connections.csv')
    interchange = pd.DataFrame(itertools.permutations(platforms['stop_name'], 2),
                               columns=['from_stop','to_stop'])
    interchange.reset_index(inplace=True,drop=True)
    interchange = pd.merge(interchange,platforms,left_on='from_stop',right_on='stop_name',
                           how='left')
    interchange.rename(columns={'stop_lon':'from_lon','stop_lat':'from_lat'},inplace=True)
    interchange.drop(columns='stop_name',inplace=True)
    interchange = pd.merge(interchange,platforms,left_on='to_stop',right_on='stop_name',
                           how='left')
    interchange.rename(columns={'stop_lon':'to_lon','stop_lat':'to_lat'},inplace=True)
    interchange.drop(columns='stop_name',inplace=True)
    interchanges = pd.concat([interchanges, interchange], axis=0)
    #interchanges = interchanges[interchanges.from_stop =]
interchanges.drop_duplicates()
interchanges.dropna(axis=0)
interchanges.reset_index(drop=True,inplace=True)
interchanges.to_csv('GTHA_connections.csv')
'''
# CONNECTION WINDOW #
Defined by Minimum and Maximum Connection Times
Rounded to nearest minute. 
Minimum Connection Time must be at least 2 minutes.
Using OSRM api
'''
url = "http://router.project-osrm.org/route/v1/foot/"
def getDistances(df):
    call = url \
        + str(df.from_lon)+','+str(df.from_lat) +\
        ';'+ str(df.to_lon)+','+str(df.to_lat)
    output = json.loads(requests.get(call).content)
    if output['code'] == 'Ok':
        return output["routes"][0]["distance"]
    else:
        return 0

# Minimum Connection Time (Walking Distance Between Stop Points)
walk_speed = 60 # m / min or 1m/s
interchanges['distance'] = interchanges.apply(getDistances,axis=1)
interchanges['Min_Connection'] = round(interchanges['distance'] / walk_speed)
# MinCT must be at least 2 minutes
interchanges['Min_Connection'].replace(to_replace=0, value=2)
interchanges['Min_Connection'].replace(to_replace=1, value=2)

# Maximum Connection Time
# Define based on a fixed value, beyond which a connection is not counted
maxCT = 10      # Maximum connection time value for connection window

# Perform analysis
services = pd.merge(times, trips, on='trip_id')
services = pd.merge(services, stops, on='stop_id', how='left')
services.drop(columns=['stop_id','stop_lon','stop_lat','service_id'], inplace=True)
services.sort_values(['arrival_time', 'departure_time'],
              axis=0, inplace=True, ascending=True)
services['stop_name'][services['stop_name'].isin(
    interchanges['from_stop'])].dropna()
# Merge interchange Connection Taker Stops and Trip Times
connections = pd.merge(interchanges, services,left_on='to_stop', right_on='stop_name')
connections.drop(columns=['trip_id','arrival_time', 'departure_time'])
connections.reindex()

# Simpy Environment Simulation
env = simpy.Environment()
tick = 0    # Simulation time ticker
connex = pd.DataFrame(columns=['arrstop','arrtime','depstop','mindep','maxdep'])  # Processing DF
while tick <= 1440:
    toc = dt.timedelta(minutes=tick) + pd.to_timedelta('04:00:00')
    print('Simulation time is ', toc)
    for idx, stn in enumerate(services['stop_name']): 
        # Connection Giver
        if services.arrival_time[idx] == toc:
            for it in connections[connections['from_stop'] == stn]['to_stop']:
                le = len(connex)
                connex.loc[le,'depstop'] = connections.to_stop.iloc[it]
                connex.loc[le,'deproute'] = connections.deproute.loc[connections.to_stop == connex.depstop]
                connex.loc[le,'mindep'] = connex.arrtime + interchanges[
                   (interchanges.from_stop==connex.arrstop)and(interchanges.to_stop==connex.depstop)]
                connex.loc[le,'maxdep'] = connex.mindep.iloc[le] + maxCT
            connex.loc[:,'arrstop'] = stn
            connex.loc[:,'arrtime'] = toc
        # Connection Taker
        if services.departure_time[idx] == toc:
            # Test for Open Connection
            for inc,dep in enumerate(connex['depstop']):
                if (dep == stn) and (connex.deproute.iloc[inc] == services.route_id[idx]):
                    connex.deptime[connex.depstop == stn] = toc
                    # Iterate up total potential connections for stop pair
                    connections.calls[interchanges.to_stop == stn] += 1
                    # Test for Successful Connection
                    if (connex.mindep.iloc[inc] <= toc)and(connex.maxdep.iloc[inc] >= toc):
                        connections.successful[connex.depstop == stn] += 1
                        # Close out connection option by deleting row
                        connex.drop(inc, axis=0)
    tick += 1 #Advance timestep by 1 minute      
 
# Final Performance
connections.punctuality = connections.successful / connections.calls
# Output results
connections.to_csv('GTHA_connections.csv')


print(connections.punctuality.describe())