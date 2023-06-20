# -*- coding: utf-8 -*-
"""
Created on Thu May 25 20:27:17 2023

@author: Jack Tattersall

CONNECTION PUNCTUALITY CALCULATOR
"""

# Required  modules to support calculations
import geopandas as gpd
import pandas as pd
import r5py
import numpy as np
import datetime as dt
from zipfile import ZipFile

'''
# GTFS Format:
Zip files must be of name AGENCYID_DATE.zip
Constituent files routes.txt and agencies.txt must include
column titled agency_id with correct AGENCYID in each entry.

Failure to adhere to these specs may lead to code issues.
'''
# Collect GTFS
Date = str("2022DEC14")     # Date of analysis as string YYYYMMMDD
# Specify list of agencies in analysis
scope = ['TTC','GO','YRT','DRT','HSR','BT','MilT','OakT','BurT','UPExpress']

# Initialize region-wide GTFS dataframes
operators = pd.empty()
agencies = pd.empty()
routes = pd.empty()
stop = pd.empty()
trips = pd.empty()
times = pd.empty()
dates = pd.empty()
shapes = pd.empty()
calendar = pd.empty()

for Agency in scope:
    ZipFile.extractall(f'{Agency}_{Date}_gtfs.zip')

    # Import as dataframes and add agency_id    
    agency = pd.read_table(f'{Agency}_{Date}_gtfs\\agency.txt')  
    Routes = pd.read_table(f'{Agency}_{Date}_gtfs\\routes.txt')
    Trips = pd.read_table(f'{Agency}_{Date}_gtfs\\trips.txt')
    Trips['agency_id'] = Agency
    Stops = pd.read_table(f'{Agency}_{Date}_gtfs\\stops.txt')
    Stops['agency_id'] = Agency
    Times = pd.read_table(f'{Agency}_{Date}_gtfs\\stop_times.txt')
    Times['agency_id'] = Agency
    Calendar = pd.read_table(f'{Agency}_{Date}_gtfs\\calendar.txt')
    calendar['agency_id'] = Agency
    Dates = pd.read_table(f'{Agency}_{Date}_gtfs\\dates.txt')
    Dates['agency_id'] = Agency
    Shapes = pd.read_table(f'{Agency}_{Date}_gtfs\\shapes.txt')
    Shapes['agency_id'] = Agency
    operators['agency_name'] = agency['agency_name']
    operators['agency_id'] = agency['agency_id']
    # Append to single region-wide GTFS dataframes
    agencies = pd.append(agency)
    routes = pd.append(Routes)
    stop = pd.append(Stops)
    trips = pd.append(Trips)
    times = pd.append(Times)
    dates = pd.append(Dates)
    shapes = pd.append(Shapes)
    calendar = pd.append(Calendar)

#convert to geodataframe:
stops = gpd.GeoDataFrame(stop, geometry=gpd.points_from_xy(stop.stop_lon,stop.stop_lat), crs = "EPSG:2958")

# Interchange Identification
'''
Search for main terminals/stations with common names
Check list of interchange hubs made up of stations/terminals with trigger words:
    Station, Terminal, Bus Terminal
    
Note, Village Station Rd is a street in Toronto with corresponding bus stop 
'''
inter = pd.empty()
inter = stops.loc[stops['stop_name'].str.contains("Station", case=False)]
inter = stops.loc[stops['stop_name'].str.contains("Terminal", case=False)]
inter = stops.loc[stops['stop_name'].str.contains("Bus Terminal", case=False)]
interchange = inter[['stop_id','stop_name','stop_lat', 'stop_lon']]
interchange.rename(columns={'agency_id':'to_agency', 'stop_id':'to_stop_id', 'stop_name':'to_stop_name',
                             'stop_lat':'to_stop_lat', 'stop_lon':'to_stop_lon'},
                    inplace=True)
fakehub = ['Station Rd', 'Station Ave', 'Station St', 
           'Terminal Rd', 'Terminal Ave', 'Terminal St']
for i in len(fakehub):
    inter = inter[inter.stop_name.contains(fakehub) == False]   

'''
Collect stops corresponding to the same interchange to create to/from selection
Hub list gives interchnages that are not published as such (Newcastle Street at Royal York Rd / Mimico GO;
                                                            Long Branch GO / Long Branch Loop)
'''

hub_list = pd.read_csv('GTHA_ConnectionHubs.csv')
# check hub list first
for i in range(len(hub_list)):
    pt = interchange.loc[interchange['stop_name'] == hub_list.hub[i]]
    addition = stops.loc[stops['stop_name'].str.contains(hub_list.althub1)]
    if hub_list.althub2[i].notna() == True:
        Addition = stops.iloc[i].str.contains(hub_list.althub2[i])
        Addition = pd.concat(addition,Addition, axis = 0)
        if hub_list.althub3[i].notna() == True:
            ADDITION = stops.iloc[i].str.contains(hub_list.althub3[i])
            addhub = pd.concat(ADDITION,Addition, axis = 0)
        else:
            addhub = Addition.copy()
    else:
        addhub = addition.copy()
  
for idx,hub in enumerate(interchange['to_stop_name']):
    key = interchange['to_stop_name'].find(' - ')
    if key != -1:
        if interchange['to_stop_name'].find('Station - ') != -1:
            Hub = hub[:key]
            Add = stops.loc[stops['stop_name'].str.contains(Hub, case=False)]['stop_name','geometry']
            Add['stop_name_to'] = hub
        else:
            Hub = hub[key:]
            Add = stops.loc[stops['stop_name'].str.contains(Hub, case=False)]['stop_name','geometry']
            Add['stop_name_to'] = hub
    cross = interchange['to_stop_name'].find(' at ')
    link = interchange['to_stop_name'].find(' and ')
    linx = interchange['to_stop_name'].find(' & ')
    if cross != -1:
        x1 = hub[:cross]
        x2 = hub[cross:]
        Add = stops.loc[stops['stop_name'].str.contains(x1, case=False) and 
                        stops['stop_name'].str.contains(x2, case=False)]['stop_name','geometry']
        Add['stop_name_to'] = hub
    elif link != -1:
        x1 = hub[:link]
        x2 = hub[link:]
        Add = stops.loc[stops['stop_name'].str.contains(x1, case=False) and 
                        stops['stop_name'].str.contains(x2, case=False)]['stop_name','geometry']
        Add['stop_name_to'] = hub
    elif linx != -1:
        x1 = hub[:linx]
        x2 = hub[linx:]
        Add = stops.loc[stops['stop_name'].str.contains(x1, case=False) and 
                        stops['stop_name'].str.contains(x2, case=False)]['stop_name','geometry']
        Add['stop_name_to'] = hub
    else:
        pass

    # Code added 2023/06/13. Review functionality satisfies objectives.
    rep = len(interchange['stop_name'] == hub)
    interchange.loc[interchange.index.repeat(rep)].reset_index(drop=True)
    interchanges = pd.merge(interchange,Add,left_on = 'to_stop_name',right_on = 'stop_name_to',
                        suffixes=('_to', '_from'))
    
interchanges = pd.concat(interchanges,Add)
# Review arrangement of final output interchanges dataframe and simplify as beneficial: remove unneeded rows, check GPS coord format for R5Py

# Check
print(interchanges.head())

# Convert to GeoPandas
connections = gpd.GeoDataFrame(interchanges, )

# Setup Transport Network for R5Py
gtha = r5py.TransportNetwork('GTHA_OSM20230525.osm.pbf')


'''
THOUGHTS:
    Including high-frequency services may be problematic especially in reverse
    Metric will punish connections from hgih-frequency to low-frequency services 
        because most services will not meet a connection taker (low-frequncy service)
    Further,
    The issue of Connection Punctuality is strongest for 
        Low frequency -> Low frequency transfers
        
    Imagine:
        Route A calls at 00, 05, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55
        Route 1 calls at 03, 33             e.g. MinCT = 2, MaxCT = 2*H = 10
        Then 15, 20,45, 50, 55 arrivals would report 0 connection
        But a connection would never have been possible in the first place.
    So, then a denominator is required that reports what connections can exist in the first place 
        i.e. departures per hour
        thus, 12 tph connection to 2 tph only gives 2 connections possible in the first place
        At this: 2/2 = 100% CONNECTION PUNCTUALITY
'''

# Minimum Connection Time (Walking Distance Between Stop Points)
transfer = r5py.TravelTimeMatrixComputer(gtha,
                                         origins=interchanges['geometry_from'], 
                                         destinations=interchanges['geometry_to'],
                                         transport_modes= r5py.LegMode.WALK)    
minCT = transfer.compute_travel_times()

interchanges['Min_Connection'] = minCT.travel_time
    
# Maximum Connection Time
    # Define based on a fixed value, beyond which a connection is not counted
maxCT = 10      # Maximum connection time value for connection window
    
# Perform analysis
'''
Issues to address
1.
Stop Ids service multiple routes/lines that are not coincident
So, to capture connections properly, must check against trips as well as times
2.

'''
serv = pd.merge(times,trips,on='trip_id')
services = serv['route_id','route_short_name','stop_id','arrival_time', 'departure_time', 'trip_id','agency_id']

for i, giver, taker in enumerate(interchange['stop_id_from','stop_id_to']):
    transfers = pd.empty
    arrcalls = services.loc[services['stop_id'] == giver]['route_id']
    depcalls = services.loc[services['stop_id'] == taker]['route_id']
    if len(arrcalls) > 1:
        callers = services.loc[services['stop_id'] == giver]['route_id']
        transfers.route = pd.concat(transfers,callers)
    else:
        transfers.route = services.loc[services['stop_id'] == giver]['route_id']
    if len(depcalls) > 1:
        callers = services.loc[services['stop_id'] == taker]['route_id']
        transfers.route = pd.concat(transfers,callers)
    else:
        transfers.route = services.loc[services['stop_id'] == taker]['route_id']
    
    # Identify giver in times dataframe.
    transfers.GiverArr = services.loc[services['stop_id'] == giver]['arrival_time']
    transfers.TakerArr = services.loc[services['stop_id'] == taker]['arrival_time']
    transfers.GiverDep = services.loc[services['stop_id'] == giver]['departure_time']
    transfers.TakerDep = services.loc[services['stop_id'] == taker]['departure_time']
    # Range for connecting service
    transfers.minDepTaker = transfers.GiverArr + interchanges[i]['Min_Connection']
    transfers.maxDepTaker = transfers.GiverArr + maxCT
    transfers.minDepGiver = transfers.TakerArr + interchanges[i]['Min_Connection']
    transfers.maxDepGiver = transfers.TakerArr + maxCT
    transfers.minArrTaker = transfers.GiverDep - interchanges[i]['Min_Connection']
    transfers.maxArrTaker = transfers.GiverDep - maxCT
    transfers.minArrGiver = transfers.TakerDep - interchanges[i]['Min_Connection']
    transfers.maxArrGiver = transfers.TakerDep - maxCT
    # Separate by route id.
    # Giver Route != Taker Route -- report in transfers dataframe
    # Same route must not call at same stop multiple times within connection window
    ## Identifying if connection met.
    # Not going to work because departures may not be lined up perfectly.
    transfers['GiverPerf'] = pd.between(,,inclusive = 'both')
    transfers['GiverPerf'] = pd.between(,,inclusive = 'both')
             

# Output results
pd.interchanges.to_csv('GTHA_connections.csv')

# Plot Results Cartographically