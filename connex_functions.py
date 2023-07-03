# -*- coding: utf-8 -*-
"""
Created on Thu May 25 20:27:17 2023

@author: Jack Tattersall

CONNECTION PUNCTUALITY CALCULATOR
"""

# Required  modules to support calculations
import geopandas as gpd
import pandas as pd
import datetime as dt

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
scope = ['TTC','GO','YRT','DRT','HSR','BT','MilT','OakT','BurT','UPExpress']
# Initialalize dataframes
routes = pd.DataFrame()
stop = pd.DataFrame()
trips = pd.DataFrame()
times = pd.DataFrame()
dates = pd.DataFrame()
calendar = pd.DataFrame()
   
for Agency in scope:
    # Import as dataframes and add agency_id     
    Routes = pd.read_csv(f'Static GTFS\\{Agency}_{Date}_gtfs\\routes.txt',
                           usecols=['agency_id','route_id','route_short_name','route_long_name','route_type'],
                           sep=',',header=0)
    Trips = pd.read_table(f'Static GTFS\\{Agency}_{Date}_gtfs\\trips.txt', 
                          usecols=['route_id','trip_id'],header=0,sep=',')
    Trips['agency_id'] = Agency
    Stops = pd.read_csv(f'Static GTFS\\{Agency}_{Date}_gtfs\\stops.txt',
                          usecols=['stop_id','stop_name','stop_lat','stop_lon'],header=0,sep=',')
    Stops['agency_id'] = Agency
    Times = pd.read_csv(f'Static GTFS\\{Agency}_{Date}_gtfs\\stop_times.txt', 
                          usecols=['trip_id','stop_id','arrival_time','departure_time'],header=0,sep=',')
    Times['agency_id'] = Agency
    '''
    Calendar = pd.read_table(f'Static GTFS\\{Agency}_{Date}_gtfs\\calendar.txt')
    Calendar['agency_id'] = Agency
    Dates = pd.read_table(f'Static GTFS\\{Agency}_{Date}_gtfs\\calendar_dates.txt')
    Dates['agency_id'] = Agency
    '''
    #Concat to single region-wide GTFS dataframes
    routes = pd.concat([routes, Routes], axis=0,ignore_index=True,sort=True)
    stop = pd.concat([stop, Stops], axis=0, ignore_index=True,sort=True)
    trips = pd.concat([trips, Trips], axis=0,ignore_index=True,sort=True)
    times = pd.concat([times, Times], axis=0,ignore_index=True,sort=True)

#convert to geodataframe:
stops = gpd.GeoDataFrame(stop, geometry=gpd.points_from_xy(stop['stop_lon'],stop['stop_lat']),crs="EPSG:2958")
# Convert to Datetimes
times.arrival_time=times.arrival_time.str.zfill(8)
times.departure_time=times.departure_time.str.zfill(8)
times.arrival_time = pd.to_timedelta(times.arrival_time)
times.departure_time = pd.to_timedelta(times.departure_time)
stops.drop(columns=['stop_lon','stop_lat'],inplace=True)
print(stops.head())
# Interchange Identification
'''
Search for main terminals/stations with common names
Check list of stop names including interchange names contained in interchange 'hub list'
Collect stops corresponding to the same interchange to create to/from selection
'''
interchanges = pd.DataFrame(columns=['from_stop','from_geometry','to_stop','to_geometry'])
interchange = pd.DataFrame()
hub_list = pd.read_csv('GTHA_ConnectionHubs.csv')

for i in range(len(hub_list)):
    interchange = stops.iloc[i].str.contains(hub_list.hub[i])[['stop_name','geometry']]
    if hub_list.alt_hub1[i] != None:
        add1 = stops.iloc[i].str.contains(hub_list.alt_hub1[i])[['stop_name','geometry']]
        interchange = pd.concat([interchange,add1], axis=0)
        if hub_list.alt_hub2[i] != None:
            add2 = stops.iloc[i].str.contains(hub_list.alt_hub2[i])[['stop_name','geometry']]
            interchange = pd.concat([interchange,add2], axis = 0)
            if hub_list.alt_hub3[i] != None:
                add3 = stops.iloc[i].str.contains(hub_list.alt_hub3[i])[['stop_name','geometry']]
                interchange = pd.concat([interchange,add3], axis = 0)
    num = len(interchange)
    print(interchange.head())
    link = interchange
    interchange.rename({'stop_name':'from_stop','geometry':'from_geometry'},inplace=True)
    interchange['from_stop'].repeat(num)
    interchange[['to_stop','to_geometry']] = link
    interchanges = pd.concat([interchanges, interchange], axis=1)s

interchanges.drop_duplicates()
print(interchange.head())
interchanges = pd.concat(interchanges,interchange)

# Check
print(interchanges.head())

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
        
Alternative to R5Py: OPEN TRIP PLANNER (OTP):
    Route Request. set walkSpeed=??
https://docs.opentripplanner.org/en/v2.3.0/RouteRequest/ 
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

serv = pd.merge(times,trips,on='trip_id')
services = serv['route_id','route_short_name','stop_id','arrival_time', 'departure_time', 'trip_id','agency_id']

for i in range(len(interchange)):
    transfers = pd.empty
    giver = interchange[i]['stop_id_from']
    taker = interchange[i]['stop_id_to']
    minCT = interchange[i]['Min_Connection']
    arrcalls = services.loc[services['stop_id'] == giver]['route_id']
    depcalls = services.loc[services['stop_id'] == taker]['route_id']
    if len(arrcalls) > 1:
        callers = services.loc[services['stop_id'] == giver]['route_id']
        transfers.deroute = pd.concat(transfers,callers)
    else:
        transfers.deroute = services.loc[services['stop_id'] == giver]['route_id']
    if len(depcalls) > 1:
        callers = services.loc[services['stop_id'] == taker]['route_id']
        transfers.toroute = pd.concat(transfers,callers)
    else:
        transfers.toroute = services.loc[services['stop_id'] == taker]['route_id']
    
    # Identify giver in times dataframe.
    for k in range(arrcalls):
        for j in range(depcalls):
            transfers.deroute = arrcalls[k]
            transfers.toroute = depcalls[j]
            transfers.GiverArr = services.loc[(services['stop_id'] == giver) and 
                                          (services['route_id'] == arrcalls[k])]['arrival_time']
            transfers.TakerArr = services.loc[services['stop_id'] == taker and 
                                          (services['route_id'] == depcalls[j])]['arrival_time']
            transfers.GiverDep = services.loc[services['stop_id'] == giver and 
                                          (services['route_id'] == arrcalls[k])]['departure_time']
            transfers.TakerDep = services.loc[services['stop_id'] == taker and 
                                          (services['route_id'] == depcalls[j])]['departure_time']
    # Range for connecting service
        transfers.minDepTaker = transfers.GiverArr + dt.timedelta(minutes = minCT)
        transfers.maxDepTaker = transfers.GiverArr + dt.timedelta(minutes = maxCT)
        transfers.minDepGiver = transfers.TakerArr + dt.timedelta(minutes = minCT)
        transfers.maxDepGiver = transfers.TakerArr + dt.timedelta(minutes = maxCT)
        transfers.minArrTaker = transfers.GiverDep - dt.timedelta(minutes = minCT)
        transfers.maxArrTaker = transfers.GiverDep - dt.timedelta(minutes = maxCT)
        transfers.minArrGiver = transfers.TakerDep - dt.timedelta(minutes = minCT)
        transfers.maxArrGiver = transfers.TakerDep - dt.timedelta(minutes = maxCT)
    # Separate by route id.
    # Giver Route != Taker Route -- report in transfers dataframe
    # Same route must not call at same stop multiple times within connection window
    ## Identifying if connection met.
    # Not going to work because departures may not be lined up perfectly.
    for x in transfers:
        transfers.sort_values(by='GiverArr', ascending=True)
        transfers['GiverDepPerf'] = pd.between(transfers.minDepTaker,transfers.maxDepTaker,inclusive = 'both')
        transfers['GiverArrPerf'] = pd.between(transfers.minArrTaker,transfers.maxArrTaker,inclusive = 'both')
        transfers['TakerArrPerf'] = pd.between(transfers.minArrGiver,transfers.minArrGiver,inclusive = 'both')
        transfers['TakerDepPerf'] = pd.between(transfers.minDepGiver,transfers.maxDepGiver,inclusive = 'both')
        
    # Final Performance:
    interchanges[i].giver_perfor = sum(transfers.TakerDepPerf == True)/len(transfers.TakerDepPerf)
    interchanges[i].taker_perfor = sum(transfers.GiverArrPerf == True)/len(transfers.GiverArrPerf)
    interchanges[i].giver_perfba = sum(transfers.GiverDepPerf == True)/len(transfers.GiverDepPerf)
    interchanges[i].taker_perfba = sum(transfers.TakerArrPerf == True)/len(transfers.TakerArrPerf)        

# Output results
pd.interchanges.to_csv('GTHA_connections.csv')

# Plot Results Cartographically