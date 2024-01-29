# -*- coding: utf-8 -*-
"""
Created on Fri Aug 11 21:20:39 2023
@author: Jack Tattersall
CONNECTION PUNCTUALITY CALCULATOR
"""
import pandas as pd
import datetime as dt
import connex_main as y
import concurrent.futures

Date = 20221214
services = pd.read_csv(f'operatingtrips {Date}.csv',dtype={'route_id':object,
            'route_short_name':object,'pickup':int,'dropoff':int}, 
            usecols=['stop_id','arrival_time','departure_time','route_id','route_short_name',
            'direction_id','route_type','agency_id','stop_sequence','pickup','dropoff'])
interchanges = pd.read_csv("GTHA_interchanges_pairs.csv", usecols=['fid','from_stop',
        'to_stop','stop_id','route_id','route_short_name','direction_id','Min_Connect'])
GO = pd.read_csv("GO_Rail_Stations.csv",usecols=['stop_name','stop_id'])
gostn = GO['stop_name'].values.tolist()
# Maximum Connection Time
# Define based on a fixed value, beyond which a connection is not counted
maxCT = dt.timedelta(minutes=10) # Maximum connection time value for connection window

services.arrival_time = pd.to_timedelta(services.arrival_time)
services.departure_time = pd.to_timedelta(services.departure_time)
interchanges.Min_Connect = pd.to_timedelta(interchanges.Min_Connect, unit='m')

connections = services.merge(interchanges,on=['stop_id','route_id','route_short_name',
                                              'direction_id'],how='inner')
connections.reset_index(inplace=True,drop=True)
perform = pd.DataFrame()

'''   
with concurrent.futures.ProcessPoolExecutor(max_workers=12) as pool:
    print("initiated")
    futurx = {pool.submit(y.main, connections, trains, i, maxCT): i for i in gostn}
    for futur in concurrent.futures.as_completed(futurx):
        transfers = futurx[futur].result()
        connection.drop(columns=['arrival_time','departure_time'],inplace=True)
        connections.reset_index(inplace=True,drop=True)
        connections.drop_duplicates()
        performance = pd.concat([performance,transfers],axis=0)
    pool.shutdown()
'''
transfers = y.main(connections, "centennial go", maxCT, 1440)
perform = pd.concat([perform,transfers],axis=0)
connections.drop(columns=['arrival_time','departure_time'],inplace=True)
connections.drop_duplicates()
connections.reset_index(inplace=True,drop=True)

# Edit and update closing/post-processing as required
perform['from_route'], perform['from_direction'], perform['from_stop'],\
    perform['to_stop'], perform['to_id'], perform['to_route'], perform['direction'] = zip(*perform.Pair)
perform.drop(columns='Pair',inplace=True)
connect = pd.merge(perform, connections, left_on=['from_stop','to_stop','to_id','to_route','direction'],
                   right_on=['from_stop','to_stop','stop_id','route_short_name','direction_id'], how='left')
connect.drop(columns=['route_id','route_short_name','direction_id'], inplace = True)
connect.drop_duplicates(keep='first', inplace=True)

# Final Performance
connect['performance'] = round((connect['Success']/connect['Connect']), 3) # Output results
connect.reset_index(drop=True)
connect.to_csv('GTHA_connections_results.csv', index=False)

print('SCORE RESULTS:')
print(connect.performance.describe())
print('MINIMUM CONNECTION TIMES:')
print(connect.Min_Connect.describe())
