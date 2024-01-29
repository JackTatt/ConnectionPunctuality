# -*- coding: utf-8 -*-
"""
Created on Thu May 25 20:27:17 2023
@author: Jack Tattersall
CONNECTION PUNCTUALITY CALCULATOR FUNCTIONS
"""
import pandas as pd
import datetime as dt
import json
import requests

# File Imports
'''
GTFS Format:
GTFS files must be already unzipped. 
Constituent files routes.txt and agencies.txt must include
column titled agency_id with correct AGENCYID in each entry.
'''
def Immigration(Date, scope):
    # Initialalize dataframe
    tours = pd.DataFrame()
    stops = pd.DataFrame()
    # Load Data Frames 
    for Agency in scope:
        # Import as dataframes and add agency_id
        Routes = pd.read_csv(f'Static GTFS\\{Agency}_{Date}_gtfs\\routes.txt', 
            usecols=['agency_id','route_id', 'route_short_name','route_type'], 
            sep=',', header=0, index_col=False, dtype={'route_id':object})
        Stops = pd.read_csv(f'Static GTFS\\{Agency}_{Date}_gtfs\\stops.txt',
            usecols=['stop_id', 'stop_name', 'stop_lat', 'stop_lon'], header=0,
            sep=',', dtype={'stop_id':object}, index_col=False)
        Times = pd.read_csv(f'Static GTFS\\{Agency}_{Date}_gtfs\\stop_times.txt',
            usecols=['trip_id','stop_id','arrival_time','departure_time','stop_sequence','pickup_type',
            'drop_off_type'],header=0,sep=',',dtype={'stop_id':object,'trip_id':object,
            'pickup_type':"Int64",'drop_off_type':"Int64"},index_col=False)      
        if Agency in ['GO','UPExpress','MiWay']:        # Sort special service_ids
            Trip = pd.read_csv(f'Static GTFS\\{Agency}_{Date}_gtfs\\trips.txt',
                    usecols=['trip_id','service_id','route_id','direction_id'],
                    header=0, sep=',', dtype={'route_id':object,'trip_id':object})
            dates = pd.read_csv(f'Static GTFS\\{Agency}_{Date}_gtfs\\calendar_dates.txt',
                                    header=0, sep=',', dtype={'date':int})
            ids = dates[dates['date']==Date]['service_id'].values.tolist()
            Trip = Trip.loc[Trip['service_id'].isin(ids)]
        if Agency in ['MilT']:   # These agencies fail to include direction_id tag
            Trip = pd.read_csv(f'Static GTFS\\{Agency}_{Date}_gtfs\\trips.txt',
                    usecols=['trip_id','service_id','route_id'], header=0,
                    sep=',', dtype={'route_id':object,'trip_id':object})
            dates = pd.read_csv(f'Static GTFS\\{Agency}_{Date}_gtfs\\calendar_dates.txt',
                                    header=0, sep=',', dtype={'date':int})
            ids = dates[dates['date']==Date]['service_id'].values.tolist()
            Trip = Trip.loc[Trip['service_id'].isin(ids)]
            Trip['direction_id'] = 0
        if Agency in ['BT','BurT','DRT','TTC','YRT']:
            Trip = pd.read_csv(f'Static GTFS\\{Agency}_{Date}_gtfs\\trips.txt',
                usecols=['trip_id','service_id','route_id','direction_id'],
                header=0, sep=',', dtype={'route_id':object,'trip_id':object})
            calendar = pd.read_csv(f'Static GTFS\\{Agency}_{Date}_gtfs\\calendar.txt',sep=',',
                        header=0, dtype={'start_date':int,'end_date':int,'route_id':object})
            ids = calendar[(calendar['start_date']<=20221214)&(calendar['end_date']
                    >=20221214)&(calendar['wednesday']==1)]['service_id'].values.tolist()
            dates = pd.read_csv(f'Static GTFS\\{Agency}_{Date}_gtfs\\calendar_dates.txt',
                                    header=0,sep=',', dtype={'date':int})
            ids = ids + dates[dates['date']==Date]['service_id'].values.tolist()
            Trip = Trip.loc[Trip['service_id'].isin(ids)]
        # Concat to single region-wide GTFS dataframes
        Stops['stop_id'] = Agency + '_' + Stops['stop_id'].str[:]
        stops = pd.concat([Stops,stops],ignore_index=True)
        # Merging dataframes into single one
        Trips = pd.merge(Routes, Trip, on='route_id')
        Tours = pd.merge(Times, Trips, on='trip_id')
        Tours.drop(columns=['trip_id','service_id'], inplace=True)
        Tours['stop_id'] = Agency + '_' + Tours['stop_id'].str[:]
        tours = pd.concat([Tours,tours],ignore_index=True)
    stops.reset_index(drop=True,inplace=True)
    tours.reset_index(drop=True,inplace=True)
    return tours,stops

# Data preprocessing
'''
Convert times to Datetime Timedelta format, rounding out seconds
Convert stop_names to all lowercase for simplicity
Combine trips, times, routes dataframes for required processing
'''
def Preprocessing(stops,calls):
    services = pd.merge(stops, calls, on='stop_id')
    services['pickup'] = services.pickup_type.fillna(0)
    services['dropoff'] = services.drop_off_type.fillna(0)
    services = services.drop(columns=['pickup_type','drop_off_type'])
    services.dropna(subset=['stop_name','stop_lat','stop_lon','route_id','route_short_name',
                    'arrival_time','departure_time','route_type'],inplace=True)
    # Convert to Datetimes
    services.arrival_time = pd.to_timedelta(services.arrival_time.str.zfill(8))
    services.departure_time = pd.to_timedelta(services.departure_time.str.zfill(8))
    # Round times to nearest minute and convert all to less than a day
    services.loc[:,'arrival_time'] = services.arrival_time.dt.round("min")
    services.loc[:,'departure_time'] = services.departure_time.dt.round("min")
    services.arrival_time = services.arrival_time - pd.to_timedelta(
        services.arrival_time.dt.days, unit='d')
    services.departure_time = services.departure_time - pd.to_timedelta(
        services.departure_time.dt.days, unit='d')
    services.sort_values(['arrival_time','departure_time'],ascending=True,inplace=True)
    services.reset_index(inplace=True,)
    return services

# Interchange Processing
def StationIdentification(pairs,services):
    service=services[['stop_id','route_id','route_short_name','direction_id']]
    pairs['WalkDistance'] = getDistances(pairs)
    interchanges = pairs.merge(service,on='stop_id',how='inner')
    interchanges.sort_values('WalkDistance',axis=0,ascending=True,inplace=True)
    interchanges.drop_duplicates(inplace=True)
    interchanges.reset_index(drop=True,inplace=True)
    return interchanges

'''
CONNECTION WINDOW: Defined by Minimum and Maximum Connection Times
Minimum Connection Time must be at least 2 minutes, using OSRM API.
'''
def getDistances(loci):
    distlist = []
    url = "http://router.project-osrm.org/route/v1/foot/"
    for index, rows in loci.iterrows():
        call = url \
        + str(rows.from_lon) + ',' + str(rows.from_lat) + \
        ';' + str(rows.to_lon) + ',' + str(rows.to_lat)
        output = json.loads(requests.get(call,timeout=10).content)
        if output['code'] == 'Ok':
            distlist.append(output["routes"][0]["distance"])
    return distlist
 
''' Connection Hub Processing Class'''
class Hub(object):
    
    def __init__(self):
        self.connex = set()
        self.conarr = set()
        self.condep = set()
        '''
Connex tuple formt:
(from_route, from_direction, from_stop, arrival_time, to_stop, to_id, to_route, direction, mindep, maxdep)
Note index = ( 0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
        '''
        self.transfers = {'Pair':[],     # Tuple of FromStop, ToStop, Route
                       'Connect':[],   # Number of connections
                       'Success':[]}   # Number of successful connections (counter)
        '''
        transfers dict 'Pair' list tuple format:
        (from_stop,to_stop, route_short_name, direction_id)
        '''
        
    def ConnectionAnalyzer(self, tic, services, maxCT, minutes):
        toc = dt.timedelta(minutes = tic) + pd.to_timedelta('07:00:00')
        print(toc)
        # Connection Giver
        service = services[services['arrival_time']==toc]
        for j, sta in service.iterrows():
            if sta['dropoff'] != 1:
            # GO Train as Connection Giver
                if sta['from_stop'] == sta['to_stop']:
                    connect = services[services['to_stop'] != sta['from_stop']][['from_stop','to_stop','stop_id',
                            'route_short_name','direction_id','Min_Connect']].drop_duplicates()
                    for j, cx in connect.iterrows():
                        mindep = toc + cx['Min_Connect']
                        maxdep = mindep + maxCT
                        self.connex.add((sta['route_short_name'],sta['direction_id'],sta['from_stop'],toc,
                            cx['to_stop'],cx['stop_id'],cx['route_short_name'],cx['direction_id'],mindep,maxdep))
            # Local Transit as Connection Giver
                else:
                    mindep = toc + sta['Min_Connect']
                    maxdep = mindep + maxCT
                    for i in services[services['to_stop']==sta['from_stop']]['route_short_name']:
                        self.connex.add((sta['route_short_name'],sta['direction_id'],sta['to_stop'],
                                         toc,sta['from_stop'],sta['stop_id'],i,0,mindep,maxdep))
                        self.connex.add((sta['route_short_name'],sta['direction_id'],sta['to_stop'],
                                         toc,sta['from_stop'],sta['stop_id'],i,1,mindep,maxdep))
                    
        # Connection Taker
        service = services[services['departure_time']==toc]
        for i, chg in service.iterrows():
            if chg['pickup'] != 1:
                for k in self.connex.copy():
                    art, adi, arr, dep, did, rte, dic = k[0], k[1], k[2], k[4], k[5], k[6], k[7]
                    if (dep==chg['to_stop'])&(did==chg['stop_id'])&\
                        (rte==chg['route_short_name'])&(dic==chg['direction_id']):
                        tp = (art, adi, arr, dep, did, rte,dic)
                        if tp not in self.transfers['Pair']:
                            self.transfers['Pair'].append(tp)
                            self.transfers['Connect'].append(0)
                            self.transfers['Success'].append(0)
                        idx = self.transfers['Pair'].index(tp)
                        if (k[8] <= toc <= k[9]):
                            self.transfers['Connect'][idx] += 1
                            self.transfers['Success'][idx] += 1
                            # Close out connection processing by deleting row
                            self.connex.remove(k)
                        # Discard Missed Connections outside Window
                        elif k[9] < toc < (k[8] + pd.to_timedelta('01:00:00')):
                            self.transfers['Connect'][idx] += 1
                            # Close out connection processing by deleting row
                            self.connex.remove(k)
                        elif toc > (k[8] + pd.to_timedelta('01:00:00')):
                                # Connection not counted. Close entirely.
                                self.connex.remove(k)
                    elif tic == (minutes - 1):    #Remove connections that are never closed by EOD.
                        # Assume no connections
                        self.connex.remove(k)
            if chg['pickup'] == 1:
                service.drop(i, axis='index')
        return True
    
    def ConnectionGiver(self, tic, services, maxCT):
        toc = dt.timedelta(minutes = tic) + pd.to_timedelta('07:00:00')
        print('Giver Clock:',toc)
        servicarr = services[services['arrival_time']==toc]
        for j, sta in servicarr.iterrows():
            if sta['dropoff'] != 1:
                if sta['from_stop'] == sta['to_stop']:
                    connect = services[services['to_stop'] != sta['from_stop']][['from_stop','to_stop','stop_id',
                            'route_short_name','direction_id','Min_Connect']].drop_duplicates()
                    for j, cx in connect.iterrows():
                        mindep = toc + cx['Min_Connect']
                        maxdep = mindep + maxCT
                        self.condep.add((sta['route_short_name'],sta['direction_id'],sta['from_stop'],toc,
                            cx['to_stop'],cx['stop_id'],cx['route_short_name'],cx['direction_id'],mindep,maxdep))
                        
        servicdep = services[services['departure_time']==toc]
        for j, sta in servicdep.iterrows():
            if sta['dropoff'] != 1:
                if sta['from_stop'] == sta['to_stop']:
                    connect = services[services['to_stop'] != sta['from_stop']].drop_duplicates()
                    for j, cx in connect.iterrows():
                        maxarr = toc - cx['Min_Connect']
                        minarr = maxarr - maxCT
                        self.conarr.add((cx['route_short_name'],cx['direction_id'],cx['to_stop'],toc,
                            cx['from_stop'],sta['stop_id'],sta['route_short_name'],sta['direction_id'],minarr,maxarr))
                
    def ConnectionTaker(self, tac, services, maxCT, minutes):
        toc = dt.timedelta(minutes = tac) + pd.to_timedelta('07:00:00')
        print('Taker Clock:',toc)
        servicdep = services[services['departure_time']==toc]
        for i, chg in servicdep.iterrows():
            if chg['pickup'] != 1:
                for k in self.condep.copy():
                    art, adi, arr, dep, did, rte, dic = k[0], k[1], k[2], k[4], k[5], k[6], k[7]
                    if (arr==chg['from_stop'])&(dep==chg['to_stop'])&(did==chg['stop_id'])\
                        &(rte==chg['route_short_name'])&(dic==chg['direction_id']):
                        tp = (art, adi, arr, dep, did, rte, dic)
                        if tp not in self.transfers['Pair']:
                            self.transfers['Pair'].append(tp)
                            self.transfers['Connect'].append(0)
                            self.transfers['Success'].append(0)
                        idx = self.transfers['Pair'].index(tp)
                        if (k[8] <= toc <= k[9]):
                            self.transfers['Connect'][idx] += 1
                            self.transfers['Success'][idx] += 1
                            # Close out connection processing by deleting row
                            self.condep.remove(k)
                        # Discard Missed Connections outside Window
                        elif k[9] < toc < (k[8] + pd.to_timedelta('01:00:00')):
                            self.transfers['Connect'][idx] += 1
                            # Close out connection processing by deleting row
                            self.condep.remove(k)
                        elif toc > (k[8] + pd.to_timedelta('01:00:00')):
                            # Connection not counted. Close entirely.
                            self.condep.remove(k)
                    elif tac == (minutes - 1):    #Remove connections that are never closed by EOD.
                        # Assume no connections
                        self.condep.remove(k)
            if chg['pickup'] == 1:
                servicdep.drop(i, axis='index')
                
            servicarr = services[services['arrival_time']==toc]
            for i, chg in servicarr.iterrows():
                if chg['pickup'] != 1:
                    for k in self.conarr.copy():
                        art, adi, arr, dep, did, rte, dic = k[0], k[1], k[2], k[4], k[5], k[6], k[7]
                        if (dep==chg['from_stop'])&(arr==chg['to_stop'])&\
                            (art==chg['route_short_name'])&(adi==chg['direction_id']):
                            tp = (art, adi, arr, dep, did, rte, dic)
                            if tp not in self.transfers['Pair']:
                                self.transfers['Pair'].append(tp)
                                self.transfers['Connect'].append(0)
                                self.transfers['Success'].append(0)
                            idx = self.transfers['Pair'].index(tp)
                            if (k[8] <= toc <= k[9]):
                                self.transfers['Connect'][idx] += 1
                                self.transfers['Success'][idx] += 1
                                # Close out connection processing by deleting row
                                self.conarr.remove(k)
                                # Discard Missed Connections outside Window
                            elif k[9] < toc < (k[8] + pd.to_timedelta('01:00:00')):
                                self.transfers['Connect'][idx] += 1
                                # Close out connection processing by deleting row
                                self.conarr.remove(k)
                            elif toc > (k[8] + pd.to_timedelta('01:00:00')):
                                # Connection not counted. Close entirely.
                                self.conarr.remove(k)
                        elif tac == (minutes - 1):    #Remove connections that are never closed by EOD.
                            # Assume no connections
                            self.conarr.remove(k)
                    if chg['pickup'] == 1:
                        servicarr.drop(i, axis='index')
        return True
                    
    def HubClosure(self):
        return self.transfers   
    