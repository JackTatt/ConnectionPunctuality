# -*- coding: utf-8 -*-
"""
Created on Sat Nov  4 13:09:15 2023
@author: Jack Tattersall
"""
import pandas as pd
import connex_functions as fx

Date = 20221214     # Date of analysis as string YYYYMMMDD
services = pd.read_csv(f'operatingtrips {Date}.csv',dtype={'route_id':object,
            'route_short_name':object}, usecols=['stop_id','arrival_time','departure_time',
            'route_id','route_short_name','direction_id','route_type','agency_id'])
pairs = pd.read_csv("GTHA_interchanges.csv")
interchanges = fx.StationIdentification(pairs, services)

# Minimum Connection Time 
walk = 60 # m / min or 1m/s
interchanges['Min_Connect'] = round(interchanges['WalkDistance'].astype(float)/walk)
# MinCT must be at least 2 minutes
interchanges['Min_Connect'].replace(to_replace=0, value=2, inplace=True)
interchanges['Min_Connect'].replace(to_replace=1, value=2, inplace=True)
pd.to_timedelta(interchanges['Min_Connect'],unit = 'm')

interchanges.to_csv('GTHA_interchanges_pairs.csv')