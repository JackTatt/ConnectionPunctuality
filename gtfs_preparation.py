# -*- coding: utf-8 -*-
"""
GTFS PREPROCESSING
Created on Mon Oct 16 13:45:04 2023
@author: Jack Tattersall
"""
import pandas as pd
import connex_functions as cfx

Date = 20221214     # Date of analysis as string YYYYMMMDD
# Specify list of agencies in analysis
Scope = ['TTC', 'GO', 'YRT', 'MiWay','DRT', 'HSR','BT', 'MilT', 'OakT', 'BurT', 'UPExpress']
## Preprocessing function
output = cfx.Immigration(Date,Scope) 
tours = output[0]
stops = output[1]

# Simplify stop names to lowercase
stops.stop_name = stops.stop_name.str.strip('"')
stops.stop_name = stops.stop_name.str.lower()

# GO Rail Station Identification
GO = pd.read_csv(f'Static GTFS\\GO_{Date}_gtfs\\stops.txt',
                        usecols=['stop_id','stop_name','stop_lat','stop_lon'],header=0)
GO.stop_name = GO.stop_name.str.lower()
GO = GO[GO.stop_id.str.isalpha() == True]  # GO Rail stations only
# Discard TTC Subway Stations and non-GTHA stations
GO = GO[~GO['stop_id'].isin(['UN','PA','KP','KE','KI','GL','AD','BA','BL','DW','DA','SM','SF','LN','NI'])]
GO['stop_id'] = 'GO_' + GO['stop_id'].str[:]

# Transit trip GTFS cleaning and processing
services = cfx.Preprocessing(stops, tours)
stops.to_csv(f'stops {Date}.csv')
services.to_csv(f'operatingtrips {Date}.csv')
GO.to_csv('GO_Rail_Stations.csv')