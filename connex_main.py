# -*- coding: utf-8 -*-
"""
Created on Wed Aug 30 14:45:34 2023
@author: Jack Tattersall
CONNECTION PUNCTUALITY CALCULATOR - MAIN FUNCTION
"""
# imports
import pandas as pd
import connex_functions as fcnx

# Main Function
def main(connections, i, maxCT, analysis_period):
    print("Processing Hub: ", i)  
    # Connecting departures dataframe
    connection = connections[connections['from_stop'] == i]
    tic = 0
    Hub = fcnx.Hub()
    while tic < analysis_period:
        # Hub.ConnectionAnalyzer(tic, connection, maxCT, analysis_period)
        Hub.ConnectionGiver(tic, connection, maxCT)
        tic+=1
    
    tac = 0
    while tac < analysis_period:
        Hub.ConnectionTaker(tac, connection, maxCT, analysis_period)
        tac+=1
    
    transfer = Hub.HubClosure()
    transfers = pd.DataFrame(transfer)
    return transfers