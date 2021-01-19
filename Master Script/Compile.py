import pandas as pd
import os as os
from os import path
import numpy as np
import sys
import errno
from pypvwatts import PVWatts

"""
This is a function that reads a file into a dataframe
and returns it, skipping row 1 and headers
"""
def GetFile(fname):
    df = pd.read_csv(fname,header=None, skiprows=2, index_col=None)
    return df
"""
This is a function that takes all the csv files in a folder
and concats them all toghether into one big sheet
"""
def gather(fname):
    #If the folder does not existm, catch and pass folder name up
    try:
        if not os.listdir(fname):
            pass
    except:
        raise OSError(fname)
    list = []
    for files in os.listdir(fname):
        if files.endswith(".csv"):
            path = fname + "/" + files
            list.append(path)
    frame = [GetFile(f) for f in list]

    #If the frame is empty, catch error and pass folder name up
    try:
        sheet = pd.concat(frame)
    except ValueError:
        raise ValueError(fname)
    return sheet

"""
This is a function that takes a production data frame, and a range,
and sums up the daily production number according to the range passed
"""
def ProductionSum(df, r):
    p = [] #Empty production array
    i = 0
    while i <= (r["Bill-Start-Date"].count() - 1):
        Start = r['Bill-Start-Date'].values[i]
        End = r['Bill-End-Date'].values[i]
        #Creates a boolean array of true false values, mask1
        mask1 = (pd.to_datetime(df['Date']) >= pd.to_datetime(Start)) & (pd.to_datetime(df['Date']) < pd.to_datetime(End))
        sum = df['Production'].loc[mask1].sum(axis=0)
        p.append(sum)
        i+=1
    return p

"""
This is a function that takes all of our data and prints it to the appropriatley
named csv file
"""
def output(list):
    for files in list:
        files.to_csv(files.name + ".csv", index=False)
"""
This is a function that takes our usage dataframe asks the user when the system was PTO'd
Then finds the range on our usage sheet where solar production should be present
"""
def setRange(df):
    ##Could do some error handling here to make sure correct start date was entered
    StartDate = raw_input("When was the system PTO? Enter as DD/MM/YYYY:\n")
    #Boolean array of true false values
    mask = df['Bill-Start-Date'] >= StartDate
    r = df[['Bill-Start-Date', 'Bill-End-Date']].loc[mask]
    return r, mask

"""
Function that takes our range, finds our APS-Production folder, calls gather on the sheets
Renames all of the columns for accessing, and then sums the production up
according to our range
"""
def getAPS(r):
    try:
        df = gather("APS-Production")
    except OSError as e:
        sys.exit("The specified folder '{}' does not exist".format(e))
    except ValueError as e:
        sys.exit("The specified folder '{}' does not have any csv files in it".format(e))

    df.columns = ['Date', 'On-Peak-Usage', 'Off-Peak-Usage', 'Other-Peak-Usage', 'Total-Delivered', 'Production', 'AVG-Temp']
    df['Date'] = pd.to_datetime(df['Date'])
    df.sort_values(by='Date',ascending=True, inplace=True)
    production = ProductionSum(df, r)
    p = pd.Series(production, name="APS-Solar-Production")
    df = pd.concat([df.reset_index(drop=True), r.reset_index(drop=True), p], axis=1)
    df.name = "APSProduction"
    return df

"""
Function that takes our range, finds our SE-Production folder, calls gather on the sheets
Renames all of the columns for accessing, and then sums the production up
according to our range
"""
def getSE(r):
    try:
        df = gather("SE-Production")
    except OSError as e:
        sys.exit("The specified folder '{}' does not exist".format(e))
    except ValueError as e:
        sys.exit("The specified folder '{}' does not have any csv files in it".format(e))

    df.columns = ['Date','Production(Wh)']
    df["Production"] = df["Production(Wh)"].div(1000)
    df['Date'] = pd.to_datetime(df['Date'])
    df.sort_values(by='Date', ascending=True, inplace=True)
    df.name = "SolarEdge-Exported"
    production = ProductionSum(df, r)
    p = pd.Series(production, name='SE-Production')
    df = pd.concat([df.reset_index(drop=True), r.reset_index(drop=True), p], axis=1)
    df.name = 'SE-Production'
    return df

"""
Function that finds our Solar-Exported folder, calls gather on the sheets
Renames all of the columns for accessing
"""
def getSolarExported():
    try:
        df = gather("Solar-Exported")
    except OSError as e:
        sys.exit("The specified folder '{}' does not exist".format(e))
    except ValueError as e:
        sys.exit("The specified folder '{}' does not have any csv files in it".format(e))

    df.columns = ['Solar-Exported', 'On-Peak','Off-Peak','Month','Range', 'End-Date']
    df.sort_values(by='End-Date', ascending=True, inplace=True)
    df.name = 'SolarExported'
    return df

def sortAddress(df):
    print("We Found more than one address, which would you like to use?")
    for i in range(0, len(df['Address'].unique() )):
        print("{}: {}".format(i, df['Address'].unique()[i]))
    choice = int(raw_input("Enter the number for the correct adddress."))


"""
Function that reads our usage sheet, labels the columnds for access,
switches Billed-Amount and Total-Bought for our report
"""
def getUsage():
    #If the file does not exist, catch and print "cannot find file"
    try:
        df = pd.read_csv('APS-Usage.csv', header=None, skiprows=1, index_col=None)
    except:
        raise OSError("APS-Usage.csv")

    cols = list(df)
    cols[5], cols[6] = cols[6], cols[5]
    df = df[cols]
    df.columns = ["Address", "Service-Plan" , "Bill-Start-Date", "Bill-End-Date", "Billing-Days", "Billed-Amount", "Total-Bought"]
    #Function call here to filter out multiple addresses
    if( (len(df['Address'].unique())) > 1 ):
        sortAddress(df)

    df["Bill-Start-Date"] = pd.to_datetime(df["Bill-Start-Date"])
    df["Bill-End-Date"] = pd.to_datetime(df["Bill-End-Date"])
    df.name = 'APSUsage'
    return df

"""
Function that gets the pvwatts info for all the arrays, sums up the monthly
and yealy numbers, then adjusts them. We take our estimated production "estprod"
and divide it by pvwatts.ac_annual, and call it eff. monthly = monthly * eff
"""
def getPVWatts():
    PVWatts.api_key = 'qkk2DRRxcBpTZXA13qOGFlwouL9ZWQaLZxKjZCdd'
    lat = float(raw_input("Enter the Lattitude of the system:"))
    lon = float(raw_input("Enter the longitude of the system:"))
    ratio = float(raw_input("Enter the DC/AC ratio:"))
    estprod = int(raw_input("Enter out estimated yearly production for the system: "))

    numArrays = int(raw_input("Enter the number total number of unique arays:"))
    monthly = [0,0,0,0,0,0,0,0,0,0,0,0]
    yearly = 0
    for i in range(0, numArrays):
        s_c = float(raw_input("Enter the system size in kWh for array {}:".format(i)))
        tilt = int(raw_input("Enter the tilt of array {}:".format(i)))
        azimuth = int(raw_input("Enter the azimuth of array {}:".format(i)))
        result = PVWatts.request(system_capacity=s_c, module_type=1, array_type=1, azimuth=azimuth,
        tilt=tilt, dataset="tmy2", losses=9,lat=lat,lon=lon,dc_ac_ratio=ratio, gcr=.4, inv_eff=99.5)
        yearly += int(result.ac_annual)
        monthly = np.add(monthly, result.ac_monthly)

    eff = float(estprod)/float(yearly)
    adj = monthly*eff
    adj  = pd.Series(adj, name='adj')
    monthly = pd.Series(monthly, name='monthly')
    return adj
"""
This is the function that does a APS RCP analysis, This will be called
by the model class once it is written.
"""
def APSAnalysis():
    try:
        APSUsage = getUsage()
    except OSError as e:
        sys.exit("The file '{}' does not exist, or is the incorrect file type".format(e))
    Range, mask = setRange(APSUsage)
    APSProduction = getAPS(Range)

    #There may not always be SE-Production, how do I handle that
    SEProduction = getSE(Range)
    SolarExported = getSolarExported()

    Base = APSUsage.loc[mask]
    adj = getPVWatts()
    report = pd.concat([Base.reset_index(drop=True), APSProduction['APS-Solar-Production'].reset_index(drop=True),
        SolarExported['Solar-Exported'].reset_index(drop=True), SEProduction['SE-Production'].reset_index(drop=True),
        adj.reset_index(drop=True)],axis=1)
    report.name ='report'

    list = [APSProduction, SolarExported, SEProduction, report]
    output(list)

APSAnalysis()
"""
TODO
1. Handle the case where they do not have SE monitoring
2. Add support for APS Net Metering
3. Add main controller class.
4. I want to type run Analysis.py fname analysis_type, where analysis_type is
    Net Metering or RCP
"""
