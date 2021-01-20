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
This is a function that takes our usage dataframe asks the user when the system was PTO'd
Then finds the range on our usage sheet where solar production should be present
"""
def setRange(df):
    ##Could do some error handling here to make sure correct start date was entered
    StartDate = input("When was the system PTO? Enter as DD/MM/YYYY:\n")
    #Boolean array of true false values
    mask = df['Bill-Start-Date'] >= StartDate
    r = df[['Bill-Start-Date', 'Bill-End-Date']].loc[mask]
    return r, mask

"""
Function that takes our range, finds our APS-Production folder, calls gather on the sheets
Renames all of the columns for accessing, and then sums the production up
according to our range
"""
def getAPS(fname, r):
    try:
        df = gather(fname + "\APS-Production")
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
    df.to_csv(fname + "\APS-Production.csv", index=False)
    return df

"""
Function that takes our range, finds our SE-Production folder, calls gather on the sheets
Renames all of the columns for accessing, and then sums the production up
according to our range
"""
def getSE(fname, r):
    try:
        df = gather(fname + "\SE-Production")
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
    df.to_csv(fname + "\SE-Production.csv", index=False)
    return df

"""
Function that finds our Solar-Exported folder, calls gather on the sheets
Renames all of the columns for accessing
"""
def getSolarExported(fname):
    try:
        df = gather(fname + "\Solar-Exported")
    except OSError as e:
        sys.exit("The specified folder '{}' does not exist".format(e))
    except ValueError as e:
        sys.exit("The specified folder '{}' does not have any csv files in it".format(e))

    df.columns = ['Solar-Exported', 'On-Peak','Off-Peak','Month','Range', 'End-Date']
    df.sort_values(by='End-Date', ascending=True, inplace=True)
    df.name = 'SolarExported'
    df.to_csv(fname + "\Solar-Exported.csv", index=False)
    return df

def sortAddress(df):
    print("We Found more than one address, which would you like to use?")
    for i in range(0, len(df['Address'].unique() )):
        print("{}: {}".format(i, df['Address'].unique()[i]))
    choice = input("Enter the number for the correct adddress.")


"""
Function that reads our usage sheet, labels the columnds for access,
switches Billed-Amount and Total-Bought for our report
"""
def getUsage(fname):
    #If the file does not exist, catch and print "cannot find file"

    try:
        df = pd.read_csv(fname + '\APS-Usage.csv', header=None, skiprows=1, index_col=None)
    except:
        raise OSError( fname + "\APS-Usage.csv")

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
    address = input("Enter the HO address: ")
    ratio = float(input("Enter the DC/AC ratio:"))
    estprod = float(input("Enter out estimated yearly production for the system: "))

    numArrays = int(input("Enter the number total number of unique arays:"))
    monthly = [0,0,0,0,0,0,0,0,0,0,0,0]
    yearly = 0
    for i in range(0, numArrays):
        s_c = float(input("Enter the system size in kWh for array {}:".format(i)))
        tilt = int(input("Enter the tilt of array {}:".format(i)))
        azimuth = int(input("Enter the azimuth of array {}:".format(i)))
        result = PVWatts.request(system_capacity=s_c, module_type=1, array_type=1, azimuth=azimuth,
        tilt=tilt, dataset="tmy2", losses=9,address=address,dc_ac_ratio=ratio, gcr=.4, inv_eff=99.5)
        yearly += int(result.ac_annual)
        monthly = np.add(monthly, result.ac_monthly)

    eff = float(estprod)/float(yearly)
    adj = monthly*eff
    adj  = pd.Series(adj, name='adj-daily-total')
    adj[0] = adj[0]/31
    adj[1] = adj[1]/28
    adj[2] = adj[2]/31
    adj[3] = adj[3]/30
    adj[4] = adj[4]/31
    adj[5] = adj[5]/30
    adj[6] = adj[6]/31
    adj[7] = adj[7]/31
    adj[8] = adj[8]/30
    adj[9] = adj[9]/31
    adj[10] = adj[10]/30
    adj[11] = adj[11]/31
    return adj
"""
This is the function that does a APS RCP analysis, This will be called
by the model class once it is written.
"""
def APSAnalysis(fname):
    try:
        APSUsage = getUsage(fname)
    except OSError as e:
        sys.exit("The file '{}' does not exist, or is the incorrect file type".format(e))
    Range, mask = setRange(APSUsage)
    APSProduction = getAPS(fname, Range)
    flag = False
    #There may not always be SE-Production, how do I handle that
    if (os.path.isdir(fname + "\SE-Production")):
        SEProduction = getSE(fname, Range)
        flag = True
    SolarExported = getSolarExported(fname)

    Base = APSUsage.loc[mask]
    adj = getPVWatts()
    report = pd.concat([Base.reset_index(drop=True), APSProduction['APS-Solar-Production'].reset_index(drop=True),
        SolarExported['Solar-Exported'].reset_index(drop=True), adj.reset_index(drop=True)],axis=1)
    report.name = (fname + '\\report.csv')
    if(flag):
        report = pd.concat([SEProduction.reset_index(drop=True)], axis=1)
    report.to_csv(report.name, index=False)

def TEPAnalysis(fname):
    adj = getPVWatts()
    adj.to_csv("Adjusted-Daily-Values.csv", index=False)

def NVEAnalysis(fname):
    adj = getPVWatts()
    adj.to_csv("Adjusted-Daily-Values.csv", index=False)

def SRPAnalysis(fname):
    adj = getPVWatts()
    adj.to_csv("Adjusted-Daily-Values.csv", index=False)

def controller(fname):
    print("What type of analysis would you like to run?")
    print("1. APS")
    print("2. TEP")
    print("3. NVE")
    print("4. SRP")
    option = int(input("Number: "))
    if option == 1:
        APSAnalysis(fname)
    if option == 2:
        TEPAnalysis(fname)
    if option == 3:
        NVEAnalysis(fname)
    if option == 4:
        SRPAnalysis(fname)

def main():
    controller(sys.argv[1])

if __name__ =="__main__":
    main()

"""
TODO
1. Manually create the range needed to run getSE(fname, range) for TEP NVE & SRP
"""
