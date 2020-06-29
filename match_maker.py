import io
import process_xml
import process_csv
import adwords_pull
import analytics_pull
import csv_parser
import google_auth
import pandas as pd
import pandasql
from datetime import date
from dateutil.relativedelta import relativedelta, SU,MO,TU,WE,TH,FR,SA
import datetime as datetime
from currency_converter import CurrencyConverter
def main(filePath):
    c = CurrencyConverter()
    today = date.today()

    mon = today + relativedelta(weekday=MO(-2)) #last MON
    tue = today + relativedelta(weekday=TU(-1)) #last TUE
    wed = today + relativedelta(weekday=WE(-1)) #last WED
    thu = today + relativedelta(weekday=TH(-1)) #last THU
    fri = today + relativedelta(weekday=FR(-1)) #last FRI
    sat = today + relativedelta(weekday=SA(-1)) #last SAT
    sun = today + relativedelta(weekday=SU(-1)) #last SUN
    weekArr = [mon,tue,wed,thu,fri,sat,sun]

    print("Executing for dates: ")
    for day in weekArr:
        print(day)
        print(day.strftime("%m"))
    
    countryArr = {"US": "ACCOUNT NAME"}
    
    print('loading XML dataframe...')
    dfXML = process_xml.main()

    print('loading adwords csv...')
    adwordsCSV = adwords_pull.get_raw_report()

    print('parsing adwords into dataframe...')
    dfCSV = csv_parser.parseToCSV(adwordsCSV,"ADW")

    print('parsing analytics into dataframe...')
    analyticsCSV = ""
    #pull for each day of the week - avoids data sampling this way but takes a bit longer
    #THIS NEEDS TO BE REFACTORED IN THE FUTURE
    for day in weekArr:#
        analyticsCSV += analytics_pull.main(str(day))
        print("Pulled date: " + str(day))
    dfAnalytics = csv_parser.parseToCSV(analyticsCSV,"ANL")
    
    dfAnalytics['date'] = dfAnalytics.apply(lambda row: CheckDateFormatAnalytics(row), axis=1)
    dfAnalytics = dfAnalytics[dfAnalytics.date != -1]

    #add month and Year to analytics
    dfAnalytics['monthNum'] = dfAnalytics.apply(lambda row: ApplyMonth(row,"ANL"), axis=1)
    dfAnalytics['yearNum'] = dfAnalytics.apply(lambda row: ApplyYear(row,"ANL"), axis=1)

    print("Done Processing APIs...")
    print("Processing Data...")

    dfXML['ID'] = dfXML['ID'].str.lower()
    dfCSV['GID'] = dfCSV['GID'].str.lower()
    dfXML['GID'] = dfXML['GID'].str.lower()
    dfCSV['OfferID'] = dfCSV['OfferID'].str.lower()


    #FILER US ONLY
    sql = f'''
    SELECT *
    FROM dfCSV 
    WHERE dfCSV.Account = "{countryArr["US"]}"
    '''
    dfCSV = pandasql.sqldf(sql, locals())

    dfCSV['monthNum'] = dfCSV.apply(lambda row: ApplyMonth(row), axis=1)
    dfCSV['yearNum'] = dfCSV.apply(lambda row: ApplyYear(row), axis=1)  

    #REMOVE THE SPACE AFTER THE GID
    GIDnew = []
    for dfCSVIndex, dfCSVRow in dfCSV.iterrows(): 
        newGID = dfCSVRow['GID'].partition(" ")[0]

        GIDnew.append(newGID)
    print(len(GIDnew))
    print(dfCSV.shape)
    dfCSV=dfCSV.drop(columns="GID")
    dfCSV.loc[:,"GID"] = GIDnew
    print("processing csv to xml matches for linking table")
    #take the XML feed and the adwords report and EXACT match the OfferIDs together
    #ATTACH GTINS TO PRODUCTS
    #   GOAL IS TO MATCH GTIN ON EVERY GROUP ID FROM ADWORDS
    #   BEST WAY TO DO IT IS GROUP ADWORDS DF BY GID TO MAKE THE FILE SMALLER
    sqlGroupAdwords = '''
    SELECT dfCSV.GID,
    SUM(dfCSV.Impressions) AS Impressions,
    SUM(dfCSV.Clicks) AS Clicks,
    SUM(dfCSV.Cost) AS Cost,
    SUM(dfCSV.ConversionValue) AS ConversionValue,
    dfCSV.monthNum as monthNum, 
    dfCSV.yearNum as yearNum,
    dfCSV.Account as Account
    FROM dfCSV
    GROUP BY dfCSV.GID, dfCSV.Account, dfCSV.monthNum, dfCSV.yearNum
    '''
    # DROP UNUSED DF
    dfCSV.iloc[0:0]
    # WRITE NEW DF
    adwordsGrpDF = pandasql.sqldf(sqlGroupAdwords, locals())

    sql = '''
    SELECT dfXML.GTIN as GTIN, 
    dfXML.ID as ID,
    dfXML.GID as GID,
    dfXML.IMG as IMG, 
    dfXML.BRAND as BRAND, 
    adwordsGrpDF.monthNum as monthNum, 
    adwordsGrpDF.yearNum as yearNum,
    adwordsGrpDF.Impressions as Impressions, 
    adwordsGrpDF.Clicks as Clicks, 
    adwordsGrpDF.ConversionValue as ConversionValue,
    adwordsGrpDF.Cost as Cost
    FROM dfXML, adwordsGrpDF 
    WHERE dfXML.GID = adwordsGrpDF.GID
    '''
    # DROP UNUSED DF
    adwordsGrpDF.iloc[0:0]
    dfXML.iloc[0:0]

    # WRITE NEW DF
    linkingTable = pandasql.sqldf(sql, locals())
    
    print("-------XML AND ADWORDS MATCHED ON OfferID VIA SQL-------")
    print("-------LINKS GTIN TO THE OFFER ID === THEREFORE GTIN TO THE Adwords Metrics-------")

    #At this point the linkingTable DF holds the adwords stats and the gtin of the products

    #popular products CVS from import into df
    #imported CSV in dataframe form
    print('parsing imported CSV into dataframe...')
    print('----------------impoted CSV dataframe----------------')
    dfICSV = process_csv.main(filePath)
    #print(dfICSV)

    #match popular products with linkingTable
    dfICSV.rename(columns={'PopularityRank' : 'Pop'},inplace=True)

    #order by popularity
    dfICSV.sort_values(by=['Pop'])

    #TAKE THE FIRST 2000 RECORDS FROM THE POPULAR PRODUCTS CSV INTO A NEW DF - This can be any number between 0 and about 10k
    dfPopM = dfICSV[:2000]

    print("-----GET PRODUCTS THEY DONT SELL-----")
    sqlNotSold = '''
    SELECT Pop, ChangeFromLastWeek, Title, GTIN, Brand, PriceRangeStart, PriceRangeEnd, WeekNumber
    FROM dfPopM
    WHERE ProductInventoryStatus = "Not in inventory"
    ORDER BY Pop
    '''
    dfNotSold = pandasql.sqldf(sqlNotSold, locals())

    #match popular products with linkingTable (adwords stats)
    count = 0
    print("-----Processing matches-----")

    sql = '''
    SELECT linkingTable.GID,
    linkingTable.Impressions,
    linkingTable.Cost,
    linkingTable.Clicks,
    linkingTable.ConversionValue,
    linkingTable.IMG,
    linkingTable.BRAND,
    linkingTable.monthNum,
    linkingTable.yearNum,
    dfPopM.Pop
    FROM linkingTable, dfPopM
    WHERE SUBSTR(linkingTable.GTIN,0,LENGTH(linkingTable.GTIN) - 1) = SUBSTR(dfPopM.GTIN,0,LENGTH(dfPopM.GTIN) - 3)

    '''
    dfPopM.iloc[0:0]

    dfPopXmlMatch = pandasql.sqldf(sql, locals())

    sqlGroupMatchTable = '''
    SELECT dfPopXmlMatch.Pop,
    dfPopXmlMatch.GID
    FROM dfPopXmlMatch
    GROUP BY dfPopXmlMatch.Pop,dfPopXmlMatch.GID
    '''
    dfPopXmlMatch = pandasql.sqldf(sqlGroupMatchTable, locals())
    
    sqlMatchAll = '''
    SELECT linkingTable.GID,
    linkingTable.Impressions,
    linkingTable.Cost,
    linkingTable.Clicks,
    linkingTable.ConversionValue,
    linkingTable.IMG,
    linkingTable.BRAND,
    linkingTable.monthNum,
    linkingTable.yearNum,
    dfPopXmlMatch.Pop
    FROM linkingTable, dfPopXmlMatch
    WHERE dfPopXmlMatch.GID = linkingTable.GID
    GROUP BY linkingTable.GID,
    linkingTable.Impressions,
    linkingTable.Cost,
    linkingTable.Clicks,
    linkingTable.ConversionValue,
    linkingTable.IMG,
    linkingTable.monthNum,
    linkingTable.yearNum,
    dfPopXmlMatch.Pop
    '''
    linkingTable.iloc[0:0]
    dfPopXmlMatch = pandasql.sqldf(sqlMatchAll, locals())

    #group the results by date and IDNew while adding the metrics together
    sqlGroup = '''
    SELECT dfPopXmlMatch.GID as GID, 
    SUM(dfPopXmlMatch.Impressions) as Impressions, 
    SUM(dfPopXmlMatch.Cost) as Cost, 
    SUM(dfPopXmlMatch.Clicks) as Clicks, 
    SUM(dfPopXmlMatch.ConversionValue) as ConversionValue, 
    dfPopXmlMatch.IMG as IMG,
    dfPopXmlMatch.BRAND as BRAND,
    dfPopXmlMatch.monthNum as monthNum,
    dfPopXmlMatch.yearNum as yearNum,
    dfPopXmlMatch.Pop
    FROM dfPopXmlMatch 
    GROUP BY dfPopXmlMatch.GID, dfPopXmlMatch.IMG, dfPopXmlMatch.Pop, dfPopXmlMatch.monthNum , dfPopXmlMatch.yearNum 
    '''
    dfPopXmlMatch = pandasql.sqldf(sqlGroup, locals())
    print('---------GROUPED THE ABOVE---------')

    sqlGroupAnalytics = '''
    SELECT dfAnalytics.productName as productName, 
    dfAnalytics.productSku as productSku,
    dfAnalytics.country as country,
    dfAnalytics.monthNum as monthNum,
    dfAnalytics.yearNum as yearNum,
    SUM(dfAnalytics.itemRevenue) as itemRevenue,
    AVG(dfAnalytics.buyToDetailRate) as buyToDetailRate
    FROM dfAnalytics
    WHERE dfAnalytics.country = "United States"
    GROUP BY dfAnalytics.country, dfAnalytics.productSku, dfAnalytics.productName, dfAnalytics.monthNum , dfAnalytics.yearNum
    '''
    dfAnalytics = pandasql.sqldf(sqlGroupAnalytics, locals())
    sql = '''
    SELECT dfPopXmlMatch.GID as ID, 
    dfPopXmlMatch.Impressions as Impressions, 
    dfPopXmlMatch.Cost as Cost, 
    dfPopXmlMatch.Clicks as Clicks, 
    dfPopXmlMatch.ConversionValue as ConversionValue, 
    dfPopXmlMatch.IMG as IMG, 
    dfPopXmlMatch.BRAND as BRAND, 
    dfPopXmlMatch.Pop as Pop,
    dfAnalytics.itemRevenue as itemRev, 
    dfAnalytics.buyToDetailRate as buyToDetailRate, 
    dfAnalytics.productName as productName, 
    dfAnalytics.country as country,
    dfAnalytics.monthNum as monthNum, 
    dfAnalytics.yearNum as yearNum
    FROM dfPopXmlMatch, dfAnalytics 
    WHERE LOWER(dfPopXmlMatch.GID) = LOWER(dfAnalytics.productSku) 
    AND dfAnalytics.monthNum = dfPopXmlMatch.monthNum
    AND dfAnalytics.yearNum = dfPopXmlMatch.yearNum
    '''
    dfPopXmlMatch.iloc[0:0]
    dfFinal = pandasql.sqldf(sql, locals())

    ######### currency converting start #########
    #COMMENT OUT THE BELOW IF YOU DONT NEED TO CONVER CURRENCY - MORE EFFICIENT WAY IN product_sizes.py - make into method in futurre updates
    newCost = []
    newROAS = []
    for dfFinalIndex, dfFinalRow in dfFinal.iterrows(): 
        newVal = c.convert(dfFinalRow['Cost'] / 1000000, 'USD', 'YOUR_CURRENCY')
        newCost.append(newVal)
        if float(newVal) <= 0:
            newROAS.append(0)
        else:
            newROAS.append(float(dfFinalRow['itemRev']) / float(newVal))

    dfFinal.loc[:,"newCost"] = newCost
    dfFinal.loc[:,"newROAS"] = newROAS
    ######### currency converting end #########

    #add a new column to df and fill it with the above week number
    weekNo = int(mon.strftime("%W")) + 1
    dfFinal["WeekNumber"] = str(weekNo) #if not string, data studio gives an error

    print("Processing Complete...")
    
    #Products they dont sell dfNotSold
    dfNotSold.to_gbq('DATASET_NAME.TABLE_NAME',
                            project_id='YOUR_PROJECT_ID', 
                            chunksize=None, 
                            if_exists='append', 
                            table_schema=None, 
                            location='LOCATION', 
                            progress_bar=True, 
                            credentials=google_auth.getCreds())

    #linked table with what they have sold dfFinal
    dfFinal.to_gbq('DATASET_NAME.TABLE_NAME', #TABLE_NAME here is refered to as CLIENT_DO_SELL in the SQL files
                            project_id='YOUR_PROJECT_ID', 
                            chunksize=None, 
                            if_exists='append', 
                            table_schema=None, 
                            location='LOCATION', 
                            progress_bar=True, 
                            credentials=google_auth.getCreds())

    #the csv from google mc
    dfICSV = dfICSV[:2000] #clip it if needed here 
    dfICSV.to_gbq('DATASET_NAME.TABLE_NAME',
                            project_id='YOUR_PROJECT_ID', 
                            chunksize=None, 
                            if_exists='append', 
                            table_schema=None, 
                            location='LOCATION', 
                            progress_bar=True, 
                            credentials=google_auth.getCreds())
    dfFinal.iloc[0:0]
    dfICSV.iloc[0:0]

 

def ApplyMonth(row,api="adw",DATE_FMT = "%Y-%m-%d"):
    #Get the month number
    if api != "adw": #analytics
        ROW_NAME = "date"
        date_str = row[ROW_NAME][0:4] + "-" + row[ROW_NAME][4:6] + "-" + row[ROW_NAME][6:8]
        currentDay = datetime.datetime.strptime(date_str, DATE_FMT)
    else: #adwords
        ROW_NAME = 'Date'
        currentDay = datetime.datetime.strptime(row[ROW_NAME], DATE_FMT)
    monthNum = currentDay.strftime("%m")
    return monthNum

def ApplyYear(row,api="adw",DATE_FMT = "%Y-%m-%d"):
    #Get the year number
    if api != "adw": #analytics
        ROW_NAME = "date"
        date_str = row[ROW_NAME][0:4] + "-" + row[ROW_NAME][4:6] + "-" + row[ROW_NAME][6:8]
        currentDay = datetime.datetime.strptime(date_str, DATE_FMT)
    else: #adwords
        ROW_NAME = 'Date'
        DATE_FMT = '%Y-%m-%d'
        currentDay = datetime.datetime.strptime(row[ROW_NAME], DATE_FMT)
    yearNum = currentDay.strftime("%Y")
    return yearNum

#This method isnt actually needed in this file but to keep things clean, ive kept it with the others - TODO: Move to new file with others
def ApplyWeek(row,api="adw",DATE_FMT = "%Y-%m-%d"):
    #Get the year number
    if api != "adw": #analytics
        ROW_NAME = "date"
        date_str = row[ROW_NAME][0:4] + "-" + row[ROW_NAME][4:6] + "-" + row[ROW_NAME][6:8]
        currentDay = datetime.datetime.strptime(date_str, DATE_FMT)
    else: #adwords
        ROW_NAME = 'Date'
        currentDay = datetime.datetime.strptime(row[ROW_NAME], DATE_FMT)
    weekNum = int(currentDay.strftime("%W")) + 1
    return weekNum

def CheckDateFormatAnalytics(s):
    try:
        int(s['date'])
        return s
    except ValueError:
        return -1
        
if __name__ == "__main__":
    main(".csv") # ADD A CUSTOM CSV HERE IF YOU NEED TO RUN THIS FILE INDIVIDUALLY