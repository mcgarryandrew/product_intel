import adwords_pull
import analytics_pull
import process_xml
import match_maker
import csv_parser
import pandas
import pandasql
import datetime
import google_auth
from google.cloud import bigquery
from datetime import date
from dateutil.relativedelta import relativedelta, SU,MO,TU,WE,TH,FR,SA
import pprint
from currency_converter import CurrencyConverter
def main():
    DECLINE_PERCENT = 0.5 #50% - Change this to fit your needs - the higher it is, the less results you will get.
    
    c = CurrencyConverter()
    #Get the individual days to pull (for Analytics to avoid data sampling) and to get the start and end date for adwords
    FourWeekList = FourWeekCreate()
    FourWeekList.sort()

    START_WEEK = int(FourWeekList[0].strftime("%W")) + 1
    END_WEEK = int(FourWeekList[-1].strftime("%W")) + 1
    START_DATE = str(FourWeekList[0])
    END_DATE = str(FourWeekList[-1])

    #ADWORDS PULL API
    adwordsCSV = adwords_pull.ProductDecline(START_DATE,END_DATE)
    adwordsDF = csv_parser.parseToCSV(adwordsCSV,"ADW")
    print("Adwords API Pull Complete")

    #ANALYTIS PULL API

    analyticsCSV = ""
    for day in FourWeekList:
        analyticsCSV += analytics_pull.main(str(day))
        print("Pulled date: " + str(day))
    print("Analytics API Pull Complete")
    
    dfAnalytics = csv_parser.parseToCSV(analyticsCSV,"ANL")
    dfAnalytics['productName'] = dfAnalytics['productName'].str.lower()
    dfAnalytics['date'] = dfAnalytics.apply(lambda row: match_maker.CheckDateFormatAnalytics(row), axis=1)
    dfAnalytics = dfAnalytics[dfAnalytics.date != -1]

    #ADD WEEK AND YEAR NUMBERS
    adwordsDF['yearNum'] = adwordsDF.apply(lambda row: match_maker.ApplyYear(row), axis=1)
    adwordsDF['weekNum'] = adwordsDF.apply(lambda row: match_maker.ApplyWeek(row), axis=1)
    dfAnalytics['yearNum'] = dfAnalytics.apply(lambda row: match_maker.ApplyYear(row,"ANL"), axis=1)
    dfAnalytics['weekNum'] = dfAnalytics.apply(lambda row: match_maker.ApplyWeek(row,"ANL"), axis=1)

    #REMOVE THE SPACE AFTER THE GID
    GIDnew = []
    for index, row in adwordsDF.iterrows(): 
        newGID = row['GID'].partition(" ")[0]
        GIDnew.append(newGID)
    adwordsDF=adwordsDF.drop(columns="GID")
    adwordsDF.loc[:,"GID"] = GIDnew


    #GET ALL THE GIDs THAT CLICKS SUM UP TO 400+ OVER THE 4 WEEK PERIOD
    sqlAdwordsPD = '''
    SELECT 
    GID, 
    SUM(Clicks) as Clicks
    FROM adwordsDF
    GROUP BY GID
    '''
    incGIDdf = pandasql.sqldf(sqlAdwordsPD, locals())
    sqlAdwordsPD = '''
    SELECT 
    GID
    FROM incGIDdf
    WHERE Clicks > 400
    '''
    incGIDdf = pandasql.sqldf(sqlAdwordsPD, locals())
    #NOW WE HAVE A LIST OF GIDs THAT HAVE MORE THAN 400 CLICKS IN THE 4 WEEK PERIOD

    #GROUP THE ORIGINAL ADWORDS DF!
    sqlAdwordsPD = '''
    SELECT 
    yearNum,
    weekNum,
    GID, 
    SUM(Impressions) as Impressions, 
    SUM(Clicks) as Clicks, 
    Sum(Cost) as Cost, 
    Account,
    SUM(ConversionValue) as ConversionValue
    FROM adwordsDF
    GROUP BY GID,Account,weekNum,yearNum
    '''
    # WRITE TO DF
    adwordsDF = pandasql.sqldf(sqlAdwordsPD, locals())
    
    #Filter the dataframe to only contain the GIDs that were found using the SQL
    cols = adwordsDF.columns[adwordsDF.columns.isin(['GID'])]
    adwordsDFnew = pandas.DataFrame()
    for index,row in incGIDdf.iterrows(): 
        adwordsDFnew = adwordsDFnew.append(adwordsDF[(adwordsDF[cols] == row['GID']).all(1)])
    adwordsDF.iloc[0:0]
    # adwordsDFnew now holds the weekly numbers of the products that add to over 400 clicks in the period when combined
    
    #Slim down the analytics data and group it together whhile summing up the values

    #This is repeated code, but im putting it here again incase there are different metrics people want for this slide specifically
    sqlGroupAnalytics = '''
    SELECT dfAnalytics.productName as productName, 
    dfAnalytics.productSku as productSku,
    dfAnalytics.country as country,
    dfAnalytics.weekNum as weekNum,
    dfAnalytics.yearNum as yearNum,
    SUM(dfAnalytics.itemRevenue) as itemRevenue,
    AVG(dfAnalytics.buyToDetailRate) as buyToDetailRate
    FROM dfAnalytics
    WHERE dfAnalytics.country = "United States"
    GROUP BY dfAnalytics.country, dfAnalytics.productSku, dfAnalytics.productName, dfAnalytics.weekNum , dfAnalytics.yearNum
    '''
    dfAnalytics = pandasql.sqldf(sqlGroupAnalytics, locals())
    dfAnalytics.to_csv('analytics_after_proc.csv')
    
    #Match analytics onto adwords DFs
    #This is repeated code, but im putting it here again incase there are different metrics people want for this slide specifically
    sql = '''
    SELECT adwordsDFnew.GID as ID, 
    adwordsDFnew.Impressions as Impressions, 
    adwordsDFnew.Cost as Cost, 
    adwordsDFnew.Clicks as Clicks, 
    adwordsDFnew.ConversionValue as ConversionValue,
    dfAnalytics.itemRevenue as itemRev, 
    dfAnalytics.buyToDetailRate as buyToDetailRate, 
    MIN(dfAnalytics.productName) as productName, 
    dfAnalytics.country as country,
    dfAnalytics.weekNum as weekNum, 
    dfAnalytics.yearNum as yearNum
    FROM adwordsDFnew 
    INNER JOIN dfAnalytics
    ON (LOWER(adwordsDFnew.GID) = LOWER(dfAnalytics.productSku)
    and adwordsDFnew.weekNum = dfAnalytics.weekNum
    and adwordsDFnew.yearNum = dfAnalytics.yearNum
    )
    GROUP BY ID,Impressions,Cost,Clicks,ConversionValue,itemRev,buyToDetailRate,country,dfAnalytics.weekNum,dfAnalytics.yearNum
    ORDER BY adwordsDFnew.GID, dfAnalytics.weekNum
    '''
    dfAnalytics.iloc[0:0]
    dfFinal = pandasql.sqldf(sql, locals())
    dfFinal = dfFinal[dfFinal.productName != '(not set)'] #This specific client has anomolies in the data, its just repeating so we remove it
    
    ####### CONVERT CURRENCY START ########
    #COMMENT OUT THE BELOW IF YOU DONT NEED TO CONVERT CURRENCY - Adjust variable names accordingly
    newCost = []
    newROAS = []
    for dfFinalIndex, dfFinalRow in dfFinal.iterrows(): 
        newVal = c.convert(dfFinalRow['Cost'] / 1000000, 'USD', 'YOUR_CURRENCY') #div by 1Million as google provides the cost in this format
        newCost.append(newVal)
        if float(newVal) <= 0:
            newROAS.append(0)
        else:
            newROAS.append(float(dfFinalRow['itemRev']) / float(newVal))

    dfFinal.loc[:,"newCost"] = newCost
    dfFinal.loc[:,"newROAS"] = newROAS
    ####### CONVERT CURRENCY END ########

    #NOW FILTER OUT ONES WITHOUTH 4 RECORDS - Keep things simple
    for index,row in incGIDdf.iterrows():
        sqlGetGID = '''
        SELECT *
        FROM dfFinal
        WHERE ID = "''' + str(row['GID']) + '''"
        ORDER BY weekNum ASC
        '''
        tempDF = pandasql.sqldf(sqlGetGID, locals()) #this should contain the 4 weeks (or less)
        if(len(tempDF.index) != 4): #doesnt have all data : remove
            dfFinal = dfFinal[dfFinal.ID != str(row['GID'])]
        else:
            START_WK_ROAS = tempDF['newROAS'].iloc[0]
            END_WK_ROAS = tempDF['newROAS'].iloc[3]
            if float(END_WK_ROAS) >= float(float(START_WK_ROAS) * DECLINE_PERCENT): #IS NOT IN DECLINE
                #NOT IN DECLINE SO REMOVE FROM LIST
                dfFinal = dfFinal[dfFinal.ID != str(row['GID'])] # remove the rows wwhere the ID is the current one

    #Whatever is left over now are the products in decline
    dfFinal = dfFinal.sort_values(by=['ID','weekNum'])
    dfFinal["WeekRange"] = str(START_WEEK) + ' - ' + str(END_WEEK) + "  -  " + str(START_DATE) + " - " + str(END_DATE)
    #dfFinal.to_csv('dfFinal.csv')
    
    dfFinal.to_gbq('DATASET_NAME.TABLE_NAME',
                            project_id='YOUR_PROJECT_ID', 
                            chunksize=None, 
                            if_exists='append',
                            table_schema=None, 
                            location='LOCATION', 
                            progress_bar=True, 
                            credentials=google_auth.getCreds())

    print("Success!")

def FourWeekCreate():
    today = date.today()
    weekArr = []
    for i in range(4):
        lastMon = -2 - i #must be -1 more than other days as it runs every monday. so it needs to look at LAST monday
        otherDays = -1 - i
        weekArr.append(today + relativedelta(weekday=MO(lastMon))) #last MON
        weekArr.append(today + relativedelta(weekday=TU(otherDays))) #last TUE
        weekArr.append(today + relativedelta(weekday=WE(otherDays))) #last WED
        weekArr.append(today + relativedelta(weekday=TH(otherDays))) #last THU
        weekArr.append(today + relativedelta(weekday=FR(otherDays))) #last FRI
        weekArr.append(today + relativedelta(weekday=SA(otherDays))) #last SAT
        weekArr.append(today + relativedelta(weekday=SU(otherDays))) #last SUN
    return weekArr



if __name__ == "__main__":
    main()