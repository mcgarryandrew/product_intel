import adwords_pull
import analytics_pull
import process_xml
import match_maker
import csv_parser
import pandas
import pandasql
import datetime
import google_auth
from datetime import date
from dateutil.relativedelta import relativedelta, SU,MO,TU,WE,TH,FR,SA
from currency_converter import CurrencyConverter

def main():
    
    #MAIN IDEA:
    #Identify the sales volume and ROAS per margin and display as a bubble chart in data studio
    c = CurrencyConverter()

    print('------------Product Margins------------')
    #analytics dates to pull individually
    #NEED TO REWORK THIS TO HAVE LESS API REQUESTS
    today = date.today()
    mon = today + relativedelta(weekday=MO(-2)) #last MON: -2 because it will run every monday so we need to look 2 mondays back
    tue = today + relativedelta(weekday=TU(-1)) #last TUE
    wed = today + relativedelta(weekday=WE(-1)) #last WED
    thu = today + relativedelta(weekday=TH(-1)) #last THU
    fri = today + relativedelta(weekday=FR(-1)) #last FRI
    sat = today + relativedelta(weekday=SA(-1)) #last SAT
    sun = today + relativedelta(weekday=SU(-1)) #last SUN
    weekArr = [mon,tue,wed,thu,fri,sat,sun]

    analyticsCSV = ""
    #pull for each day of the week - avoids data sampling this way but takes a bit longer
    for day in weekArr:
        analyticsCSV += analytics_pull.main(str(day))
        print("Pulled date: " + str(day))
    dfAnalytics = csv_parser.parseToCSV(analyticsCSV,"ANL")

    dfAnalytics['date'] = dfAnalytics.apply(lambda row: match_maker.CheckDateFormatAnalytics(row), axis=1)
    dfAnalytics = dfAnalytics[dfAnalytics.date != -1]
    dfAnalytics['date'] = dfAnalytics.apply(lambda row: formatDate(row), axis=1)

    print('-----Analytics Dataframe-----')

    #adwords
    adwordsCSV = adwords_pull.get_raw_report()

    dfAdwords = csv_parser.parseToCSV(adwordsCSV,"ADW")

    #REMOVE THE SPACE AFTER THE GID - WILL VARY DEPENDING ON CLIENTS
    GIDnew = []
    for dfAdwordsIndex, dfAdwordsRow in dfAdwords.iterrows(): 
        newGID = dfAdwordsRow['GID'].partition(" ")[0]

        GIDnew.append(newGID)

    dfAdwords=dfAdwords.drop(columns="GID")
    dfAdwords.loc[:,"GID"] = GIDnew

    print('-----Adwords Dataframe-----')
    
    sql = '''
    SELECT dfAdwords.Date, SUM(dfAdwords.Cost) AS Cost, dfAdwords.GID
    FROM dfAdwords
    GROUP BY dfAdwords.Date, dfAdwords.GID
    '''
    dfAdwords = pandasql.sqldf(sql, locals())

    print('-----Grouped Adwords Dataframe-----')

    #XML
    dfXML = process_xml.main(margins=True)

    dfXML['GID'] = dfXML['GID'].str.lower()
    dfAnalytics['productSku'] = dfAnalytics['productSku'].str.lower()

    print('-----Default XML Dataframe-----')


    sql = '''
    SELECT dfXML.Margin, dfXML.GID, dfXML.Brand
    FROM dfXML
    GROUP BY dfXML.Margin, dfXML.GID, dfXML.Brand
    '''

    dfXML = pandasql.sqldf(sql, locals())

    print('-----Grouped XML Dataframe-----')

    sql = '''
    SELECT dfXML.Margin, dfXML.Brand, dfAnalytics.date, dfAnalytics.itemQuantity, dfAnalytics.productSku,
    dfAnalytics.productName, dfAnalytics.itemRevenue
    FROM dfAnalytics
    INNER JOIN dfXML ON dfXML.GID = dfAnalytics.productSku
    '''
    dfXML = pandasql.sqldf(sql, locals())

    print('-----Joined XML Dataframe-----')
    print(dfXML)


    sql = '''
    SELECT dfXML.Margin, dfXML.Brand, dfXML.date, dfXML.itemQuantity, dfXML.productSku,
    dfXML.productName, dfXML.itemRevenue, dfAdwords.Cost
    FROM dfXML
    INNER JOIN dfAdwords ON dfXML.Date = dfAdwords.Date AND dfXML.productSku = dfAdwords.GID
    '''

    dfXML = pandasql.sqldf(sql, locals())
    print('-----Joined XML Dataframe-----')
    print(dfXML)

    newCost = []
    newROAS = []
    for dfXMLIndex, dfXMLRow in dfXML.iterrows(): 
        newVal = c.convert(dfXMLRow['Cost'] / 1000000, 'USD', 'YOUR_CURRENCY')
        newCost.append(newVal)
        if float(newVal) <= 0:
            newROAS.append(0)
        else:
            newROAS.append(float(dfXMLRow['itemRevenue']) / float(newVal))

    dfXML.loc[:,"newCost"] = newCost
    dfXML.loc[:,"newROAS"] = newROAS

    print('-----Final XML Dataframe-----')
    
    sql = '''
    SELECT 
    date,
    Margin,
    (SUM(CAST(itemRevenue as float64)) / sum(CAST(newCost as float64))) as ROAS,
    SUM(CAST(itemQuantity as int64)) as salesVolume
    FROM dfXML
    GROUP BY Margin, date
    '''
    dfXML = pandasql.sqldf(sql, locals())

    dfXML.to_gbq('DATASET_NAME.TABLE_NAME',
                        project_id='YOUR_PROJECT_ID', 
                        chunksize=None, 
                        if_exists='append', 
                        table_schema=None, 
                        location='LOCATION', 
                        progress_bar=True, 
                        credentials=google_auth.getCreds())


#Fix analytics date format
def formatDate(row):
    ROW_NAME = "date"
    date_str = row[ROW_NAME][0:4] + "-" + row[ROW_NAME][4:6] + "-" + row[ROW_NAME][6:8]
    return date_str


if __name__ == "__main__":
    main()