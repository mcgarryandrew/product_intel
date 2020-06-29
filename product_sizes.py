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
import re
from currency_converter import CurrencyConverter

def main():
    
    c = CurrencyConverter()

    print('------------Product Sizes------------')

    #analytics
    today = date.today()
    mon = today + relativedelta(weekday=MO(-2)) #last MON
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
        analyticsCSV += analytics_pull.main(str(day),sizes=True)
        print("Pulled date: " + str(day))
    dfAnalytics = csv_parser.parseToCSV(analyticsCSV,"ANL")

    print('-----Analytics Dataframe-----')
    #Get rid of non Shoes (for the sake of this client - will vary depending on category) and unneeded fields.
    #Save to dataframe for further processing
    sql = '''
    SELECT
    date,
    productSku,
    size,
    itemRevenue,
    itemQuantity
    FROM dfAnalytics
    WHERE productType = 'Shoes'
    '''
    dfAnalytics = pandasql.sqldf(sql, locals())
    
    #Now we only have shoes and required fields. 
    
    #STRING MANIPULATION FOR THIS CLIENT TO MATCH ID + SIZE
    #This may not be needed for all clients and will vary depending on their data
    dfAnalytics['size'] = dfAnalytics['size'].apply(lambda row: row.replace("US",""))
    dfAnalytics['size'] = dfAnalytics['size'].apply(lambda row: row.replace("UK",""))
    dfAnalytics['size'] = dfAnalytics['size'].apply(lambda row: row.replace("Wm","W"))
    dfAnalytics['size'] = dfAnalytics['size'].apply(lambda row: row.replace(" ",""))

    dfAnalytics['date'] = dfAnalytics.apply(lambda row: match_maker.CheckDateFormatAnalytics(row), axis=1)
    dfAnalytics = dfAnalytics[dfAnalytics.date != -1]
    dfAnalytics['date'] = dfAnalytics['date'].apply(lambda row: str(row)[0:4] + "-" + str(row)[4:6] + "-" + str(row)[6:8])
    dfAnalytics['NEWSKU'] = (dfAnalytics['productSku'] + '-' + dfAnalytics['size']).str.lower()
    ##make the sizes all numbers - remove letters - REGEX TIME :D
    dfAnalytics['size'] = dfAnalytics['size'].apply(lambda row: re.sub(r'[^\d.]+', '', str(row))) 

    #Flatten to remove dupes
    sql = '''
    SELECT 
    date,
    size,
    SUM(itemRevenue) AS itemRevenue,
    SUM(itemQuantity) AS itemQuantity
    FROM dfAnalytics
    GROUP BY date,size
    '''
    dfAnalytics = pandasql.sqldf(sql, locals())
    #-------------------ADWORDS----------------------#
    
    adwordsCSV = adwords_pull.get_raw_report()

    dfAdwords = csv_parser.parseToCSV(adwordsCSV,"ADW")

    print('-----Adwords Dataframe-----')

    sql = '''
    SELECT dfAdwords.Date, dfAdwords.OfferID, dfAdwords.Cost
    FROM dfAdwords
    '''
    dfAdwords = pandasql.sqldf(sql, locals())
    #some string manipulation - we are appending the sizes to the end of the group id so we can identify the specific products
    dfAdwords['OfferID'] = dfAdwords['OfferID'].apply(lambda row: (str(row).partition("|")[0]).lower())
    dfAdwords['OfferID'] = dfAdwords['OfferID'].str.lower()
    dfAdwords['Cost'] = dfAdwords['Cost'].apply(lambda row: c.convert(row / 1000000, 'USD', 'YOUR_CURRENCY')) # convery currency
    dfAdwords['size'] = dfAdwords['OfferID'].apply(lambda row: str(row).split('-')[-1]) # more string manipulation - will vary by client
    dfAdwords['size'] = dfAdwords['size'].apply(lambda row: re.sub(r'[^\d.]+', '', str(row)))
    
    #flatten to remove dupes
    sql = '''
    SELECT 
    Date,size,SUM(Cost) AS Cost
    FROM dfAdwords
    GROUP BY Date,size
    '''
    dfAdwords = pandasql.sqldf(sql, locals())
    #--------------------ADWORDS DONE----------------------#
    #JOIN ANALYTICS AND ADWORDS
    sql = '''
    SELECT dfAnalytics.date,
    dfAnalytics.size,
    SUM(dfAnalytics.itemRevenue) / SUM(dfAdwords.Cost) as ROAS,
    SUM(dfAnalytics.itemQuantity) as salesVolume
    FROM dfAnalytics,dfAdwords
    WHERE dfAdwords.Date = dfAnalytics.date 
    AND dfAdwords.size = dfAnalytics.size
    GROUP BY dfAnalytics.date,dfAnalytics.size
    '''
    dfFinal = pandasql.sqldf(sql, locals())
    dfFinal = dfFinal.fillna(-0.01) # could drop them too
    
    dfFinal.to_gbq('DATASET_NAME.TABLE_NAME',
                            project_id='YOUR_PROJECT_ID', 
                            chunksize=None, 
                            if_exists='append', 
                            table_schema=None, 
                            location='LOCATION', 
                            progress_bar=True, 
                            credentials=google_auth.getCreds())
    return
   

if __name__ == "__main__":
    main()