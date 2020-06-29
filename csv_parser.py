import csv
import datetime
import io
import pandas as pd
import pandas_gbq
import google_auth
def parseToCSV(REPORT_TEXT_AS_CSV, TYPE):

    f = io.StringIO(REPORT_TEXT_AS_CSV)

    if TYPE == "ADW": #ADWORDS RENAMING
        #MAIN REPORT
        csvfile = pd.read_csv(f,skiprows=1)
        csvfile.rename(columns={'Item Id' : 'OfferID'},inplace=True)
        csvfile.rename(columns={'Day' : 'Date'},inplace=True)
        csvfile.rename(columns={'Product type (1st level)' : 'GID'},inplace=True)
        csvfile.rename(columns={'Total conv. value' : 'ConversionValue'},inplace=True)
    elif TYPE == "ANL": #ANALYTICS RENAMING
        csvfile = pd.read_csv(f,skiprows=0)
        csvfile.rename(columns={'ga:date' : 'date'},inplace=True)
        csvfile.rename(columns={'ga:productName' : 'productName'},inplace=True)
        csvfile.rename(columns={'ga:productSku' : 'productSku'},inplace=True)
        csvfile.rename(columns={'ga:itemRevenue' : 'itemRevenue'},inplace=True)
        csvfile.rename(columns={'ga:buyToDetailRate' : 'buyToDetailRate'},inplace=True)
        csvfile.rename(columns={'ga:country' : 'country'},inplace=True)
        csvfile.rename(columns={'ga:quantityAddedToCart' : 'quantityAddedToCart'},inplace=True)
        csvfile.rename(columns={'ga:itemQuantity' : 'itemQuantity'},inplace=True)
        csvfile.rename(columns={'ga:uniquePurchases' : 'uniquePurchases'},inplace=True)
        csvfile.rename(columns={'ga:dimensionX' : 'size'},inplace=True)
        csvfile.rename(columns={'ga:productCategoryLevelX' : 'productType'},inplace=True)
        return csvfile

    return csvfile

    
    
    