import csv
import io
import pandas as pd
import pandas_gbq
import datetime as datetime
from datetime import date
from dateutil.relativedelta import relativedelta, SU,MO,TU,WE,TH,FR,SA
import google_auth
def main(filePath):
    csvfile = pd.read_csv(filePath) #read file
    csvfile = csvfile.astype({'Popularity rank': 'int32'}) #convert to int
    csvfile = csvfile.astype({'GTIN': 'str'}) #convert to string
    csvfile = csvfile.sort_values(by=['Popularity rank']) #sort it

    csvfile = csvfile.drop(['Category ID','Category path','Ranking category ID','Ranking category path', 'Currency'],axis=1) #drop the cols we dont want

    #Get the week number based on the monday
    today = date.today()
    mon = today + relativedelta(weekday=MO(-2)) #last MON
    weekNum = int(mon.strftime("%W")) + 1
   
    #add a new column to csv and fill it with the above week number AS A STR
    csvfile["WeekNumber"] = str(weekNum)

    csvfile.rename(columns={'Popularity rank' : 'PopularityRank',
                            'Change from last week' : 'ChangeFromLastWeek',
                            'Brand inventory status' : 'BrandInventoryStatus',
                            'Product inventory status' : 'ProductInventoryStatus',
                            'Price range start' : 'PriceRangeStart',
                            'Price range end' : 'PriceRangeEnd'
                            },inplace=True)

    #csv into df
    #foreach row in csv file (index is just to open ram for speed)
    data = []
    for index, row in csvfile.iterrows():
        data += [[
                row['PopularityRank'], row['ChangeFromLastWeek'],
                row['Title'], row['GTIN'],
                row['Brand'], row['BrandInventoryStatus'], 
                row['ProductInventoryStatus'], row['PriceRangeStart'],
                row['PriceRangeEnd'], row['WeekNumber']
                ]]
        
    df = pd.DataFrame(data, columns = ['PopularityRank', 'ChangeFromLastWeek',
                                        'Title', 'GTIN',
                                        'Brand', 'BrandInventoryStatus',
                                        'ProductInventoryStatus', 'PriceRangeStart',
                                        'PriceRangeEnd', 'WeekNumber'])

    
    return df


if __name__ == "__main__":
    main() # custom file path here if you need to run this individually (you shouldnt though)