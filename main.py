import match_maker
import csv_downloader
import product_decline
import product_margins
import product_sizes
import os
from datetime import date
if __name__ == "__main__":
    today = date.today()
    NEW_NAME = today.strftime("%Y-%m-%d") + ".csv"
    DOWNLOAD_PATH = 'FULL/PATH/Downloads/USA/current'
    FINAL_PATH = 'FULL/PATH/Downloads/USA/finished'
    CSV_PATH = csv_downloader.DownloadMerchantCSV(DOWNLOAD_PATH,NEW_NAME)
    match_maker.main(CSV_PATH)    
    os.rename(CSV_PATH, os.path.join(FINAL_PATH, NEW_NAME))
    product_decline.main()
    product_margins.main()
    product_sizes.main()
    print("All Tasks Completed Successfully")
    