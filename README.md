NOTE: This is development code. There are a lot of efficiency and good practice improvements to be made. Feel free to improve upon it!
NOTE 2: When refering to functions or methods, we mean the same thing.

## Notable things to improve
- Reduce the number of API calls. This can be done by making a dedicated file for all API calls and storing the result for reuse.
- Add error catching to API calls. This will avoid timeout errors!
- Add general error catching and ways to deal with them

## Account Set-Up
Since we will be working with Google Analytics and Goolge Ads APIs, you will need read only permissions and a developer token which you can apply for [here](https://developers.google.com/google-ads/api/docs/first-call/dev-token)

Next, you will need to set up a [service account](https://developers.google.com/identity/protocols/oauth2/service-account) and make sure you add the [BigQuery Scope](https://developers.google.com/identity/protocols/oauth2/scopes#bigquery)

Once this is all set up, you will need to obtain a [refresh token](https://developers.google.com/google-ads/api/docs/first-call/refresh-token) from Google to keep account authentication working over time.

Lastly, you need to fill the env.py and creds.json files with the appropriate information Gooogle supplies you with while creating the Service Account.

Done! Now it's time to look at the code..

## Software Set-Up

Python Version 3.7 was used.

Rename the ```env.py.default``` file to ```env.py```
Rename the ```cred.json.default``` file to ```cred.json```

Set up a virtual environment

```bash
pip3 install virtualenv
```
Specify a path for the Virtual Environment within your workspace
```bash
virtualenv env
```
Activate in Windows
```bash
env\Scripts\activate
```
Activate in Linux
```bash
source env/bin/activate
```

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install the requirements.

```bash
pip3 install -r requirements.txt
```

## Entry Point and Environment
```
env.py
```
Environment file which holds all your sensitive information.

```
main.py
```
This file is your entry point into the software. 

Set your download path which tells [Selenium](https://selenium-python.readthedocs.io/) where to save the 'Popular Products' CSV from Merchant Center.

Set your final path. This is where the CSV will be moved to once processing is complete

## Merchant Center Download Automation
```
csv_downloader.py
```
This file sets the parameters for the chromedriver. Selenium will use this to automate the download process for the CSV.

Make sure you're phone authenticated in chrome on the machine you plan to use before running the secript.

Put your merchant Center login information in the env.py file for this to work.

This CSV will be refered to as the ***Popular Products File*** from here onwards.

## Matching Products
```
match_maker.py
```
This is the file that does most of the work using SQL and Pandas.
- Get the required data from the product feed (this code assumes the feed is in XML format)
- Pull data from Google Ads and Google Analytics
- Apply any required filters
- Match GTINs from the Popular Products File onto the Adwords and Analytics data. These can be perfect or partial matches. We have done partial matching using SQL. Adjust the code accordingly:
```python
    sql = '''
    SELECT linkingTable.GID, linkingTable.Impressions, linkingTable.Cost, linkingTable.Clicks, linkingTable.ConversionValue, linkingTable.IMG, linkingTable.BRAND, linkingTable.monthNum, linkingTable.yearNum, dfPopM.Pop
    FROM linkingTable, dfPopM
    WHERE SUBSTR(linkingTable.GTIN,0,LENGTH(linkingTable.GTIN) - 1) = SUBSTR(dfPopM.GTIN,0,LENGTH(dfPopM.GTIN) - 3)'''
```
- Convert currency if required
- Work out specific metrics and KPIs e.g. Derive ROAS from Cost and Revenue instead of pulling ROAS from the API (if you can).
- Add week numbers to the data so we can identify which week the specific row of data belongs to. As the data is being looked at ***weekly***, the daily date column was removed effective SQL grouping. We mention this in the code's comments.
-Write the results to BigQuery

***Several other files are called from match_maker.py***

```
process_xml.py
adwords_pull.py
analytics_pull.py
csv_parser.py
```

***Process_xml:***
This file is where the data feed is processed. We use ***urllib*** to get the URL of the live data feed. However, you can eaily pass a local file into the file by changing the line:
```
xmldoc = ET.parse(url)
```
TO
```
xmldoc = ET.parse('dir/to/local/file.csv')
```
We have taken namspace into consideration as mentioned in the comments. Namespace may vary depending on the product data feed.
 
The main method takes several optional boolean parameters: **margins**, **decline** and **size**.

These parameters specify which file is calling the function. E.g If the file concerning product margins is calling the method then **margins** will be passed as ***True***.

If nothing is passed or all parameters are False ***match_maker.py*** is assumed to be calling the method by default. 

***adwords_pull:***
Pretty simple file. This just holds the API requests. The **get_raw_report** method is used for everything except the product_decline.py file. 

Product decline utilises the **ProductDecline** method.

The API responses are passed to ***csv_parser.py*** alongside a parameter **'ANL'** (analytics) or **'ADW'** (Adwords) identifying which API response needs to be processed.

You can adjust the metrics being pulled form the API, ***just make sure you refelct these changes in the csv_parser.py file.***

***adwords_pull:***
This file is similar to adowrds_pull.py. This simply pulls from the Analytics API and processes the response into CSV format.

There are API pulling methods: **get_report** and **get_report_sizes**

The first one is used for all files except the product_sizes.py, which is where get_report_sizes is used. Additionally, these API requests use segements which are completely optional depending on your use case.


## Product Margins and Product Sizes
These files are pretty similar. Product margin looks at the sales volume and ROAS of each margin (percentage discount). The data is processed using SQL and passed into BigQuery.
Product Sizes looks at the sales volume and ROAS for each size of shoe. This processes data in a similar way to product margins, but with a little more SQL.

Product sizes require a little string manipulation because of the formatting of some of the sizes - everything needed to be consistent. You can just adjust this accordingly to fit your needs.

## Product Decline
This file looks at the decline of a product over a 4 week period. The rule we used was: ***If the product has more than 400 clicks and has declined more than 50% in the last week (of the 4 week period) vs the first week*** then add it to the list of declining products.

This works by pulling each individual day of the 4 week period from Analytics (to avoid data sampling). Next, we match the Analytics and Adwords data together with some SQL.
We then loop through the dataframe and make sure each unique product group ID has data for the 4 week period - if not, then remove it from the dataframe. We use this loop to check if the **last** week of the period has declined more than 50% vs the **first** week.

Once processing is complete, only products that fit the rule above will remain in the dataframe. These are then written to BigQuery.

## BigQuery
There should now be a collection of 6 tables in the BigQuery project. This is the raw data you can use to create your dashbaords!
We have written a few SQL scripts which we use throughout the [demo dashboard](https://datastudio.google.com/u/1/reporting/1bBekRDZ7-ZRwFWQvOytI1_hw1r49sisA/page/OgDRB)

These can be found in the ***BQSQL*** folder.

You can use this code to create new Views in BigQuery.

***How the code works:***
```sql
WITH x AS 
(
  SELECT AVG(Pop),ID FROM `YOUR_PROJECT_ID.WL_DASH.CLIENT_DO_SELL`  
  WHERE Pop IN (
    SELECT MIN(Pop) 
    FROM `YOUR_PROJECT_ID.WL_DASH.CLIENT_DO_SELL` 
    GROUP BY ID,MonthNum,YearNum
  )
  GROUP BY ID
)
```
This snippet is using a temp table to uniquely select the highest popularity of a product if it has a duplicate. E.g If the same product (may be the same product but a slightly different style, so it appears as a duplicate since we are using product group IDs) has a popularity of 50 **and** 70, the data would naturally be doubled. Therefore, it's important to make sure each product is unique. This can be done in python, but we prefered to use SQL as to not alter the original data.

```sql
SELECT
    `YOUR_PROJECT_ID.WL_DASH.CLIENT_DO_SELL` .ID as ID, 
    AVG(Impressions) AS Impressions,
    AVG(newCost) AS newCost,
    ROUND(AVG(Clicks),0) AS Clicks,
    AVG(ConversionValue) AS ConversionValue,
    AVG(Clicks) / AVG(Impressions) AS CTR,
    MAX(IMG) AS IMG, 
    BRAND,
    AVG(`YOUR_PROJECT_ID.WL_DASH.CLIENT_DO_SELL` .Pop) AS Pop,
    AVG(CAST(itemRev as float64)) as itemRev,
    AVG(CAST(buyToDetailRate AS float64) / 100) as BTDR,
    AVG(CAST(newROAS AS float64)) as ROAS,
    productName,
    country,
    `YOUR_PROJECT_ID.WL_DASH.CLIENT_DO_SELL` .MonthNum as Monthum,
    `YOUR_PROJECT_ID.WL_DASH.CLIENT_DO_SELL` .YearNum
FROM
  `YOUR_PROJECT_ID.WL_DASH.CLIENT_DO_SELL` 
INNER JOIN x 
ON `YOUR_PROJECT_ID.WL_DASH.CLIENT_DO_SELL` .ID = x.ID
GROUP BY
  `YOUR_PROJECT_ID.WL_DASH.CLIENT_DO_SELL`.MonthNum,YearNum,ID,BRAND,productName,country
```
The above code is standard SQL which uses the temp table to Join it's unique data onto the original table. 

Use the given code and create your own Views in BigQuery.

## Finishing things up
Lastly, you need to add the BigQuery tables and/or views as datasources in [DataStudio](https://support.google.com/datastudio/answer/6283323?hl=en).
Now you can begin putting charts together!


## Deployment
This project can be deployed anywhere! We looked at AWS EC2, Heroku and a Raspberry Pi. However, you can deploy it to any system as long as you have a [compatible chromedriver](https://chromedriver.chromium.org/downloads) (preferably the latest version). Don't forget to add a cron job to the crontab file or scheduler (for Heroku deployments) if you want to repeat this weekly!

## Scheduling 
Most deployment methods will be on a Linux based system. 

The below is a simple cron job for running the main.py file every monday morning.
```bash
0 4 * * 1 /home/pi/Desktop/product_intel_sns_NOFLASK/sns_flask_app_prods/env/bin/python /home/pi/Desktop/product_intel_sns_NOFLASK/sns_flask_app_prods/main.py >> /home/pi/Desktop/product_intel_sns_NOFLASK/sns_flask_app_prods/output.log 2>&1
```
If you plan on deploying to Heroku, you can use the [Advanced Scheduler](https://elements.heroku.com/addons/advanced-scheduler).

## AWS EC2
This is probably one of the more popular options. You can set up the **micro** instance using the Amazon Linux 2 AMI. Make sure you secure things properly by adding restrictions! Just pull the files once you finish editing and testing. 

## Heroku
Another fantastic option. A real perk is the clean interface and no need to add the sensitive data into an env file (heroku has their own system for managing this). If you decide to deploy using Heroku, you will need look into using the chromedriver for their system.


## License
[MIT](https://choosealicense.com/licenses/mit/)
