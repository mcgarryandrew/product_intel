"""Hello Analytics Reporting API V4."""
import os
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import sys
import env
import pandas as pd
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = os.getenv('SERVICE_ACC_FILE')
VIEW_ID = os.getenv('ANALYTICS_VIEW_ID')

def initialize_analyticsreporting():
  """Initializes an Analytics Reporting API V4 service object.

  Returns:
    An authorized Analytics Reporting API V4 service object.
  """
  credentials = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE_LOCATION, SCOPES)

  # Build the service object.
  analytics = build('analyticsreporting', 'v4', credentials=credentials)

  return analytics

#################################################################################
#################################################################################
#################################################################################
#USE THIS TO HELP: https://ga-dev-tools.appspot.com/dimensions-metrics-explorer/#
#################################################################################
#################################################################################
#################################################################################

def get_report(analytics,start_date):
  """Queries the Analytics Reporting API V4.

  Args:
    analytics: An authorized Analytics Reporting API V4 service object.
  Returns:
    The Analytics Reporting API V4 response.
  """
  report = analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          "samplingLevel": "LARGE",
          'dateRanges': [{'startDate': start_date, 'endDate': start_date}], 
          'metrics': [{'expression': 'ga:itemRevenue'},
                      {'expression': 'ga:buyToDetailRate'},
                      {'expression': 'ga:quantityAddedToCart'},
                      {'expression': 'ga:itemQuantity'},
                      {'expression': 'ga:uniquePurchases'}
                     ],
          'dimensions': [{'name': 'ga:date'},{'name': 'ga:productName'},{'name': 'ga:productSku'},{'name': 'ga:country'},{'name': 'ga:segment'}],
          'segments': [ #use segments as you please, see what works for the client
            {
              "dynamicSegment":
              {
                "name":"[API] GOOGL CPC USERS",
                "userSegment":
                {
                  "segmentFilters":[
                  {
                    "simpleSegment":
                    {
                      "orFiltersForSegment":[
                      {
                        "segmentFilterClauses":[
                        {
                          "dimensionFilter":
                          {
                            "dimensionName":"ga:country",
                            "expressions":["United States"],
                            "operator":"EXACT"
                          }
                        }]
                      }]
                    }
                  }]
                }
              }
            }
          ],
          "filtersExpression":"ga:sourceMedium==google / cpc", #https://developers.google.com/analytics/devguides/reporting/core/v3/reference#filters
          "includeEmptyRows": True
        }]
      }
  ).execute()

  return report

def get_report_sizes(analytics,start_date):
  """Queries the Analytics Reporting API V4.

  Args:
    analytics: An authorized Analytics Reporting API V4 service object.
  Returns:
    The Analytics Reporting API V4 response.
  """
  report = analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          "samplingLevel": "LARGE",
          'dateRanges': [{'startDate': start_date, 'endDate': start_date}], 
          'metrics': [{'expression': 'ga:itemRevenue'}, 
                      {'expression': 'ga:buyToDetailRate'}, #NOT NEEDED JUST YET - WILL BE USEFUL FOR FUTURE
                      {'expression': 'ga:quantityAddedToCart'}, #NOT NEEDED JUST YET - WILL BE USEFUL FOR FUTURE
                      {'expression': 'ga:itemQuantity'},
                      {'expression': 'ga:uniquePurchases'}
                     ],
          'dimensions': [{'name': 'ga:date'},
                        {'name': 'ga:productName'},
                        {'name': 'ga:productSku'},
                        {'name': 'ga:country'},
                        {'name': 'ga:segment'}, #THIS MAY VARY DEPENDING ON CLIENT
                        {'name': 'ga:dimension9'}, #THIS MAY VARY DEPENDING ON CLIENT
                        {'name': 'ga:productCategoryLevelX'}], #THIS MAY VARY DEPENDING ON CLIENT
          'segments': [
            {
              "dynamicSegment":
              {
                "name":"[API] GOOGL CPC USERS",
                "userSegment":
                {
                  "segmentFilters":[
                  {
                    "simpleSegment":
                    {
                      "orFiltersForSegment":[
                      {
                        "segmentFilterClauses":[
                        {
                          "dimensionFilter":
                          {
                            "dimensionName":"ga:country",
                            "expressions":["United States"],
                            "operator":"EXACT"
                          }
                        }]
                      }]
                    }
                  }]
                }
              }
            }
          ],
          "filtersExpression":"ga:sourceMedium==google / cpc", #https://developers.google.com/analytics/devguides/reporting/core/v3/reference#filters
          "includeEmptyRows": True
        }]
      }
  ).execute()

  return report

#process the response
def print_response(response):
  for report in response.get('reports', []):
    columnHeader = report.get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])

    HEADERS = ""
    for dimHeader in dimensionHeaders:
      HEADERS += dimHeader + ","

    for metricHeader in metricHeaders:
      HEADERS += metricHeader['name'] + ","

    HEADERS = HEADERS[:-1] + "\n"

    FINAL_DATA = ""
    for row in report.get('data', {}).get('rows', []):
      dimensions = row.get('dimensions', [])
      dateRangeValues = row.get('metrics', [])
    
      DATA = ""
      for dimension in dimensions:
        DATA += dimension + ","

      for i, values in enumerate(dateRangeValues):
        for metric in values.get('values'):
          DATA += metric + ","

      DATA = DATA[:-1] + "\n"
      FINAL_DATA += DATA
  COMBINED = HEADERS + FINAL_DATA
  return(COMBINED)

def main(start_date,sizes=False):
  if sizes:
    analytics = initialize_analyticsreporting()
    response = get_report_sizes(analytics,start_date)
    return print_response(response)
  else:
    analytics = initialize_analyticsreporting()
    response = get_report(analytics,start_date)
    return print_response(response)


if __name__ == '__main__':
  main()
