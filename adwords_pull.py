#!/usr/bin/env python
#
# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import locale
import sys
import _locale
from googleads import adwords
from oauth2client import GOOGLE_REVOKE_URI, GOOGLE_TOKEN_URI, client
import create_client
import os
import pandas as pd
TIME_FRAME = 'LAST_WEEK' # CUSTOM_DATE for a larger date range // The seven-day period starting with previous Monday. LAST_WEEK
_locale._getdefaultlocale = (lambda *args: ['en_US', 'UTF-8'])

def get_raw_report():

  #pulling these values from a credential storage.
  CLIENT_ID = os.getenv('CLIENT_ID')
  CLIENT_SECRET = os.getenv('CLIENT_SECRET')
  REFRESH_TOKEN = os.getenv('REFRESH_TOKEN')
  # # AdWords API information.
  DEVELOPER_TOKEN = os.getenv('DEVELOPER_TOKEN')
  CLIENT_CUSTOMER_ID = os.getenv('CLIENT_CUSTOMER_ID')


  client = create_client.createClient(CLIENT_ID,CLIENT_SECRET,REFRESH_TOKEN,DEVELOPER_TOKEN,CLIENT_CUSTOMER_ID)
  report_downloader = client.GetReportDownloader(version='v201809')

  # Create report definition.
  report = {
      'reportName': 'Shopping - Product Titles - 7 days',
      'dateRangeType': TIME_FRAME, #The seven-day period starting with previous Monday.
      'reportType': 'SHOPPING_PERFORMANCE_REPORT',
      'downloadFormat': 'CSV',
      'selector': {
          'fields': ['Date', 'OfferId','ProductTypeLX', 'Impressions', 'Clicks', 'Cost','AccountDescriptiveName','ConversionValue']
          # ,'dateRange': { #keep this in an emergency to pull a chunk of data if something is missing or goes wrong - historical
          #   'min': '20200518',
          #   'max': '20200524'
          # }
      }
  }

        
  return report_downloader.DownloadReportAsString(
        report, 
        #report_definition,
        skip_report_header=False, 
        skip_column_header=False,
        skip_report_summary=True, 
        include_zero_impressions=False)


def ProductDecline(START_DAY,END_DAY):
  #pulling these values from a credential storage.
  CLIENT_ID = os.getenv('CLIENT_ID')
  CLIENT_SECRET = os.getenv('CLIENT_SECRET')
  REFRESH_TOKEN = os.getenv('REFRESH_TOKEN')
  # # AdWords API information.
  DEVELOPER_TOKEN = os.getenv('DEVELOPER_TOKEN')
  CLIENT_CUSTOMER_ID = os.getenv('CLIENT_CUSTOMER_ID')


  client = create_client.createClient(CLIENT_ID,CLIENT_SECRET,REFRESH_TOKEN,DEVELOPER_TOKEN,CLIENT_CUSTOMER_ID)
  report_downloader = client.GetReportDownloader(version='v201809')

  # Create report definition.
  report = {
      'reportName': 'Shopping - Product Titles - 7 days',
      'dateRangeType': 'CUSTOM_DATE', #Custom date range
      'reportType': 'SHOPPING_PERFORMANCE_REPORT',
      'downloadFormat': 'CSV',
      'selector': {
          'fields': ['Date', 'ProductTypeLX', 'Impressions', 'Clicks', 'Cost','AccountDescriptiveName','ConversionValue']
          ,'dateRange': { 
            'min': START_DAY, #dynamic -4 weeks start date
            'max': END_DAY #end date (sunday) of the 4 week period
          }
          #COULD USE THIS TO FILTER OR SQL LATER - we do the latter - produces same results - tested
          # ,'predicates':{
          #   'field': 'Clicks',
          #   'operator': 'GREATER_THAN',
          #   'values': '0'
          # }
      }
  }

  return report_downloader.DownloadReportAsString(
        report, 
        #report_definition,
        skip_report_header=False, 
        skip_column_header=False,
        skip_report_summary=True, 
        include_zero_impressions=False)


if __name__ == "__main__":
    get_raw_report()