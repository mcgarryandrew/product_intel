from google.oauth2 import service_account
import os
import json
#UNCOMMENT THIS FOR LOCAL
import env

def getCreds():

    SCOPES = ['https://www.googleapis.com/auth/bigquery']

    #use the following for local run
    SERVICE_ACCOUNT_FILE = os.getenv('SERVICE_ACC_FILE')
    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    return credentials