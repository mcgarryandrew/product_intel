#!/usr/bin/env python
#
# Copyright 2014 Google Inc. All Rights Reserved.
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

from googleads import adwords
from googleads import oauth2
import os
import env
# OAuth2 credential information. In a real application, you'd probably be

def createClient(client_id, client_secret, refresh_token, developer_token,client_customer_id):
    oauth2_client = oauth2.GoogleRefreshTokenClient(
    client_id, client_secret, refresh_token, access_token=os.getenv('ACCESS_TOKEN'))
    adwords_client = adwords.AdWordsClient(developer_token, oauth2_client,client_customer_id=client_customer_id)
    
    return adwords_client