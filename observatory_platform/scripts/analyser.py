#!/usr/bin/python3

# Copyright 2020 Curtin University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Author: Tuan Chien

from observatory_platform.dataquality.mag import MagAnalyser
from elasticsearch_dsl import connections
import pydata_google_auth
from google.oauth2 import service_account


# Temporary stuff for GCP credentials
SCOPES = [
    'https://www.googleapis.com/auth/cloud-platform',
    'https://www.googleapis.com/auth/drive',
]
# credentials = service_account.Credentials.from_service_account_file('path/to/key.json')
credentials = pydata_google_auth.get_user_credentials(
    SCOPES,
    # Set auth_local_webserver to True to have a slightly more convienient
    # authorization flow. Note, this doesn't work if you're running from a
    # notebook on a remote sever, such as over SSH or with Google Colab.
    auth_local_webserver=True
)


def init_es_connection():
    user = 'tuan.chien'
    password = 'tuancoki'
    hostname = '2de27cd7b8724f519461a303757a3f52.us-west1.gcp.cloud.es.io'
    port = '9243'
    connections.create_connection(hosts=[f'https://{user}:{password}@{hostname}:{port}'], timeout=60)


if __name__ == '__main__':
    init_es_connection()

    mag = MagAnalyser()
    mag.run()
