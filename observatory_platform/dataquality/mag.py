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

import pandas as pd
import pydata_google_auth

from datetime import datetime, timezone
from google.oauth2 import service_account
from jinja2 import Environment, PackageLoader
from scipy.spatial.distance import jensenshannon

from elasticsearch.helpers import bulk

from elasticsearch_dsl import (
    connections,
    Search
)

from observatory_platform.dataquality.es_mag import (
    MagReleaseEs,
    FieldsOfStudyLevel0
)

from observatory_platform.dataquality.utils import (
    proportion_delta
)

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


class MagAnalyser:
    '''
    Perform data quality analysis on a Microsoft Academic Graph release, and save the results to ElasticSearch and
    BigQuery (maybe).
    '''

    def __init__(self, project_id='academic-observatory-dev', dataset_id='mag'):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.env = Environment(loader=PackageLoader('observatory_platform', 'database/workflows/sql'))
        self.suffix_tpl = self.env.get_template('select_table_suffixes.sql.jinja2')
        self.select_tpl = self.env.get_template('select_table.sql.jinja2')

    def fields_of_study(self):
        '''
        Analyse FieldsOfStudy.
        '''

        table_id = 'FieldsOfStudy'
        end_date = datetime.now(timezone.utc)

        # See if there are any previously computed records we can load
        es_out = []  # Data to write out to ES
        # Nothing has been computed.  Populate ES for the first time.

        try:
            es_count = Search(index=MagReleaseEs.Index.name).count()
        except:
            es_count = 0

        if es_count == 0:
            sql = self.suffix_tpl.render(project_id=self.project_id, dataset_id=self.dataset_id, table_id=table_id,
                                         end_date=end_date)
            ts = pd.read_gbq(sql, project_id=self.project_id)
            previous_ts = ts['suffix'][0].date().strftime('%Y%m%d')
            previous_table_id = f'{table_id}{previous_ts}'
            sql = self.select_tpl.render(
                project_id=self.project_id, dataset_id=self.dataset_id, table_id=previous_table_id,
                columns=['FieldOfStudyId, NormalizedName, PaperCount, CitationCount'], order_by='FieldOfStudyId',
                where='Level = 0'
            )
            previous_counts = pd.read_gbq(sql, project_id=self.project_id)

            for i in range(len(ts['suffix'])):
                current_ts = ts['suffix'][i].date().strftime('%Y%m%d')
                current_table_id = f'{table_id}{current_ts}'
                sql = self.select_tpl.render(
                    project_id=self.project_id, dataset_id=self.dataset_id, table_id=current_table_id,
                    columns=['FieldOfStudyId, NormalizedName, PaperCount, CitationCount'], order_by='FieldOfStudyId',
                    where='Level = 0'
                )
                current_counts = pd.read_gbq(sql, project_id=self.project_id)

                # Meta data structure for elastic search
                es_mag = MagReleaseEs(release=ts['suffix'][i].date())

                # Check Id equality
                id_unchanged = (
                            current_counts['FieldOfStudyId'].to_list() == previous_counts['FieldOfStudyId'].to_list())

                # Check NormalizedName equality
                normalized_unchanged = current_counts['NormalizedName'].to_list() == previous_counts[
                    'NormalizedName'].to_list()

                es_mag_fos_level0 = FieldsOfStudyLevel0(field_ids=current_counts['FieldOfStudyId'].to_list(),
                                                        field_ids_unchanged=id_unchanged,
                                                        normalized_names=current_counts['NormalizedName'].to_list(),
                                                        normalized_names_unchanged=normalized_unchanged,
                                                        paper_counts=current_counts['PaperCount'].to_list(),
                                                        citation_counts=current_counts['CitationCount'].to_list()
                                                        )

                # Only do computation if ids and names unchanged
                if id_unchanged and normalized_unchanged:
                    es_mag_fos_level0.delta_ppaper = proportion_delta(current_counts['PaperCount'],
                                                                      previous_counts['PaperCount']).to_list()
                    es_mag_fos_level0.delta_pcitations = proportion_delta(current_counts['CitationCount'],
                                                                          previous_counts['CitationCount']).to_list()
                    es_mag_fos_level0.js_dist_paper = jensenshannon(current_counts['PaperCount'],
                                                                    previous_counts['PaperCount'])
                    es_mag_fos_level0.js_dist_citation = jensenshannon(current_counts['CitationCount'],
                                                                       previous_counts['CitationCount'])

                es_mag.fields_of_study_level0 = es_mag_fos_level0
                es_out.append(es_mag)

                # Loop maintenance
                previous_counts = current_counts

            bulk(
                connections.get_connection(),
                (doc.to_dict(include_meta=True) for doc in es_out),
                refresh=True,
            )

    def affiliations(self):
        '''
        Analyse Affiliations.
        '''

        # SELECT AffiliationId, NormalizedName, PaperCount, PaperFamilyCount, CitationCount FROM `academic-observatory-dev.mag.Affiliations20200901` ORDER BY AffiliationId


def init_es_connection():
    user = ''
    password = ''
    hostname = ''
    port = ''
    connections.create_connection(hosts=[f'https://{user}:{password}@{hostname}:{port}'], timeout=60)


if __name__ == '__main__':
    init_es_connection()

    mag = MagAnalyser()
    mag.fields_of_study()

    # s = Search(index=MagReleaseEs.Index.name).query()
    # response = s.execute()
    # len(response)
    # s.delete()