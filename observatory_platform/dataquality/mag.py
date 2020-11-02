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

import logging
import pandas as pd
import pydata_google_auth

from datetime import datetime, timezone
from google.oauth2 import service_account
from jinja2 import Environment, PackageLoader
from scipy.spatial.distance import jensenshannon
from typing import Union

from elasticsearch.helpers import bulk

from elasticsearch_dsl import (
    connections,
    Search
)

from pandas import DataFrame

from observatory_platform.dataquality.es_mag import (
    MagFosL0Metrics,
    MagFosL0Counts
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

    FOS_TABLE_ID = 'FieldsOfStudy'

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

        end_date = datetime.now(timezone.utc)

        # Get the releases.
        sql = self.suffix_tpl.render(project_id=self.project_id, dataset_id=self.dataset_id,
                                     table_id=MagAnalyser.FOS_TABLE_ID, end_date=end_date)
        releases = reversed(pd.read_gbq(sql, project_id=self.project_id)['suffix'])
        logging.info(f'Found {len(releases)} MAG releases in BigQuery.')

        # See how many releases have already been saved.
        try:
            es_count = Search(index=MagFosL0Metrics.Index.name).count()
        except:
            es_count = 0

        # No new releases, do nothing.
        if len(releases) <= es_count:
            logging.info('No new MAG releases in BigQuery.')
            return

        if es_count == 0:
            logging.info('No data found in elastic search. Calculating for all releases.')
            previous_counts = self._get_fos_counts(releases, 'previous')
        else:
            dstr = releases[es_count - 1].date().isoformat()
            previous_counts = self._get_es_magfosl0_counts(dstr)
            logging.info('Previous records found. Adding records for the latest releases.')
            if previous_counts is None:
                logging.warning('Inconsistent records found in elastic search. Recalculating all releases.')
                es_count = 0
                self._clear_es_records(MagFosL0Metrics.Index.name)
                self._clear_es_records(MagFosL0Counts.Index.name)
                previous_counts = self._get_fos_counts(releases, 'previous')

        # Construct elastic search documents
        docs = []
        for i in range(es_count, len(releases)):
            current_counts = self._get_fos_counts(releases, i)
            self._create_es_fosl0_docs(docs, current_counts, previous_counts, releases[i].date())

            # Loop maintenance
            previous_counts = current_counts

        # Bulk document index on ElasticSearch
        bulk(
            connections.get_connection(),
            (doc.to_dict(include_meta=True) for doc in docs),
            refresh=True,
        )

    def _create_es_fosl0_docs(self, docs, current_counts, previous_counts, release):
        id_unchanged = (
                current_counts['FieldOfStudyId'].to_list() == previous_counts['FieldOfStudyId'].to_list())

        normalized_unchanged = current_counts['NormalizedName'].to_list() == previous_counts[
            'NormalizedName'].to_list()

        if id_unchanged and normalized_unchanged:
            delta_ppaper = proportion_delta(current_counts['PaperCount'],
                                            previous_counts['PaperCount']).to_list()
            delta_pcitations = proportion_delta(current_counts['CitationCount'],
                                                previous_counts['CitationCount']).to_list()

        # Populate counts
        for i in range(len(current_counts['PaperCount'])):
            fosl0_counts = MagFosL0Counts(release=release)
            fosl0_counts.field_id = current_counts['FieldOfStudyId'][i]
            fosl0_counts.normalized_name = current_counts['NormalizedName'][i]
            fosl0_counts.paper_count = current_counts['PaperCount'][i]
            fosl0_counts.citation_count = current_counts['CitationCount'][i]
            if id_unchanged and normalized_unchanged:
                fosl0_counts.delta_ppaper = delta_ppaper[i]
                fosl0_counts.delta_pcitations = delta_pcitations[i]
            docs.append(fosl0_counts)

        # Populate metrics
        fosl0_metrics = MagFosL0Metrics(release=release)
        fosl0_metrics.field_ids_unchanged = id_unchanged
        fosl0_metrics.normalized_names_unchanged = normalized_unchanged

        if id_unchanged and normalized_unchanged:
            fosl0_metrics.js_dist_paper = jensenshannon(current_counts['PaperCount'],
                                             previous_counts['PaperCount'])
            fosl0_metrics.js_dist_citation = jensenshannon(current_counts['CitationCount'],
                                                previous_counts['CitationCount'])
        docs.append(fosl0_metrics)

        return docs

    def _clear_es_records(self, index):
        s = Search(index=index).query()
        s.delete()

    def _get_es_magfosl0_counts(self, release: str) -> Union[None, DataFrame]:
        s = Search(index=MagFosL0Counts.Index.name).query('match', release=release)
        response = s.execute()

        # Something went wrong with ES records. Delete existing and recompute them.
        if len(response.hits) == 0:
            return None

        cmp_id = lambda x, y : x.field_id < y.field_id
        data = {
            'FieldOfStudyId': sorted([x.field_id for x in response.hits], cmp_id),
            'NormalizedName': sorted([x.normalized_name for x in response.hits], cmp_id),
            'PaperCount': sorted([x.paper_count for x in response.hits], cmp_id),
            'CitationCount': sorted([x.citation_count for x in response.hits], cmp_id),
        }

        return pd.DataFrame(data=data)

    def _get_fos_counts(self, releases, target: Union[int, str]):
        if target == 'previous':
            ts = releases[0].date().strftime('%Y%m%d')
        else:
            ts = releases[target].date().strftime('%Y%m%d')

        table_id = f'{MagAnalyser.FOS_TABLE_ID}{ts}'
        sql = self.select_tpl.render(
            project_id=self.project_id, dataset_id=self.dataset_id, table_id=table_id,
            columns=['FieldOfStudyId, NormalizedName, PaperCount, CitationCount'], order_by='FieldOfStudyId',
            where='Level = 0'
        )
        return pd.read_gbq(sql, project_id=self.project_id)

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