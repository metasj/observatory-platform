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

# Author: Aniek Roelofs, James Diprose, Tuan Chien

import datetime
from typing import List

import pendulum
from observatory.platform.telescopes.telescope import Release, Telescope
from observatory.platform.utils.airflow_utils import AirflowVars
from observatory.platform.utils.template_utils import blob_name, \
    bq_load_shard, \
    table_ids_from_path, \
    upload_files_from_list


class SnapshotRelease(Release):
    def __init__(self, dag_id: str, release_date: pendulum.Pendulum, download_files_regex: str = None,
                 extract_files_regex: str = None, transform_files_regex: str = None):
        """ Construct a SnapshotRelease instance
        :param dag_id: the id of the DAG.
        :param release_date: the release date (used to construct release_id).
        :param download_files_regex: regex pattern that is used to find files in download folder
        :param extract_files_regex: regex pattern that is used to find files in extract folder
        :param transform_files_regex: regex pattern that is used to find files in transform folder
        """
        self.release_date = release_date
        release_id = f'{dag_id}_{release_date.strftime("%Y_%m_%d")}'
        super().__init__(dag_id, release_id, download_files_regex, extract_files_regex, transform_files_regex)


class SnapshotTelescope(Telescope):
    def __init__(self, dag_id: str, start_date: datetime, schedule_interval: str, dataset_id: str, catchup: bool = True,
                 queue: str = 'default', max_retries: int = 3, max_active_runs: int = 1, schema_prefix: str = '',
                 schema_version: str = None, airflow_vars: list = None, airflow_conns: list = None):
        """ Construct a SnapshotTelescope instance.
        :param dag_id: the id of the DAG.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param dataset_id: the dataset id.
        :param catchup: whether to catchup the DAG or not.
        :param queue: the Airflow queue name.
        :param max_retries: the number of times to retry each task.
        :param max_active_runs: the maximum number of DAG runs that can be run at once.
        :param schema_prefix: the prefix used to find the schema path
        :param schema_version: the version used to find the schema path
        :param airflow_vars: list of airflow variable keys, for each variable it is checked if it exists in airflow
        :param airflow_conns: list of airflow connection keys, for each connection it is checked if it exists in airflow
        """
        super().__init__(dag_id, start_date, schedule_interval, catchup, queue, max_retries, max_active_runs,
                         schema_prefix, schema_version, airflow_vars, airflow_conns)
        self.dataset_id = dataset_id
        # Set transform_bucket_name as required airflow variable
        if not airflow_vars:
            airflow_vars = []
        self.airflow_vars = list(set([AirflowVars.TRANSFORM_BUCKET] + airflow_vars))
        self.airflow_conns = airflow_conns

    def upload_transformed(self, releases: List[SnapshotRelease], **kwargs):
        """ Task to upload each transformed release to a google cloud bucket
        :param releases:
        :param kwargs:
        :return:
        """
        for release in releases:
            upload_files_from_list(release.transform_files, release.transform_bucket)

    def bq_load(self, releases: List[SnapshotRelease], **kwargs):
        """ Task to load each transformed release to BigQuery.
        :param releases: a list of GRID releases.
        :return: None.
        """

        # Load each transformed release
        for release in releases:
            for transform_path in release.transform_files:
                transform_blob = blob_name(transform_path)
                table_id, _ = table_ids_from_path(transform_path)
                bq_load_shard(release.release_date, transform_blob, self.dataset_id, table_id, self.schema_prefix,
                              self.schema_version, self.description)

    def cleanup(self, releases: List[SnapshotRelease], **kwargs):
        """ Delete files of downloaded, extracted and transformed release.
        :param releases: the oapen metadata release
        :return: None.
        """
        # Delete files from each release
        for release in releases:
            release.cleanup()
