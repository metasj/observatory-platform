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

from abc import (
    ABC, abstractmethod
)
import logging
import pandas as pd
import pydata_google_auth
import datetime

from collections import OrderedDict
from datetime import timezone
from google.oauth2 import service_account
from jinja2 import Environment, PackageLoader
from scipy.spatial.distance import jensenshannon
from typing import Union, Any, List, Hashable, Tuple, Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from elasticsearch import NotFoundError
from elasticsearch_dsl.document import IndexMeta

from elasticsearch_dsl import (
    connections,
    Search,
    Document,

)

from pandas import DataFrame

from observatory_platform.dataquality.analyser import DataQualityAnalyser
from observatory_platform.dataquality.es_mag import (
    MagFosL0Metrics,
    MagFosL0Counts,
    MagPapersMetrics,
    MagPapersYearCount,
    MagPapersFieldYearCount,
    MagDoiCountsDocType,
)

from observatory_platform.dataquality.utils import (
    proportion_delta,
)

from observatory_platform.utils.autofetchcache import AutoFetchCache
from observatory_platform.utils.es_utils import (
    get_or_init_doc_count,
    delete_index,
    clear_index,
    search_by_release,
    search_count_by_release,
    bulk_index,
    init_doc,
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


class MagAnalyserModule(ABC):
    """
    Analysis module interface for the MAG analyser.
    """

    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'run') and callable(subclass.run) and
                hasattr(subclass, 'name') and callable(subclass.name) and
                hasattr(subclass, 'erase') and callable(subclass.erase)
                or NotImplemented)

    @abstractmethod
    def run(self, **kwargs):
        """
        Run the analyser.
        @param kwargs: Optional key value arguments to pass into an analyser. See individual analyser documentation.
        """

        raise NotImplementedError

    @abstractmethod
    def erase(self, index: bool = False, **kwargs):
        """
        Erase elastic search records used by the module and possibly delete the index.
        @param index: If index=True, will also delete indices.
        @param kwargs: Optional key value arguments to pass into an analyser. See individual analyser documentation.
        """

        raise NotImplementedError

    def name(self) -> str:
        """ Get the name of the module.
        @return: Name of the module.
        """

        return self.__class__.__name__


class MagCacheKey:
    """ Cache key names """

    RELEASES = 'releases'  # MAG release dates
    FOSL0 = 'fosl0_'  # concatenated with a date string (%Y%m%d). List of MAG FoS Level 0 fields.
    TPL_ENV = 'tpl_env'  # Template environment
    TPL_RELEASES = 'tpl_releases'  # Template for getting releases from BQ
    TPL_SELECT = 'tpl_select'  # Template for BQ SELECT call


class MagTableKey:
    """ BQ table and column names. """

    TID_PAPERS = 'Papers'
    TID_FOS = 'FieldsOfStudy'

    COL_PAP_COUNT = 'PaperCount'
    COL_CIT_COUNT = 'CitationCount'
    COL_NORM_NAME = 'NormalizedName'
    COL_FOS_ID = 'FieldOfStudyId'

    COL_DOI = 'Doi'
    COL_YEAR = 'Year'
    COL_TOTAL = 'total'
    COL_FAMILY_ID = 'FamilyId'
    COL_DOC_TYPE = 'DocType'


class FieldsOfStudyLevel0Module(MagAnalyserModule):
    """ MagAnalyser module to compute a profile on the MAG Level 0 FieldsOfStudy information. """

    ES_FOS_ID = 'field_id'

    def __init__(self, project_id: str, dataset_id: str, cache):
        """ Initialise the module.
        @param project_id: Project ID in BigQuery.
        @param dataset_id: Dataset ID in BigQuery.
        @param cache: Analyser cache to use.
        """

        logging.info(f'Initialising {self.name()}')
        self._project_id = project_id
        self._dataset_id = dataset_id
        self._cache = cache
        self._num_es_metrics = get_or_init_doc_count(MagFosL0Metrics)
        self._num_es_counts = get_or_init_doc_count(MagFosL0Counts)

    def run(self, **kwargs):
        """ Run this module.
        @param kwargs: Not used.
        """

        logging.info(f'Running {self.name()}')
        releases = self._cache[MagCacheKey.RELEASES]
        num_releases = len(releases)

        if num_releases == self._num_es_metrics and num_releases == self._num_es_counts:
            return

        if self._num_es_counts == 0 or self._num_es_metrics == 0:
            logging.info('No data found in elastic search. Calculating for all releases.')
            previous_counts = self._get_bq_counts(releases[0])
        else:
            logging.info('Retrieving elastic search records.')
            previous_counts = FieldsOfStudyLevel0Module._get_es_counts(releases[self._num_es_metrics - 1].isoformat())

            if previous_counts is None:
                logging.warning('Inconsistent records found in elastic search. Recalculating all releases.')
                self._num_es_counts = 0
                self.erase()
                previous_counts = self._get_bq_counts(releases[0])

        # Construct elastic search documents
        docs = self._construct_es_docs(releases, previous_counts)

        # Save documents in ElasticSearch
        bulk_index(docs)

    def erase(self, index: bool = False, **kwargs):
        """
        Erase elastic search records used by the module and delete the index.
        @param index: If index=True, will also delete indices.
        @param kwargs: Unused.
        """

        clear_index(MagFosL0Metrics)
        clear_index(MagFosL0Counts)

        if index:
            delete_index(MagFosL0Metrics)
            delete_index(MagFosL0Counts)

    def _construct_es_docs(self, releases: List[datetime.date], previous_counts: DataFrame) -> List[Document]:
        """
        Calculate metrics and construct elastic search documents.
        @param releases: List of MAG release dates.
        @param previous_counts: Count information from the previous release.
        @return: List of elastic search documents representing the computed metrics.
        """

        docs = list()
        for i in range(self._num_es_counts, len(releases)):
            release = releases[i]
            current_counts = self._get_bq_counts(release)
            curr_fosid = current_counts[MagTableKey.COL_FOS_ID].to_list()
            curr_fosname = current_counts[MagTableKey.COL_NORM_NAME].to_list()
            prev_fosid = previous_counts[MagTableKey.COL_FOS_ID].to_list()
            prev_fosname = previous_counts[MagTableKey.COL_NORM_NAME].to_list()

            id_unchanged = curr_fosid == prev_fosid
            normalized_unchanged = curr_fosname == prev_fosname

            ts = release.strftime('%Y%m%d')
            self._cache[f'{MagCacheKey.FOSL0}{ts}'] = list(zip(curr_fosid, curr_fosname))

            dppaper = None
            dpcitations = None
            if id_unchanged and normalized_unchanged:
                dppaper = proportion_delta(current_counts[MagTableKey.COL_PAP_COUNT],
                                           previous_counts[MagTableKey.COL_PAP_COUNT])
                dpcitations = proportion_delta(current_counts[MagTableKey.COL_CIT_COUNT],
                                               previous_counts[MagTableKey.COL_CIT_COUNT])

            # Populate counts
            counts = FieldsOfStudyLevel0Module._construct_es_counts(releases[i], current_counts, dppaper, dpcitations)
            logging.info(f'Constructed {len(counts)} MagFosL0Counts documents.')
            docs.extend(counts)

            # Populate metrics
            metrics = FieldsOfStudyLevel0Module._construct_es_metrics(releases[i], current_counts, previous_counts,
                                                                      id_unchanged, normalized_unchanged)
            logging.info(f'Constructed 1 MagFosL0Metrics document.')
            docs.append(metrics)

            # Loop maintenance
            previous_counts = current_counts
        return docs

    @staticmethod
    def _construct_es_counts(release: datetime.date, current_counts: DataFrame, dppaper: DataFrame,
                             dpcitations: DataFrame) -> List[MagFosL0Counts]:
        """ Constructs the MagFosL0Counts documents.
        @param release: MAG release date we are generating a document for.
        @param current_counts: Counts for the current release.
        @param dppaper: Difference of proportions for the paper count between current and last release.
        @param dpcitations: Difference of proportions for the citation count between current and last release.
        @return: List of MagFosL0Counts documents.
        """

        docs = list()
        for i in range(len(current_counts[MagTableKey.COL_PAP_COUNT])):
            fosl0_counts = MagFosL0Counts(release=release)
            fosl0_counts.field_id = current_counts[MagTableKey.COL_FOS_ID][i]
            fosl0_counts.normalized_name = current_counts[MagTableKey.COL_NORM_NAME][i]
            fosl0_counts.paper_count = current_counts[MagTableKey.COL_PAP_COUNT][i]
            fosl0_counts.citation_count = current_counts[MagTableKey.COL_CIT_COUNT][i]
            if dppaper is not None:
                fosl0_counts.delta_ppaper = dppaper[i]
            if dpcitations is not None:
                fosl0_counts.delta_pcitations = dpcitations[i]
            docs.append(fosl0_counts)
        return docs

    @staticmethod
    def _construct_es_metrics(release: datetime.date, current_counts: DataFrame, previous_counts: DataFrame,
                              id_unchanged: bool, normalized_unchanged: bool) -> MagFosL0Metrics:
        """ Constructs the MagFosL0Metrics documents.
        @param release: MAG release date we are generating a document for.
        @param current_counts: Counts for the current release.
        @param current_counts: Counts for the previous release.
        @param id_unchanged: boolean indicating whether the id has changed between releases.
        @param normalized_unchanged: boolean indicating whether the normalized names have changed between releases.
        @return: MagFosL0Metrics document.
        """

        metrics = MagFosL0Metrics(release=release)
        metrics.field_ids_unchanged = id_unchanged
        metrics.normalized_names_unchanged = normalized_unchanged

        if id_unchanged and normalized_unchanged:
            metrics.js_dist_paper = jensenshannon(current_counts[MagTableKey.COL_PAP_COUNT],
                                                  previous_counts[MagTableKey.COL_PAP_COUNT])
            metrics.js_dist_citation = jensenshannon(current_counts[MagTableKey.COL_CIT_COUNT],
                                                     previous_counts[MagTableKey.COL_CIT_COUNT])
        return metrics

    def _get_bq_counts(self, release: datetime.date) -> DataFrame:
        """
        Get the count information from BigQuery table.
        @param release: Release to pull data from.
        @return: Count information.
        """

        ts = release.strftime('%Y%m%d')
        table_id = f'{MagTableKey.TID_FOS}{ts}'
        sql = self._cache[MagCacheKey.TPL_SELECT].render(
            project_id=self._project_id, dataset_id=self._dataset_id, table_id=table_id,
            columns=[MagTableKey.COL_FOS_ID, MagTableKey.COL_NORM_NAME,
                     MagTableKey.COL_PAP_COUNT, MagTableKey.COL_CIT_COUNT],
            order_by=MagTableKey.COL_FOS_ID,
            where='Level = 0'
        )
        return pd.read_gbq(sql, project_id=self._project_id, progress_bar_type=None)

    @staticmethod
    def _get_es_counts(release: str) -> Union[None, DataFrame]:
        """ Retrieve the MagFosL0Counts documents already indexed in elastic search for a given release.
        @param release: Relevant release date.
        @return Retrieved count information.
        """

        hits = search_by_release(MagFosL0Counts, release, 'field_id')

        # Something went wrong with ES records. Delete existing and recompute them.
        if len(hits) == 0:
            return None

        data = {
            MagTableKey.COL_FOS_ID: [x.field_id for x in hits],
            MagTableKey.COL_NORM_NAME: [x.normalized_name for x in hits],
            MagTableKey.COL_PAP_COUNT: [x.paper_count for x in hits],
            MagTableKey.COL_CIT_COUNT: [x.citation_count for x in hits],
        }

        return pd.DataFrame(data=data)


class PaperMetricsModule(MagAnalyserModule):
    """ MagAnalyser module to compute some basic metrics for the Papers dataset in MAG. """

    def __init__(self, project_id: str, dataset_id: str, cache):
        """ Initialise the module.
        @param project_id: Project ID in BigQuery.
        @param dataset_id: Dataset ID in BigQuery.
        @param cache: Analyser cache to use.
        """

        logging.info(f'Initialising {self.name()}')
        self._project_id = project_id
        self._dataset_id = dataset_id
        self._cache = cache
        self._es_count = get_or_init_doc_count(MagPapersMetrics)
        self._tpl_null_count = cache[MagCacheKey.TPL_ENV].get_template('null_count.sql.jinja2')

    def run(self, **kwargs):
        """ Run the module.
        @param kwargs: Unused.
        """

        logging.info(f'Running {self.name()}')
        eps = 1e-9
        releases = self._cache[MagCacheKey.RELEASES]
        num_releases = len(releases)

        if self._es_count == num_releases:
            return

        docs = list()
        for i in range(self._es_count, num_releases):
            if search_count_by_release(MagPapersMetrics, releases[i]) > 0:
                continue
            null_metrics = self._get_paper_null_counts(releases[i])
            es_paper_metrics = MagPapersMetrics(release=releases[i].isoformat())
            es_paper_metrics.total = null_metrics[MagTableKey.COL_TOTAL][0] + eps

            es_paper_metrics.null_year = null_metrics[MagTableKey.COL_YEAR][0] + eps
            es_paper_metrics.null_doi = null_metrics[MagTableKey.COL_DOI][0] + eps
            es_paper_metrics.null_doctype = null_metrics[MagTableKey.COL_DOC_TYPE][0] + eps
            es_paper_metrics.null_familyid = null_metrics[MagTableKey.COL_FAMILY_ID][0] + eps

            es_paper_metrics.pnull_year = es_paper_metrics.null_year / es_paper_metrics.total
            es_paper_metrics.pnull_doi = es_paper_metrics.null_doi / es_paper_metrics.total
            es_paper_metrics.pnull_doctype = es_paper_metrics.null_doctype / es_paper_metrics.total
            es_paper_metrics.pnull_familyid = es_paper_metrics.null_familyid / es_paper_metrics.total
            docs.append(es_paper_metrics)

        logging.info(f'Constructed {len(docs)} MagPapersMetrics documents.')

        if len(docs) > 0:
            bulk_index(docs)

    def erase(self, index: bool = False, **kwargs):
        """
        Erase elastic search records used by the module and delete the index.
        @param index: If index=True, will also delete indices.
        @param kwargs: Unused.
        """

        clear_index(MagPapersMetrics)

        if index:
            delete_index(MagPapersMetrics)

    def _get_paper_null_counts(self, release: datetime.date) -> DataFrame:
        """ Get the null counts of some Papers fields for a given release.
        @param release: Release date.
        @return Null count information.
        """

        ts = release.strftime('%Y%m%d')
        table_id = f'{MagTableKey.TID_PAPERS}{ts}'
        sql = self._tpl_null_count.render(
            project_id=self._project_id, dataset_id=self._dataset_id, table_id=table_id,
            null_count=[MagTableKey.COL_DOI, MagTableKey.COL_DOC_TYPE, MagTableKey.COL_YEAR,
                        MagTableKey.COL_FAMILY_ID]
        )
        return pd.read_gbq(sql, project_id=self._project_id, progress_bar_type=None)


class PaperYearsCountModule(MagAnalyserModule):
    """ MagAnalyser module to compute paper counts by year from MAG. """

    def __init__(self, project_id: str, dataset_id: str, cache):
        """ Initialise the module.
        @param project_id: Project ID in BigQuery.
        @param dataset_id: Dataset ID in BigQuery.
        @param cache: Analyser cache to use.
        """

        logging.info(f'Initialising {self.name()}')
        self._project_id = project_id
        self._dataset_id = dataset_id
        self._cache = cache
        self._tpl_group_count = cache[MagCacheKey.TPL_ENV].get_template('group_count.sql.jinja2')
        init_doc(MagPapersYearCount)

    def run(self, **kwargs):
        """ Run the module.
        @param kwargs: Unused.
        """

        logging.info(f'Running {self.name()}')
        releases = self._cache[MagCacheKey.RELEASES]
        num_releases = len(releases)

        docs = list()
        for i in range(num_releases):
            if search_count_by_release(MagPapersYearCount, releases[i]) > 0:
                continue
            year, counts = self._get_paper_year_count(releases[i])

            for j in range(len(year)):
                paper_count = MagPapersYearCount(release=releases[i].isoformat(), year=year[j], count=counts[j])
                docs.append(paper_count)

        logging.info(f'Constructed {len(docs)} MagPapersYearCount documents.')

        if len(docs) > 0:
            bulk_index(docs)

    def erase(self, index: bool = False, **kwargs):
        """
        Erase elastic search records used by the module and delete the index.
        @param index: If index=True, will also delete indices.
        @param kwargs: Unused.
        """

        clear_index(MagPapersYearCount)

        if index:
            delete_index(MagPapersYearCount)

    def _get_paper_year_count(self, release: datetime.date) -> Tuple[List[int], List[int]]:
        """ Get paper counts by year.
        @param release: Relevant release to get data for.
        @return: Tuple of year and count information.
        """

        ts = release.strftime('%Y%m%d')
        table_id = f'{MagTableKey.TID_PAPERS}{ts}'
        sql = self._tpl_group_count.render(
            project_id=self._project_id, dataset_id=self._dataset_id, table_id=table_id,
            column='Year', where='Year IS NOT NULL'
        )
        df = pd.read_gbq(sql, project_id=self._project_id, progress_bar_type=None)
        return df['Year'].to_list(), df['count'].to_list()


class PapersFieldYearCountModule(MagAnalyserModule):
    """
    MagAnalyser module to compute the paper counts per field per year.
    """

    def __init__(self, project_id: str, dataset_id: str, cache):
        """ Initialise the module.
        @param project_id: Project ID in BigQuery.
        @param dataset_id: Dataset ID in BigQuery.
        @param cache: Analyser cache to use.
        """

        logging.info(f'Initialising {self.name()}')
        self._project_id = project_id
        self._dataset_id = dataset_id
        self._cache = cache
        self._tpl_count_per_field = cache[MagCacheKey.TPL_ENV].get_template('mag_fos_count_perfield.sql.jinja2')
        init_doc(MagPapersFieldYearCount)

    def run(self, **kwargs):
        """ Run the module.
        @param kwargs: Unused.
        """

        logging.info(f'Running {self.name()}')
        releases = self._cache[MagCacheKey.RELEASES]

        docs = list()
        for release in releases:
            if search_count_by_release(MagPapersFieldYearCount, release.isoformat()) > 0:
                continue
            ts = release.strftime('%Y%m%d')
            fos_ids = self._cache[f'{MagCacheKey.FOSL0}{ts}']

            year_counts = list()
            logging.info(f'Fetching release {ts}')
            with ThreadPoolExecutor(max_workers=MagAnalyser.BQ_SESSION_LIMIT) as executor:
                futures = list()
                for id, name in fos_ids:
                    futures.append(executor.submit(self._get_year_counts, id, ts))

                for future in as_completed(futures):
                    year_counts.append(future.result())

            for year_count in year_counts:
                for year, count in year_count:
                    doc = MagPapersFieldYearCount(release=release.isoformat(), field_name=name, field_id=id, year=year,
                                                  count=count)
                    docs.append(doc)

        logging.info(f'Indexing {len(docs)} MagPapersFieldYearCount documents.')
        if len(docs) > 0:
            bulk_index(docs)

    def erase(self, index: bool = False, **kwargs):
        """
        Erase elastic search records used by the module and delete the index.
        @param index: If index=True, will also delete indices.
        @param kwargs: Unused.
        """

        clear_index(MagPapersFieldYearCount)

        if index:
            delete_index(MagPapersFieldYearCount)

    def _get_year_counts(self, id: int, ts: str) -> Iterator[Tuple[int, int]]:
        """ Get the paper counts per field per year for each level 0 field of study.
        @param id: FieldOfStudy id to pull data for.
        @param ts: timestamp to use as a suffix for the table id.
        @return: zip(year, count) information.
        """

        sql = self._tpl_count_per_field.render(project_id=self._project_id, dataset_id=self._dataset_id, release=ts,
                                               fos_id=id)
        df = pd.read_gbq(sql, project_id=self._project_id, progress_bar_type=None)
        return zip(df['Year'].to_list(), df['count'].to_list())


class DoiCountDocTypeModule(MagAnalyserModule):
    """
    MagAnalyser module to compute Doi counts by DocType.
    """

    BQ_DOC_COUNT = 'doc_count'
    BQ_NULL_COUNT = 'null_count'

    def __init__(self, project_id: str, dataset_id: str, cache):
        """ Initialise the module.
        @param project_id: Project ID in BigQuery.
        @param dataset_id: Dataset ID in BigQuery.
        @param cache: Analyser cache to use.
        """

        logging.info(f'Initialising {self.name()}')
        self._project_id = project_id
        self._dataset_id = dataset_id
        self._cache = cache
        init_doc(MagDoiCountsDocType)

    def run(self, **kwargs):
        """ Run the module.
        @param kwargs: Unused.
        """

        logging.info(f'Running {self.name()}')
        releases = self._cache[MagCacheKey.RELEASES]

        docs = list()
        with ThreadPoolExecutor(max_workers=MagAnalyser.BQ_SESSION_LIMIT) as executor:
            futures = list()
            for release in releases:
                futures.append(executor.submit(self._construct_es_docs, release))

            for future in as_completed(futures):
                docs.extend(future.result())

        if len(docs) > 0:
            bulk_index(docs)

    def erase(self, index: bool = False, **kwargs):
        """
        Erase elastic search records used by the module and delete the index.
        @param index: If index=True, will also delete indices.
        @param kwargs: Unused.
        """

        clear_index(MagDoiCountsDocType)

        if index:
            delete_index(MagDoiCountsDocType)

    def _construct_es_docs(self, release: datetime.date) -> MagDoiCountsDocType:
        """
        Construct the elastic search documents for a given release.
        @param release: Release timestamp.
        @return List of MagDoiCountsDocType docs.
        """

        ts = release.strftime('%Y%m%d')
        logging.info(f'DoiCountDocTypeModule processing release {ts}')

        if search_count_by_release(MagDoiCountsDocType, release.isoformat()) > 0:
            return

        docs = list()
        counts = self._get_bq_counts(ts)
        n_counts = len(counts[MagTableKey.COL_DOC_TYPE])
        for i in range(n_counts):
            count = counts[DoiCountDocTypeModule.BQ_DOC_COUNT][i]
            no_doi = counts[DoiCountDocTypeModule.BQ_NULL_COUNT][i]
            doc_type = counts[MagTableKey.COL_DOC_TYPE][i]
            if doc_type is None:
                doc_type = "null"

            doc = MagDoiCountsDocType(release=release.isoformat(), doc_type=doc_type, count=count, no_doi=no_doi,
                                      pno_doi=no_doi / count)
            docs.append(doc)

        return docs


    def _get_bq_counts(self, ts: str):
        """
        Get the Doi counts by DocType from the BigQuery table.
        @param ts: Timestamp to use as table suffix.
        """

        columns = [MagTableKey.COL_DOC_TYPE,
                   f'SUM(CASE WHEN {MagTableKey.COL_DOC_TYPE} IS NULL THEN 1 ELSE 1 END) AS {DoiCountDocTypeModule.BQ_DOC_COUNT}',
                   f'COUNTIF({MagTableKey.COL_DOI} IS NULL) AS {DoiCountDocTypeModule.BQ_NULL_COUNT}'
                   ]

        sql = self._cache[MagCacheKey.TPL_SELECT].render(project_id=self._project_id, dataset_id=self._dataset_id,
                                                         table_id=f'{MagTableKey.TID_PAPERS}{ts}', columns=columns,
                                                         group_by=MagTableKey.COL_DOC_TYPE,
                                                         order_by=MagTableKey.COL_DOC_TYPE)
        df = pd.read_gbq(sql, project_id=self._project_id, progress_bar_type=None)
        return df


class MagAnalyser(DataQualityAnalyser):
    """
    Perform data quality analysis on a Microsoft Academic Graph release, and save the results to ElasticSearch and
    BigQuery (maybe).
    """

    ES_FOS_ID = 'field_id'
    ARG_MODULES = 'modules'
    BQ_SESSION_LIMIT = 20  # Limit is 100.

    def __init__(self, project_id: str = 'academic-observatory-dev', dataset_id: str = 'mag',
                 modules: Union[None, List[MagAnalyserModule]] = None):

        logging.info('Initialising MagAnalyser')

        self._project_id = project_id
        self._dataset_id = dataset_id
        self._end_date = datetime.datetime.now(timezone.utc)
        self._cache = AutoFetchCache()
        self._init_cache()
        self._modules = self._load_modules(modules)

    def run(self, **kwargs):
        """ Entry point for the analyser.

        @param kwargs: Optional arguments.
        modules=list() a list of module names to run in order.
        """

        logging.info('Running MagAnalyserModule modules.')
        modules = self._modules

        if MagAnalyser.ARG_MODULES in kwargs:
            modules = self._load_modules(kwargs[MagAnalyser.ARG_MODULES])

        for module in modules.values():
            module.run()

    def erase(self, index: bool = False, **kwargs):
        """
        Erase elastic search records used by all modules in the analyser.
        @param index: If index=True, will also delete indices.
        @param kwargs: Unused.
        """

        for module in self._modules.values():
            module.erase(index)

    def _init_cache(self):
        """ Initialise some common things in the auto fetcher cache. """

        self._init_tpl_env_fetcher()
        self._init_tpl_releases_fetcher()
        self._init_tpl_select_fetcher()

        self._init_releases_fetcher()
        self._init_fosl0_fetcher()

    def _init_tpl_env_fetcher(self):
        """ Initialise the Jinja template environment object fetcher. """

        self._cache.set_fetcher(MagCacheKey.TPL_ENV, lambda _: Environment(
            loader=PackageLoader('observatory_platform', 'database/workflows/sql')))

    def _init_tpl_releases_fetcher(self):
        """ Initialise the Jinja releases template fetcher. """

        tpl_env = self._cache[MagCacheKey.TPL_ENV]
        releases = tpl_env.get_template('select_table_suffixes.sql.jinja2')
        self._cache.set_fetcher(MagCacheKey.TPL_RELEASES, lambda _: releases)

    def _init_tpl_select_fetcher(self):
        """ Initialise the Jinja select template fetcher. """

        tpl_env = self._cache[MagCacheKey.TPL_ENV]
        select = tpl_env.get_template('select_table.sql.jinja2')
        self._cache.set_fetcher(MagCacheKey.TPL_SELECT, lambda _: select)

    def _init_releases_fetcher(self):
        """ Initialise the releases cache fetcher. """

        self._cache.set_fetcher(MagCacheKey.RELEASES, lambda _: self._get_releases())

    def _init_fosl0_fetcher(self):
        """ Initialise the fields of study level 0 id/name fetcher. """

        releases = self._cache[MagCacheKey.RELEASES]
        for release in releases:
            ts = release.strftime('%Y%m%d')
            self._cache.set_fetcher(f'{MagCacheKey.FOSL0}{ts}', lambda key: self._get_fosl0(key))

    def _load_modules(self, modules: Union[None, List[MagAnalyserModule]]) -> OrderedDict:
        """ Load the modules into an ordered dictionary for use.
        @param modules: None or a list of modules you want to load into an ordered dictionary of modules.
        @return: Ordered dictionary of modules.
        """

        mods = OrderedDict()
        # Override with user supplied list.
        if modules is not None:
            for module in modules:
                if not issubclass(type(module), MagAnalyserModule):
                    raise ValueError(f'{module} is not a MagAnalyserModule')

                mods[module.name()] = module

        # Use default modules.
        else:
            default_modules = list()
            default_modules.append(FieldsOfStudyLevel0Module(self._project_id, self._dataset_id, self._cache))
            default_modules.append(PaperMetricsModule(self._project_id, self._dataset_id, self._cache))
            default_modules.append(PaperYearsCountModule(self._project_id, self._dataset_id, self._cache))
            default_modules.append(PapersFieldYearCountModule(self._project_id, self._dataset_id, self._cache))
            default_modules.append(DoiCountDocTypeModule(self._project_id, self._dataset_id, self._cache))

            for module in default_modules:
                mods[module.name()] = module

        return mods

    def _get_releases(self) -> List[datetime.date]:
        """ Get the list of MAG releases from BigQuery.
        @return: List of MAG release dates sorted in ascending order.
        """

        sql = self._cache[MagCacheKey.TPL_RELEASES].render(project_id=self._project_id, dataset_id=self._dataset_id,
                                                           table_id=MagTableKey.TID_FOS, end_date=self._end_date)
        rel_list = list(reversed(pd.read_gbq(sql, project_id=self._project_id, progress_bar_type=None)['suffix']))
        releases = [release.date() for release in rel_list]
        logging.info(f'Found {len(releases)} MAG releases in BigQuery.')
        return releases

    def _get_fosl0(self, key) -> List[Tuple[int, str]]:
        """ Get the level 0 fields of study for a given MAG release from BigQuery.
        @param key: Key used to access cache. Suffix is release date.
        @return: Tuple of (id, name) for each level 0 field of study.
        """

        table_suffix = key[key.find('_') + 1:]
        sql = self._cache[MagCacheKey.TPL_SELECT].render(project_id=self._project_id, dataset_id=self._dataset_id,
                                                         table_id=f'{MagTableKey.TID_FOS}{table_suffix}',
                                                         columns=[MagTableKey.COL_FOS_ID, MagTableKey.COL_NORM_NAME],
                                                         where='Level = 0',
                                                         order_by=MagTableKey.COL_FOS_ID)

        df = pd.read_gbq(sql, project_id=self._project_id, progress_bar_type=None)
        ids = df[MagTableKey.COL_FOS_ID].to_list()
        names = df[MagTableKey.COL_NORM_NAME].to_list()

        return list(zip(ids, names))