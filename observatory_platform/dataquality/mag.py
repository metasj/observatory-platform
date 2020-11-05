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
from typing import Union, Any, List, Hashable, Tuple

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
        return (hasattr(subclass, 'run') and callable(subclass.run)
                and hasattr(subclass, 'name') and callable(subclass.name)
                or NotImplemented)

    @abstractmethod
    def name(self) -> str:
        """ Get the name of the module.
        @return: Name of the module.
        """

        raise NotImplementedError

    @abstractmethod
    def run(self, **kwargs):
        """
        Run the analyser.
        @param kwargs: Optional key value arguments to pass into an analyser. See individual analyser documentation.
        """

        raise NotImplementedError


class FieldsOfStudyLevel0Module(MagAnalyserModule):
    """ MagAnalyser module to compute a profile on the MAG Level 0 FieldsOfStudy information. """

    CACHE_ID_PREFIX = 'fosl0id_'
    CACHE_NAME_PREFIX = 'fosl0name_'
    BQ_PAPER_COUNT = 'PaperCount'
    BQ_CITATION_COUNT = 'CitationCount'
    BQ_NORMALIZED_NAME = 'NormalizedName'
    BQ_FOS_ID = 'FieldOfStudyId'
    ES_FOS_ID = 'field_id'

    def __init__(self, project_id: str, dataset_id: str, cache):
        """ Initialise the module.
        @param project_id: Project ID in BigQuery.
        @param dataset_id: Dataset ID in BigQuery.
        @param cache: Analyser cache to use.
        """

        self._project_id = project_id
        self._dataset_id = dataset_id
        self._cache = cache

        self._tpl_env = Environment(loader=PackageLoader('observatory_platform', 'database/workflows/sql'))
        self._tpl_releases = self._tpl_env.get_template('select_table_suffixes.sql.jinja2')
        self._tpl_select = self._tpl_env.get_template('select_table.sql.jinja2')

        self._num_es_metrics = get_or_init_doc_count(MagFosL0Metrics)
        self._num_es_counts = get_or_init_doc_count(MagFosL0Counts)

    def name(self) -> str:
        """ Get the name of the module.
        @return: Name of the module.
        """

        return 'FieldsOfStudyLevel0Module'

    def run(self, **kwargs):
        """ Run this module.
        @param kwargs: Not used.
        """

        releases = self._cache[MagAnalyser.CACHE_RELEASES]
        num_releases = len(releases)

        if num_releases == self._num_es_metrics and num_releases == self._num_es_counts:
            return

        if self._num_es_counts == 0 or self._num_es_metrics == 0:
            logging.info('No data found in elastic search. Calculating for all releases.')
            previous_counts = self._get_bq_counts(releases[0])
        else:
            logging.info('Retrieving elastic search records.')
            previous_counts = FieldsOfStudyLevel0Module._get_es_counts(releases[self._num_es_counts - 1].isoformat())

            if previous_counts is None:
                logging.warning('Inconsistent records found in elastic search. Recalculating all releases.')
                self._num_es_counts = 0
                clear_index(MagFosL0Metrics.Index.name)
                clear_index(MagFosL0Counts.Index.name)
                previous_counts = self._get_bq_counts(releases[0])

        # Construct elastic search documents
        docs = self._construct_es_docs(releases, previous_counts)

        # Save documents in ElasticSearch
        bulk_index(docs)

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
            curr_fosid = current_counts[FieldsOfStudyLevel0Module.BQ_FOS_ID].to_list()
            curr_fosname = current_counts[FieldsOfStudyLevel0Module.BQ_NORMALIZED_NAME].to_list()
            prev_fosid = previous_counts[FieldsOfStudyLevel0Module.BQ_FOS_ID].to_list()
            prev_fosname = previous_counts[FieldsOfStudyLevel0Module.BQ_NORMALIZED_NAME].to_list()

            id_unchanged = curr_fosid == prev_fosid
            normalized_unchanged = curr_fosname == prev_fosname

            ts = release.strftime('%Y%m%d')
            self._cache[f'{FieldsOfStudyLevel0Module.CACHE_ID_PREFIX}{ts}'] = curr_fosid
            self._cache[f'{FieldsOfStudyLevel0Module.CACHE_NAME_PREFIX}{ts}'] = curr_fosname

            dppaper = None
            dpcitations = None
            if id_unchanged and normalized_unchanged:
                dppaper = proportion_delta(current_counts[FieldsOfStudyLevel0Module.BQ_PAPER_COUNT],
                                           previous_counts[FieldsOfStudyLevel0Module.BQ_PAPER_COUNT])
                dpcitations = proportion_delta(current_counts[FieldsOfStudyLevel0Module.BQ_CITATION_COUNT],
                                               previous_counts[FieldsOfStudyLevel0Module.BQ_CITATION_COUNT])

            # Populate counts
            counts = FieldsOfStudyLevel0Module._construct_es_counts(releases[i], current_counts, dppaper, dpcitations)
            docs.extend(counts)

            # Populate metrics
            metrics = FieldsOfStudyLevel0Module._construct_es_metrics(releases[i], current_counts, previous_counts,
                                                                      id_unchanged, normalized_unchanged)
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
        for i in range(len(current_counts[FieldsOfStudyLevel0Module.BQ_PAPER_COUNT])):
            fosl0_counts = MagFosL0Counts(release=release)
            fosl0_counts.field_id = current_counts[FieldsOfStudyLevel0Module.BQ_FOS_ID][i]
            fosl0_counts.normalized_name = current_counts[FieldsOfStudyLevel0Module.BQ_NORMALIZED_NAME][i]
            fosl0_counts.paper_count = current_counts[FieldsOfStudyLevel0Module.BQ_PAPER_COUNT][i]
            fosl0_counts.citation_count = current_counts[FieldsOfStudyLevel0Module.BQ_CITATION_COUNT][i]
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
            metrics.js_dist_paper = jensenshannon(current_counts[FieldsOfStudyLevel0Module.BQ_PAPER_COUNT],
                                                  previous_counts[FieldsOfStudyLevel0Module.BQ_PAPER_COUNT])
            metrics.js_dist_citation = jensenshannon(current_counts[FieldsOfStudyLevel0Module.BQ_CITATION_COUNT],
                                                     previous_counts[FieldsOfStudyLevel0Module.BQ_CITATION_COUNT])
        return metrics

    def _get_bq_counts(self, release: datetime.date) -> DataFrame:
        """
        Get the count information from BigQuery table.
        @param release: Release to pull data from.
        @return: Count information.
        """

        ts = release.strftime('%Y%m%d')
        table_id = f'{MagAnalyser.FOS_TABLE_ID}{ts}'
        sql = self._tpl_select.render(
            project_id=self._project_id, dataset_id=self._dataset_id, table_id=table_id,
            columns=[FieldsOfStudyLevel0Module.BQ_FOS_ID, FieldsOfStudyLevel0Module.BQ_NORMALIZED_NAME,
                     FieldsOfStudyLevel0Module.BQ_PAPER_COUNT, FieldsOfStudyLevel0Module.BQ_CITATION_COUNT],
            order_by=FieldsOfStudyLevel0Module.BQ_FOS_ID,
            where='Level = 0'
        )
        return pd.read_gbq(sql, project_id=self._project_id)

    @staticmethod
    def _get_es_counts(release: str) -> Union[None, DataFrame]:
        """ Retrieve the MagFosL0Counts documents already indexed in elastic search for a given release.
        @param release: Relevant release date.
        @return Retrieved count information.
        """

        hits = search_by_release(MagFosL0Counts.Index.name, release, 'field_id')

        # Something went wrong with ES records. Delete existing and recompute them.
        if len(hits) == 0:
            return None

        data = {
            FieldsOfStudyLevel0Module.BQ_FOS_ID: [x.field_id for x in hits],
            FieldsOfStudyLevel0Module.BQ_NORMALIZED_NAME: [x.normalized_name for x in hits],
            FieldsOfStudyLevel0Module.BQ_PAPER_COUNT: [x.paper_count for x in hits],
            FieldsOfStudyLevel0Module.BQ_CITATION_COUNT: [x.citation_count for x in hits],
        }

        return pd.DataFrame(data=data)


class PaperMetricsModule(MagAnalyserModule):
    """ MagAnalyser module to compute some basic metrics for the Papers dataset in MAG. """

    BQ_DOI = 'Doi'
    BQ_YEAR = 'Year'
    BQ_TOTAL = 'total'
    BQ_FAMILY_ID = 'FamilyId'
    BQ_DOC_TYPE = 'DocType'

    def __init__(self, project_id: str, dataset_id: str, cache):
        """ Initialise the module.
        @param project_id: Project ID in BigQuery.
        @param dataset_id: Dataset ID in BigQuery.
        @param templates: Jinja templates used for MAG Analyser.
        @param cache: Analyser cache to use.
        """

        self._project_id = project_id
        self._dataset_id = dataset_id
        self._cache = cache
        self._es_count = get_or_init_doc_count(MagPapersMetrics)

        self._tpl_env = Environment(loader=PackageLoader('observatory_platform', 'database/workflows/sql'))
        self._tpl_null_count = self._tpl_env.get_template('null_count.sql.jinja2')

    def name(self) -> str:
        """ Get the module name.
        @return: Module name.
        """

        return 'PaperMetricsModule'

    def run(self, **kwargs):
        """ Run the module.
        @param kwargs: Unused.
        """

        eps = 1e-9
        releases = self._cache[MagAnalyser.CACHE_RELEASES]
        num_releases = len(releases)

        if self._es_count == num_releases:
            return

        docs = list()
        for i in range(self._es_count, num_releases):
            if search_count_by_release(MagPapersMetrics.Index.name, releases[i]) > 0:
                continue
            null_metrics = self._get_paper_null_counts(releases[i])
            es_paper_metrics = MagPapersMetrics(release=releases[i].isoformat())
            es_paper_metrics.total = null_metrics[PaperMetricsModule.BQ_TOTAL][0] + eps

            es_paper_metrics.null_year = null_metrics[PaperMetricsModule.BQ_YEAR][0] + eps
            es_paper_metrics.null_doi = null_metrics[PaperMetricsModule.BQ_DOI][0] + eps
            es_paper_metrics.null_doctype = null_metrics[PaperMetricsModule.BQ_DOC_TYPE][0] + eps
            es_paper_metrics.null_familyid = null_metrics[PaperMetricsModule.BQ_FAMILY_ID][0] + eps

            es_paper_metrics.pnull_year = es_paper_metrics.null_year / es_paper_metrics.total
            es_paper_metrics.pnull_doi = es_paper_metrics.null_doi / es_paper_metrics.total
            es_paper_metrics.pnull_doctype = es_paper_metrics.null_doctype / es_paper_metrics.total
            es_paper_metrics.pnull_familyid = es_paper_metrics.null_familyid / es_paper_metrics.total
            docs.append(es_paper_metrics)

        if len(docs) > 0:
            bulk_index(docs)

    def _get_paper_null_counts(self, release: datetime.date) -> DataFrame:
        """ Get the null counts of some Papers fields for a given release.
        @param release: Release date.
        @return Null count information.
        """

        ts = release.strftime('%Y%m%d')
        table_id = f'{MagAnalyser.PAPERS_TABLE_ID}{ts}'
        sql = self._tpl_null_count.render(
            project_id=self._project_id, dataset_id=self._dataset_id, table_id=table_id,
            null_count=[PaperMetricsModule.BQ_DOI, PaperMetricsModule.BQ_DOC_TYPE, PaperMetricsModule.BQ_YEAR,
                        PaperMetricsModule.BQ_FAMILY_ID]
        )
        return pd.read_gbq(sql, project_id=self._project_id)


class PaperYearsCountModule(MagAnalyserModule):
    """ MagAnalyser module to compute paper counts by year from MAG. """

    def __init__(self, project_id: str, dataset_id: str, cache):
        """ Initialise the module.
        @param project_id: Project ID in BigQuery.
        @param dataset_id: Dataset ID in BigQuery.
        @param templates: Jinja templates used for MAG Analyser.
        @param cache: Analyser cache to use.
        """

        self._project_id = project_id
        self._dataset_id = dataset_id
        self._cache = cache

        self._tpl_env = Environment(loader=PackageLoader('observatory_platform', 'database/workflows/sql'))
        self._tpl_group_count = self._tpl_env.get_template('group_count.sql.jinja2')
        init_doc(MagPapersYearCount)

    def name(self) -> str:
        """ Get the module name.
        @return: Module name.
        """
        return 'PaperYearsCountModule'

    def run(self, **kwargs):
        """ Run the module.
        @param kwargs: Unused.
        """

        releases = self._cache[MagAnalyser.CACHE_RELEASES]
        num_releases = len(releases)

        docs = list()
        for i in range(num_releases):
            if search_count_by_release(MagPapersYearCount.Index.name, releases[i]) > 0:
                continue
            year, counts = self._get_paper_year_count(releases[i])

            for j in range(len(year)):
                paper_count = MagPapersYearCount(release=releases[i].isoformat(), year=year[j], count=counts[j])
                docs.append(paper_count)

        if len(docs) > 0:
            bulk_index(docs)

    def _get_paper_year_count(self, release: datetime.date) -> Tuple[List[int], List[int]]:
        """ Get paper counts by year.
        @param release: Relevant release to get data for.
        @return: Tuple of year and count information.
        """

        ts = release.strftime('%Y%m%d')
        table_id = f'{MagAnalyser.PAPERS_TABLE_ID}{ts}'
        sql = self._tpl_group_count.render(
            project_id=self._project_id, dataset_id=self._dataset_id, table_id=table_id,
            column='Year', where='Year IS NOT NULL'
        )
        df = pd.read_gbq(sql, project_id=self._project_id)
        return df['Year'].to_list(), df['count'].to_list()


class MagAnalyser(DataQualityAnalyser):
    """
    Perform data quality analysis on a Microsoft Academic Graph release, and save the results to ElasticSearch and
    BigQuery (maybe).
    """

    CACHE_RELEASES = 'releases'
    PAPERS_TABLE_ID = 'Papers'
    FOS_TABLE_ID = 'FieldsOfStudy'
    ARG_MODULES = 'modules'

    def __init__(self, project_id: str ='academic-observatory-dev', dataset_id:str='mag',
                 modules: Union[None, List[MagAnalyserModule]] = None):
        self._project_id = project_id
        self._dataset_id = dataset_id
        self._end_date = datetime.datetime.now(timezone.utc)
        self._cache = AutoFetchCache()

        self._tpl_env = Environment(loader=PackageLoader('observatory_platform', 'database/workflows/sql'))
        self._tpl_releases = self._tpl_env.get_template('select_table_suffixes.sql.jinja2')
        self._cache.set_fetcher(MagAnalyser.CACHE_RELEASES, lambda _: self._get_releases())
        self._modules = self._load_modules(modules)

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
            fosl0 = FieldsOfStudyLevel0Module(self._project_id, self._dataset_id, self._cache)
            paper_metrics = PaperMetricsModule(self._project_id, self._dataset_id, self._cache)
            paper_years_count = PaperYearsCountModule(self._project_id, self._dataset_id, self._cache)

            mods[fosl0.name()] = fosl0
            mods[paper_metrics.name()] = paper_metrics
            mods[paper_years_count.name()] = paper_years_count

        return mods

    def run(self, **kwargs):
        """ Entry point for the analyser.

        @param kwargs: Optional arguments.
        modules=list() a list of module names to run in order.
        """

        modules = self._modules

        if MagAnalyser.ARG_MODULES in kwargs:
            modules = self._load_modules(kwargs[MagAnalyser.ARG_MODULES])

        for module in modules.values():
            module.run()

    def _get_releases(self) -> List[datetime.date]:
        """ Get the list of MAG releases from BigQuery.
        @return: List of MAG release dates sorted in ascending order.
        """

        sql = self._tpl_releases.render(project_id=self._project_id, dataset_id=self._dataset_id,
                                        table_id=MagAnalyser.FOS_TABLE_ID, end_date=self._end_date)
        rel_list = list(reversed(pd.read_gbq(sql, project_id=self._project_id)['suffix']))
        releases = [release.date() for release in rel_list]
        logging.info(f'Found {len(releases)} MAG releases in BigQuery.')
        return releases
