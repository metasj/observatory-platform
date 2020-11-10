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

from datetime import datetime
from elasticsearch_dsl import (
    Boolean,
    Date,
    Document,
    InnerDoc,
    Join,
    Keyword,
    Long,
    Nested,
    Object,
    Text,
    connections,
    Float,
    Double,
    Integer,
)

class MagDocIndexSettings:
    """ Generic settings the documents can use. """
    settings = {
        'number_of_shards': 2,
        'number_of_replicas': 0
    }

class NSDocument(Document):
    """ Abstract Document. Prevent serialization. Cut down on boiler plate in the class definitions. """

    @classmethod
    def _matches(cls, _):
        """ Abstract class. Do not deserialize. """

        return False


class MagPapersFieldYearCount(NSDocument):
    """ Number of papers per field per year. """

    release = Date(required=True, default_timezone='UTC')
    field_name = Keyword(required=True)
    field_id = Long(required=True)
    year = Long(required=True)
    count = Long(required=True)

    class Index:
        name = 'dataquality-mag-papers-field-year'
        settings = MagDocIndexSettings.settings


class MagPapersYearCount(NSDocument):
    """ Number of papers in MAG for a given year. """

    release = Date(required=True, default_timezone='UTC')
    year = Long(required=True)
    count = Long(required=True)

    class Index:
        name = 'dataquality-mag-papers-year'
        settings = MagDocIndexSettings.settings


class MagPapersMetrics(NSDocument):
    """ Some aggregate metrics for the Papers dataset in MAG. """

    release = Date(required=True, default_timezone='UTC')
    total = Long(required=True)
    null_year = Long(required=True)
    null_doi = Long(required=True)
    null_doctype = Long(required=True)
    null_familyid = Long(required=True)
    pnull_year = Double(required=True)
    pnull_doi = Double(required=True)
    pnull_doctype = Double(required=True)
    pnull_familyid = Double(required=True)

    class Index:
        name = 'dataquality-mag-papers-metrics'
        settings = MagDocIndexSettings.settings


class MagFosLevelCount(NSDocument):
    """ Number of Fields of study per level, and the number of documents at each level. """

    release = Date(required=True, default_timezone='UTC')
    level = Long(required=True)
    level_count = Long(required=True)
    num_papers = Long(required=True)
    num_citations = Long(required=True)

    class Index:
        name = 'dataquality-mag-fos-level-count'
        settings = MagDocIndexSettings.settings

# TODO
class MagAvgFosPaperYear(NSDocument):
    """ Calculate average number of field of study labels per paper per year.
    Sum up the number of field of study labels for a year, and divide by number of papers for that year.

     """


class MagFosL0Metrics(NSDocument):
    """ Level 0 Fields Of Study metrics on the relative subject labels on each paper. Note that a paper can have several
        labels.
    """

    release = Date(required=True, default_timezone='UTC')
    field_ids_unchanged = Boolean(required=True)
    normalized_names_unchanged = Boolean(required=True)

    js_dist_paper = Double()
    js_dist_citation = Double()

    class Index:
        name = 'dataquality-mag-fieldsofstudy-l0-metrics'
        settings = MagDocIndexSettings.settings


class MagFosL0Counts(NSDocument):
    """ Aggregate counts and proportions per field of study from the FieldsOfStudy dataset. """

    release = Date(required=True, default_timezone='UTC')
    field_id = Long(required=True)
    normalized_name = Keyword(required=True)
    paper_count = Long(required=True)
    citation_count = Long(required=True)
    delta_ppaper = Double(required=True)
    delta_pcitations = Double(required=True)

    class Index:
        name = 'dataquality-mag-fieldsofstudy-l0-counts'
        settings = {
            'number_of_shards': 2,
            'number_of_replicas': 0
        }


class MagDoiCountsDocType(NSDocument):
    """ Aggregate counts and proportions of documents with no DOI by document type. """

    release = Date(required=True, default_timezone='UTC')
    doc_type = Keyword(required=True)
    count = Long(required=True)
    no_doi = Long(required=True)
    pno_doi = Double(required=True)

    class Index:
        name = 'dataquality-mag-doi-counts'
        settings = MagDocIndexSettings.settings

class MagDoiCountsDocTypeYear(NSDocument):
    """ Aggregate doi counts by year. """
    release = Date(required=True, default_timezone='UTC')
    doc_type = Keyword(required=True)
    year = Long(required=True)
    count = Long(required=True)
    no_doi = Long(required=True)
    pno_doi = Double(required=True)

    class Index:
        name = 'dataquality-mag-doi-counts-year'
        settings = MagDocIndexSettings.settings