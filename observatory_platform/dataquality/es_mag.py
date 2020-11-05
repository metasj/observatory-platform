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

class MagPapersYearCount(Document):
    """ Number of papers in MAG for a given year. """

    release = Date(required=True, default_timezone='UTC')
    year = Long(required=True)
    count = Long(required=True)

    class Index:
        name = 'dataquality-mag-papers-year'
        settings = {
            'number_of_shards': 2,
            'number_of_replicas': 0
        }

    @classmethod
    def _matches(cls, _):
        """ MagPapersYearCount is an abstract class, make sure it never gets used for deserialization. """

        return False

    def save(self, **kwargs):
        """ Indexes the document in elastic search. """

        if self.release is None:
            self.release = datetime.now()
        return super(MagPapersYearCount, self).save(**kwargs)


class MagPapersMetrics(Document):
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
        settings = {
            'number_of_shards': 2,
            'number_of_replicas': 0
        }

    @classmethod
    def _matches(cls, _):
        """MagPapersMetrics is an abstract class, make sure it never gets used for deserialization. """

        return False

    def save(self, **kwargs):
        """ Indexes the document in elastic search. """

        if self.release is None:
            self.release = datetime.now()
        return super(MagPapersMetrics, self).save(**kwargs)

class MagFosL0Metrics(Document):
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
        settings = {
            'number_of_shards': 2,
            'number_of_replicas': 0
        }

    @classmethod
    def _matches(cls, _):
        """ MagFosL0Metrics is an abstract class, make sure it never gets used for deserialization. """

        return False

    def save(self, **kwargs):
        """ Indexes the document in elastic search. """

        if self.release is None:
            self.release = datetime.now()
        return super(MagFosL0Metrics, self).save(**kwargs)


class MagFosL0Counts(Document):
    """ Aggregate counts and proportions per field of study from the FieldsOfStudy dataset. """

    release = Date(required=True, default_timezone='UTC')
    field_id = Long(required=True)
    normalized_name = Text(required=True)
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

    @classmethod
    def _matches(cls, _):
        """ MagFosL0Counts is an abstract class, make sure it never gets used for deserialization. """

        return False

    def save(self, **kwargs):
        """ Indexes the document in elastic search. """

        if self.release is None:
            self.release = datetime.now()
        return super(MagFosL0Counts, self).save(**kwargs)

