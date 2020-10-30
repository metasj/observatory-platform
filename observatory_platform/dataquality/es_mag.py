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


class FieldsOfStudyLevel0(InnerDoc):
    field_ids = Long(required=True, multi=True)
    field_ids_unchanged = Boolean(required=True)
    normalized_names = Text(required=True, multi=True)
    normalized_names_unchanged = Boolean(required=True)
    paper_counts = Long(required=True, multi=True)
    citation_counts = Long(required=True, multi=True)
    delta_ppaper = Double(multi=True)
    delta_pcitations = Double(multi=True)
    js_dist_paper = Double(multi=True)
    js_dist_citation = Double(multi=True)


class MagReleaseEs(Document):
    ''' Base class for Microsoft Academic Graph release data quality analytics. '''

    release = Date(required=True, default_timezone='UTC')
    fields_of_study_level0 = Object(FieldsOfStudyLevel0)

    class Index:
        name = 'dataquality-mag'
        settings = {
            'number_of_shards': 2,
            'number_of_replicas': 0
        }

    @classmethod
    def _matches(cls, hits):
        '''
        MagReleaseDQ is an abstract class, make sure it never gets used for deserialization.
        @param hits: unused.
        '''

        return False

    def add_fields_of_study(self, field_ids, normalized_names, paper_counts, citation_counts, created=None,
                            commit=True):
        self.fields_of_study = FieldsOfStudy(
            field_ids=field_ids, normalized_names=normalized_names, paper_counts=paper_counts,
            citation_counts=citation_counts, created=created or datetime.now())
        if commit:
            self.save()
        return self.fields_of_study

    def save(self, **kwargs):
        # if there is no date, use now
        if self.release is None:
            self.release = datetime.now()
        return super(MagReleaseEs, self).save(**kwargs)
