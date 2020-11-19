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


# Author: Aniek Roelofs

"""
The Directory of Open Access Books (DOAB) is a directory of open-access peer reviewed scholarly books.
Its aim is to increase discoverability of books. Academic publishers provide metadata of their Open Access books to DOAB.
It is available both through OAI-PMH harvesting and as a csv file.
"""

import csv
import json
import jsonlines
import logging
import os
import pathlib
import pendulum
import xmltodict
from datetime import datetime
from pendulum.parsing.exceptions import ParserError
from types import SimpleNamespace
from typing import Tuple, Union
from airflow.exceptions import AirflowException
from requests.exceptions import RetryError

from observatory_platform.utils.config_utils import (AirflowVar,
                                                     SubFolder)
from observatory_platform.utils.telescope_stream import (StreamTelescope, StreamRelease)
from observatory_platform.utils.url_utils import retry_session, get_ao_user_agent
from observatory_platform.utils.telescope_utils import convert

#TODO
# isbn have '-' sometimes, so not an integer, leave in?


class OapenMetadataRelease(StreamRelease):
    @property
    def csv_path(self) -> str:
        return self.get_path(SubFolder.downloaded, 'oapen_metadata', 'csv')


def create_release(start_date: pendulum.Pendulum, end_date: pendulum.Pendulum, telescope: SimpleNamespace,
                   first_release: bool = False) -> OapenMetadataRelease:
    """ Create a release based on the start and end date.

    :param start_date: Start date of this run
    :param end_date: End date of this run
    :param telescope: Contains telescope properties
    :param first_release: Whether this is the first release to be obtained
    :return: Release instance
    """
    release = OapenMetadataRelease(start_date, end_date, telescope, first_release)
    return release


def download(release: OapenMetadataRelease) -> bool:
    """ Download DOAB metadata from a csv file.

    :param release: Release instance
    :return: True if download is successful
    """
    logging.info('Downloading csv')
    headers = {
        'User-Agent': f'{get_ao_user_agent()}'
    }
    response = retry_session().get(release.telescope.csv_url, headers=headers)
    if response.status_code == 200:
        with open(release.csv_path, 'w') as f:
            f.write(response.content.decode('utf-8'))
        logging.info('Download csv successful')
        return True
    else:
        raise AirflowException(f'Download csv unsuccessful, {response.text}')


def transform(release):
    """ For each row, combine the OAI-PMH and csv results into one json object.
    The column names are structure are slightly changed to make it readable by BigQuery.

    :param release: Release instance
    :return: None
    """
    with open(release.csv_path, 'r') as f:
        csv_entries = [{k: v for k, v in row.items()} for row in csv.DictReader(f, skipinitialspace=True)]

    # restructure entries dict
    nested_fields = set([key.rsplit('.', 1)[0] for key in csv_entries[0].keys() if len(key.rsplit('.', 1)) > 1])
    list_fields = {'oapen.grant.number', 'oapen.grant.acronym', 'oapen.relation.hasChapter_dc.title',
                   'dc.subject.classification', 'oapen.grant.program', 'oapen.redirect', 'oapen.imprint',
                   'dc.subject.other', 'oapen.notes', 'dc.title', 'collection', 'dc.relation.ispartofseries',
                   'BITSTREAM Download URL', 'BITSTREAM ISBN', 'oapen.collection', 'dc.contributor.author',
                   'oapen.remark.public', 'oapen.relation.isPublisherOf', 'dc.contributor.other',
                   'dc.contributor.editor', 'dc.relation.isreplacedbydouble', 'dc.date.issued',
                   'dc.description.abstract', 'BITSTREAM Webshop URL', 'dc.type', 'dc.identifier.isbn',
                   'dc.description.provenance', 'oapen.grant.project', 'oapen.relation.isbn', 'dc.identifier',
                   'dc.date.accessioned', 'BITSTREAM License', 'oapen.relation.isFundedBy_grantor.name',
                   'dc.relation.isnodouble', 'dc.language'}
    entries = change_keys(csv_entries, convert, nested_fields, list_fields)

    # write out the transformed data
    with open(release.transform_path, 'w') as json_out:
        with jsonlines.Writer(json_out) as writer:
            for entry in entries:
                writer.write_all([entry])


class OapenMetadataTelescope(StreamTelescope):
    telescope = SimpleNamespace(dag_id='oapen_metadata', dataset_id='oapen', main_table_id='metadata',
                                schedule_interval='@weekly', start_date=datetime(2018, 5, 14),
                                partition_table_id='metadata_partitions', description='the description',
                                queue='default', max_retries=3, bq_merge_days=0,
                                merge_partition_field='id',
                                updated_date_field='dc.date.available',
                                download_ext='csv', extract_ext='csv', transform_ext='jsonl',
                                airflow_vars=[AirflowVar.data_path.get(), AirflowVar.project_id.get(),
                                              AirflowVar.data_location.get(),
                                              AirflowVar.download_bucket_name.get(),
                                              AirflowVar.transform_bucket_name.get()],
                                airflow_conns=[], schema_version='',
                                csv_url='https://library.oapen.org/download-export?format=csv')

    # optional
    create_release_custom = create_release

    # required
    download_custom = download
    transform_custom = transform


def change_keys(obj, convert, nested_fields, list_fields):
    """ Recursively goes through the dictionary obj, replaces keys with the convert function, adds info from the csv
    file based on the ISBN and other small modifications.

    :param obj: object, can be of any type
    :param convert: convert function
    :return: Updated dictionary
    """
    if isinstance(obj, (str, int, float)):
        return obj
    if isinstance(obj, dict):
        new = obj.__class__()
        for k, v in list(obj.items()):
            if v == '':
                continue
            if k in list_fields:
                if k == 'BITSTREAM ISBN':
                    v = v.replace('-', '')
                if k == 'dc.subject.other':
                    v = v.replace(';', '||')
                    v = v.replace(',', '||')
                v = list(set([x.strip() for x in v.split('||')]))
                if k == 'dc.date.issued':
                    v = [pendulum.parse(date).to_date_string() for date in v]
                if k == 'dc.subject.classification':
                    # Get classification code for custom added column
                    classification_code = []
                    # tmp_list = []
                    for c in v:
                        if c.startswith('bic Book Industry Communication::'):
                            code = c.split('::')[-1].split(' ')[0]
                            classification_code.append(code)
                            # c = c[len('bic Book Industry Communication::'):]
                            # c_list = c.split('::')
                            # tmp_list += c_list
                        else:
                            classification_code.append(c)
                    classification_code = list(set(classification_code))
                    #         tmp_list.append(c)
                    # v = list(set(tmp_list))
            if k in nested_fields:
                k = k + '.value'
            if k.rsplit('.', 1)[0] in nested_fields:
                fields = k.split('.')
                tmp = new
                for key in fields:
                    key = convert(key)
                    try:
                        tmp[key]
                    except KeyError:
                        if key == fields[-1]:
                            tmp[key] = change_keys(v, convert, nested_fields, list_fields)
                            if key == 'classification':
                                # Add classification code column
                                tmp['classification_code'] = change_keys(classification_code, convert, nested_fields,
                                                                         list_fields)
                        else:
                            tmp[key] = {}
                    tmp = tmp[key]
            else:
                new[convert(k)] = change_keys(v, convert, nested_fields, list_fields)
    elif isinstance(obj, (list, set, tuple)):
        new = obj.__class__(change_keys(v, convert, nested_fields, list_fields) for v in obj)
    else:
        return obj
    return new

# test = [key.split('.')[0:-1] for key in obj.keys()]
# sorted(set([tuple(key.split('.')[0:-1]) for key in obj.keys()]),key=len)
# fields = set([key.rsplit('.',1)[0] for key in obj.keys()])
