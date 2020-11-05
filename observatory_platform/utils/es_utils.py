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

from elasticsearch_dsl import Search, Index, Document, connections
from elasticsearch import NotFoundError
from elasticsearch_dsl.document import IndexMeta
from elasticsearch.helpers import bulk

from typing import Union, List


def init_doc(es_doc: IndexMeta):
    """ Initialise the mappings in elastic search if the document doesn't exist. Will not reinitialise if a mapping
    already exists.

    @param es_doc: Document to initialise.
    """

    try:
        Search(index=es_doc.Index.name).count()
    except NotFoundError:
        es_doc.init()


def get_or_init_doc_count(es_doc: IndexMeta) -> int:
    """ Get the number of documents stored in elastic search.  If the index doesn't exist, create it.
    @param es_doc: IndexMeta class for the document we want to check.
    @return: Number of documents currently stored in elastic search.
    """

    try:
        # Query number of documents in elastic search.
        count = Search(index=es_doc.Index.name).count()
    except NotFoundError:
        # No documents found. (Re)-initialise index mappings in case it is the first run.
        count = 0
        es_doc.init()

    return count


def clear_index(index: str):
    """ Remove all documents from an index.
    @param index: Index name to delete.
    """

    Search(index=index).query().delete()


def delete_index(index: str):
    """ Delete an index from elastic search.
    @param index: Index name to delete.
    """

    Index(index).delete()


def search_count_by_release(index: str, release: str):
    """ Get the number of documents found in an index for a release.
    @param index: Document index name.
    @param release: Release date.
    @return: Number of documents found.
    """

    return Search(index=index).query('match', release=release).count()

def search_by_release(index: str, release: str, sort_field: Union[None, str] = None):
    """ Search elastic search index for a particular release document.
    @param index: Document index name.
    @param release: Release date.
    @param sort_field: Field to sort by or None if not sorting.
    @return: List of documents found.
    """

    s = Search(index=index).query('match', release=release)
    if sort_field:
        s = s.sort(sort_field)
    return list(s.scan())

def bulk_index(docs: List[Document]):
    """ Perform a batched index on a list of documents in elastic search.
    @param docs: List of documents to index.
    """

    bulk(connections.get_connection(), (doc.to_dict(include_meta=True) for doc in docs), refresh=True)