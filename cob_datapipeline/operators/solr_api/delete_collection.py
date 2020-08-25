# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
This module holds classes associated to the deletion of Solr collections.
"""

from airflow.utils.decorators import apply_defaults
from cob_datapipeline.models import ListVariable
from cob_datapipeline.operators import SolrApiBaseOperator,\
        BatchMixin,\
        ListVariableMixin

class DeleteCollection(SolrApiBaseOperator):
    """
    This operator is used to delete a specified Solr collection.

    This operator has the following features:
    * It will log the response by default.
    * It will throw an exception if collection is not deleted (by_default).
    * It will apply an on_success method if given.
    * It will apply an on_failure method if given.
    * It will rescue from failure if rescue_failure param is True (False by default)
    * It will skip a collection if it is in skip_included list.
    * It will skip a collection if it matches skip_matching matcher.
    * It keeps track of collections it has processed.
    * It will fail if a blank skip_included value is provided.

    :param solr_conn_id: The id for the Solr connection (required).
    :type solr_conn_id: str

    :param name: The name of the Solr collection to delete (required).
    :type name: str

    :param skip_included: A list of collection names to skip deleting (optional).
    :type skip_included: [str]

    :param skip_matching: A regex string to skip matching collection (optional).
    :type skip_matching: str

    :param rescue_failure: Continue if any collection deletion fails (False by default).
    :type rescue_failure: bool

    :param on_success: Function to run on collection value on delete success (optional).
    :type on_success: function(str)

    :param on_failure: Function to run on collection value on delete failure (optional).
    :type on_failure: function(str, response)
    """
    template_fields = ['data', 'name']

    @apply_defaults
    def __init__(self, name, *args, **kwargs):

        data = {'action': 'DELETE', 'name': name}
        super().__init__(data=data, *args, **kwargs)


class DeleteCollectionBatch(BatchMixin, DeleteCollection):
    """
    This operator is used to iterate over a batch of collections and delete them.

    This operator has the following features:
    * It will iterate over all collections by default (even on error).

    :param solr_conn_id: The id for the Solr connection (required).
    :type solr_conn_id: str

    :param collections: A list of collection names to be deleted (required).
    :type collections: [str]

    :param skip_included: A list of collection names to skip deleting (optional).
    :type skip_included: [str]

    :param skip_matching A regex string to skip matching collection (optional).
    :type skip_matching: str

    :param skip_from_last: Do not iterate past this count from last (optional).
    :type list_variable: int

    :param rescue_failure: Continue if any collection deletion fails (True by default).
    :type rescue_failure: bool

    :param on_success: Function to run on collection value on delete success (optional).
    :type on_success: function(str)

    :param on_failure: Function to run on collection value on delete failure (optional).
    :type on_failure: function(str, response)
    """
    template_fields = ['names']

    @apply_defaults
    def __init__(
            self,
            collections,
            *args, **kwargs):

        super().__init__(*args, **kwargs)
        self.names = collections


class DeleteCollectionListVariable(ListVariableMixin, DeleteCollectionBatch):
    """
    This operator is used to iterate over a collection saved in a list variable.

    This operator has the following features:
    * It will iterate over all collections by default (even on error).
    * Once complete it resets the list variable per configuration.

    :param solr_conn_id: The id for the Solr connection (required).
    :type solr_conn_id: str

    :param list_variable: The name of an airflow list variable to use (required)
    :type list_variable: str

    :param skip_included: A list of collection names to skip deleting (optional).
    :type skip_included: [str]

    :param skip_matching A regex string to skip matching collection (optional).
    :type skip_matching: str

    :param skip_from_last: Do not iterate past this count from last (1 by default)
    :type skip_from_last: int

    :param rescue_failure: Continue if any collection deletion fails (True by default).
    :type rescue_failure: bool

    :param on_success: Function to run on collection value on delete success (optional)
    :type on_success: fuction(str)

    :param on_failure: Function to run on collection value on delete failure (optional)
    :type on_failure: function(str, response)

    :param ignore_matching_failtures: A regex string used to ignore matching error messages \
            in list_variable context. ('Could not find collection' by default)
    :type ignore_matching_failtures: str
    """
    template_fields = ['list_variable']

    @apply_defaults
    def __init__(
            self,
            list_variable,
            *args,
            ignore_matching_failtures='Could not find collection',
            **kwargs):

        collections = ListVariable.get(list_variable)
        super().__init__(
            list_variable=list_variable,
            collections=collections,
            ignore_matching_failtures=ignore_matching_failtures,
            *args, **kwargs)
