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
This module holds classes associated to the deletion of Solr collection alias.
"""

from cob_datapipeline.models import ListVariable
from cob_datapipeline.operators import SolrApiBaseOperator,\
        BatchMixin,\
        ListVariableMixin


class DeleteAlias(SolrApiBaseOperator):
    # pylint: disable=too-many-instance-attributes
    """
    This operator is used to delete a specified Solr collection.

    This operator has the following features:
    * It will log the response by default.
    * It will throw an exception if collection alias is not deleted (by_default).
    * It will apply an on_success method if given.
    * It will apply an on_failure method if given.
    * It will rescue from failure if rescue_failure param is True (False by default)
    * It will skip a collection if it is in skip_included list.
    * It will skip a collection if it matches skip_matching matcher.
    * It keeps track of collections it has processed.

    :param solr_conn_id: The id for the Solr connection (required).
    :type solr_conn_id: str

    :param name: The name of the Solr collection alias to delete (required).
    :type name: str

    :param skip_included: A list of collection alias names to skip deleting (optional).
    :type skip_included: [str]

    :param skip_matching: A regex string to skip matching collection alias (optional).
    :type skip_matching: str

    :param rescue_failure: Continue if any collection alias deletion fails (False by default).
    :type rescue_failure: bool

    :param on_success: Function to run on collection alias name on delete success (optional).
    :type on_success: function(str)

    :param on_failure: Function to run on collection alias name on delete failure (optional).
    :type on_failure: function(str, response)
    """
    template_fields = ['data', 'name']

    def __init__(self, name: str, **kwargs):

        data = {'action': 'DELETEALIAS', 'name': name}
        super().__init__(data=data, **kwargs)


class DeleteAliasBatch(BatchMixin, DeleteAlias):
    """
    This operator is used to iterate over a batch of aliases and delete them.

    This operator has the following features:
    * It will iterate over all aliases by default (even on error).

    :param solr_conn_id: The id for the Solr connection (required).
    :type solr_conn_id: str

    :param aliases: A list of collection alias names to be deleted (required).
    :type aliases: [str]

    :param skip_included: A list of collection alias names to skip deleting (optional).
    :type skip_included: [str]

    :param skip_matching A regex string to skip matching collection alias (optional).
    :type skip_matching: str

    :param skip_from_last: Do not iterate past this count from last (optional).
    :type skip_from_last: int

    :param rescue_failure: Continue if any collection alias deletion fails (True by default).
    :type rescue_failure: bool

    :param on_success: Function to run on collection alias value on delete success (optional).
    :type on_success: function(str)

    :param on_failure: Function to run on collection alias value on delete failure (optional).
    :type on_failure: function(str, response)
    """
    template_fields = ['names']

    def __init__(
            self,
            *,
            aliases: list[str],
            **kwargs):

        super().__init__(**kwargs)
        self.names = aliases

class DeleteAliasListVariable(ListVariableMixin, DeleteAliasBatch):
    # pylint: disable=too-many-ancestors
    """
    This operator is used to iterate over a collection alias saved in a list variable.

    This operator has the following features:
    * It will iterate over all collection aliases by default (even on error).
    * Once complete it resets the list variable per configuration.

    :param solr_conn_id: The id for the Solr connection (required).
    :type solr_conn_id: str

    :param list_variable: The name of an airflow list variable to use (required)
    :type list_variable: str

    :param skip_included: A list of collection names to skip deleting (optional).
    :type skip_included: [str]

    :param skip_matching A regex string to skip matching collection (optional).
    :type skip_matching: str

    :param skip_from_last: Do not iterate past this count from last (optional)
    :type skip_from_last: int

    :param rescue_failure: Continue if any collection deletion fails (True by default).
    :type rescue_failure: bool

    :param on_success: Function to run on collection alias value on delete success (optional)
    :type on_success: fuction(str)

    :param on_failure: Function to run on collection alias value on delete failure (optional)
    :type on_failure: function(str, response)

    :param ignore_matching_failtures: A regex string used to ignore matching error messages \
            in list_variable context. (None by default)
    :type ignore_matching_failtures: str
    """
    template_fields = ['list_variable']

    def __init__(
            self,
            *,
            list_variable: str,
            **kwargs):

        aliases = ListVariable.get(list_variable)
        super().__init__(aliases=aliases, list_variable=list_variable, **kwargs)
