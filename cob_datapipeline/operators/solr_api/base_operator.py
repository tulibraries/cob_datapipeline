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
This module holds classes associated to the Solr Collections API
"""

from json import JSONDecodeError
from re import match
from typing import Dict, Optional, Sequence
from airflow.models import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.utils.decorators import apply_defaults
from cob_datapipeline.exceptions import SafetyCheckException
from cob_datapipeline.models import ListVariable

def get_failed_reason(response):
    """
    Extract the failure reason from JSON from the Solr API response.

    :param  response: The request response from Solr API.
    :type response: requests.models.Response
    """
    try:
        return response.json()['error']['msg']
    except (JSONDecodeError, KeyError):
        return f'{response.status_code}:{response.reason}'

def _safety_check(skip_included):
    """
    Checks that if skip_included is set, then it does not include a blank
    value.  This is an extra safety measure against the possiblity of meaning
    to skip deleting something but not configuring it properly.
    """
    if skip_included:
        for name in skip_included:
            if name in ['None', None, '']:
                raise SafetyCheckException("skip_included MUST NOT contain any blank values.")
        return skip_included
    return []

class SolrApiBaseOperator(BaseOperator):
    # pylint: disable=too-many-instance-attributes
    """
    This defines a basic operator class to be used by specific Solr API implementors.

    This operator has the following features:
    * It will log the response by default.
    * It will throw an exception if request fails to proces (by_default).
    * It will apply an on_success method if given.
    * It will apply an on_failure method if given.
    * It will rescue from failure if rescue_failure param is True (False by default)
    * It will skip a named endpoint data if name is in skip_included list.
    * It will skip a named endpoint data if it matches skip_matching matcher.
    * It keeps track of items it has processed.

    :param solr_conn_id: The id for the Solr connection (required).
    :type solr_conn_id: str

    :param data: The Solr API expected from the implementer (required)
    :type data: dic

    :param skip_included: A list of data names to skip deleting (optional).
    :type skip_included: [str]

    :param skip_matching: A regex string to skip matching data name (optional).
    :type skip_matching: str

    :param rescue_failure: Continue if endpoint fails (False by default).
    :type rescue_failure: bool

    :param on_success: Function to run on data['name'] value on action success (optional).
    :type on_success: function(data)

    :param on_failure: Function to run on data['name'] value on action failure (optional).
    :type on_failure: function(str, response)
    """

    template_fields: Sequence[str] = ('data', 'name', 'skip_included', 'skip_matching')

    @apply_defaults
    def __init__(
            self,
            *,
            solr_conn_id: str,
            data: Dict,
            name="",
            log_response: bool = True,
            skip_included: Sequence[str] = None,
            skip_matching: Sequence[str] = None,
            rescue_failure=False,
            on_success: bool = None,
            on_failure: Optional = None,
            **kwargs):

        super().__init__(**kwargs)
        self.solr_conn_id = solr_conn_id
        self.log_response = log_response
        self.name = name
        self.skip_included = _safety_check(skip_included)
        self.skip_matching = skip_matching
        self.rescue_failure = rescue_failure
        self.on_success = on_success
        self.on_failure = on_failure
        self.processed_items = []
        self.path = '/solr/admin/collections'
        self.data = data
        if data:
            self.name = data.get('name', None)

    def execute(self, context=None):
        """
         We want to be able  to use this method in a batch context. Therefore, we need
         to be able to both make the collection name required and be able to
         change it dynamically.
        """
        if context:
            self.name = context.get('name', self.name)
            self.data['name'] = self.name

        if self.name in self.skip_included:
            self.log.info('Skipping processing %s because it is included in %s',
                          self.name, self.skip_included)
            self.processed_items.append(
                {'name': self.name, 'status': 'SKIPPED', 'reason': 'skip_included'})
            return

        if self.skip_matching and match(self.skip_matching, self.name):
            self.log.info('Skipping processing %s because it matches "%s"',
                          self.name, self.skip_matching)
            self.processed_items.append(
                {'name': self.name, 'status': 'SKIPPED', 'reason': 'skip_matching'})
            return

        self.log.info('Attempting to process %s item.', self.name)
        http = HttpHook('GET', http_conn_id=self.solr_conn_id)
        response = http.run(
            self.path, data=self.data,
            extra_options={'check_response': False})

        if self.log_response:
            self.log.info(response.text)

        if response.ok:
            # Order important, on_success MUST come after http.run().
            self._on_success(self.name)
            self.processed_items.append({'name': self.name, 'status': 'OK'})

        else:
            self._on_failure(self.name, response)
            self.processed_items.append(
                {'name': self.name,
                 'status': 'FAILED',
                 'reason': get_failed_reason(response)})

        if not (self.rescue_failure or self.on_failure):
            response.raise_for_status()


    def _on_success(self, name):
        if self.on_success:
            self.on_success(name)

    def _on_failure(self, name, response):
        if self.on_failure:
            self.on_failure(name, response)


class BatchMixin(BaseOperator):
    """
    This mixin is intended to add batch processing for operators that can
    be executed with name=name execute method signature.  The mixing assumes
    that the attribute self.names has been configured by the class that
    incorporates it.
    """
    @apply_defaults
    def __init__(
            self,
            *,
            name: str = None,
            skip_from_last: int = 1,
            rescue_failure: bool = True, **kwargs):

        super().__init__(name=name, **kwargs)
        self.skip_from_last = skip_from_last
        self.rescue_failure=rescue_failure
        self.name = name

    def execute(self, context=None):
        count_to_last = len(self.names)
        for name in self.names:
            if count_to_last <= self.skip_from_last:
                self.log.info('Skipping the last %s items: %s',
                              self.skip_from_last,
                              name)
                self.processed_items.append(
                    {'name': name, 'status': 'SKIPPED', 'reason': 'skip_from_last'})
                continue

            super().execute({'name': name})
            count_to_last = count_to_last - 1


class ListVariableMixin(BaseOperator):
    """
    This mixin is intended to allow  solr api operators to use a ListVariable
    in place of a list of things and then to update the list variable once all
    the items have been processed.
    """
    @apply_defaults
    def __init__(
            self,
            *,
            list_variable,
            rescue_failure=True,
            ignore_matching_failtures=None,
            **kwargs):

        super().__init__(**kwargs)
        self.list_variable = list_variable
        self.rescue_failure = rescue_failure
        self.ignore_matching_failtures = ignore_matching_failtures

    def execute(self, context=None):
        super().execute(context)
        self.reset_list_variable()

    def reset_list_variable(self):
        """
        Resets configured list_variable value to be only non processed values.
        """
        ListVariable.set(self.list_variable, self._new_list_variable())

    def _new_list_variable(self):
        skipped = []
        failed = []
        for item in self.processed_items:
            if item['status'] == 'SKIPPED':
                skipped.append(item['name'])

            if self._is_failed(item):
                failed.append(item['name'])

        return failed + skipped

    def _is_failed(self, item):
        ignore_failure = False
        failed = item['status'] == 'FAILED'

        if failed and self.ignore_matching_failtures:
            ignore_failure = bool(match(self.ignore_matching_failtures, item['reason']))

        return item['status'] == 'FAILED' and not ignore_failure
