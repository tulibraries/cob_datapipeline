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
This module holds the ListVariable class and it's methods.
"""
import logging

from json.decoder import JSONDecodeError
from airflow.utils.db import provide_session
from airflow.models import Variable

log = logging.getLogger(__name__)

class ListVariable(Variable):
    """
    Airflow Variable wrapper.

    Makes it easier to share assumptions about Airflow variables that are
    lists.
    """
    __NO_DEFAULT_SENTINEL = object()

    @classmethod
    @provide_session
    def get(cls,
            key,  # type: str
            default_var=__NO_DEFAULT_SENTINEL,  # type: Any
            deserialize_json=False,  # type: bool
            session=None):
        try:
            list_var = super().get(
                key,
                default_var=[],
                deserialize_json=True,
                session=session)

        except JSONDecodeError:
            list_var = super().get(
                key,
                default_var=default_var,
                deserialize_json=deserialize_json,
                session=session)

        if list_var  in ['', None, 'None']:
            return []

        return list_var

    @classmethod
    @provide_session
    def set(cls,
            key,
            value,  # type: Any
            serialize_json=True,  # type: bool
            session=None):
        super().set(
            key,
            value,
            serialize_json=serialize_json,
            session=session)

    @classmethod
    @provide_session
    def push(cls,
             key,
             value,
             session=None,
             skip_blank=False,
             unique=False):
        # pylint: disable=unused-argument,too-many-arguments
        """
        Get the list variable, push a value into it, then reset it.
        """
        if value in [None, ''] and skip_blank:
            log.info('Skipping empty value push.')
            return

        list_var = cls.get(key)
        if (value not in list_var) or not unique:
            list_var.append(value)

            cls.set(key, list_var)
