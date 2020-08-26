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
This module contains the PushVariable airflow operator.
"""
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from cob_datapipeline.models import ListVariable

class PushVariable(BaseOperator):
    """
    This operator is used to push a value onto an airflow list variable.  The
    operator has the following other features:

    * If the variable does not already exist, then it's created.
    * If the variable value is empty then it gets reset to []
    * Skips if the value being pushed is blank (by default).
    * Only adds value if it's unique (by default)

    :param name: The name of the airflow variable to push into.
    :type name: str

    :param value: The value to push into the airflow variable.
    :type value: str

    :param skip_blank: Whether to skip pushing blank values (True by default).
    :type value: bool

    :param unique: Whether to only add unique values (True by default).
    :type value: bool
    """

    template_fields = ['value']

    @apply_defaults
    def __init__(
            self,
            name,
            *args,
            value=None,
            skip_blank=True,
            unique=True,
            **kwargs):

        super().__init__(*args, **kwargs)
        self.name = name
        self.value = value
        self.skip_blank = skip_blank
        self.unique = unique


    def execute(self, context=None):
        ListVariable.push(
            self.name, self.value,
            skip_blank=self.skip_blank,
            unique=self.unique)
