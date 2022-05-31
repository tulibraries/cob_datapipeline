# pylint: disable=missing-docstring,line-too-long

import unittest
from unittest.mock import patch
import requests_mock
from airflow.models import DAG
from airflow.models.taskinstance import TaskInstance as TI
from airflow.models.renderedtifields import RenderedTaskInstanceFields as RTIF
from airflow.utils.state import State
from airflow import settings
from cob_datapipeline.models import ListVariable
from cob_datapipeline.operators import DeleteAlias,\
        DeleteAliasBatch,\
        DeleteAliasListVariable
from tests.helpers import get_connection
from tests.helpers import DEFAULT_DATE

class DeleteAliasTest(unittest.TestCase):

    def test_correct_data(self):
        """
        Most of the functionality for this class is taken care of by the
        SolrApiBaseOperator class. So this is mostly a  check to make sure we
        set the required argument for the parent class.
        """
        task = DeleteAlias(
            task_id='test_task',
            name='foo',
            solr_conn_id='solr_conn_id')

        self.assertEqual(task.data, {'name': 'foo', 'action': 'DELETEALIAS'})

class DeleteAliasBatchTest(unittest.TestCase):

    def test_correct_init(self):
        """
        Most of the functionality for this class is taken care of by the
        BatchMixin mixin. So this is mostly a  check to make sure we
        intialize the task as expected.
        """

        task = DeleteAliasBatch(
            task_id='test_task',
            aliases=['foo', 'bar'],
            skip_from_last=0,
            solr_conn_id='solr_conn_id')

        self.assertEqual(task.names, ['foo', 'bar'])
        self.assertEqual(task.skip_from_last, 0)
        self.assertTrue(task.rescue_failure)
        self.assertEqual(task.data, {'name': None, 'action': 'DELETEALIAS'})

class DeleteAliasListVariableTest(unittest.TestCase):

    def test_correct_init(self):
        """
        Most of the functionality for this class is taken care of by the
        ListVariableMixin mixin. So this is mostly a  check to make sure we
        intialize the task as expected.
        """
        ListVariable.set('foo', ['fe', 'fi', 'fo'])
        task = DeleteAliasListVariable(
            task_id='test_task',
            list_variable='foo',
            solr_conn_id='solr_conn_id')

        self.assertEqual(task.names, ['fe', 'fi', 'fo'])
        self.assertEqual(task.skip_from_last, 1)
        self.assertTrue(task.rescue_failure)
        self.assertEqual(task.data, {'name': None, 'action': 'DELETEALIAS'})


    @requests_mock.mock()
    def test_execute_smoke(self, mock_request):
        mock_request.get(
            'https://testhost/solr/admin/collections?action=DELETEALIAS&name=fool',
            status_code=200,
            text='{\n  "responseHeader":{\n    "status":0,\n    "QTime":38}}\n',
            reason='OK')


        with patch('airflow.hooks.base_hook.BaseHook.get_connection',
                   side_effect=get_connection):
            aliases = ['fool']
            ListVariable.set('fool', aliases)

            DeleteAliasListVariable(
                task_id='test_task',
                list_variable='fool',
                skip_from_last=0,
                solr_conn_id='solr_conn_id').execute()

            self.assertEqual(ListVariable.get('fool'), [])

    def test_execute_with_existing_templated_value(self):
        session = settings.Session()
        dag = DAG(dag_id='test_dag_2', start_date=DEFAULT_DATE)
        with dag:
            dag_run = dag.create_dagrun(
                    run_id="test_execute_with_existing_templated_value", state=State.SUCCESS,
                    execution_date=DEFAULT_DATE, start_date=DEFAULT_DATE,
                    session=session,
                    )
            task = DeleteAliasListVariable(
                task_id='test_task',
                list_variable='{{task_instance.task_id}}',
                solr_conn_id='solr_conn_id',
                dag=dag)
            task_instance = TI(task=task, run_id=dag_run.run_id, state=State.SUCCESS)
            rendered_ti_fields = RTIF(ti=task_instance)

            self.assertEqual(rendered_ti_fields.rendered_fields.get('list_variable'), 'test_task')
