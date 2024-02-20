# pylint: disable=missing-docstring,line-too-long

import datetime
import unittest
from unittest.mock import patch
import logging

import requests
import requests_mock

from airflow import settings
from airflow.models import DAG
from airflow.models.taskinstance import TaskInstance as TI
from airflow.models.renderedtifields import RenderedTaskInstanceFields as RTIF
from airflow.utils.state import State

from cob_datapipeline.models import ListVariable
from cob_datapipeline.operators import DeleteCollection,\
        DeleteCollectionBatch, DeleteCollectionListVariable
from tests.helpers import get_connection, DEFAULT_DATE

class DeleteCollectionTest(unittest.TestCase):
    """
    Most of the functionality for DeleteCollection is managed by the
    SolrApiBaseOperator these test really act as a proxy test for that class.
    """
    def setUp(self):
        session = requests.Session()
        adapter = requests_mock.Adapter()
        session.mount('mock', adapter)

    @requests_mock.mock()
    def test_some_default_logs(self, mock_request):
        mock_request.get(
            'https://testhost/solr/admin/collections?action=DELETE&name=foo',
            status_code=200,
            text='{"status":{"status": 200}}',
            reason='OK')

        with self.assertLogs('airflow.task.operators') as log, patch(
                'airflow.hooks.base.BaseHook.get_connection',
                side_effect=get_connection):
            DeleteCollection(
                task_id='test_task',
                name='foo',
                solr_conn_id='solr_conn_id').execute()

        expected_log_message = 'INFO:airflow.task.operators.cob_datapipeline.operators.solr_api.delete_collection.DeleteCollection:Attempting to process foo item.'
        response_log = 'INFO:airflow.task.operators.cob_datapipeline.operators.solr_api.delete_collection.DeleteCollection:{"status":{"status": 200}}'

        self.assertIn(expected_log_message, log.output)
        self.assertIn(response_log, log.output)


    @requests_mock.mock()
    def test_execute_delete_successful(self, mock_request):
        mock_request.get(
            'https://testhost/solr/admin/collections?action=DELETE&name=foo',
            status_code=200,
            text='{"status":{"status": 200}}',
            reason='OK')
        with self.assertLogs('airflow.task.operators') as log, patch(
                'airflow.hooks.base.BaseHook.get_connection',
                side_effect=get_connection):
            task = DeleteCollection(
                task_id='test_task',
                name='foo',
                solr_conn_id='solr_conn_id')
            task.execute()
            self.assertEqual(task.processed_items, [{'name': 'foo', 'status': 'OK'}])
        msg = 'INFO:airflow.task.operators.cob_datapipeline.operators.solr_api.delete_collection.DeleteCollection:{"status":{"status": 200}}'
        self.assertIn(msg, log.output)

    @requests_mock.mock()
    def test_execute_delete_successful_with_on_success(self, mock_request):
        mock_request.get(
            'https://testhost/solr/admin/collections?action=DELETE&name=foo',
            status_code=200,
            text='{"status":{"status": 200}}',
            reason='OK')
        with self.assertLogs() as log, patch(
                'airflow.hooks.base.BaseHook.get_connection',
                side_effect=get_connection
                ):

            def on_success(name):
                logging.info('Yay %s!', name)

            DeleteCollection(
                task_id='test_task',
                name='foo',
                solr_conn_id='solr_conn_id',
                on_success=on_success).execute()
            self.assertIn('INFO:root:Yay foo!', log.output)


    @requests_mock.mock()
    def test_execute_delete_not_successful(self, mock_request):
        mock_request.get(
            'https://testhost/solr/admin/collections?action=DELETE&name=foo',
            status_code=400,
            text='{"status":{"status": 400}}',
            reason='FooBar')
        with patch(
                'airflow.hooks.base.BaseHook.get_connection',
                side_effect=get_connection
                ), self.assertRaises(requests.exceptions.HTTPError):
            task = DeleteCollection(
                task_id='test_task',
                name='foo',
                solr_conn_id='solr_conn_id')
            task.execute()
            self.assertEqual(task.processed_items,
                             {'name': 'foo', 'status': 'FAILED', 'reason': '400:FooBar'})

    @requests_mock.mock()
    def test_execute_delete_successful_with_on_failure(self, mock_request):
        mock_request.get(
            'https://testhost/solr/admin/collections?action=DELETE&name=foo',
            status_code=400,
            text='{"status":{"status": 400}}',
            reason='FAILED')
        with self.assertLogs() as log, patch(
                'airflow.hooks.base.BaseHook.get_connection',
                side_effect=get_connection
                ):

            def on_failure(name, _):
                logging.info('Boo %s!', name)

            DeleteCollection(
                task_id='test_task',
                name='foo',
                solr_conn_id='solr_conn_id',
                on_failure=on_failure).execute()
            self.assertIn('INFO:root:Boo foo!', log.output)

    def test_execute_skip_included(self):
        with self.assertLogs('airflow.task.operators') as log, patch(
                'airflow.hooks.base.BaseHook.get_connection',
                side_effect=get_connection
                ):
            task = DeleteCollection(
                task_id='test_task',
                name='foo',
                solr_conn_id='solr_conn_id',
                skip_included=['foo'])
            task.execute()
            self.assertEqual(task.processed_items,
                             [{'name': 'foo', 'status': 'SKIPPED', 'reason': 'skip_included'}])

        skip_log_message = "INFO:airflow.task.operators.cob_datapipeline.operators.solr_api.delete_collection.DeleteCollection:Skipping processing foo because it is included in ['foo']"
        self.assertIn(skip_log_message, log.output)

        log_message = 'INFO:airflow.task.operators:Attempting to delete foo collection.'
        self.assertNotIn(log_message, log.output)

    def test_execute_skip_matching(self):
        with self.assertLogs('airflow.task.operators') as log, patch(
                'airflow.hooks.base.BaseHook.get_connection',
                side_effect=get_connection
                ):
            task = DeleteCollection(
                task_id='test_task',
                name='foo',
                solr_conn_id='solr_conn_id',
                skip_matching='fo.*')
            task.execute()
            self.assertEqual(task.processed_items,
                             [{'name': 'foo', 'status': 'SKIPPED', 'reason': 'skip_matching'}])

        skip_log_message = 'INFO:airflow.task.operators.cob_datapipeline.operators.solr_api.delete_collection.DeleteCollection:Skipping processing foo because it matches "fo.*"'
        self.assertIn(skip_log_message, log.output)

        log_message = 'INFO:airflow.task.operators:Attempting to delete foo collection.'
        self.assertNotIn(log_message, log.output)



class DeleteCollectionBatchTest(unittest.TestCase):
    def setUp(self):
        session = requests.Session()
        adapter = requests_mock.Adapter()
        session.mount('mock', adapter)

    def test__init__(self):
        task = DeleteCollectionBatch(
            task_id='test_task',
            collections=['foo'],
            solr_conn_id='solr_conn_id')
        self.assertEqual(task.skip_from_last, 1)

    @requests_mock.mock()
    def test_execute_with_multiple_collections(self, mock_request):
        mock_request.get(
            'https://testhost/solr/admin/collections?action=DELETE&name=foo',
            status_code=200,
            text='{ "collection": "foo" }',
            reason='OK')

        mock_request.get(
            'https://testhost/solr/admin/collections?action=DELETE&name=bar',
            status_code=200,
            text='{ "collection": "bar" }',
            reason='OK')

        with self.assertLogs('airflow.task.operators') as log, patch(
                'airflow.hooks.base.BaseHook.get_connection',
                side_effect=get_connection):
            collections = ['foo', 'bar']
            DeleteCollectionBatch(
                task_id='test_task',
                collections=collections,
                skip_from_last=0,
                solr_conn_id='solr_conn_id').execute()

        log_message_foo = 'INFO:airflow.task.operators.cob_datapipeline.operators.solr_api.delete_collection.DeleteCollectionBatch:Attempting to process foo item.'
        self.assertIn(log_message_foo, log.output)

        response_log_foo = 'INFO:airflow.task.operators.cob_datapipeline.operators.solr_api.delete_collection.DeleteCollectionBatch:{ "collection": "foo" }'
        self.assertIn(response_log_foo, log.output)

        log_message_bar = 'INFO:airflow.task.operators.cob_datapipeline.operators.solr_api.delete_collection.DeleteCollectionBatch:Attempting to process bar item.'
        self.assertIn(log_message_bar, log.output)

        response_log_bar = 'INFO:airflow.task.operators.cob_datapipeline.operators.solr_api.delete_collection.DeleteCollectionBatch:{ "collection": "bar" }'
        self.assertIn(response_log_bar, log.output)

    @requests_mock.mock()
    def test_execute_with_multiple_collections_and_skip_included(self, mock_request):
        mock_request.get(
            'https://testhost/solr/admin/collections?action=DELETE&name=foo',
            status_code=200,
            text='{ "collection": "foo" }',
            reason='OK')

        with self.assertLogs('airflow.task.operators') as log, patch(
                'airflow.hooks.base.BaseHook.get_connection',
                side_effect=get_connection):
            collections = ['foo', 'bar']
            DeleteCollectionBatch(
                task_id='test_task',
                collections=collections,
                solr_conn_id='solr_conn_id',
                skip_included=['bar']).execute()

        log_message_foo = 'INFO:airflow.task.operators.cob_datapipeline.operators.solr_api.delete_collection.DeleteCollectionBatch:Attempting to process foo item.'
        self.assertIn(log_message_foo, log.output)

        response_log_foo = 'INFO:airflow.task.operators.cob_datapipeline.operators.solr_api.delete_collection.DeleteCollectionBatch:{ "collection": "foo" }'
        self.assertIn(response_log_foo, log.output)

        log_message_bar = 'INFO:airflow.task.operators.cob_datapipeline.operators.solr_api.delete_collection.DeleteCollectionBatch:Attempting to process bar item.'
        self.assertNotIn(log_message_bar, log.output)

        response_log_bar = 'INFO:airflow.task.operators.cob_datapipeline.operators.solr_api.delete_collection.DeleteCollectionBatch:{ "collection": "bar" }'
        self.assertNotIn(response_log_bar, log.output)

    @requests_mock.mock()
    def test_execute_with_multiple_collections_and_skip_matching(self, mock_request):
        mock_request.get(
            'https://testhost/solr/admin/collections?action=DELETE&name=foo',
            status_code=200,
            text='{ "collection": "foo" }',
            reason='OK')

        with self.assertLogs('airflow.task.operators') as log, patch(
                'airflow.hooks.base.BaseHook.get_connection',
                side_effect=get_connection):
            collections = ['foo', 'bar']
            DeleteCollectionBatch(
                task_id='test_task',
                collections=collections,
                skip_from_last=0,
                solr_conn_id='solr_conn_id',
                skip_matching='ba.*').execute()

        log_message_foo = 'INFO:airflow.task.operators.cob_datapipeline.operators.solr_api.delete_collection.DeleteCollectionBatch:Attempting to process foo item.'
        self.assertIn(log_message_foo, log.output)

        response_log_foo = 'INFO:airflow.task.operators.cob_datapipeline.operators.solr_api.delete_collection.DeleteCollectionBatch:{ "collection": "foo" }'
        self.assertIn(response_log_foo, log.output)

        log_message_bar = 'INFO:airflow.task.operators.cob_datapipeline.operators.solr_api.delete_collection.DeleteCollectionBatch:Attempting to process bar item.'
        self.assertNotIn(log_message_bar, log.output)

        response_log_bar = 'INFO:airflow.task.operators.cob_datapipeline.operators.solr_api.delete_collection.DeleteCollectionBatch:{ "collection": "bar" }'
        self.assertNotIn(response_log_bar, log.output)

        skip_log_message = 'INFO:airflow.task.operators.cob_datapipeline.operators.solr_api.delete_collection.DeleteCollectionBatch:Skipping processing bar because it matches "ba.*"'
        self.assertIn(skip_log_message, log.output)

    @requests_mock.mock()
    def test_execute_with_multiple_collections_and_skip_from_last(self, mock_request):
        mock_request.get(
            'https://testhost/solr/admin/collections?action=DELETE&name=foo',
            status_code=200,
            text='{ "collection": "foo" }',
            reason='OK')

        with self.assertLogs('airflow.task.operators') as log, patch(
                'airflow.hooks.base.BaseHook.get_connection',
                side_effect=get_connection):
            collections = ['foo', 'bar']
            task = DeleteCollectionBatch(
                task_id='test_task',
                collections=collections,
                solr_conn_id='solr_conn_id',
                skip_from_last=1)
            task.execute()
            self.assertIn({'name': 'bar', 'status': 'SKIPPED', 'reason': 'skip_from_last'},
                          task.processed_items)

        log_message_foo = 'INFO:airflow.task.operators.cob_datapipeline.operators.solr_api.delete_collection.DeleteCollectionBatch:Attempting to process foo item.'
        self.assertIn(log_message_foo, log.output)

        response_log_foo = 'INFO:airflow.task.operators.cob_datapipeline.operators.solr_api.delete_collection.DeleteCollectionBatch:{ "collection": "foo" }'
        self.assertIn(response_log_foo, log.output)

        log_message_bar = 'INFO:airflow.task.operators.cob_datapipeline.operators.solr_api.delete_collection.DeleteCollectionBatch:Attempting to process bar item.'
        self.assertNotIn(log_message_bar, log.output)

        response_log_bar = 'INFO:airflow.task.operators.cob_datapipeline.operators.solr_api.delete_collection.DeleteCollectionBatch:{ "collection": "bar" }'
        self.assertNotIn(response_log_bar, log.output)

        skip_log_message = 'INFO:airflow.task.operators.cob_datapipeline.operators.solr_api.delete_collection.DeleteCollectionBatch:Skipping the last 1 items: bar'
        self.assertIn(skip_log_message, log.output)

class DeleteCollectionListVariableTest(unittest.TestCase):
    def setUp(self):
        session = requests.Session()
        adapter = requests_mock.Adapter()
        session.mount('mock', adapter)
        ListVariable.delete('foo')


    @requests_mock.mock()
    def test_execute_with_multiple_collections(self, mock_request):
        mock_request.get(
            'https://testhost/solr/admin/collections?action=DELETE&name=foo',
            status_code=200,
            text='{ "collection": "foo" }',
            reason='OK')

        mock_request.get(
            'https://testhost/solr/admin/collections?action=DELETE&name=bar',
            status_code=200,
            text='{ "collection": "bar" }',
            reason='OK')

        with self.assertLogs('airflow.task.operators') as log, patch(
                'airflow.hooks.base.BaseHook.get_connection',
                side_effect=get_connection):
            collections = ['foo', 'bar']
            ListVariable.set('foo', collections)

            DeleteCollectionListVariable(
                task_id='test_task',
                list_variable='foo',
                skip_from_last=0,
                solr_conn_id='solr_conn_id').execute()

        log_message_foo = 'INFO:airflow.task.operators.cob_datapipeline.operators.solr_api.delete_collection.DeleteCollectionListVariable:Attempting to process foo item.'
        self.assertIn(log_message_foo, log.output)

        response_log_foo = 'INFO:airflow.task.operators.cob_datapipeline.operators.solr_api.delete_collection.DeleteCollectionListVariable:{ "collection": "foo" }'
        self.assertIn(response_log_foo, log.output)

        log_message_bar = 'INFO:airflow.task.operators.cob_datapipeline.operators.solr_api.delete_collection.DeleteCollectionListVariable:Attempting to process bar item.'
        self.assertIn(log_message_bar, log.output)

        response_log_bar = 'INFO:airflow.task.operators.cob_datapipeline.operators.solr_api.delete_collection.DeleteCollectionListVariable:{ "collection": "bar" }'
        self.assertIn(response_log_bar, log.output)

    def test_reset_list_variable_does_not_exist(self):
        DeleteCollectionListVariable(
            task_id='test_task',
            list_variable='foo',
            solr_conn_id='solr_conn_id').reset_list_variable()
        self.assertEqual(ListVariable.get('foo'), [])

    def test_reset_list_variable_default(self):
        ListVariable.set('foo', ['foo', 'bar'])
        task = DeleteCollectionListVariable(
            task_id='test_task',
            list_variable='foo',
            solr_conn_id='solr_conn_id')

        task.processed_items = [{'name': 'foo', 'status': 'OK'}]
        task.reset_list_variable()
        self.assertEqual(ListVariable.get('foo'), [])

    def test_reset_list_variable_with_mixed_skipped(self):
        task = DeleteCollectionListVariable(
            task_id='test_task',
            list_variable='foo',
            solr_conn_id='solr_conn_id',
            )
        task.processed_items = [
            {'name': 'buzz', 'status': 'OK'},
            {'name': 'foo', 'status': 'SKIPPED', 'reason': 'skip_included'},
            {'name': 'bar', 'status': 'SKIPPED', 'reason': 'skip_matching'},
            {'name': 'bizz', 'status': 'SKIPPED', 'reason': 'skip_from_last'},
            ]
        task.reset_list_variable()
        self.assertEqual(ListVariable.get('foo'), ['foo', 'bar', 'bizz'])

    def test_reset_list_variable_with_failed(self):
        task = DeleteCollectionListVariable(
            task_id='test_task',
            list_variable='foo',
            solr_conn_id='solr_conn_id',
            )
        task.processed_items = [
            {'name': 'buzz', 'status': 'OK'},
            {'name': 'foo', 'status': 'SKIPPED', 'reason': 'skip_included'},
            {'name': 'bar', 'status': 'SKIPPED', 'reason': 'skip_matching'},
            {'name': 'bizz', 'status': 'FAILED', 'reason': 'skip_from_last'},
            ]
        task.reset_list_variable()
        self.assertEqual(ListVariable.get('foo'), ['bizz', 'foo', 'bar'])

    @requests_mock.mock()
    def test_execute_ignore_could_not_find_collection(self, mock_request):
        mock_request.get(
            'https://testhost/solr/admin/collections?action=DELETE&name=foo',
            status_code=400,
            text='{\n  "responseHeader":{\n    "status":400,\n    "QTime":34},\n  "Operation delete caused exception:":"org.apache.solr.common.SolrException:org.apache.solr.common.SolrException: Could not find collection : foo",\n  "exception":{\n    "msg":"Could not find collection : foo",\n    "rspCode":400},\n  "error":{\n    "metadata":[\n      "error-class","org.apache.solr.common.SolrException",\n      "root-error-class","org.apache.solr.common.SolrException"],\n    "msg":"Could not find collection : foo",\n    "code":400}}\n',
            reason='Bad Request')


        with patch('airflow.hooks.base.BaseHook.get_connection',
                   side_effect=get_connection):
            collections = ['foo']
            ListVariable.set('foo', collections)

            DeleteCollectionListVariable(
                task_id='test_task',
                list_variable='foo',
                skip_from_last=0,
                solr_conn_id='solr_conn_id').execute()

            self.assertEqual(ListVariable.get('foo'), [])

    def test_execute_with_existing_templated_value(self):
        session = settings.Session()
        dag = DAG(dag_id='test_dag_3', start_date=DEFAULT_DATE)
        data_interval = (DEFAULT_DATE, DEFAULT_DATE + datetime.timedelta(days=1))
        with dag:
            dag_run = dag.create_dagrun(
                    run_id="test_execute_with_existing_templated_value", state=State.SUCCESS,
                    data_interval=data_interval,
                    execution_date=DEFAULT_DATE, start_date=DEFAULT_DATE,
                    session=session,
                    )
            task = DeleteCollectionListVariable(
                task_id='test_task',
                list_variable='{{task_instance.task_id}}',
                solr_conn_id='solr_conn_id',
                dag=dag)
            task_instance = TI(task=task, run_id=dag_run.run_id, state=State.SUCCESS)
            rendered_ti_fields = RTIF(ti=task_instance)

            self.assertEqual(rendered_ti_fields.rendered_fields.get('list_variable'), 'test_task')
