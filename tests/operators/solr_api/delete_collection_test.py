# pylint: disable=missing-docstring,line-too-long

import unittest
from unittest.mock import patch
import logging

import requests
import requests_mock

from airflow.models import DAG
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance as TI
from airflow.models.renderedtifields import RenderedTaskInstanceFields as RTIF
from airflow.settings import Session
from airflow.utils.state import DagRunState, State
from airflow.utils.types import DagRunType

from cob_datapipeline.models import ListVariable
from cob_datapipeline.operators import DeleteCollection,\
        DeleteCollectionBatch, DeleteCollectionListVariable
from tests.helpers import get_connection, DEFAULT_DATE


def logged_messages(mock_info):
    messages = []
    for call in mock_info.call_args_list:
        if not call.args:
            continue

        message = call.args[0]
        if len(call.args) > 1:
            try:
                message = message % call.args[1:]
            except TypeError:
                message = " ".join(str(arg) for arg in call.args)

        messages.append(str(message))

    return "\n".join(messages)

class DeleteCollectionTest(unittest.TestCase):
    """
    Most of the functionality for DeleteCollection is managed by the
    SolrApiBaseOperator these test really act as a proxy test for that class.
    """
    def setUp(self):
        session = requests.Session()
        adapter = requests_mock.Adapter()
        session.mount('mock', adapter)
        self.delete_collection_logger = DeleteCollection(
            task_id='log_probe',
            name='foo',
            solr_conn_id='solr_conn_id').log.name

    @requests_mock.mock()
    def test_some_default_logs(self, mock_request):
        mock_request.get(
            'https://testhost/solr/admin/collections?action=DELETE&name=foo',
            status_code=200,
            text='{"status":{"status": 200}}',
            reason='OK')

        with patch(
                'airflow.sdk.bases.hook.BaseHook.get_connection',
                side_effect=get_connection):
            task = DeleteCollection(
                task_id='test_task',
                name='foo',
                solr_conn_id='solr_conn_id')
            with patch.object(task.log, 'info') as mock_info:
                task.execute()

        logged = logged_messages(mock_info)
        self.assertIn('Attempting to process foo item.', logged)
        self.assertIn('{"status":{"status": 200}}', logged)


    @requests_mock.mock()
    def test_execute_delete_successful(self, mock_request):
        mock_request.get(
            'https://testhost/solr/admin/collections?action=DELETE&name=foo',
            status_code=200,
            text='{"status":{"status": 200}}',
            reason='OK')
        with patch(
                'airflow.sdk.bases.hook.BaseHook.get_connection',
                side_effect=get_connection):
            task = DeleteCollection(
                task_id='test_task',
                name='foo',
                solr_conn_id='solr_conn_id')
            with patch.object(task.log, 'info') as mock_info:
                task.execute()
            self.assertEqual(task.processed_items, [{'name': 'foo', 'status': 'OK'}])
        self.assertIn('{"status":{"status": 200}}', logged_messages(mock_info))

    @requests_mock.mock()
    def test_execute_delete_successful_with_on_success(self, mock_request):
        mock_request.get(
            'https://testhost/solr/admin/collections?action=DELETE&name=foo',
            status_code=200,
            text='{"status":{"status": 200}}',
            reason='OK')
        with self.assertLogs() as log, patch(
                'airflow.sdk.bases.hook.BaseHook.get_connection',
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
                'airflow.sdk.bases.hook.BaseHook.get_connection',
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
                'airflow.sdk.bases.hook.BaseHook.get_connection',
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
        with patch(
                'airflow.sdk.bases.hook.BaseHook.get_connection',
                side_effect=get_connection
                ):
            task = DeleteCollection(
                task_id='test_task',
                name='foo',
                solr_conn_id='solr_conn_id',
                skip_included=['foo'])
            with patch.object(task.log, 'info') as mock_info:
                task.execute()
            self.assertEqual(task.processed_items,
                             [{'name': 'foo', 'status': 'SKIPPED', 'reason': 'skip_included'}])

        logged = logged_messages(mock_info)
        self.assertIn("Skipping processing foo because it is included in ['foo']", logged)
        self.assertNotIn('Attempting to delete foo collection.', logged)

    def test_execute_skip_matching(self):
        with patch(
                'airflow.sdk.bases.hook.BaseHook.get_connection',
                side_effect=get_connection
                ):
            task = DeleteCollection(
                task_id='test_task',
                name='foo',
                solr_conn_id='solr_conn_id',
                skip_matching='fo.*')
            with patch.object(task.log, 'info') as mock_info:
                task.execute()
            self.assertEqual(task.processed_items,
                             [{'name': 'foo', 'status': 'SKIPPED', 'reason': 'skip_matching'}])

        logged = logged_messages(mock_info)
        self.assertIn('Skipping processing foo because it matches "fo.*"', logged)
        self.assertNotIn('Attempting to delete foo collection.', logged)



class DeleteCollectionBatchTest(unittest.TestCase):
    def setUp(self):
        session = requests.Session()
        adapter = requests_mock.Adapter()
        session.mount('mock', adapter)
        self.delete_collection_batch_logger = DeleteCollectionBatch(
            task_id='log_probe',
            collections=['foo'],
            solr_conn_id='solr_conn_id').log.name

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

        with patch(
                'airflow.sdk.bases.hook.BaseHook.get_connection',
                side_effect=get_connection):
            collections = ['foo', 'bar']
            task = DeleteCollectionBatch(
                task_id='test_task',
                collections=collections,
                skip_from_last=0,
                solr_conn_id='solr_conn_id')
            with patch.object(task.log, 'info') as mock_info:
                task.execute()

        logged = logged_messages(mock_info)
        self.assertIn('Attempting to process foo item.', logged)
        self.assertIn('{ "collection": "foo" }', logged)
        self.assertIn('Attempting to process bar item.', logged)
        self.assertIn('{ "collection": "bar" }', logged)

    @requests_mock.mock()
    def test_execute_with_multiple_collections_and_skip_included(self, mock_request):
        mock_request.get(
            'https://testhost/solr/admin/collections?action=DELETE&name=foo',
            status_code=200,
            text='{ "collection": "foo" }',
            reason='OK')

        with patch(
                'airflow.sdk.bases.hook.BaseHook.get_connection',
                side_effect=get_connection):
            collections = ['foo', 'bar']
            task = DeleteCollectionBatch(
                task_id='test_task',
                collections=collections,
                solr_conn_id='solr_conn_id',
                skip_included=['bar'])
            with patch.object(task.log, 'info') as mock_info:
                task.execute()

        logged = logged_messages(mock_info)
        self.assertIn('Attempting to process foo item.', logged)
        self.assertIn('{ "collection": "foo" }', logged)
        self.assertNotIn('Attempting to process bar item.', logged)
        self.assertNotIn('{ "collection": "bar" }', logged)

    @requests_mock.mock()
    def test_execute_with_multiple_collections_and_skip_matching(self, mock_request):
        mock_request.get(
            'https://testhost/solr/admin/collections?action=DELETE&name=foo',
            status_code=200,
            text='{ "collection": "foo" }',
            reason='OK')

        with patch(
                'airflow.sdk.bases.hook.BaseHook.get_connection',
                side_effect=get_connection):
            collections = ['foo', 'bar']
            task = DeleteCollectionBatch(
                task_id='test_task',
                collections=collections,
                skip_from_last=0,
                solr_conn_id='solr_conn_id',
                skip_matching='ba.*')
            with patch.object(task.log, 'info') as mock_info:
                task.execute()

        logged = logged_messages(mock_info)
        self.assertIn('Attempting to process foo item.', logged)
        self.assertIn('{ "collection": "foo" }', logged)
        self.assertNotIn('Attempting to process bar item.', logged)
        self.assertNotIn('{ "collection": "bar" }', logged)
        self.assertIn('Skipping processing bar because it matches "ba.*"', logged)

    @requests_mock.mock()
    def test_execute_with_multiple_collections_and_skip_from_last(self, mock_request):
        mock_request.get(
            'https://testhost/solr/admin/collections?action=DELETE&name=foo',
            status_code=200,
            text='{ "collection": "foo" }',
            reason='OK')

        with patch(
                'airflow.sdk.bases.hook.BaseHook.get_connection',
                side_effect=get_connection):
            collections = ['foo', 'bar']
            task = DeleteCollectionBatch(
                task_id='test_task',
                collections=collections,
                solr_conn_id='solr_conn_id',
                skip_from_last=1)
            with patch.object(task.log, 'info') as mock_info:
                task.execute()
            self.assertIn({'name': 'bar', 'status': 'SKIPPED', 'reason': 'skip_from_last'},
                          task.processed_items)

        logged = logged_messages(mock_info)
        self.assertIn('Attempting to process foo item.', logged)
        self.assertIn('{ "collection": "foo" }', logged)
        self.assertNotIn('Attempting to process bar item.', logged)
        self.assertNotIn('{ "collection": "bar" }', logged)
        self.assertIn('Skipping the last 1 items: bar', logged)

class DeleteCollectionListVariableTest(unittest.TestCase):
    def setUp(self):
        session = requests.Session()
        adapter = requests_mock.Adapter()
        session.mount('mock', adapter)
        ListVariable.delete('foo')
        self.delete_collection_list_logger = DeleteCollectionListVariable(
            task_id='log_probe',
            list_variable='foo',
            solr_conn_id='solr_conn_id').log.name


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

        with patch(
                'airflow.sdk.bases.hook.BaseHook.get_connection',
                side_effect=get_connection):
            collections = ['foo', 'bar']
            ListVariable.set('foo', collections)

            task = DeleteCollectionListVariable(
                task_id='test_task',
                list_variable='foo',
                skip_from_last=0,
                solr_conn_id='solr_conn_id')
            with patch.object(task.log, 'info') as mock_info:
                task.execute()

        logged = logged_messages(mock_info)
        self.assertIn('Attempting to process foo item.', logged)
        self.assertIn('{ "collection": "foo" }', logged)
        self.assertIn('Attempting to process bar item.', logged)
        self.assertIn('{ "collection": "bar" }', logged)

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


        with patch('airflow.sdk.bases.hook.BaseHook.get_connection',
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
        dag = DAG(dag_id='test_dag_3', start_date=DEFAULT_DATE)
        run_id = "test_execute_with_existing_templated_value"
        session = Session()
        session.add(DagRun(
            dag_id=dag.dag_id,
            run_id=run_id,
            logical_date=DEFAULT_DATE,
            run_after=DEFAULT_DATE,
            state=DagRunState.RUNNING,
            run_type=DagRunType.MANUAL,
        ))
        session.commit()
        task = DeleteCollectionListVariable(
            task_id='test_task',
            list_variable='{{task_instance.task_id}}',
            solr_conn_id='solr_conn_id',
            dag=dag)
        task_instance = TI(task=task, run_id=run_id, dag_version_id=None, state=State.SUCCESS)
        rendered_ti_fields = RTIF(ti=task_instance)

        self.assertEqual(rendered_ti_fields.rendered_fields.get('list_variable'), 'test_task')
