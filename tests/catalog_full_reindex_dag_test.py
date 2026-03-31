"""Unit Tests for the TUL Cob Catalog Full Reindex DAG."""
import unittest
import requests_mock
import airflow
import uuid

from datetime import datetime, timezone
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models import TaskInstance as TI
from airflow.models.dagrun import DagRun
from airflow.settings import Session
from airflow.utils.state import DagRunState, State
from airflow.utils.types import DagRunType
from unittest.mock import patch
from tests.helpers import get_connection
from cob_datapipeline.catalog_full_reindex_dag import DAG,\
        CATALOG_PRE_PRODUCTION_HARVEST_FROM_DATE, \
        split_list
        

class TestCatalogFullReindexDag(unittest.TestCase):
    """Unit Tests for solrcloud catalog full reindex dag file."""

    def setUp(self):
        """Method to set up the DAG Class instance for testing."""
        self.tasks = list(map(lambda t: t.task_id, DAG.tasks))

    def test_dag_loads(self):
        """Unit test that the DAG identifier is set correctly."""
        self.assertEqual(DAG.dag_id, "catalog_full_reindex")


    def test_dag_tasks_present(self):
        """Unit test that the DAG instance contains the expected tasks."""
        self.assertEqual(self.tasks, [
            "safety_check",
            "verify_prod_collection",
            "set_s3_namespace",
            "list_alma_s3_data.get_list",
            "list_alma_s3_data.split_list",
            "list_boundwith_s3_data",
            "prepare_boundwiths",
            "prepare_alma_data.prepare_alma_data_0",
            "prepare_alma_data.prepare_alma_data_1",
            "prepare_alma_data.prepare_alma_data_2",
            "create_collection",
            "push_collection",
            "delete_collections",
            "get_num_solr_docs_pre",
            "list_s3_marc_files.get_list",
            "list_s3_marc_files.split_list",
            "index_sftp_marc.index_sftp_marc_0",
            "index_sftp_marc.index_sftp_marc_1",
            "index_sftp_marc.index_sftp_marc_2",
            "solr_commit",
            "update_variables",
            "get_num_solr_docs_post",
            "slack_success_post",
            ])

    def test_dag_task_order(self):
        """Unit test that the DAG instance contains the expected dependencies."""
        expected_task_deps = {
            "set_s3_namespace": ["safety_check", "verify_prod_collection"],
            "list_alma_s3_data.get_list": ["set_s3_namespace"],
            "list_alma_s3_data.split_list": ["list_alma_s3_data.get_list"],
            "list_boundwith_s3_data": ["set_s3_namespace"],
            "prepare_boundwiths": ["list_boundwith_s3_data"],
            "prepare_alma_data.prepare_alma_data_0": [
                "list_alma_s3_data.split_list", "prepare_boundwiths"
                ],
            "prepare_alma_data.prepare_alma_data_1": [
                "list_alma_s3_data.split_list", "prepare_boundwiths"
                ],
            "prepare_alma_data.prepare_alma_data_2": [
                "list_alma_s3_data.split_list", "prepare_boundwiths"
                ],
            "create_collection": [
                "prepare_alma_data.prepare_alma_data_0",
                "prepare_alma_data.prepare_alma_data_1",
                "prepare_alma_data.prepare_alma_data_2",
                "prepare_boundwiths"],
            "push_collection": ["create_collection"],
            "delete_collections": ["push_collection"],
            "get_num_solr_docs_pre": ["delete_collections"],
            "list_s3_marc_files.get_list": ["get_num_solr_docs_pre"],
            "list_s3_marc_files.split_list": ["list_s3_marc_files.get_list"],
            "index_sftp_marc.index_sftp_marc_0": ["list_s3_marc_files.split_list"],
            "index_sftp_marc.index_sftp_marc_1": ["list_s3_marc_files.split_list"],
            "index_sftp_marc.index_sftp_marc_2": ["list_s3_marc_files.split_list"],
            "solr_commit": [
                "index_sftp_marc.index_sftp_marc_0",
                "index_sftp_marc.index_sftp_marc_1",
                "index_sftp_marc.index_sftp_marc_2",
                ],
            "update_variables": ["solr_commit"],
            "get_num_solr_docs_post": ["update_variables"],
            "slack_success_post": ["get_num_solr_docs_post"],
        }

        for task, upstream_tasks in expected_task_deps.items():
            upstream_list = [up_task.task_id for up_task in DAG.get_task(task).upstream_list]
            upstream_list.sort()
            upstream_tasks.sort()
            self.assertEqual(upstream_tasks, upstream_list)

    def _render_task(self, task_id):
        session = Session()
        run_id = f"{task_id}_{uuid.uuid4()}"
        logical_date = datetime.now(timezone.utc)
        session.add(DagRun(
            dag_id=DAG.dag_id,
            run_id=run_id,
            logical_date=logical_date,
            run_after=logical_date,
            state=DagRunState.RUNNING,
            run_type=DagRunType.MANUAL,
        ))
        session.commit()

        task = DAG.get_task(task_id)
        task_instance = TI(
            task=task,
            run_id=run_id,
            dag_version_id=None,
            state=State.SUCCESS
        )
        task.render_template_fields(task_instance.get_template_context())
        return task

    def test_delete_collections_is_runtime_wrapper(self):
        task = DAG.get_task("delete_collections")
        self.assertIsInstance(task, PythonOperator)
        self.assertEqual(task.python_callable.__name__, "delete_collections_runtime")

    def test_verify_prod_collection_endpoint_renders(self):
        task = self._render_task("verify_prod_collection")
        self.assertEqual(task.endpoint, "/okcomputer/solr/foo")

    def test_create_collection_name_renders(self):
        task = self._render_task("create_collection")
        self.assertEqual(task.op_kwargs["collection"], "FOO")

    def test_solr_commit_endpoint_renders(self):
        task = self._render_task("solr_commit")
        self.assertEqual(task.endpoint, "/solr/FOO/update?commit=true")

    @requests_mock.mock()
    def test_verify_prod_collection_task(self, mock_request):

        mock_request.get(
            'https://testhost/okcomputer/solr/foo',
            status_code=200,
            text='OK')

        with DAG, patch('airflow.sdk.bases.hook.BaseHook.get_connection',
                   side_effect=get_connection):

            self._render_task("verify_prod_collection").execute(None)


    @requests_mock.mock()
    def test_verify_prod_collection_task_with_fail(self, mock_request):

        mock_request.get(
            'https://testhost/okcomputer/solr/foo',
            status_code=400,
            reason='Not Found',
            text='Boo')

        with DAG, patch('airflow.sdk.bases.hook.BaseHook.get_connection',
                side_effect=get_connection), self.assertRaises(airflow.exceptions.AirflowException):

            self._render_task("verify_prod_collection").execute(None)

    def test_we_calculate_correct_harvest_from_date(self):
        self.assertEqual(CATALOG_PRE_PRODUCTION_HARVEST_FROM_DATE, "2020-06-07T00:00:00Z" )



CATALOG_INDEXING_MULTIPLIER = 3
class TestSplitListFunction(unittest.TestCase):

    def test_split_list_equal_chunks(self):
        a_list = [1, 2, 3, 4, 5, 6]
        groups_count = 2
        expected_output = [[1, 2, 3], [4, 5, 6]]
        result = list(split_list(a_list, groups_count))
        self.assertEqual(result, expected_output)

    def test_split_list_smaller_chunk(self):
        a_list = [1, 2, 3, 4, 5]
        groups_count = 2
        expected_output = [[1, 2, 3], [4, 5]]
        result = list(split_list(a_list, groups_count))
        self.assertEqual(result, expected_output)

    def test_split_list_large_chunk(self):
        a_list = [1, 2, 3]
        groups_count = 5
        expected_output = [[1], [2], [3], [], []]
        result = list(split_list(a_list, groups_count))
        self.assertEqual(result, expected_output)

    def test_split_list_one_chunk(self):
        a_list = [1, 2, 3]
        groups_count = 1
        expected_output = [[1, 2, 3]]
        result = list(split_list(a_list, groups_count))
        self.assertEqual(result, expected_output)

    def test_split_list_zero_chunk(self):
        a_list = [1, 2, 3]
        groups_count = 0
        expected_output = []
        result = list(split_list(a_list, groups_count))
        self.assertEqual(result, expected_output)

    def test_split_list_empty(self):
        a_list = []
        groups_count = 2
        expected_output = [[], []]
        result = list(split_list(a_list, groups_count))
        self.assertEqual(result, expected_output)
if __name__ == '__main__':
    unittest.main()
