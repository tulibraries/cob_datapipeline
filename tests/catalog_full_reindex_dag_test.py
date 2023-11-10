"""Unit Tests for the TUL Cob Catalog Full Reindex DAG."""
import os
import unittest
from unittest.mock import patch
import requests
import requests_mock
import airflow
from cob_datapipeline.catalog_full_reindex_dag import DAG, CATALOG_PRE_PRODUCTION_HARVEST_FROM_DATE
from tests.helpers import get_connection

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
            "list_alma_s3_data",
            "list_boundwith_s3_data",
            "prepare_boundwiths",
            "prepare_alma_data",
            "create_collection",
            "push_collection",
            "delete_collections",
            "get_num_solr_docs_pre",
            "index_sftp_marc",
            "solr_commit",
            "update_variables",
            "get_num_solr_docs_post",
            "slack_success_post",
            "get_num_solr_docs_current_prod",
            ])

    def test_dag_task_order(self):
        """Unit test that the DAG instance contains the expected dependencies."""
        expected_task_deps = {
            "set_s3_namespace": ["safety_check", "verify_prod_collection"],
            "list_alma_s3_data": ["set_s3_namespace"],
            "list_boundwith_s3_data": ["set_s3_namespace"],
            "prepare_boundwiths": ["list_boundwith_s3_data"],
            "prepare_alma_data": ["list_alma_s3_data", "prepare_boundwiths"],
            "create_collection": ["prepare_alma_data", "prepare_boundwiths"],
            "push_collection": ["create_collection"],
            "delete_collections": ["push_collection"],
            "get_num_solr_docs_pre": ["delete_collections"],
            "index_sftp_marc": ["get_num_solr_docs_pre"],
            "solr_commit": ["index_sftp_marc"],
            "update_variables": ["solr_commit"],
            "get_num_solr_docs_post": ["update_variables"],
            "slack_success_post": ["get_num_solr_docs_post"],
            "get_num_solr_docs_current_prod": [],
        }
        for task, upstream_tasks in expected_task_deps.items():
            upstream_list = [up_task.task_id for up_task in DAG.get_task(task).upstream_list]
            self.assertCountEqual(upstream_tasks, upstream_list)

    def test_index_sftp_marc(self):
        """Test that we index sftp files"""
        airflow_home = airflow.models.Variable.get("AIRFLOW_HOME")
        task = DAG.get_task("index_sftp_marc")
        expected_bash_path = airflow_home + "/dags/cob_datapipeline/scripts/ingest_marc.sh "
        self.assertEqual(task.env["HOME"], os.getcwd())
        self.assertEqual(task.bash_command, expected_bash_path)

    @requests_mock.mock()
    def test_verify_prod_collection_task(self, mock_request):

        mock_request.get(
            'https://testhost/okcomputer/solr/foo',
            status_code=200,
            text='OK')

        with DAG, patch('airflow.hooks.base.BaseHook.get_connection',
                   side_effect=get_connection):

            DAG.get_task("verify_prod_collection").execute(None)


    @requests_mock.mock()
    def test_verify_prod_collection_task_with_fail(self, mock_request):

        mock_request.get(
            'https://testhost/okcomputer/solr/foo',
            status_code=400,
            reason='Not Found',
            text='Boo')

        with DAG, patch('airflow.hooks.base.BaseHook.get_connection',
                side_effect=get_connection), self.assertRaises(airflow.exceptions.AirflowException):

            DAG.get_task("verify_prod_collection").execute(None)

    def test_we_calculate_correct_harvest_from_date(self):
        self.assertEqual(CATALOG_PRE_PRODUCTION_HARVEST_FROM_DATE, "2020-06-07T00:00:00Z" )
