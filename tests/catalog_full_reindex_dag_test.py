"""Unit Tests for the TUL Cob Catalog Full Reindex DAG."""
import os
import unittest
from unittest.mock import patch
import requests
import requests_mock
import airflow
from cob_datapipeline.catalog_full_reindex_dag import DAG,\
        CATALOG_PRE_PRODUCTION_HARVEST_FROM_DATE, \
        split_list
        
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



CATALOG_INDEXING_MULTIPLIER = 3
class TestSplitListFunction(unittest.TestCase):

    def test_split_list_equal_chunks(self):
        a_list = [1, 2, 3, 4, 5, 6]
        chunk_size = 2
        expected_output = [[1, 2], [3, 4], [5, 6]]
        result = list(split_list(a_list, chunk_size))
        self.assertEqual(result, expected_output)

    def test_split_list_smaller_chunk(self):
        a_list = [1, 2, 3, 4, 5]
        chunk_size = 2
        expected_output = [[1, 2], [3, 4], [5]]
        result = list(split_list(a_list, chunk_size))
        self.assertEqual(result, expected_output)

    def test_split_list_large_chunk(self):
        a_list = [1, 2, 3]
        chunk_size = 5
        expected_output = [[1, 2, 3]]
        result = list(split_list(a_list, chunk_size))
        self.assertEqual(result, expected_output)

    def test_split_list_empty(self):
        a_list = []
        chunk_size = 2
        expected_output = []
        result = list(split_list(a_list, chunk_size))
        self.assertEqual(result, expected_output)
if __name__ == '__main__':
    unittest.main()

