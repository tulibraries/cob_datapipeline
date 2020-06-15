"""Unit Tests for the TUL Cob Catalog Full Reindex DAG."""
import os
import unittest
import airflow
from cob_datapipeline.catalog_full_reindex_dag import DAG

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
            "set_s3_namespace",
            "list_alma_s3_data",
            "list_boundwith_s3_data",
            "prepare_boundwiths",
            "prepare_alma_data",
            "create_collection",
            "get_num_solr_docs_pre",
            "index_sftp_marc",
            "solr_commit",
            "get_num_solr_docs_post",
            "get_num_solr_docs_current_prod",
            "slack_post_succ"
            ])

    def test_dag_task_order(self):
        """Unit test that the DAG instance contains the expected dependencies."""
        expected_task_deps = {
            "set_s3_namespace": ["safety_check"],
            "list_alma_s3_data": ["set_s3_namespace"],
            "list_boundwith_s3_data": ["set_s3_namespace"],
            "prepare_boundwiths": ["list_boundwith_s3_data"],
            "prepare_alma_data": ["list_alma_s3_data"],
            "create_collection": ["prepare_alma_data", "prepare_boundwiths"],
            "get_num_solr_docs_pre": ["create_collection"],
            "index_sftp_marc": ["get_num_solr_docs_pre"],
            "solr_commit": ["index_sftp_marc"],
            "get_num_solr_docs_post": ["solr_commit"],
            "get_num_solr_docs_current_prod": [],
            "slack_post_succ": ["get_num_solr_docs_post", "get_num_solr_docs_current_prod"],
        }
        for task, upstream_tasks in expected_task_deps.items():
            upstream_list = [up_task.task_id for up_task in DAG.get_task(task).upstream_list]
            self.assertCountEqual(upstream_tasks, upstream_list)

    def test_index_sftp_marc(self):
        """Test that we index sftp files"""
        airflow_home = airflow.models.Variable.get("AIRFLOW_HOME")
        task = DAG.get_task("index_sftp_marc")
        expected_bash_path = airflow_home + "/dags/cob_datapipeline/scripts/sc_ingest_marc.sh "
        self.assertEqual(task.env["HOME"], os.getcwd())
        self.assertEqual(task.bash_command, expected_bash_path)
