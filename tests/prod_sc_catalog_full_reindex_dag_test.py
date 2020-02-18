"""Unit Tests for the TUL Cob Catalog Full Reindex DAG."""
import os
import unittest
import airflow
from cob_datapipeline.prod_sc_catalog_full_reindex_dag import DAG

class TestCatalogFullReindexScDag(unittest.TestCase):
    """Unit Tests for solrcloud catalog full reindex dag file."""

    def setUp(self):
        """Method to set up the DAG Class instance for testing."""
        self.tasks = list(map(lambda t: t.task_id, DAG.tasks))

    def test_dag_loads(self):
        """Unit test that the DAG identifier is set correctly."""
        self.assertEqual(DAG.dag_id, "prod_sc_catalog_full_reindex")

    def test_dag_tasks_present(self):
        """Unit test that the DAG instance contains the expected tasks."""
        self.assertEqual(self.tasks, [
            "set_collection_name",
            "get_num_solr_docs_pre",
            "list_alma_s3_data",
            "prepare_boundwiths",
            "prepare_alma_data",
            "create_collection",
            "index_sftp_marc",
            "archive_s3_data",
            "solr_alias_swap",
            "solr_commit",
            "get_num_solr_docs_post",
            "slack_post_succ"
            ])

    def test_dag_task_order(self):
        """Unit test that the DAG instance contains the expected dependencies."""
        expected_task_deps = {
            "get_num_solr_docs_pre": ["set_collection_name"],
            "list_alma_s3_data": ["get_num_solr_docs_pre"],
            "prepare_boundwiths": ["list_alma_s3_data"],
            "prepare_alma_data": ["prepare_boundwiths"],
            "create_collection": ["prepare_alma_data"],
            "index_sftp_marc": ["create_collection"],
            "archive_s3_data": ["index_sftp_marc"],
            "solr_alias_swap": ["archive_s3_data"],
            "solr_commit": ["solr_alias_swap"],
            "get_num_solr_docs_post": ["solr_alias_swap"],
            "slack_post_succ": ["get_num_solr_docs_post"],
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

    def test_archive_s3_data(self):
        """Test that we move indexed s3 files to archived foler"""
        airflow_home = airflow.models.Variable.get("AIRFLOW_HOME")
        task = DAG.get_task("archive_s3_data")
        expected_bash_path = airflow_home + "/dags/cob_datapipeline/scripts/sc_archive_s3_data.sh "
        self.assertEqual(task.env["AIRFLOW_HOME"], os.getcwd())
        self.assertEqual(task.env["AWS_ACCESS_KEY_ID"], "puppy")
        self.assertEqual(task.env["AWS_SECRET_ACCESS_KEY"], "chow")
        self.assertEqual(task.env["BUCKET"], "test_bucket")
        self.assertEqual(task.env["SOURCE_FOLDER"], "almasftp")
        self.assertIn("almasftp/archived", task.env["DEST_FOLDER"])
        self.assertEqual(task.bash_command, expected_bash_path)
