"""Unit Tests for the TUL Cob AZ Reindex DAG."""
import os
import unittest
import airflow
from cob_datapipeline.tul_cob_az_reindex_dag import AZ_DAG

class TestTulCobAZReindexDag(unittest.TestCase):
    """Primary Class for Testing the TUL Cob Reindex DAG."""

    def setUp(self):
        """Method to set up the DAG Class instance for testing."""
        self.tasks = list(map(lambda t: t.task_id, AZ_DAG.tasks))

    def test_dag_loads(self):
        """Unit test that the DAG identifier is set correctly."""
        self.assertEqual(AZ_DAG.dag_id, "tul_cob_az_reindex")

    def test_dag_tasks_present(self):
        """Unit test that the DAG instance contains the expected tasks."""
        self.assertEqual(self.tasks, [
            "get_num_solr_docs_pre",
            "git_pull_tulcob",
            "get_database_docs",
            "ingest_databases",
            "get_num_solr_docs_post",
            "slack_post_succ",
            ])

    def test_dag_task_order(self):
        """Unit test that the DAG instance contains the expected dependencies."""
        expected_task_deps = {
            "git_pull_tulcob": "get_num_solr_docs_pre",
            "get_database_docs": "git_pull_tulcob",
            "ingest_databases": "get_database_docs",
            "get_num_solr_docs_post": "ingest_databases",
            "slack_post_succ": "get_num_solr_docs_post",
        }

        for task, upstream_task in expected_task_deps.items():
            actual_ut = AZ_DAG.get_task(task).upstream_list[0].task_id
            self.assertEqual(upstream_task, actual_ut)

    def test_get_database_docs(self):
        """Unit test that the DAG instance can find required database harvest bash script."""
        task = AZ_DAG.get_task("get_database_docs")
        airflow_home = airflow.models.Variable.get("AIRFLOW_HOME")
        expected_bash_path = airflow_home + "/dags/cob_datapipeline/scripts/get_database_docs.sh"
        self.assertEqual(task.bash_command, expected_bash_path)
        self.assertEqual(task.env["AZ_CLIENT_ID"], "AZ_CLIENT_ID")
        self.assertEqual(task.env["AZ_CLIENT_SECRET"], "AZ_CLIENT_SECRET")

    def test_ingest_databases_task(self):
        """Unit test that the DAG instance can find required solr indexing bash script."""
        task = AZ_DAG.get_task("ingest_databases")
        airflow_home = airflow.models.Variable.get("AIRFLOW_HOME")
        expected_bash_path = airflow_home + "/dags/cob_datapipeline/scripts/ingest_databases.sh"
        self.assertEqual(task.bash_command, expected_bash_path)
        self.assertEqual(task.env["SOLR_AZ_URL"], "http://127.0.0.1:8983/solr/az-database")
        self.assertEqual(task.env["AIRFLOW_HOME"], os.getcwd())
        self.assertEqual(task.env["AIRFLOW_DATA_DIR"], os.getcwd() + "/data")
        self.assertEqual(task.env["AIRFLOW_LOG_DIR"], os.getcwd() + "/logs")
