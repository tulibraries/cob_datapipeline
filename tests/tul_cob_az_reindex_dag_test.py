# Unit Tests for the TUL Cob AZ Reindex DAG.
import os
import unittest
import airflow
from cob_datapipeline.tul_cob_az_reindex_dag import AZ_DAG
from cob_datapipeline.task_ingest_databases import ingest_databases

class TestTulCobAZReindexDag(unittest.TestCase):
    # Primary Class for Testing the TUL Cob Reindex DAG.

    def setUp(self):
        """Method to set up the DAG Class instance for testing."""
        self.tasks = list(map(lambda t: t.task_id, DAG.tasks))

    def test_dag_loads(self):
        # Unit test that the DAG identifier is set correctly.
        self.assertEqual(AZ_DAG.dag_id, "tul_cob_az_reindex")

    def test_dag_interval_is_variable(self):
        """Unit test that the DAG schedule is set by configuration"""
        self.assertEqual(DAG.schedule_interval, "@weekly")

    def test_dag_tasks_present(self):
        """Unit test that the DAG instance contains the expected tasks."""
        self.assertEqual(self.tasks, [
            "remote_trigger_message",
            "require_dag_run",
            "set_collection_name",
            "get_num_solr_docs_pre",
            "ingest_databases",
            "get_num_solr_docs_post",
            "slack_post_succ",
            ])

    def test_dag_task_order(self):
        """Unit test that the DAG instance contains the expected dependencies."""
        expected_task_deps = {
            "ingest_databases": "get_num_solr_docs_pre",
            "get_num_solr_docs_post": "ingest_databases",
            "slack_post_succ": "get_num_solr_docs_post",
        }

        for task, upstream_task in expected_task_deps.items():
            actual_ut = DAG.get_task(task).upstream_list[0].task_id
            self.assertEqual(upstream_task, actual_ut)

    def test_ingest_databases_task(self):
        """Unit test that the DAG instance can find required solr indexing bash script."""
        task = DAG.get_task("index_databases")
        airflow_home = airflow.models.Variable.get("AIRFLOW_HOME")
        expected_bash_path = airflow_home + "/dags/cob_datapipeline/scripts/ingest_databases.sh "
        self.assertEqual(task.bash_command, expected_bash_path)
        self.assertEqual(task.env["HOME"], os.getcwd())
        self.assertEqual(task.env["AZ_BRANCH"], "AZ_BRANCH")
        self.assertEqual(task.env["AZ_CLIENT_ID"], "AZ_CLIENT_ID")
        self.assertEqual(task.env["AZ_CLIENT_SECRET"], "AZ_CLIENT_SECRET")
        self.assertEqual(task.env["AZ_DELETE_SWITCH"], "--delete")
        self.assertEqual(task.env["SOLR_AZ_URL"], "http://127.0.0.1:8983/solr/az-database")
