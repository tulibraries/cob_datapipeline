"""Unit Tests for the SolrCloud AZ Index Generic DAG."""
import os
import unittest
import airflow
from cob_datapipeline.sc_az_reindex_dag import DAG

class TestScAZReindexDag(unittest.TestCase):
    """Primary Class for Testing the TUL Cob Reindex DAG."""

    def setUp(self):
        """Method to set up the DAG Class instance for testing."""
        self.tasks = list(map(lambda t: t.task_id, DAG.tasks))

    def test_dag_loads(self):
        """Unit test that the DAG identifier is set correctly."""
        self.assertEqual(DAG.dag_id, "sc_az_reindex")

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
            "create_collection",
            "index_databases",
            "solr_alias_swap",
            "get_num_solr_docs_post",
            "slack_post_succ",
            ])

    def test_dag_task_order(self):
        """Unit test that the DAG instance contains the expected dependencies."""
        expected_task_deps = {
            "require_dag_run": "remote_trigger_message",
            "get_num_solr_docs_pre": "require_dag_run",
            "set_collection_name": "get_num_solr_docs_pre",
            "create_collection": "get_num_solr_docs_pre",
            "index_databases": "create_collection",
            "solr_alias_swap": "index_databases",
            "get_num_solr_docs_post": "solr_alias_swap",
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
        self.assertIn("http://127.0.0.1:8983/solr/tul_cob-az-2", task.env["SOLR_AZ_URL"])
