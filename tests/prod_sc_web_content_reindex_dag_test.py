"""Unit Tests for the TUL Cob Web Content Index DAG."""
import os
import unittest
import airflow
from cob_datapipeline.prod_sc_web_content_reindex_dag import DAG

class TestScWebContentReindexDag(unittest.TestCase):
    """Primary Class for Testing the TUL Cob Web Content DAG."""

    def setUp(self):
        """Method to set up the DAG Class instance for testing."""
        self.tasks = list(map(lambda t: t.task_id, DAG.tasks))

    def test_dag_loads(self):
        """Unit test that the DAG identifier is set correctly."""
        self.assertEqual(DAG.dag_id, "prod_sc_web_content_reindex")

    def test_dag_interval_is_variable(self):
        """Unit test that the DAG schedule is set by configuration"""
        self.assertEqual(DAG.schedule_interval, "WEB_CONTENT_SCHEDULE_INTERVAL")

    def test_dag_tasks_present(self):
        """Unit test that the DAG instance contains the expected tasks."""
        self.assertEqual(self.tasks, [
            "set_collection_name",
            "get_num_solr_docs_pre",
            "create_collection",
            "index_web_content",
            "get_num_solr_docs_post",
            "solr_alias_swap",
            "slack_post_succ"
            ])

    def test_dag_task_order(self):
        """Unit test that the DAG instance contains the expected dependencies."""
        expected_task_deps = {
            "set_collection_name": "get_num_solr_docs_pre",
            "create_collection": "set_collection_name",
            "index_web_content": "create_collection",
            "get_num_solr_docs_post": "index_web_content",
            "solr_alias_swap": "get_num_solr_docs_post",
            "slack_post_succ": "solr_alias_swap",
        }

        for task, upstream_task in expected_task_deps.items():
            actual_ut = DAG.get_task(task).upstream_list[0].task_id
            self.assertEqual(upstream_task, actual_ut)

    def test_index_web_content_task(self):
        """Unit test that the DAG instance can find required solr indexing bash script."""
        task = DAG.get_task("index_web_content")
        airflow_home = airflow.models.Variable.get("AIRFLOW_HOME")
        expected_bash_path = airflow_home + "/dags/cob_datapipeline/scripts/ingest_web_content.sh "
        self.assertEqual(task.bash_command, expected_bash_path)
        self.assertEqual(task.env["HOME"], os.getcwd())
        self.assertIn("http://127.0.0.1:8983/solr/tul_cob-web-2", task.env["SOLR_WEB_URL"])
        self.assertEqual(task.env["WEB_CONTENT_BRANCH"], "WEB_CONTENT_BRANCH")
        self.assertEqual(task.env["WEB_CONTENT_BASE_URL"], "WEB_CONTENT_BASE_URL")
