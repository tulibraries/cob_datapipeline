# Unit Tests for the TUL Cob Web Content Index DAG.
import os
import unittest
import airflow
from cob_datapipeline.tul_cob_web_content_index_dag import WEB_CONTENT_DAG

class TestWebContentindexDag(unittest.TestCase):
    # Primary Class for Testing the TUL Cob Web Content DAG.

    def setUp(self):
        # Method to set up the DAG Class instance for testing.
        self.tasks = list(map(lambda t: t.task_id, WEB_CONTENT_DAG.tasks))

    def test_dag_loads(self):
        # Unit test that the DAG identifier is set correctly.
        self.assertEqual(WEB_CONTENT_DAG.dag_id, "tul_cob_web_content_index")

    def test_dag_interval_is_variable(self):
        # Unit test that the DAG schedule is set by configuration
        self.assertEqual(WEB_CONTENT_DAG.schedule_interval, "WEB_CONTENT_SCHEDULE_INTERVAL")

    def test_dag_tasks_present(self):
        # Unit test that the DAG instance contains the expected tasks.
        self.assertEqual(self.tasks, [
            "get_num_solr_docs_pre",
            "ingest_web_content",
            "get_num_solr_docs_post",
            "slack_post_succ",
            ])

    def test_dag_task_order(self):
        # Unit test that the DAG instance contains the expected dependencies.
        expected_task_deps = {
            "ingest_web_content": "get_num_solr_docs_pre",
            "get_num_solr_docs_post": "ingest_web_content",
            "slack_post_succ": "get_num_solr_docs_post",
        }

        for task, upstream_task in expected_task_deps.items():
            actual_ut = WEB_CONTENT_DAG.get_task(task).upstream_list[0].task_id
            self.assertEqual(upstream_task, actual_ut)

    def test_ingest_web_content_task(self):
        # Unit test that the DAG instance can find required solr indexing bash script.
        task = WEB_CONTENT_DAG.get_task("ingest_web_content")
        airflow_home = airflow.models.Variable.get("AIRFLOW_HOME")
        expected_bash_path = airflow_home + "/dags/cob_datapipeline/scripts/ingest_web_content.sh "
        self.assertEqual(task.bash_command, expected_bash_path)
        self.assertEqual(task.env["AIRFLOW_HOME"], os.getcwd())
        self.assertEqual(task.env["AIRFLOW_DATA_DIR"], os.getcwd() + "/data")
        self.assertEqual(task.env["AIRFLOW_LOG_DIR"], os.getcwd() + "/logs")
        self.assertEqual(task.env["SOLR_WEB_URL"], "http://127.0.0.1:8983/solr/WEB_CONTENT_CORE")
        self.assertEqual(task.env["WEB_CONTENT_BRANCH"], "WEB_CONTENT_BRANCH")
        self.assertEqual(task.env["WEB_CONTENT_BASIC_AUTH_USER"], "WEB_CONTENT_BASIC_AUTH_USER")
        self.assertEqual(task.env["WEB_CONTENT_BASIC_AUTH_PASSWORD"], "WEB_CONTENT_BASIC_AUTH_PASSWORD")
        self.assertEqual(task.env["WEB_CONTENT_BASE_URL"], "WEB_CONTENT_BASE_URL")
