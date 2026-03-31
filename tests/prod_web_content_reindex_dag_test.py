"""Unit Tests for the Prod TUL Cob Web Content Index DAG."""
import os
import unittest
os.environ.setdefault("WEB_CONTENT_SCHEDULE_INTERVAL", "@weekly")

from airflow.models import TaskInstance as TI
from airflow.models.dagrun import DagRun
from airflow.settings import Session
from airflow.utils.state import DagRunState, State
from airflow.utils.types import DagRunType

from cob_datapipeline.prod_web_content_reindex_dag import DAG
from tests.helpers import DEFAULT_DATE

class TestProdWebContentReindexDag(unittest.TestCase):
    """Primary Class for Testing the TUL Cob Web Content DAG."""

    def setUp(self):
        """Method to set up the DAG Class instance for testing."""
        self.tasks = list(map(lambda t: t.task_id, DAG.tasks))

    def test_dag_loads(self):
        """Unit test that the DAG identifier is set correctly."""
        self.assertEqual(DAG.dag_id, "prod_web_content_reindex")

    def test_dag_interval_is_variable(self):
        """Unit test that the DAG schedule is set by configuration"""
        self.assertEqual(DAG.schedule, "@weekly")

    def test_dag_tasks_present(self):
        """Unit test that the DAG instance contains the expected tasks."""
        self.assertEqual(self.tasks, [
            "set_collection_name",
            "get_num_solr_docs_pre",
            "create_collection",
            "index_web_content",
            "get_num_solr_docs_post",
            "solr_alias_swap",
            "push_alias",
            "delete_aliases",
            "push_collection",
            "delete_collections",
            ])

    def test_dag_task_order(self):
        """Unit test that the DAG instance contains the expected dependencies."""
        expected_task_deps = {
            "set_collection_name": "get_num_solr_docs_pre",
            "create_collection": "set_collection_name",
            "index_web_content": "create_collection",
            "get_num_solr_docs_post": "index_web_content",
            "solr_alias_swap": "get_num_solr_docs_post",
            "push_alias": "solr_alias_swap",
            "delete_aliases": "push_alias",
            "push_collection": "delete_aliases",
            "delete_collections": "push_collection",
        }

        for task, upstream_task in expected_task_deps.items():
            actual_ut = DAG.get_task(task).upstream_list[0].task_id
            self.assertEqual(upstream_task, actual_ut)

    def test_index_web_content_task(self):
        """Unit test that the DAG instance can find required solr indexing bash script."""
        task = self.render_task("index_web_content", DEFAULT_DATE.replace(minute=1))
        expected_bash_path = os.getcwd() + "/dags/cob_datapipeline/scripts/ingest_web_content.sh "
        self.assertEqual(task.bash_command, expected_bash_path)
        self.assertEqual(task.env["HOME"], os.getcwd())
        self.assertEqual(task.env["SOLR_WEB_URL"], "http://127.0.0.1:8983/solr/tul_cob-web-2-collection_test")
        self.assertIn("http://127.0.0.2", task.env["WEB_CONTENT_BASE_URL"])
        self.assertEqual(task.env["WEB_CONTENT_BRANCH"], "WEB_CONTENT_BRANCH")

    def render_task(self, task_id, logical_date):
        """Method to render templated fields for a task."""
        session = Session()
        run_id = f"test_run_{task_id}_{logical_date.minute}"
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
            state=State.SUCCESS,
        )
        set_collection_ti = TI(
            task=DAG.get_task("set_collection_name"),
            run_id=run_id,
            dag_version_id=None,
            state=State.SUCCESS,
        )
        set_collection_ti.xcom_push(key="return_value", value="collection_test")
        context = task_instance.get_template_context()
        task.render_template_fields(context)
        return task
