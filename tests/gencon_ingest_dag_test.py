"""Unit Tests for the TUL Cob GENCON Index DAG."""
import os
import unittest
from unittest.mock import patch

from airflow.models import TaskInstance as TI
from airflow.models.dagrun import DagRun
from airflow.settings import Session
from airflow.utils.state import DagRunState, State
from airflow.utils.types import DagRunType

from cob_datapipeline.gencon_ingest_dag import DAG
from tests.helpers import DEFAULT_DATE, get_runtime_template_context


class TestGenconIngestDag(unittest.TestCase):
    """Primary class for testing the TUL Cob GENCON DAG."""

    def setUp(self):
        """Set up the DAG class instance for testing."""
        self.tasks = list(map(lambda t: t.task_id, DAG.tasks))

    def test_dag_loads(self):
        """Unit test that the DAG identifier is set correctly."""
        self.assertEqual(DAG.dag_id, "gencon_index")

    def test_dag_interval_is_variable(self):
        """Unit test that the DAG schedule is set by configuration."""
        self.assertEqual(DAG.schedule, "@monthly")

    def test_dag_tasks_present(self):
        """Unit test that the DAG instance contains the expected tasks."""
        self.assertEqual(self.tasks, [
            "get_num_solr_docs_pre",
            "set_collection_name",
            "safety_check",
            "create_collection",
            "index_gencon",
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
            "safety_check": "set_collection_name",
            "create_collection": "safety_check",
            "index_gencon": "create_collection",
            "get_num_solr_docs_post": "index_gencon",
            "solr_alias_swap": "get_num_solr_docs_post",
            "push_alias": "solr_alias_swap",
            "delete_aliases": "push_alias",
            "push_collection": "delete_aliases",
            "delete_collections": "push_collection",
        }

        for task, upstream_task in expected_task_deps.items():
            actual_ut = DAG.get_task(task).upstream_list[0].task_id
            self.assertEqual(upstream_task, actual_ut)

    def test_index_gencon_task(self):
        """Unit test that the DAG instance can find the indexing bash script."""
        task = self.render_task("index_gencon", DEFAULT_DATE.replace(minute=1))
        expected_bash_path = os.getcwd() + "/dags/cob_datapipeline/scripts/ingest_gencon.sh "
        self.assertEqual(task.bash_command, expected_bash_path)
        self.assertEqual(task.env["HOME"], os.getcwd())
        self.assertEqual(task.env["GIT_BRANCH"], "main")
        self.assertEqual(task.env["GENCON_TEMP_PATH"], "/tmp/gencon")
        self.assertEqual(task.env["GENCON_CSV_S3"], "s3://tulib-airflow-prod/gencon")
        self.assertEqual(
            task.env["SOLR_URL"],
            "http://127.0.0.1:8983/solr/gencon50-v3.0.1-collection_test",
        )

    def test_safety_check_renders_target_collection(self):
        """Unit test that the safety check renders the target collection name."""
        task = self.render_task("safety_check", DEFAULT_DATE.replace(minute=2))
        self.assertEqual(task.op_kwargs["alias"], "gencon50-v3.0.1-prod")
        self.assertEqual(task.op_kwargs["collection"], "gencon50-v3.0.1-collection_test")

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
        context = get_runtime_template_context(task_instance)
        with patch.object(context["ti"], "xcom_pull", return_value="collection_test"):
            task.render_template_fields(context)
        return task
