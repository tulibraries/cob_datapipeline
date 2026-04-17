"""Unit Tests for the TUL Cob GENCON Index DAG."""
import os
import unittest

from airflow.models import TaskInstance as TI
from airflow.models.dagrun import DagRun
from airflow.settings import Session
from airflow.utils.state import DagRunState, State
from airflow.utils.types import DagRunType

from cob_datapipeline.gencon_ingest_dag import DAG
from tests.helpers import DEFAULT_DATE, get_rendered_task_fields


class TestGenconIngestDag(unittest.TestCase):
    """Primary Class for Testing the TUL Cob GENCON DAG."""

    def setUp(self):
        """Method to set up the DAG Class instance for testing."""
        self.tasks = list(map(lambda t: t.task_id, DAG.tasks))

    def render_task(self, task_id, logical_date):
        """Method to render templated fields for a task."""
        task = DAG.get_task(task_id)
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

        task_instance = TI(
            task=task,
            run_id=run_id,
            dag_version_id=None,
            state=State.SUCCESS
        )

        return task, get_rendered_task_fields(task_instance)

    def test_dag_loads(self):
        """Unit test that the DAG identifier is set correctly."""
        self.assertEqual(DAG.dag_id, "gencon_index")

    def test_dag_interval_is_variable(self):
        """Unit test that the DAG schedule is set by configuration."""
        self.assertEqual(DAG.schedule, "@monthly")

    def test_dag_tasks_present(self):
        """Unit test that the DAG instance contains the expected tasks."""
        self.assertEqual(self.tasks, [
            "index_gencon",
        ])

    def test_index_gencon_task(self):
        """Unit test that the DAG instance can find required solr indexing bash script."""
        _, rendered_fields = self.render_task(
            "index_gencon",
            DEFAULT_DATE.replace(minute=1),
        )
        rendered_env = rendered_fields.get("env", {})
        expected_bash_path = os.getcwd() + "/dags/cob_datapipeline/scripts/ingest_gencon.sh "
        self.assertEqual(rendered_fields.get("bash_command"), expected_bash_path)
        self.assertEqual(rendered_env.get("HOME"), os.getcwd())
        self.assertEqual(rendered_env.get("GIT_BRANCH"), "main")
        self.assertEqual(rendered_env.get("GENCON_TEMP_PATH"), "/tmp/gencon")
        self.assertEqual(rendered_env.get("GENCON_CSV_S3"), "s3://tulib-airflow-prod/gencon")
        self.assertIn("http://127.0.0.1:8983/solr/gencon50-v3.0.1", rendered_env.get("SOLR_WEB_URL", ""))
