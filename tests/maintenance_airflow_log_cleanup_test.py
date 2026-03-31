"""Unit Tests for the maintenance airflow log cleanup DAG."""
import importlib.util
import os
import unittest

from airflow.models import TaskInstance as TI
from airflow.models.dagrun import DagRun
from airflow.models.renderedtifields import RenderedTaskInstanceFields as RTIF
from airflow.settings import Session
from airflow.utils.state import DagRunState, State
from airflow.utils.types import DagRunType

from tests.helpers import DEFAULT_DATE


MODULE_PATH = os.path.join(
    os.getcwd(),
    "cob_datapipeline",
    "maintenance_airflow-log-cleanup.py",
)
MODULE_SPEC = importlib.util.spec_from_file_location(
    "maintenance_airflow_log_cleanup",
    MODULE_PATH,
)
MODULE = importlib.util.module_from_spec(MODULE_SPEC)
MODULE_SPEC.loader.exec_module(MODULE)
DAG = MODULE.dag


class TestMaintenanceAirflowLogCleanupDag(unittest.TestCase):
    """Primary Class for Testing the maintenance airflow log cleanup DAG."""

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
            conf={},
        ))
        session.commit()

        task_instance = TI(
            task=task,
            run_id=run_id,
            dag_version_id=None,
            state=State.SUCCESS,
        )

        return RTIF(ti=task_instance).rendered_fields

    def test_dag_loads(self):
        """Unit test that the DAG identifier is set correctly."""
        self.assertEqual(DAG.dag_id, "maintenance_airflow-log-cleanup")

    def test_dag_schedule(self):
        """Unit test that the DAG schedule is set correctly."""
        self.assertEqual(DAG.schedule, "@daily")

    def test_cleanup_task_renders(self):
        """Unit test that a cleanup task renders the default max log age."""
        cleanup_task_id = next(
            task.task_id for task in DAG.tasks if task.task_id.startswith("log_cleanup_worker_num_")
        )
        rendered_fields = self.render_task(
            cleanup_task_id,
            DEFAULT_DATE.replace(minute=1),
        )
        self.assertIn("MAX_LOG_AGE_IN_DAYS", rendered_fields.get("bash_command", ""))
        self.assertIn("Using Default '30'", rendered_fields.get("bash_command", ""))
