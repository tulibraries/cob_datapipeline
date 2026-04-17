"""Unit Tests for the DSpace harvest DAG"""
import unittest

from airflow.models import TaskInstance as TI
from airflow.models.dagrun import DagRun
from airflow.settings import Session
from airflow.utils.state import DagRunState, State
from airflow.utils.types import DagRunType

from cob_datapipeline.dspace_harvest_dag import DAG
from tests.helpers import DEFAULT_DATE, render_task_instance_fields

class TestDspaceHarvestDag(unittest.TestCase):
    """Unit Tests for the DSpace harvest DAG"""

    def setUp(self):
        """Method to set up the DAG Class instance for testing."""
        self.tasks = list(map(lambda t: t.task_id, DAG.tasks))

    def test_dag_loads(self):
        """Unit test that the DAG identifier is set correctly."""
        self.assertEqual(DAG.dag_id, "dspace_harvest")

    def test_dag_tasks_present(self):
        """Unit test that the DAG instance contains the expected tasks."""
        self.assertEqual(self.tasks, [
            "oai_harvest",
            "cleanup_data",
            "xsl_transform",
            "list_s3_files",
            "s3_to_sftp"
            ])

    def test_dag_task_order(self):
        """Unit test that the DAG instance contains the expected dependencies."""
        expected_task_deps = {
            "oai_harvest": [],
            "cleanup_data": ["oai_harvest"],
            "xsl_transform": ["cleanup_data"],
            "list_s3_files": ["xsl_transform"],
            "s3_to_sftp": ["list_s3_files"]
        }
        for task, upstream_tasks in expected_task_deps.items():
            upstream_list = [up_task.task_id for up_task in DAG.get_task(task).upstream_list]
            self.assertCountEqual(upstream_tasks, upstream_list)

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
        return render_task_instance_fields(task_instance)

    def test_oai_harvest_task(self):
        """Unit test that oai_harvest dag has kwargs."""
        task = self.render_task(
            "oai_harvest",
            DEFAULT_DATE.replace(minute=1),
        )
        rendered_kwargs = task.op_kwargs
        self.assertEqual(rendered_kwargs.get("bucket_name"), "test_bucket")
        self.assertEqual(rendered_kwargs.get("oai_endpoint"), "foobar")
        self.assertEqual(rendered_kwargs.get("included_sets"), ["col_20.500.12613_11"])

    def test_s3_to_sftp_bucket_renders_from_variable(self):
        """Unit test that s3_to_sftp resolves the S3 bucket from the Airflow Variable."""
        task = self.render_task(
            "s3_to_sftp",
            DEFAULT_DATE.replace(minute=2),
        )
        self.assertEqual(task.s3_bucket, "test_bucket")
