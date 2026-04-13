"""Unit Tests for the TUL Cob Move Alma SFTP to S3 Dag."""
import unittest

from airflow.models import TaskInstance as TI
from airflow.models.dagrun import DagRun
from airflow.models.renderedtifields import RenderedTaskInstanceFields as RTIF
from airflow.settings import Session
from airflow.utils.state import DagRunState, State
from airflow.utils.types import DagRunType

from cob_datapipeline.boundwith_move_alma_sftp_to_s3_dag import DAG
from tests.helpers import DEFAULT_DATE


class TestMoveAlmaSFTPBwToS3Dag(unittest.TestCase):
    """Primary Class for Testing the  TUL Cob Move Alma SFTP to S3 Dag."""

    def setUp(self):
        """Method to set up the DAG Class instance for testing."""
        self.tasks = list(map(lambda t: t.task_id, DAG.tasks))

    def test_dag_loads(self):
        """Unit test that the DAG identifier is set correctly."""
        self.assertEqual(DAG.dag_id, "boundwith_move_alma_sftp_to_s3")

    def test_dag_interval_is_variable(self):
        """Unit test that the DAG schedule is set by configuration."""
        self.assertIsNone(DAG.schedule)

    def test_dag_tasks_present(self):
        """Unit test that the DAG instance contains the expected tasks."""
        self.assertEqual(self.tasks, [
            "get_list_of_alma_sftp_files_to_transer",
            "move_file_to_s3",
            "archive_files_in_sftp",
            "update_variables",
            ])

    def test_move_file_to_s3_bucket_renders_from_variable(self):
        """Unit test that the S3 bucket resolves from the Airflow Variable at runtime."""
        task = DAG.get_task("move_file_to_s3")
        session = Session()
        run_id = "test_run_render_bucket"
        logical_date = DEFAULT_DATE.replace(minute=1)

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

        rendered_ti_fields = RTIF(ti=task_instance)

        self.assertEqual(
            rendered_ti_fields.rendered_fields.get("s3_bucket"),
            "test_bucket"
        )
