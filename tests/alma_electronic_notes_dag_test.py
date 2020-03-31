"""Unit Tests for the alma electronic notes harvest DAG."""
import os
import unittest
import airflow
from cob_datapipeline.alma_electronic_notes_dag import DAG

class TestAlmaElectronicNotesDag(unittest.TestCase):
    """Unit Tests for alma electronic notes dag file."""

    def setUp(self):
        """Method to set up the DAG Class instance for testing."""
        self.tasks = list(map(lambda t: t.task_id, DAG.tasks))

    def test_dag_loads(self):
        """Unit test that the DAG identifier is set correctly."""
        self.assertEqual(DAG.dag_id, "alma_electronic_notes")

    def test_dag_tasks_present(self):
        """Unit test that the DAG instance contains the expected tasks."""
        self.assertEqual(self.tasks, [
            "set_datetime",
            "harvest_notes",
            "s3_to_server",
            "reload_electronic_notes",
            ])

    def test_harvest_notes(self):
        """Test that we move harvest to s3"""
        airflow_home = airflow.models.Variable.get("AIRFLOW_HOME")
        task = DAG.get_task("harvest_notes")
        expected_bash_path = airflow_home + "/dags/cob_datapipeline/scripts/harvest_notes.sh "
        self.assertEqual(task.env["AWS_ACCESS_KEY_ID"], "puppy")
        self.assertEqual(task.env["AWS_SECRET_ACCESS_KEY"], "chow")
        self.assertEqual(task.env["BUCKET"], "test_bucket")
        self.assertEqual(task.bash_command, expected_bash_path)
