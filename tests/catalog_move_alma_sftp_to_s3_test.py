"""Unit Tests for the TUL Cob Move Alma SFTP to S3 Dag."""
import os
import unittest
import airflow
from cob_datapipeline.catalog_move_alma_sftp_to_s3_dag import DAG

class TestMoveAlmaSFTPToS3Dag(unittest.TestCase):
    """Primary Class for Testing the  TUL Cob Move Alma SFTP to S3 Dag."""

    def setUp(self):
        """Method to set up the DAG Class instance for testing."""
        self.tasks = list(map(lambda t: t.task_id, DAG.tasks))

    def test_dag_loads(self):
        """Unit test that the DAG identifier is set correctly."""
        self.assertEqual(DAG.dag_id, "catalog_move_alma_sftp_to_s3")

    def test_dag_interval_is_variable(self):
        """Unit test that the DAG schedule is set by configuration."""
        self.assertEqual(DAG.schedule_interval, None)

    def test_dag_tasks_present(self):
        """Unit test that the DAG instance contains the expected tasks."""
        self.assertEqual(self.tasks, [
            "get_list_of_alma_sftp_files_to_transer",
            "move_file_to_s3",
            "archive_files_in_sftp",
            "update_variables",
            ])
