"""Unit Tests for the maintenance scheduler log cleanup DAG."""
import unittest

from cob_datapipeline.maintenance_scheduler_log_cleanup import dag as DAG


class TestMaintenanceSchedulerLogCleanupDag(unittest.TestCase):
    """Primary Class for Testing the maintenance scheduler log cleanup DAG."""

    def test_dag_loads(self):
        """Unit test that the DAG identifier is set correctly."""
        self.assertEqual(DAG.dag_id, "maintenance_scheduler_log_cleanup")

    def test_dag_schedule(self):
        """Unit test that the DAG schedule is set correctly."""
        self.assertEqual(DAG.schedule, "@daily")

    def test_cleanup_task(self):
        """Unit test that the cleanup task contains the expected find command."""
        task = DAG.get_task("cleanup_old_folders")
        self.assertIn("/var/lib/airflow/airflow-app/logs/scheduler", task.bash_command)
        self.assertIn("-mtime +30", task.bash_command)
