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
