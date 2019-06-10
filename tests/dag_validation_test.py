"""Unit Tests for Basic Validation of All DAGs."""
import unittest
from airflow.models import DagBag

class TestDagIntegrity(unittest.TestCase):
    """Primary Class for Testing all DAGs' validation."""

    LOAD_SECOND_THRESHOLD = 2

    def setUp(self):
        """Method to set up the DAG Validation Class instance for testing."""
        self.dagbag = DagBag()

    def test_import_dags(self):
        """Unit Test runs import of all DAGs defined & log Airflow errors."""
        self.assertFalse(
            len(self.dagbag.import_errors),
            'DAG import failures. Errors: {}'.format(
                self.dagbag.import_errors
            )
        )
