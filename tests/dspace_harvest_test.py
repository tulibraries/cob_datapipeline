"""Unit Tests for the DSpace harvest DAG"""
from datetime import datetime, timedelta
import unittest
from tulflow import tasks
import airflow

from cob_datapipeline.dspace_harvest_dag import DAG

class TestDspaceHarvestDag(unittest.TestCase):
    """Unit Tests for the DSpace harvest DAG"""

    def setUp(self):
        """Method to set up the DAG Class instance for testing."""
        self.tasks = list(map(lambda t: t.task_id, DAG.tasks))
