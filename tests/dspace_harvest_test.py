"""Unit Tests for the DSpace harvest DAG"""
import unittest
import airflow
from cob_datapipeline.dspace_harvest_dag import DAG

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
            ])

    def test_oai_harvest_task(self):
        """Unit test that oai_harvest dag has kwargs."""
        task = DAG.get_task("oai_harvest")
        self.assertEqual(task.op_kwargs["bucket_name"], "test_bucket")
        self.assertEqual(task.op_kwargs["oai_endpoint"], "foobar")
