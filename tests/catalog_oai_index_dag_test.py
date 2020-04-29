"""Unit Tests for the TUL Cob Catalog Full Reindex DAG."""
import os
import unittest
import airflow
from cob_datapipeline.catalog_oai_index_dag import create_dag
from datetime import datetime, timedelta

class TestTemplate(unittest.TestCase):
    def setUp(self):
        self.dag = create_dag("qa", "0.1.0")

    def test_create_dag(self):
        """Assert expected DAG exists."""
        self.assertEqual(self.dag.dag_id, "qa_sc_catalog_oai_harvest")

    def test_set_collection_name_task(self):
        task = self.dag.get_task("set_collection_name")
        self.assertIn(task.python_callable.__qualname__, "datetime.strftime")
