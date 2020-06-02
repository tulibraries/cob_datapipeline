"""Unit Tests for the TUL Cob Catalog Full Reindex DAG."""
import os
import unittest
import airflow
from cob_datapipeline.preproduction_catalog_oai_harvest_dag import DAG

class TestPreproductionCatalogOaiHarvest(unittest.TestCase):
    """Unit Tests for solrcloud catalog full reindex dag file."""

    def setUp(self):
        self.airflow_home = airflow.models.Variable.get("AIRFLOW_HOME")

    def test_index_deletes_oai_marc(self):
        task = DAG.get_task("index_deletes_oai_marc")
        expected_bash_path = self.airflow_home + "/dags/cob_datapipeline/scripts/sc_ingest_marc.sh "
        self.assertEqual(task.env["COMMAND"], "delete")
        self.assertEqual(task.bash_command, expected_bash_path)

    def test_index_ingest_oai_marc(self):
        task = DAG.get_task("index_updates_oai_marc")
        expected_bash_path = self.airflow_home + "/dags/cob_datapipeline/scripts/sc_ingest_marc.sh "
        self.assertEqual(task.env["COMMAND"], "ingest")
        self.assertEqual(task.bash_command, expected_bash_path)
