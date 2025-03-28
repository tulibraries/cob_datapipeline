"""Unit Tests for the TUL Cob Catalog PreProduction OAI Harvest DAG."""
import os
import unittest
import airflow
from cob_datapipeline.catalog_preproduction_oai_harvest_dag import DAG

class TestCatalogPreproductionOaiHarvest(unittest.TestCase):
    """Unit Tests for solrcloud catalog preproduction oai harvest dag file."""

    def setUp(self):
        self.airflow_home = airflow.models.Variable.get("AIRFLOW_HOME")

    def test_index_deletes_oai_marc(self):
        task = DAG.get_task("index_deletes_oai_marc")
        expected_bash_path = self.airflow_home + "/dags/cob_datapipeline/scripts/ingest_marc.sh "
        self.assertEqual(task.env["COMMAND"], "delete --suppress")
        self.assertEqual(task.bash_command, expected_bash_path)

    def test_index_ingest_oai_marc(self):
        task = DAG.get_task("index_updates_oai_marc")
        expected_bash_path = self.airflow_home + "/dags/cob_datapipeline/scripts/ingest_marc.sh "
        self.assertEqual(task.env["COMMAND"], "ingest")
        self.assertEqual(task.bash_command, expected_bash_path)

