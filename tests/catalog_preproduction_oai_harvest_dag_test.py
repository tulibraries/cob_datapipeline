"""Unit Tests for the TUL Cob Catalog PreProduction OAI Harvest DAG."""
import os
import unittest
import airflow
from unittest.mock import Mock
from parameterized import parameterized
from cob_datapipeline.catalog_preproduction_oai_harvest_dag import DAG, choose_indexing_branch


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

    @parameterized.expand([
        ({"updated": 834, "deleted": 2}, "updates_and_deletes_branch"),
        ({"updated": 834, "deleted": 0}, "updates_only_branch"),
        ({"updated": 0, "deleted": 2}, "deletes_only_branch"),
        ({"updated": 0, "deleted": 0}, "no_updates_no_deletes_branch"),
    ])
    def test_choose_indexing_branch(self, harvest_data, expected_branch):
        # Mock the TaskInstance (ti) object and its xcom_pull method
        mock_ti = Mock()
        mock_ti.xcom_pull.return_value = harvest_data

        # Mock kwargs to pass to the function
        mock_kwargs = {'ti': mock_ti}

        # Call the function with the mocked kwargs
        result = choose_indexing_branch(**mock_kwargs)

        # Assert that the function returns the correct branch
        assert result == expected_branch
