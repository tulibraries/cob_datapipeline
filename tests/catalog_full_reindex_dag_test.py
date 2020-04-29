"""Unit Tests for the TUL Cob Catalog Full Reindex DAG."""
import os
import unittest
import airflow
from cob_datapipeline.catalog_full_reindex_dag import create_dag
from datetime import datetime, timedelta

class TestTemplate(unittest.TestCase):
    def setUp(self):
        self.dag = create_dag("qa", "0.1.0")

    def test_create_dag(self):
        """Assert expected DAG exists."""
        self.assertEqual(self.dag.dag_id, "qa_sc_catalog_full_reindex")

    def test_set_collection_name_task(self):
        task = self.dag.get_task("set_collection_name")
        self.assertIn(task.python_callable.__qualname__, "datetime.strftime")

    def test_solr_alias_swap_task(self):
        task = self.dag.get_task("solr_alias_swap")
        self.assertEqual(task.data["name"], "tul_cob-catalog-0-qa")

    def test_xsl_transform_task(self):
        task = self.dag.get_task("index_sftp_marc")
        self.assertEqual(task.env["GIT_BRANCH"], "CATALOG_QA_BRANCH" )
        self.assertEqual(task.env["SOLR_URL"], "http://127.0.0.1:8983/solr/tul_cob-catalog-0-{{ ti.xcom_pull(task_ids='set_collection_name') }}")
