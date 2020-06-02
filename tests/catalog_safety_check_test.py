import unittest
import airflow
from cob_datapipeline.catalog_safety_check import safety_check


class TestCatalogSafetyCheck(unittest.TestCase):
    """Primary Class for Testing all DAGs' validation."""

    def test_safety_check(self):
        airflow.models.Variable.set("CATALOG_PRODUCTION_SOLR_COLLECTION", "FOO")
        airflow.models.Variable.set("CATALOG_PRE_PRODUCTION_SOLR_COLLECTION", "FOO")
        self.assertRaises(Exception, safety_check)

        airflow.models.Variable.set("CATALOG_PRE_PRODUCTION_SOLR_COLLECTION", "BAR")
        self.assertEqual(None, safety_check())
