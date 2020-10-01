import json
import requests_mock
import unittest
import airflow
from airflow.hooks.base_hook import BaseHook
from cob_datapipeline import helpers
from re import compile as re_compile


class TestDetermineMostRecentDate(unittest.TestCase):
    """Primary Class for Testing Logic for Picking Most Recent Alma SFTP files"""

    def test_determine_most_recent_date(self):
        files_list = ["alma_bibs__1234_test_1", "alma_bibs__1235_test_1", "alma_bibs__1235_test_2", "alma_bibs__12340_test_1"]
        self.assertEqual(helpers.determine_most_recent_date(files_list), 12340)

    def test_catalog_safety_check(self):
        airflow.models.Variable.set("CATALOG_PRODUCTION_SOLR_COLLECTION", "FOO")
        airflow.models.Variable.set("CATALOG_PRE_PRODUCTION_SOLR_COLLECTION", "FOO")
        self.assertRaises(Exception, helpers.catalog_safety_check)

        airflow.models.Variable.set("CATALOG_PRE_PRODUCTION_SOLR_COLLECTION", "BAR")
        self.assertEqual(None, helpers.catalog_safety_check())

    def test_catalog_collection_name(self):
        # When CATALOG_PRE_PRODUCTION_SOLR_COLLECTION is defined.
        airflow.models.Variable.set("CATALOG_PRE_PRODUCTION_SOLR_COLLECTION", "BAR")
        name = helpers.catalog_collection_name(
                configset="bizz",
                cob_index_version="buzz")
        self.assertEqual(name, "BAR")

        # When CATALOG_PRE_PRODUCTION_SOLR_COLLECTION is defined but set to None
        airflow.models.Variable.set("CATALOG_PRE_PRODUCTION_SOLR_COLLECTION", None)
        name = helpers.catalog_collection_name(
                configset="bizz",
                cob_index_version="buzz")
        expected_name = "bizz.buzz-{{ ti.xcom_pull(task_ids='set_s3_namespace') }}"
        self.assertEqual(name, expected_name)

        # When CATALOG_PRE_PRODUCTION_SOLR_COLLECTION is not defined
        airflow.models.Variable.delete("CATALOG_PRE_PRODUCTION_SOLR_COLLECTION")
        name = helpers.catalog_collection_name(
                configset="bizz",
                cob_index_version="buzz")
        expected_name = "bizz.buzz-{{ ti.xcom_pull(task_ids='set_s3_namespace') }}"
        self.assertEqual(name, expected_name)

    @requests_mock.mock()
    def test_catalog_create_missing(self, request_mock):
        conn = BaseHook.get_connection("SOLRCLOUD")

        # When collection present.
        response = json.dumps({
            "responseHeader": {"status":0, "QTime":1},
            "collections": ["foo"],
            })
        request_mock.get(re_compile(".*"), text=response)

        with self.assertLogs() as log:
            helpers.catalog_create_missing_collection(collection="foo", conn=conn)
        self.assertIn("INFO:root:Collection foo already present. Skipping collection creation.", log.output)

        # When collection is NOT present.
        response = json.dumps({
            "responseHeader": {"status":0, "QTime":1},
            "collections": [],
            })
        request_mock.get(re_compile(".*"), text=response)

        with self.assertLogs() as log:
            helpers.catalog_create_missing_collection(collection="foo", conn=conn)

        self.assertIn("INFO:root:Collection foo created", log.output)
