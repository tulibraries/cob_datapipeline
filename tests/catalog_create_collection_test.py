import unittest
import airflow
from airflow.hooks.base_hook import BaseHook
from cob_datapipeline.catalog_create_collection import collection_name
from cob_datapipeline.catalog_create_collection import create_missing_collection
import json
import requests_mock
from re import compile as re_compile


class TestCatalogSafetyCheck(unittest.TestCase):
    """Primary Class for Testing all DAGs' validation."""


    def test_collection_name(self):
        # When CATALOG_PRE_PRODUCTION_SOLR_COLLECTION is defined.
        airflow.models.Variable.set("CATALOG_PRE_PRODUCTION_SOLR_COLLECTION", "BAR")
        name = collection_name(
                configset="bizz",
                cob_index_version="buzz")
        self.assertEqual(name, "BAR")

        # When CATALOG_PRE_PRODUCTION_SOLR_COLLECTION is defined but set to None
        airflow.models.Variable.set("CATALOG_PRE_PRODUCTION_SOLR_COLLECTION", None)
        name = collection_name(
                configset="bizz",
                cob_index_version="buzz")
        expected_name = "{{ configset }}.{{ cob_index_version }}-{{ ti.xcom_pull(task_ids='set_s3_namespace') }}"
        self.assertEqual(name, expected_name)

        # When CATALOG_PRE_PRODUCTION_SOLR_COLLECTION is not defined
        airflow.models.Variable.delete("CATALOG_PRE_PRODUCTION_SOLR_COLLECTION")
        name = collection_name(
                configset="bizz",
                cob_index_version="buzz")
        expected_name = "{{ configset }}.{{ cob_index_version }}-{{ ti.xcom_pull(task_ids='set_s3_namespace') }}"
        self.assertEqual(name, expected_name)

    @requests_mock.mock()
    def test_create_missing(self, request_mock):
        conn = BaseHook.get_connection("SOLRCLOUD")

        # When collection present.
        response = json.dumps({
            "responseHeader": {"status":0, "QTime":1},
            "collections": ["foo"],
            })
        request_mock.get(re_compile(".*"), text=response)

        with self.assertLogs() as log:
            create_missing_collection(collection="foo", conn=conn)
        self.assertIn("INFO:root:Collection foo already present. Skipping collection creation.", log.output)

        # When collection is NOT present.
        response = json.dumps({
            "responseHeader": {"status":0, "QTime":1},
            "collections": [],
            })
        request_mock.get(re_compile(".*"), text=response)

        with self.assertLogs() as log:
            create_missing_collection(collection="foo", conn=conn)

        self.assertIn("INFO:root:Collection foo created", log.output)
