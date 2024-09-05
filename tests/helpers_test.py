"""Unit Tests for helper methods used in DAGs."""
import json
import unittest
from re import compile as re_compile
import logging
import boto3
from lxml import etree
from moto import mock_aws
import requests_mock
import airflow
from airflow.hooks.base import BaseHook
from unittest.mock import Mock
from parameterized import parameterized
from cob_datapipeline import helpers

class TestDetermineMostRecentDate(unittest.TestCase):
    """Primary Class for Testing Logic for Picking Most Recent Alma SFTP files"""

    def test_determine_most_recent_date(self):
        files_list = ["alma_bibs__1234_test_1", "alma_bibs__1235_test_1", "alma_bibs__1235_test_2", "alma_bibs__12340_test_1"]
        self.assertEqual(helpers.determine_most_recent_date(files_list), 12340)

    def test_determine_most_recent_date_boundwith(self):
        files_list = ["alma_bibs__boundwith2_1234_test_1", "alma_bibs__boundwith2_1235_test_1", "alma_bibs__boundwith2_1235_test_2", "alma_bibs__boundwith_12340_test_1"]
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

class TestCleanupMetadata(unittest.TestCase):
    """Primary Class for Testing data manipulation for DSpace DAG"""

    op_kwargs = {
        "source_prefix": "dspace/test/new-updated",
        "destination_prefix": "dspace/test/cleaned",
        "bucket_name": "tulib-airflow-test",
        "access_id": "puppy",
        "access_secret": "chow"
    }
    @mock_aws
    def test_cleanup_metadata(self):
        """Test Pulling S3 XML, Cleaning up data, & Writing to S3."""

        # setup kwargs for test runs
        access_id = self.op_kwargs.get("access_id")
        access_secret = self.op_kwargs.get("access_secret")
        bucket = self.op_kwargs.get("bucket_name")
        test_key = self.op_kwargs.get("source_prefix") + "/dspace-sample.xml"

        # create expected mocked s3 resources
        conn = boto3.client("s3", aws_access_key_id=access_id, aws_secret_access_key=access_secret)
        conn.create_bucket(Bucket=bucket)
        conn.put_object(Bucket=bucket, Key=test_key, Body=open("tests/fixtures/dspace-sample.xml").read())
        test_content_exists = conn.get_object(Bucket=bucket, Key=test_key)
        test_object_exists = conn.list_objects(Bucket=bucket)
        self.assertEqual(test_content_exists["Body"].read(), open("tests/fixtures/dspace-sample.xml", "rb").read())
        self.assertEqual(test_content_exists["ResponseMetadata"]["HTTPStatusCode"], 200)
        self.assertEqual(test_object_exists["Contents"][0]["Key"], test_key)

        # run tests
        with self.assertLogs() as log:
            helpers.cleanup_metadata(**self.op_kwargs)
        self.assertIn("INFO:root:Beginning namespace cleanup", log.output)
        self.assertIn("INFO:root:Removing namespaces from: dspace/test/new-updated/dspace-sample.xml", log.output)
        test_output_objects = conn.list_objects(Bucket=bucket, Prefix=self.op_kwargs.get("destination_prefix"))
        self.assertEqual(test_output_objects["ResponseMetadata"]["HTTPStatusCode"], 200)
        test_output_objects_ar = [object.get("Key") for object in test_output_objects["Contents"]]
        self.assertEqual(test_output_objects_ar, ["dspace/test/cleaned/dspace-sample.xml"])
        test_output_content = etree.fromstring(conn.get_object(Bucket=bucket, Key="dspace/test/cleaned/dspace-sample.xml")["Body"].read())
        should_match_output = etree.fromstring(open("tests/fixtures/dspace-sample-output.xml", "rb").read())
        self.assertEqual(
            etree.tostring(test_output_content, pretty_print=True),
            etree.tostring(should_match_output, pretty_print=True)
        )

class TestChooseIndexingBranch(unittest.TestCase):
    """Primary Class for Testing Logic for Picking Most Recent Alma SFTP files"""
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
        result = helpers.choose_indexing_branch(**mock_kwargs)

        # Assert that the function returns the correct branch
        assert result == expected_branch
