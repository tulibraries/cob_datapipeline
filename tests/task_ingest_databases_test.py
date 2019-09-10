import os
import unittest
import airflow
from cob_datapipeline.task_ingest_databases import ingest_databases
from cob_datapipeline.task_ingest_databases import get_solr_url

class TestIngestDatabasesTask(unittest.TestCase):
    """Unit Test for ingest databases task file."""


    def test_ingest_databases_task_az_url_override(self):
        "Test that we can override az_url"
        conn = airflow.models.Connection(
                conn_id="test",
                conn_type="http",
                host="example.com",
                port="8983")
        core = "foo"
        task = ingest_databases(None, conn, "test_ingest_databases_task", "https://example.com/foo")
        self.assertEqual(task.env["SOLR_AZ_URL"], "https://example.com/foo")

    def test_ingest_databases_task_adds_user_password(self):
        conn = airflow.models.Connection(
                conn_id="test",
                conn_type="http",
                host="example.com",
                port="8983",
                login="foo",
                password="bar"
                )
        core = "foo"
        task = ingest_databases(None, conn, "test_ingest_databases_task", "https://example.com/foo")
        self.assertEqual(task.env["SOLR_AUTH_USER"], "foo")
        self.assertEqual(task.env["SOLR_AUTH_PASSWORD"], "bar")

    def test_get_solr_url_without_http_in_host(self):
        conn = airflow.models.Connection(
                conn_id="test",
                conn_type="http",
                host="example.com",
                port="8983")
        core = "foo"
        self.assertEqual(get_solr_url(conn, core), "http://example.com:8983/solr/foo")

    def test_get_solr_url_with_http_in_host(self):
        conn = airflow.models.Connection(
                conn_id="test",
                conn_type="http",
                host="https://example.com",
                port="8983")
        core = "foo"
        self.assertEqual(get_solr_url(conn, core), "https://example.com:8983/solr/foo")

    def test_get_solr_url_without_port(self):
        conn = airflow.models.Connection(
                conn_id="test",
                conn_type="http",
                host="https://example.com")
        core = "foo"
        self.assertEqual(get_solr_url(conn, core), "https://example.com/solr/foo")
