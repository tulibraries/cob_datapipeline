import os
import unittest
import airflow
from cob_datapipeline.task_ingest_web_content import ingest_web_content

class TestIngestWebContentTask(unittest.TestCase):
    """Unit Test for ingest web content task file."""

    def test_ingest_web_content_task_web_url_override(self):
        "Test that we can override web_url"
        conn = airflow.models.Connection(host="example.com")
        core = "foo"
        task = ingest_web_content(None, conn, "test_ingest_web_content_task", "https://example.com/foo")
        self.assertEqual(task.env["SOLR_WEB_URL"], "https://example.com/foo")

    def test_ingest_web_content_with_delete(self):
        conn = airflow.models.Connection(host="foo")
        task = ingest_web_content(None, conn, delete=True)
        self.assertEqual(task.env["DELETE_SWITCH"], "--delete")

    def test_ingest_web_content_without_delete(self):
        conn = airflow.models.Connection(host="foo")
        task = ingest_web_content(None, conn)
        self.assertEqual(task.env.get("DELETE_SWITCH"), None)

    def test_ingest_web_content_task_adds_user_password(self):
        conn = airflow.models.Connection(
                host="example.com",
                login="foo",
                password="bar"
                )
        core = "foo"
        task = ingest_web_content(None, conn, "test_ingest_web_content_task", "https://example.com/foo")
        self.assertEqual(task.env["SOLR_AUTH_USER"], "foo")
        self.assertEqual(task.env["SOLR_AUTH_PASSWORD"], "bar")
