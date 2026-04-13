import os
import requests_mock
import unittest

from unittest.mock import patch, MagicMock

from cob_datapipeline.backup_collections_dag import backup_collections_dag as DAG


class TestBackupCollectionsDAG(unittest.TestCase):

    def setUp(self):
        # Disable Airflow example DAGs (or they throw an error)
        os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
        # Load the DAG to be tested
        self.assertEqual(DAG.dag_id, "backup_collections")
        self.dag = DAG

    @patch("airflow.sdk.Connection.get")
    def test_get_collections(self, mock_get_connection):
        # Mock the Solr connection used by the task at runtime.
        mock_get_connection.return_value = MagicMock(
            host="http://127.0.0.1",
            login="admin",
            password="password"
        )

        with requests_mock.Mocker() as mock_request:
            # Mock the Solr collections list API response.
            mock_request.get(
                "http://127.0.0.1/solr/admin/collections?action=List",
                status_code=200,
                text='{"status":{"status": 200}, "collections": ["collection1", "collection2"]}',
                reason="OK"
            )

            task = self.dag.get_task(task_id="get_collections")

            # Simulate running the task and pushing XCom (you may want to mock the task execution)
            result = task.execute({})

        # Validate the result
        self.assertIsNotNone(result)
        self.assertEqual(result, ["collection1", "collection2"])

    @patch("tulflow.solr_api_utils.SolrApiUtils.get_collections")
    @patch("tulflow.solr_api_utils.SolrApiUtils.get_from_solr_api")
    @patch("airflow.sdk.Connection.get")
    @patch("airflow.providers.slack.notifications.slack.SlackNotifier.notify")
    def test_backup_collection_success(
        self,
        mock_slack_notifier,
        mock_get_connection,
        mock_get_from_solr_api,
        mock_get_collections
    ):
        mock_get_from_solr_api.return_value = MagicMock(status_code=200)
        mock_get_collections.return_value = ["collection1", "collection2"]

        # Mock connection retrieval
        mock_get_connection.return_value = MagicMock(
            host="http://127.0.0.1",
            login="admin",
            password="password"
        )

        dag = self.dag

        # Get the tasks
        get_collections_task = dag.get_task("get_collections")
        backup_collections_task = dag.get_task("backup_collections")

        # Test the get_collections task
        collections = get_collections_task.python_callable()

        # Assert collections were returned
        self.assertEqual(collections, ["collection1", "collection2"])

        # Test the backup_collections task
        backup_collections_task.python_callable(collections)

        # Ensure that the collections were backed up
        mock_get_from_solr_api.assert_any_call(
            "/solr/admin/collections?action=BACKUP&name=collection1&collection=collection1&location=/srv/backups"
        )
        mock_get_from_solr_api.assert_any_call(
            "/solr/admin/collections?action=BACKUP&name=collection2&collection=collection2&location=/srv/backups"
        )

        # Ensure success callback is triggered
        self.assertEqual(mock_get_from_solr_api.call_count, 2)


if __name__ == "__main__":
    unittest.main()
