import os
import requests_mock
import unittest
from datetime import datetime, timezone

from unittest.mock import patch, MagicMock

import cob_datapipeline.backup_collections_dag as backup_module
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
            "/solr/admin/collections?action=BACKUP&name=collection1&collection=collection1&location=/srv/backups",
            timeout=(10, 900),
        )
        mock_get_from_solr_api.assert_any_call(
            "/solr/admin/collections?action=BACKUP&name=collection2&collection=collection2&location=/srv/backups",
            timeout=(10, 900),
        )
        # Ensure success callback is triggered
        self.assertEqual(mock_get_from_solr_api.call_count, 2)

    @patch("cob_datapipeline.backup_collections_dag.get_solr_db")
    def test_delete_old_backups_deletes_expired_points_and_purges_files(
        self, mock_get_solr_db
    ):
        solr_db = MagicMock()
        mock_get_solr_db.return_value = solr_db
        solr_db.get_from_solr_api.side_effect = [
            self._solr_response(
                {
                    "responseHeader": {"status": 0},
                    "backups": [
                        {"backupId": 1, "startTime": "2024-01-01T00:00:00Z"},
                        {"backupId": 2, "startTime": "2024-02-20T00:00:00Z"},
                    ],
                }
            ),
            self._solr_response({"responseHeader": {"status": 0}}),
            self._solr_response({"responseHeader": {"status": 0}}),
        ]

        backup_module.delete_old_backups(
            "collection1", now=datetime(2024, 3, 1, tzinfo=timezone.utc)
        )

        calls = solr_db.get_from_solr_api.call_args_list
        self.assertEqual(calls[0].args[0], (
            "/solr/admin/collections?action=LISTBACKUP&name=collection1"
            "&location=%2Fsrv%2Fbackups"
        ))
        self.assertEqual(calls[1].args[0], (
            "/solr/admin/collections?action=DELETEBACKUP&name=collection1"
            "&location=%2Fsrv%2Fbackups&backupId=1"
        ))
        self.assertEqual(calls[2].args[0], (
            "/solr/admin/collections?action=DELETEBACKUP&name=collection1"
            "&location=%2Fsrv%2Fbackups&purgeUnused=true"
        ))

    @patch("cob_datapipeline.backup_collections_dag.get_solr_db")
    def test_delete_old_backups_keeps_newest_point_when_all_are_expired(
        self, mock_get_solr_db
    ):
        solr_db = MagicMock()
        mock_get_solr_db.return_value = solr_db
        solr_db.get_from_solr_api.side_effect = [
            self._solr_response(
                {
                    "responseHeader": {"status": 0},
                    "backups": [
                        {"backupId": 1, "startTime": "2024-01-01T00:00:00Z"},
                        {"backupId": 2, "startTime": "2024-01-02T00:00:00Z"},
                    ],
                }
            ),
            self._solr_response({"responseHeader": {"status": 0}}),
            self._solr_response({"responseHeader": {"status": 0}}),
        ]

        backup_module.delete_old_backups(
            "collection1", now=datetime(2024, 3, 1, tzinfo=timezone.utc)
        )

        delete_call = solr_db.get_from_solr_api.call_args_list[1]
        self.assertIn("backupId=1", delete_call.args[0])
        self.assertNotIn("backupId=2", delete_call.args[0])

    @patch("cob_datapipeline.backup_collections_dag.get_solr_db")
    def test_delete_old_backups_does_not_delete_recent_points(self, mock_get_solr_db):
        solr_db = MagicMock()
        mock_get_solr_db.return_value = solr_db
        solr_db.get_from_solr_api.return_value = self._solr_response(
            {
                "responseHeader": {"status": 0},
                "backups": [
                    {"backupId": 1, "startTime": "2024-02-20T00:00:00Z"},
                ],
            }
        )

        backup_module.delete_old_backups(
            "collection1", now=datetime(2024, 3, 1, tzinfo=timezone.utc)
        )

        solr_db.get_from_solr_api.assert_called_once()

    @patch("cob_datapipeline.backup_collections_dag.get_solr_db")
    def test_delete_old_backups_fails_before_deleting_on_list_error(
        self, mock_get_solr_db
    ):
        solr_db = MagicMock()
        mock_get_solr_db.return_value = solr_db
        solr_db.get_from_solr_api.return_value = self._solr_response({}, status_code=500)

        with self.assertRaises(RuntimeError):
            backup_module.delete_old_backups("collection1")

        solr_db.get_from_solr_api.assert_called_once()

    def test_cleanup_runs_after_backup_task(self):
        backup_task = self.dag.get_task("backup_collections")
        cleanup_task = self.dag.get_task("delete_old_solr_backups")

        self.assertIn(backup_task, cleanup_task.upstream_list)

    @staticmethod
    def _solr_response(payload, status_code=200):
        response = MagicMock(status_code=status_code)
        response.json.return_value = payload
        return response


if __name__ == "__main__":
    unittest.main()
