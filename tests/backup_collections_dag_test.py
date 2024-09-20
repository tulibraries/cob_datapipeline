import os
import requests
import requests_mock
import unittest

from airflow.models import DagBag, TaskInstance, DagRun
from unittest.mock import patch, MagicMock
from airflow.utils.dates import days_ago
from airflow.utils.state import State
from datetime import datetime, timezone
from tulflow.solr_api_utils import SolrApiUtils


class TestBackupCollectionsDAG(unittest.TestCase):

    def setUp(self):
        # Disable Airflow example DAGs (or they throw an error)
        os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"

        # Load the DAG to be tested
        paths_to_dags = os.path.abspath("../cob_datapipeline/cob_datapipeline")
        dag_bag = DagBag(dag_folder=paths_to_dags)

        dag = dag_bag.get_dag(dag_id="backup_collections")
        self.assertIsNotNone(dag)
        self.assertEqual(dag.dag_id, "backup_collections")
        self.dag = dag

    @requests_mock.mock()
    def test_get_collections(self, mock_request):
        mock_request.get(
                "http://127.0.0.1/solr/admin/collections?action=List",
                status_code=200,
                text='{"status":{"status": 200}, "collections": ["collection1", "collection2"]}',
                reason='OK')

        task = self.dag.get_task(task_id='get_collections')
        # Simulate running the task and pushing XCom (you may want to mock the task execution)
        result = task.execute({})

        # Validate the result
        self.assertIsNotNone(result)
        self.assertEqual(result, ["collection1", "collection2"])

    @patch("tulflow.solr_api_utils.SolrApiUtils.get_collections")
    @patch("tulflow.solr_api_utils.SolrApiUtils.get_from_solr_api")
    @patch("airflow.providers.slack.notifications.slack.SlackNotifier.notify")
    def test_backup_collection_success(self, mock_slack_notifier, mock_get_from_solr_api, mock_get_collections):
        mock_get_from_solr_api.return_value = MagicMock(status_code=200)
        mock_get_collections.return_value = ["collection1", "collection2"]

        dag = self.dag

        # Set up the execution date
        execution_date = datetime.now(timezone.utc)
        dag_run = dag.create_dagrun(
                state=State.RUNNING,
                execution_date=execution_date,
                start_date=execution_date,
                data_interval=(execution_date, execution_date),
                run_type='manual'
                )

        # Get the tasks
        get_collections_task = dag.get_task('get_collections')
        backup_collections_task = dag.get_task('backup_collections')

        # Test the get_collections task
        ti_get_collections = TaskInstance(get_collections_task, execution_date=execution_date)
        ti_get_collections.run()
        collections = ti_get_collections.xcom_pull(task_ids='get_collections')

        # Assert collections were returned
        self.assertEqual(collections, ["collection1", "collection2"])

        # Test the backup_collections task
        ti_backup_collections = TaskInstance(backup_collections_task, execution_date=execution_date)
        ti_backup_collections.run()

        # Ensure that the collections were backed up
        mock_get_from_solr_api.assert_any_call('/solr/admin/collections?action=BACKUP&name=collection1&collection=collection1&location=/srv/backups')
        mock_get_from_solr_api.assert_any_call('/solr/admin/collections?action=BACKUP&name=collection2&collection=collection2&location=/srv/backups')

        # Ensure success callback is triggered
        self.assertEqual(mock_get_from_solr_api.call_count, 2)

        # Assert that the Slack notification was sent
        mock_slack_notifier.assert_called()


if __name__ == "__main__":
    unittest.main()
