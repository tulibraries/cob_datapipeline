from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

from airflow.sdk import dag, task, Connection
from tulflow.solr_api_utils import SolrApiUtils
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.slack.notifications.slack import send_slack_notification


BACKUP_LOCATION = "/srv/backups"
BACKUP_RETENTION_DAYS = 30

slackpostonsuccess = send_slack_notification(channel="infra_alerts", username="airflow", text=":partygritty: {{ dag_run.logical_date }} DAG {{ dag.dag_id }} success: {{ ti.log_url }}")
slackpostonfail = send_slack_notification(channel="infra_alerts", username="airflow", text=":poop: Task failed: {{ dag.dag_id }} {{ ti.task_id }} {{ dag_run.logical_date }} {{ ti.log_url }}")

def get_solr_db():
    conn = Connection.get("SOLRCLOUD-WRITER")
    return SolrApiUtils(
        solr_url=conn.host,
        auth_user=conn.login,
        auth_pass=conn.password,
    )

def backup_collection(collection: str):
    backup_path = f"/solr/admin/collections?action=BACKUP&name={collection}&collection={collection}&location={BACKUP_LOCATION}"
    response = get_solr_db().get_from_solr_api(backup_path, timeout=(10, 900))
    if response.status_code == 200:
        print(f"Successfully backed up collection: {collection}")
    else:
        raise Exception(f"Failed to back up collection: {collection}")


def _backup_api_path(action: str, collection: str, **params):
    query = {
        "action": action,
        "name": collection,
        "location": BACKUP_LOCATION,
        **params,
    }
    return "/solr/admin/collections?" + urlencode(query)


def _solr_response_json(response, operation: str):
    if response.status_code != 200:
        raise RuntimeError(f"Failed to {operation}: HTTP {response.status_code}")

    payload = response.json()
    response_header = payload.get("responseHeader", {})
    if response_header.get("status", 0) != 0:
        raise RuntimeError(f"Failed to {operation}: {payload}")
    return payload


def delete_old_backups(collection: str, now=None):
    solr_db = get_solr_db()
    list_path = _backup_api_path("LISTBACKUP", collection)
    list_response = solr_db.get_from_solr_api(list_path, timeout=(10, 900))
    backup_points = _solr_response_json(
        list_response, f"list backups for collection {collection}"
    ).get("backups", [])

    if not backup_points:
        return

    current_time = now or datetime.now(timezone.utc)
    if current_time.tzinfo is None:
        current_time = current_time.replace(tzinfo=timezone.utc)
    cutoff = current_time - timedelta(days=BACKUP_RETENTION_DAYS)
    parsed_points = []
    for backup_point in backup_points:
        backup_id = backup_point["backupId"]
        start_time = datetime.fromisoformat(
            backup_point["startTime"].replace("Z", "+00:00")
        )
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        parsed_points.append((start_time, backup_id, backup_point))

    newest_backup_id = max(parsed_points, key=lambda point: point[0])[1]
    old_points = [
        backup_point
        for start_time, backup_id, backup_point in parsed_points
        if start_time < cutoff and backup_id != newest_backup_id
    ]

    for backup_point in old_points:
        delete_path = _backup_api_path(
            "DELETEBACKUP", collection, backupId=backup_point["backupId"]
        )
        delete_response = solr_db.get_from_solr_api(delete_path, timeout=(10, 900))
        _solr_response_json(
            delete_response,
            f"delete backup {backup_point['backupId']} for collection {collection}",
        )

    if old_points:
        purge_path = _backup_api_path("DELETEBACKUP", collection, purgeUnused="true")
        purge_response = solr_db.get_from_solr_api(purge_path, timeout=(10, 900))
        _solr_response_json(
            purge_response, f"purge unused backup files for collection {collection}"
        )

# Define the DAG using TaskFlow API
@dag(
        dag_id="backup_collections",
        start_date=datetime(2023, 9, 17),
        schedule="0 6 * * *",
        catchup=False,
        on_failure_callback=[slackpostonfail],
        default_args={ "retries": 2 },
        )
def backup_collections_dag():

    # Task to get the list of collections
    @task
    def get_collections():
        return get_solr_db().get_collections()

    # Task to iterate over the collections and trigger backups
    @task
    def backup_collections(collections: list):
        for collection in collections:
            backup_collection(collection)

    # Delete only backup points older than the retention period.
    @task
    def delete_old_solr_backups(collections: list):
        for collection in collections:
            delete_old_backups(collection)

    # Post Success
    success = EmptyOperator(
            task_id="slack_success_post",
            on_success_callback=[slackpostonsuccess],
           )

    # Set up the task dependencies
    collections = get_collections()
    backup_task = backup_collections(collections)
    delete_task = delete_old_solr_backups(collections)
    backup_task >> delete_task >> success


# Instantiate the DAG
backup_collections_dag = backup_collections_dag()
