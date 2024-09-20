from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from datetime import datetime
from tulflow.solr_api_utils import SolrApiUtils
from airflow.providers.slack.notifications.slack import send_slack_notification
import pdb

slackpostonsuccess = send_slack_notification(channel="infra_alerts", username="airflow", text=":partygritty: {{ dag_run.logical_date }} DAG {{ dag.dag_id }} success: {{ ti.log_url }}")
slackpostonfail = send_slack_notification(channel="infra_alerts", username="airflow", text=":poop: Task failed: {{ dag.dag_id }} {{ ti.task_id }} {{ dag_run.logical_date }} {{ ti.log_url }}")

CONN = BaseHook.get_connection("SOLRCLOUD-WRITER")
DB = SolrApiUtils(
        solr_url=CONN.host,
        auth_user=CONN.login,
        auth_pass=CONN.password,)

def backup_collection(collection: str):
    backup_path = f"/solr/admin/collections?action=BACKUP&name={collection}&collection={collection}&location=/srv/backups"
    response = DB.get_from_solr_api(backup_path)
    if response.status_code == 200:
        print(f"Successfully backed up collection: {collection}")
    else:
        raise Exception(f"Failed to back up collection: {collection}")

# Define the DAG using TaskFlow API
@dag(
        dag_id="backup_collections",
        start_date=datetime(2023, 9, 17),
        schedule_interval="@daily",  # Adjust the schedule as needed
        catchup=False,
        on_failure_callback=[slackpostonfail],
        default_args={ "retries": 2 },
        )
def backup_collections_dag():

    # Task to get the list of collections
    @task
    def get_collections():
        return DB.get_collections()

    # Task to iterate over the collections and trigger backups
    @task(on_success_callback=[slackpostonsuccess])
    def backup_collections(collections: list):
        for collection in collections:
            backup_collection(collection)

    # Set up the task dependencies
    collections = get_collections()
    backup_collections(collections)

# Instantiate the DAG
backup_collections_dag = backup_collections_dag()
