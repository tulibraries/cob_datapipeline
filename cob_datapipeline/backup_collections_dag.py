from airflow.sdk import dag, task, Connection
from datetime import datetime
from tulflow.solr_api_utils import SolrApiUtils
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.slack.notifications.slack import send_slack_notification
from airflow.providers.ssh.operators.ssh import SSHOperator

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
    backup_path = f"/solr/admin/collections?action=BACKUP&name={collection}&collection={collection}&location=/srv/backups"
    response = get_solr_db().get_from_solr_api(backup_path)
    if response.status_code == 200:
        print(f"Successfully backed up collection: {collection}")
    else:
        raise Exception(f"Failed to back up collection: {collection}")

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

    # Delete the old backups
    delete_task = SSHOperator(
            task_id="delete_old_solr_backups",
            ssh_conn_id="SOLR_NETWORKED_DRIVE",
            command="sudo find /backups/ -type d -mindepth 1 -maxdepth 1 -mtime +30 -exec rm -rf {} +",
            cmd_timeout=None,
            )

    # Post Success
    success = EmptyOperator(
            task_id="slack_success_post",
            on_success_callback=[slackpostonsuccess],
           )

    # Set up the task dependencies
    collections = get_collections()
    backup_collections(collections) >> delete_task >> success


# Instantiate the DAG
backup_collections_dag = backup_collections_dag()
