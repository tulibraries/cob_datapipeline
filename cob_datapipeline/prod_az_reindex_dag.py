"""Airflow DAG to index AZ Databases into Solr."""
from datetime import datetime, timedelta
import airflow
import os
import pendulum
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from cob_datapipeline.notifiers import send_collection_notification
from cob_datapipeline.tasks.task_solr_get_num_docs import task_solrgetnumdocs
from cob_datapipeline.operators import\
        PushVariable, DeleteAliasListVariable, DeleteCollectionListVariable
from tulflow import tasks
from airflow.providers.slack.notifications.slack import send_slack_notification

slackpostonsuccess = send_collection_notification(channel="blacklight_project")
slackpostonfail = send_slack_notification(channel="infra_alerts", username="airflow", text=":poop: Task failed: {{ dag.dag_id }} {{ ti.task_id }} {{ dag_run.logical_date }} {{ ti.log_url }}")


"""
INIT SYSTEMWIDE VARIABLES

check for existence of systemwide variables shared across tasks that can be
initialized here if not found (i.e. if this is a new installation) & defaults exist
"""

AIRFLOW_HOME = Variable.get("AIRFLOW_HOME")
AIRFLOW_USER_HOME = Variable.get("AIRFLOW_USER_HOME")
SCHEDULE_INTERVAL = Variable.get("AZ_INDEX_SCHEDULE_INTERVAL")

# Get Solr URL & Collection Name for indexing info; error out if not entered
SOLR_CONN = BaseHook.get_connection("SOLRCLOUD-WRITER")
SOLR_CONFIG = Variable.get("AZ_SOLR_CONFIG", deserialize_json=True)
# {"configset": "tul_cob-az-2", "replication_factor": 4}
CONFIGSET = SOLR_CONFIG.get("configset")
ALIAS = CONFIGSET + "-prod"
REPLICATION_FACTOR = SOLR_CONFIG.get("replication_factor")

#Databases AZ Springshare creds
AZ_CLIENT_ID = Variable.get("AZ_CLIENT_ID")
AZ_CLIENT_SECRET = Variable.get("AZ_CLIENT_SECRET")
AZ_BRANCH = Variable.get("AZ_PROD_BRANCH")

# CREATE DAG
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2018, 12, 13, tz="UTC"),
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": [slackpostonfail],
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

DAG = airflow.DAG(
    "prod_az_reindex",
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    schedule=SCHEDULE_INTERVAL
)

"""
CREATE TASKS
Tasks with all logic contained in a single operator can be declared here.
Tasks with custom logic are relegated to individual Python files.
"""

SET_COLLECTION_NAME = BashOperator(
    task_id="set_collection_name",
    bash_command="echo " + pendulum.now().format("YYYY-MM-DD_HH-mm-ss"),
    dag=DAG
)

GET_NUM_SOLR_DOCS_PRE = task_solrgetnumdocs(
    DAG,
    ALIAS,
    "get_num_solr_docs_pre",
    conn_id=SOLR_CONN.conn_id
)

CREATE_COLLECTION = tasks.create_sc_collection(
    DAG,
    SOLR_CONN.conn_id,
    CONFIGSET + "-{{ ti.xcom_pull(task_ids='set_collection_name') }}",
    REPLICATION_FACTOR,
    CONFIGSET
)

INDEX_DATABASES = BashOperator(
    task_id="index_az",
    bash_command=AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/ingest_databases.sh ",
    env={**os.environ, **{
        "HOME": AIRFLOW_USER_HOME,
        "SOLR_AZ_URL": tasks.get_solr_url(SOLR_CONN, CONFIGSET + "-{{ ti.xcom_pull(task_ids='set_collection_name') }}"),
        "AZ_CLIENT_ID": AZ_CLIENT_ID,
        "AZ_CLIENT_SECRET": AZ_CLIENT_SECRET,
        "AZ_BRANCH": AZ_BRANCH,
        "SOLR_AUTH_PASSWORD": SOLR_CONN.password if SOLR_CONN.password else "",
        "SOLR_AUTH_USER": SOLR_CONN.login if SOLR_CONN.login else "",
    }},
    dag=DAG
)

GET_NUM_SOLR_DOCS_POST = task_solrgetnumdocs(
    DAG,
    CONFIGSET +"-{{ ti.xcom_pull(task_ids='set_collection_name') }}",
    "get_num_solr_docs_post",
    conn_id=SOLR_CONN.conn_id)

SOLR_ALIAS_SWAP = tasks.swap_sc_alias(
    DAG,
    SOLR_CONN.conn_id,
    CONFIGSET +"-{{ ti.xcom_pull(task_ids='set_collection_name') }}",
    ALIAS
)

PUSH_ALIAS = PushVariable(
    task_id="push_alias",
    name="AZ_PROD_ALIASES",
    value=ALIAS,
    dag=DAG)

DELETE_ALIASES = DeleteAliasListVariable(
    task_id="delete_aliases",
    solr_conn_id="SOLRCLOUD",
    list_variable="AZ_PROD_ALIASES",
    skip_from_last=2,
    skip_included=[ALIAS],
    dag=DAG)

PUSH_COLLECTION = PushVariable(
    task_id="push_collection",
    name="AZ_PROD_COLLECTIONS",
    value=CONFIGSET +"-{{ ti.xcom_pull(task_ids='set_collection_name') }}",
    dag=DAG)

DELETE_COLLECTIONS = DeleteCollectionListVariable(
    task_id="delete_collections",
    solr_conn_id="SOLRCLOUD",
    list_variable="AZ_PROD_COLLECTIONS",
    skip_from_last=2,
    skip_included=[CONFIGSET +"-{{ ti.xcom_pull(task_ids='set_collection_name') }}"],
    on_success_callback=[slackpostonsuccess],
    dag=DAG)

# SET UP TASK DEPENDENCIES
SET_COLLECTION_NAME.set_upstream(GET_NUM_SOLR_DOCS_PRE)
CREATE_COLLECTION.set_upstream(SET_COLLECTION_NAME)
INDEX_DATABASES.set_upstream(CREATE_COLLECTION)
GET_NUM_SOLR_DOCS_POST.set_upstream(INDEX_DATABASES)
SOLR_ALIAS_SWAP.set_upstream(GET_NUM_SOLR_DOCS_POST)
PUSH_ALIAS.set_upstream(SOLR_ALIAS_SWAP)
DELETE_ALIASES.set_upstream(PUSH_ALIAS)
PUSH_COLLECTION.set_upstream(DELETE_ALIASES)
DELETE_COLLECTIONS.set_upstream(PUSH_COLLECTION)
