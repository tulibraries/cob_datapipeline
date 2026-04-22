"""Airflow DAG to index GENCON collections into Solr."""
import airflow
import pendulum

from datetime import timedelta
from tulflow import tasks
from airflow.models import Variable
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.slack.notifications.slack import send_slack_notification
from cob_datapipeline import helpers
from cob_datapipeline.notifiers import send_collection_notification
from cob_datapipeline.operators import (
    DeleteAliasListVariable,
    DeleteCollectionListVariable,
    PushVariable,
)
from cob_datapipeline.tasks.task_solr_get_num_docs import task_solrgetnumdocs

slackpostonsuccess = send_collection_notification(channel="gen-con")
slackpostonfail = send_slack_notification(
    channel="infra_alerts",
    username="airflow",
    text=":poop: Task failed: {{ dag.dag_id }} {{ ti.task_id }} {{ dag_run.logical_date }} {{ ti.log_url }}",
)

"""
INIT SYSTEMWIDE VARIABLES

check for existence of systemwide variables shared across tasks that can be
initialized here if not found (i.e. if this is a new installation) & defaults exist
"""

AIRFLOW_HOME = "{{ var.value.AIRFLOW_HOME }}"
AIRFLOW_USER_HOME = "{{ var.value.AIRFLOW_USER_HOME }}"

SCHEDULE = Variable.get("GENCON_INDEX_SCHEDULE")

GENCON_INDEX_BRANCH = "{{ var.value.GENCON_INDEX_BRANCH }}"
GENCON_TEMP_PATH = "{{ var.value.GENCON_TEMP_PATH }}"
GENCON_CSV_S3 = "{{ var.value.GENCON_CSV_S3 }}"

# Get S3 data bucket variables
AIRFLOW_DATA_BUCKET = "{{ var.value.AIRFLOW_DATA_BUCKET }}"

# Get Solr URL & Collection Name for indexing info; error out if not entered
SOLR_CONN_ID = "SOLRCLOUD-WRITER"
CONFIGSET = "{{ var.json.GENCON_SOLR_CONFIG.configset }}"
ALIAS = CONFIGSET + "-prod"
REPLICATION_FACTOR = "{{ var.json.GENCON_SOLR_CONFIG.replication_factor }}"
COLLECTION = CONFIGSET + "-{{ ti.xcom_pull(task_ids='set_collection_name') }}"
SOLR_URL = tasks.get_solr_url_template(SOLR_CONN_ID, COLLECTION)

# CREATE DAG
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2018, 12, 13, tz="UTC"),
    "on_failure_callback": [slackpostonfail],
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

DAG = airflow.DAG(
    "gencon_index",
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    schedule=SCHEDULE,
)

"""
CREATE TASKS
Tasks with all logic contained in a single operator can be declared here.
Tasks with custom logic are relegated to individual Python files.
"""

GET_NUM_SOLR_DOCS_PRE = task_solrgetnumdocs(
    DAG,
    ALIAS,
    "get_num_solr_docs_pre",
    conn_id=SOLR_CONN_ID,
)

SET_COLLECTION_NAME = BashOperator(
    task_id="set_collection_name",
    bash_command="echo {{ logical_date.strftime('%Y-%m-%d_%H-%M-%S') }}",
    dag=DAG,
)

SAFETY_CHECK = PythonOperator(
    task_id="safety_check",
    python_callable=helpers.solr_reindex_safety_check,
    op_kwargs={
        "alias": ALIAS,
        "collection": COLLECTION,
    },
    dag=DAG,
)

CREATE_COLLECTION = tasks.create_sc_collection(
    DAG,
    SOLR_CONN_ID,
    COLLECTION,
    REPLICATION_FACTOR,
    CONFIGSET,
)

INDEX_GENCON = BashOperator(
    task_id="index_gencon",
    bash_command=AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/ingest_gencon.sh ",
    retries=1,
    env={
        **helpers.airflow_s3_env(),
        **helpers.solr_auth_env(),
        "BUCKET": AIRFLOW_DATA_BUCKET,
        "GIT_BRANCH": GENCON_INDEX_BRANCH,
        "HOME": AIRFLOW_USER_HOME,
        "GENCON_TEMP_PATH": GENCON_TEMP_PATH,
        "GENCON_CSV_S3": GENCON_CSV_S3,
        "SOLR_URL": SOLR_URL,
    },
    dag=DAG,
)

GET_NUM_SOLR_DOCS_POST = task_solrgetnumdocs(
    DAG,
    COLLECTION,
    "get_num_solr_docs_post",
    conn_id=SOLR_CONN_ID,
)

SOLR_ALIAS_SWAP = tasks.swap_sc_alias(
    DAG,
    SOLR_CONN_ID,
    COLLECTION,
    ALIAS,
)

PUSH_ALIAS = PushVariable(
    task_id="push_alias",
    name="GENCON_PROD_ALIASES",
    value=ALIAS,
    dag=DAG,
)

DELETE_ALIASES = DeleteAliasListVariable(
    task_id="delete_aliases",
    solr_conn_id="SOLRCLOUD",
    list_variable="GENCON_PROD_ALIASES",
    skip_from_last=2,
    skip_included=[ALIAS],
    dag=DAG,
)

PUSH_COLLECTION = PushVariable(
    task_id="push_collection",
    name="GENCON_PROD_COLLECTIONS",
    value=COLLECTION,
    dag=DAG,
)

DELETE_COLLECTIONS = DeleteCollectionListVariable(
    task_id="delete_collections",
    solr_conn_id="SOLRCLOUD",
    list_variable="GENCON_PROD_COLLECTIONS",
    skip_from_last=2,
    skip_included=[COLLECTION],
    on_success_callback=[slackpostonsuccess],
    dag=DAG,
)

# SET UP TASK DEPENDENCIES
SET_COLLECTION_NAME.set_upstream(GET_NUM_SOLR_DOCS_PRE)
SAFETY_CHECK.set_upstream(SET_COLLECTION_NAME)
CREATE_COLLECTION.set_upstream(SAFETY_CHECK)
INDEX_GENCON.set_upstream(CREATE_COLLECTION)
GET_NUM_SOLR_DOCS_POST.set_upstream(INDEX_GENCON)
SOLR_ALIAS_SWAP.set_upstream(GET_NUM_SOLR_DOCS_POST)
PUSH_ALIAS.set_upstream(SOLR_ALIAS_SWAP)
DELETE_ALIASES.set_upstream(PUSH_ALIAS)
PUSH_COLLECTION.set_upstream(DELETE_ALIASES)
DELETE_COLLECTIONS.set_upstream(PUSH_COLLECTION)
