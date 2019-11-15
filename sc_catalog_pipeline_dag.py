"""Airflow DAG to perform a partial re-index of tul_cob catalog from OAI into Solr."""
from datetime import datetime, timedelta
import airflow
from airflow.contrib.operators.s3_list_operator import S3ListOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from cob_datapipeline.sc_xml_parse import prepare_oai_boundwiths, delete_oai_solr, update_variables
from cob_datapipeline.task_sc_get_num_docs import task_solrgetnumdocs
from tulflow.harvest import oai_to_s3, perform_xml_lookup
from tulflow.tasks import create_sc_collection, execute_slackpostonfail, get_solr_url, swap_sc_alias
from cob_datapipeline.task_slackpost import task_slackpostonsuccess

"""
INIT SYSTEMWIDE VARIABLES

check for existence of systemwide variables shared across tasks that can be
initialized here if not found (i.e. if this is a new installation)
"""

AIRFLOW_HOME = Variable.get("AIRFLOW_HOME")
AIRFLOW_USER_HOME = Variable.get("AIRFLOW_USER_HOME")

# Alma OAI Harvest Variables
ALMA_OAI_PUBLISH_INTERVAL = Variable.get("ALMA_OAI_PUBLISH_INTERVAL")
ALMA_OAI_DELTA = timedelta(hours=int(ALMA_OAI_PUBLISH_INTERVAL))
ALMA_HARVEST_FROM_DATE = Variable.get("ALMA_HARVEST_FROM_DATE")
NOW = datetime.now().strftime("%Y-%m-%dT%H:%M:%Sz")
ALMA_HARVEST_UNTIL_DATE = Variable.get("ALMA_HARVEST_UNTIL_DATE", default_var=NOW)
ALMA_UNTIL_DATE_RAW = datetime.strptime(ALMA_HARVEST_UNTIL_DATE, "%Y-%m-%dT%H:%M:%Sz")
ALMA_HARVEST_FROM_DATE_NEW = (ALMA_UNTIL_DATE_RAW - ALMA_OAI_DELTA).strftime("%Y-%m-%dT%H:%M:%Sz")
ALMA_OAI_MD_PREFIX = "marc21"
ALMA_OAI_SET = "blacklight"
ALMA_OAI_BW_SET = "blacklight-bw"
ALMA_OAI_ENDPOINT = Variable.get("ALMA_OAI_ENDPOINT")

# cob_index Indexer Library Variables
GIT_BRANCH = Variable.get("GIT_PULL_TULCOB_BRANCH_NAME")
LATEST_RELEASE = Variable.get("GIT_PULL_TULCOB_LATEST_RELEASE")

# Get Solr URL & Collection Name for indexing info; error out if not entered
SOLR_CONN = BaseHook.get_connection("SOLRCLOUD")
ALIAS = Variable.get("CATALOG_CONFIGSET")
SOLR_URL = get_solr_url(SOLR_CONN, ALIAS)

# Get S3 data bucket variables
AIRFLOW_S3 = BaseHook.get_connection("AIRFLOW_S3")
TIMESTAMP = "{{ execution_date.strftime('%Y-%m-%d_%H-%M-%S') }}"
AIRFLOW_DATA_BUCKET = Variable.get("AIRFLOW_DATA_BUCKET")

# CREATE DAG
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2018, 12, 13, 3),
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": execute_slackpostonfail,
    "provide_context": True,
    "retries": 0,
    "retry_delay": timedelta(minutes=10)
}

DAG = airflow.DAG(
    "sc_catalog_pipeline",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    catchup=False,
    schedule_interval=timedelta(hours=int(ALMA_OAI_PUBLISH_INTERVAL))
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
    conn_id=SOLR_CONN.conn_id
)

BW_OAI_HARVEST = PythonOperator(
    task_id='bw_oai_harvest',
    provide_context=True,
    python_callable=oai_to_s3,
    op_kwargs={
        "access_id": AIRFLOW_S3.login,
        "access_secret": AIRFLOW_S3.password,
        "bucket_name": AIRFLOW_DATA_BUCKET,
        "harvest_from_date": ALMA_HARVEST_FROM_DATE,
        "harvest_until_date": ALMA_HARVEST_UNTIL_DATE,
        "metadata_prefix": ALMA_OAI_MD_PREFIX,
        "oai_endpoint": ALMA_OAI_ENDPOINT,
        "records_per_file": 10000,
        "s3_conn": AIRFLOW_S3,
        "set": ALMA_OAI_BW_SET,
        "timestamp": TIMESTAMP + "/bw"
    },
    dag=DAG
)

LIST_ALMA_BW_S3_DATA = S3ListOperator(
    task_id="list_alma_bw_s3_data",
    bucket=AIRFLOW_DATA_BUCKET,
    prefix=DAG.dag_id + "/" + TIMESTAMP  + "/bw/",
    delimiter="/",
    aws_conn_id=AIRFLOW_S3.conn_id,
    dag=DAG
)

PREPARE_BOUNDWITHS = PythonOperator(
    task_id='prepare_boundwiths',
    provide_context=True,
    python_callable=prepare_oai_boundwiths,
    op_kwargs={
        "AWS_ACCESS_KEY_ID": AIRFLOW_S3.login,
        "AWS_SECRET_ACCESS_KEY": AIRFLOW_S3.password,
        "BUCKET": AIRFLOW_DATA_BUCKET,
        "DEST_FOLDER": DAG.dag_id + "/" + TIMESTAMP + "/lookup.tsv",
        "S3_KEYS": "{{ ti.xcom_pull(task_ids='list_alma_bw_s3_data') }}",
        "SOURCE_FOLDER": DAG.dag_id + "/" + TIMESTAMP  + "/bw"
    },
    dag=DAG
)

OAI_HARVEST = PythonOperator(
    task_id='oai_harvest',
    provide_context=True,
    python_callable=oai_to_s3,
    op_kwargs={
        "access_id": AIRFLOW_S3.login,
        "access_secret": AIRFLOW_S3.password,
        "bucket_name": AIRFLOW_DATA_BUCKET,
        "harvest_from_date": ALMA_HARVEST_FROM_DATE,
        "harvest_until_date": ALMA_HARVEST_UNTIL_DATE,
        "lookup_key": DAG.dag_id + "/" + TIMESTAMP + "/lookup.tsv",
        "metadata_prefix": ALMA_OAI_MD_PREFIX,
        "oai_endpoint": ALMA_OAI_ENDPOINT,
        "parser": perform_xml_lookup,
        "records_per_file": 1000,
        "set": ALMA_OAI_SET,
        "timestamp": TIMESTAMP
    },
    dag=DAG
)

SC_INDEX_OAI_MARC = BashOperator(
    task_id="index_updates_oai_marc",
    bash_command=AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/sc_ingest_marc.sh ",
    env={
        "AWS_ACCESS_KEY_ID": AIRFLOW_S3.login,
        "AWS_SECRET_ACCESS_KEY": AIRFLOW_S3.password,
        "BUCKET": AIRFLOW_DATA_BUCKET,
        "FOLDER": DAG.dag_id + "/" + TIMESTAMP  + "/new-updated",
        "GIT_BRANCH": GIT_BRANCH,
        "HOME": AIRFLOW_USER_HOME,
        "LATEST_RELEASE": LATEST_RELEASE,
        "SOLR_AUTH_USER": SOLR_CONN.login,
        "SOLR_AUTH_PASSWORD": SOLR_CONN.password,
        "SOLR_URL": SOLR_URL
    },
    dag=DAG
)

SC_DELETE_OAI_MARC = PythonOperator(
    task_id="index_deletes_oai_marc",
    provide_context=True,
    python_callable=delete_oai_solr,
    op_kwargs={
        "ALIAS": ALIAS,
        "AWS_ACCESS_KEY_ID": AIRFLOW_S3.login,
        "AWS_SECRET_ACCESS_KEY": AIRFLOW_S3.password,
        "BUCKET": AIRFLOW_DATA_BUCKET,
        "SOURCE_PREFIX": DAG.dag_id + "/" + TIMESTAMP  + "/deleted",
        "OAI_PREFIX": "oai:alma.01TULI_INST:",
        "SOLR_CONN": SOLR_CONN.conn_id
    },
    dag=DAG
)

GET_NUM_SOLR_DOCS_POST = task_solrgetnumdocs(
    DAG,
    ALIAS,
    "get_num_solr_docs_post",
    conn_id=SOLR_CONN.conn_id
)

UPDATE_DATE_VARIABLES = PythonOperator(
    task_id="update_date_variables",
    provide_context=True,
    python_callable=update_variables,
    op_kwargs={
        "UPDATE": {
            "ALMA_HARVEST_FROM_DATE": ALMA_HARVEST_FROM_DATE_NEW
        }
    },
    dag=DAG
)

POST_SLACK = PythonOperator(
    task_id="slack_post_succ",
    python_callable=task_slackpostonsuccess,
    provide_context=True,
    dag=DAG
)

# SET UP TASK DEPENDENCIES
BW_OAI_HARVEST.set_upstream(GET_NUM_SOLR_DOCS_PRE)
LIST_ALMA_BW_S3_DATA.set_upstream(BW_OAI_HARVEST)
PREPARE_BOUNDWITHS.set_upstream(LIST_ALMA_BW_S3_DATA)
OAI_HARVEST.set_upstream(PREPARE_BOUNDWITHS)
SC_INDEX_OAI_MARC.set_upstream(OAI_HARVEST)
SC_DELETE_OAI_MARC.set_upstream(OAI_HARVEST)
GET_NUM_SOLR_DOCS_POST.set_upstream(SC_INDEX_OAI_MARC)
GET_NUM_SOLR_DOCS_POST.set_upstream(SC_DELETE_OAI_MARC)
UPDATE_DATE_VARIABLES.set_upstream(GET_NUM_SOLR_DOCS_POST)
POST_SLACK.set_upstream(UPDATE_DATE_VARIABLES)
