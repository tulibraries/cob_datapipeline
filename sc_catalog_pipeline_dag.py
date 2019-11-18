"""Generic Airflow DAG to perform a partial re-index of tul_cob catalog from OAI into SolrCloud."""
from datetime import datetime, timedelta
import airflow
from airflow.contrib.operators.s3_list_operator import S3ListOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from cob_datapipeline.sc_xml_parse import prepare_oai_boundwiths, delete_oai_solr, update_variables
from cob_datapipeline.task_sc_get_num_docs import task_solrgetnumdocs
from cob_datapipeline.task_slackpost import task_catalog_slackpostonsuccess
from tulflow import harvest, tasks

"""
LOCAL FUNCTIONS
Functions / any code with processing logic should be elsewhere, tested, etc.
This is where to put functions that haven't been abstracted out yet.
"""

def require_dag_run(dag_run):
    """Simple function to check an environment has been configured."""
    if not "env" in dag_run.conf:
        raise Exception("Dag run must have env configured.")

"""
INIT SYSTEMWIDE VARIABLES

check for existence of systemwide variables shared across tasks that can be
initialized here if not found (i.e. if this is a new installation)
"""

AIRFLOW_HOME = Variable.get("AIRFLOW_HOME")
AIRFLOW_USER_HOME = Variable.get("AIRFLOW_USER_HOME")

# Alma OAI Harvest Dates
ALMA_OAI_PUBLISH_INTERVAL = Variable.get("ALMA_OAI_PUBLISH_INTERVAL")
ALMA_OAI_DELTA = timedelta(hours=int(ALMA_OAI_PUBLISH_INTERVAL))
ALMA_HARVEST_FROM_DATE = Variable.get("ALMA_HARVEST_FROM_DATE")
NOW = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
ALMA_HARVEST_UNTIL_DATE = Variable.get("ALMA_HARVEST_UNTIL_DATE", default_var=NOW)
ALMA_UNTIL_DATE_RAW = datetime.strptime(ALMA_HARVEST_UNTIL_DATE, "%Y-%m-%dT%H:%M:%SZ")
ALMA_HARVEST_FROM_DATE_NEW = (ALMA_UNTIL_DATE_RAW - ALMA_OAI_DELTA).strftime("%Y-%m-%dT%H:%M:%SZ")

# Alma OAI Harvest Variables (besides Dates)
ALMA_OAI_CONFIG = Variable.get("ALMA_OAI_CONFIG", deserialize_json=True)
# {
#   "endpoint": "https://temple.alma.exlibrisgroup.com/view/oai/01TULI_INST/request",
#   "included_sets": ["blacklight"],
#   "md_prefix": "marc21"
# }
ALMA_OAI_MD_PREFIX = ALMA_OAI_CONFIG.get("md_prefix")
ALMA_OAI_INCLUDED_SETS = ALMA_OAI_CONFIG.get("included_sets")
ALMA_OAI_ENDPOINT = ALMA_OAI_CONFIG.get("endpoint")

ALMA_OAI_BW_CONFIG = Variable.get("ALMA_OAI_BW_CONFIG", deserialize_json=True)
# {
#   "endpoint": "https://temple.alma.exlibrisgroup.com/view/oai/01TULI_INST/request",
#   "included_sets": ["blacklight-bw"],
#   "md_prefix": "marc21"
# }
ALMA_OAI_BW_MD_PREFIX = ALMA_OAI_BW_CONFIG.get("md_prefix")
ALMA_OAI_BW_INCLUDED_SETS = ALMA_OAI_BW_CONFIG.get("included_sets")
ALMA_OAI_BW_ENDPOINT = ALMA_OAI_BW_CONFIG.get("endpoint")

# cob_index Indexer Library Variables
GIT_BRANCH = Variable.get("GIT_PULL_TULCOB_BRANCH_NAME")
LATEST_RELEASE = Variable.get("GIT_PULL_TULCOB_LATEST_RELEASE")

# Get Solr URL & Collection Name for indexing info; error out if not entered
SOLR_CONN = BaseHook.get_connection("SOLRCLOUD-WRITER")
COB_SOLR_CONFIG = Variable.get("COB_SOLR_CONFIG", deserialize_json=True)
# {"configset": "tul_cob-catalog-2", "replication_factor": 2}
CONFIGSET = COB_SOLR_CONFIG.get("configset")

# Get S3 data bucket variables
AIRFLOW_S3 = BaseHook.get_connection("AIRFLOW_S3")
AIRFLOW_DATA_BUCKET = Variable.get("AIRFLOW_DATA_BUCKET")

# CREATE DAG
DEFAULT_ARGS = {
    "owner": "cob",
    "depends_on_past": False,
    "start_date": datetime(2018, 12, 13, 3),
    "on_failure_callback": tasks.execute_slackpostonfail,
    "provide_context": True,
    "retries": 0,
    "retry_delay": timedelta(minutes=10)
}

DAG = airflow.DAG(
    "sc_catalog_pipeline",
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    schedule_interval=timedelta(hours=int(ALMA_OAI_PUBLISH_INTERVAL))
)

"""
CREATE TASKS

Tasks with all logic contained in a single operator can be declared here.
Tasks with custom logic are relegated to individual Python files.
"""

# Single task to provide message about which controller triggered the DAG
REMOTE_TRIGGER_MESSAGE = BashOperator(
    task_id="remote_trigger_message",
    bash_command='echo "Remote trigger: \'{{ dag_run.conf["message"] }}\'"',
    dag=DAG,
)

REQUIRE_DAG_RUN = PythonOperator(
    task_id="require_dag_run",
    python_callable=require_dag_run,
    provide_context=True,
    dag=DAG,
)

SET_COLLECTION_NAME = PythonOperator(
    task_id="set_collection_name",
    python_callable=datetime.now().strftime,
    op_args=["%Y-%m-%d_%H-%M-%S"],
    dag=DAG
)

GET_NUM_SOLR_DOCS_PRE = task_solrgetnumdocs(
    DAG,
    CONFIGSET + "-{{ dag_run.conf.get('env') }}",
    "get_num_solr_docs_pre",
    conn_id=SOLR_CONN.conn_id
)

BW_OAI_HARVEST = PythonOperator(
    task_id='bw_oai_harvest',
    provide_context=True,
    python_callable=harvest.oai_to_s3,
    op_kwargs={
        "access_id": AIRFLOW_S3.login,
        "access_secret": AIRFLOW_S3.password,
        "bucket_name": AIRFLOW_DATA_BUCKET,
        "harvest_from_date": None,
        "harvest_until_date": None,
        "metadata_prefix": ALMA_OAI_BW_MD_PREFIX,
        "oai_endpoint": ALMA_OAI_BW_ENDPOINT,
        "records_per_file": 10000,
        "included_sets": ALMA_OAI_BW_INCLUDED_SETS,
        "timestamp": "{{ ti.xcom_pull(task_ids='set_collection_name') }}/bw"
    },
    dag=DAG
)

LIST_ALMA_BW_S3_DATA = S3ListOperator(
    task_id="list_alma_bw_s3_data",
    bucket=AIRFLOW_DATA_BUCKET,
    prefix=DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/bw/",
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
        "DEST_FOLDER": DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/lookup.tsv",
        "S3_KEYS": "{{ ti.xcom_pull(task_ids='list_alma_bw_s3_data') }}",
        "SOURCE_FOLDER": DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/bw"
    },
    dag=DAG
)

OAI_HARVEST = PythonOperator(
    task_id='oai_harvest',
    provide_context=True,
    python_callable=harvest.oai_to_s3,
    op_kwargs={
        "access_id": AIRFLOW_S3.login,
        "access_secret": AIRFLOW_S3.password,
        "bucket_name": AIRFLOW_DATA_BUCKET,
        "harvest_from_date": ALMA_HARVEST_FROM_DATE,
        "harvest_until_date": ALMA_HARVEST_UNTIL_DATE,
        "lookup_key": DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/lookup.tsv",
        "metadata_prefix": ALMA_OAI_MD_PREFIX,
        "oai_endpoint": ALMA_OAI_ENDPOINT,
        "parser": harvest.perform_xml_lookup,
        "records_per_file": 1000,
        "included_sets": ALMA_OAI_INCLUDED_SETS,
        "timestamp": "{{ ti.xcom_pull(task_ids='set_collection_name') }}"
    },
    dag=DAG
)

INDEX_UPDATES_OAI_MARC = BashOperator(
    task_id="index_updates_oai_marc",
    bash_command=AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/sc_ingest_marc.sh ",
    env={
        "AWS_ACCESS_KEY_ID": AIRFLOW_S3.login,
        "AWS_SECRET_ACCESS_KEY": AIRFLOW_S3.password,
        "BUCKET": AIRFLOW_DATA_BUCKET,
        "FOLDER": DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/new-updated",
        "GIT_BRANCH": GIT_BRANCH,
        "HOME": AIRFLOW_USER_HOME,
        "LATEST_RELEASE": LATEST_RELEASE,
        "SOLR_AUTH_USER": SOLR_CONN.login,
        "SOLR_AUTH_PASSWORD": SOLR_CONN.password,
        "SOLR_URL": tasks.get_solr_url(SOLR_CONN, CONFIGSET + "-{{ ti.xcom_pull(task_ids='set_collection_name') }}"),
    },
    dag=DAG
)

INDEX_DELETES_OAI_MARC = PythonOperator(
    task_id="index_deletes_oai_marc",
    provide_context=True,
    python_callable=delete_oai_solr,
    op_kwargs={
        "ALIAS": CONFIGSET + "-{{ ti.xcom_pull(task_ids='set_collection_name') }}",
        "AWS_ACCESS_KEY_ID": AIRFLOW_S3.login,
        "AWS_SECRET_ACCESS_KEY": AIRFLOW_S3.password,
        "BUCKET": AIRFLOW_DATA_BUCKET,
        "OAI_PREFIX": "oai:alma.01TULI_INST:",
        "SOLR_CONN": SOLR_CONN.conn_id,
        "SOURCE_PREFIX": DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/deleted"
    },
    dag=DAG
)

GET_NUM_SOLR_DOCS_POST = task_solrgetnumdocs(
    DAG,
    CONFIGSET + "-{{ ti.xcom_pull(task_ids='set_collection_name') }}",
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
    python_callable=task_catalog_slackpostonsuccess,
    provide_context=True,
    dag=DAG
)

# SET UP TASK DEPENDENCIES
REQUIRE_DAG_RUN.set_upstream(REMOTE_TRIGGER_MESSAGE)
GET_NUM_SOLR_DOCS_PRE.set_upstream(REQUIRE_DAG_RUN)
SET_COLLECTION_NAME.set_upstream(GET_NUM_SOLR_DOCS_PRE)
BW_OAI_HARVEST.set_upstream(SET_COLLECTION_NAME)
LIST_ALMA_BW_S3_DATA.set_upstream(BW_OAI_HARVEST)
PREPARE_BOUNDWITHS.set_upstream(LIST_ALMA_BW_S3_DATA)
OAI_HARVEST.set_upstream(PREPARE_BOUNDWITHS)
INDEX_UPDATES_OAI_MARC.set_upstream(OAI_HARVEST)
INDEX_DELETES_OAI_MARC.set_upstream(OAI_HARVEST)
GET_NUM_SOLR_DOCS_POST.set_upstream(INDEX_UPDATES_OAI_MARC)
GET_NUM_SOLR_DOCS_POST.set_upstream(INDEX_DELETES_OAI_MARC)
UPDATE_DATE_VARIABLES.set_upstream(GET_NUM_SOLR_DOCS_POST)
POST_SLACK.set_upstream(UPDATE_DATE_VARIABLES)
