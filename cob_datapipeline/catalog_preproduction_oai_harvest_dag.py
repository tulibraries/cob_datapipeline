"""Airflow DAG to perform a partial index of tul_cob catalog from OAI into Pre Production Solr Collection."""
from datetime import datetime, timedelta
import os
from tulflow import harvest, tasks
import airflow
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.hooks.base  import BaseHook
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python  import PythonOperator
from cob_datapipeline.tasks.xml_parse import prepare_oai_boundwiths, update_variables
from cob_datapipeline.tasks.task_solr_get_num_docs import task_solrgetnumdocs
from cob_datapipeline.tasks.task_slack_posts import catalog_slackpostonsuccess
from cob_datapipeline import helpers

"""
INIT SYSTEMWIDE VARIABLES
check for existence of systemwide variables shared across tasks that can be
initialized here if not found (i.e. if this is a new installation)
"""

AIRFLOW_HOME = Variable.get("AIRFLOW_HOME")
AIRFLOW_USER_HOME = Variable.get("AIRFLOW_USER_HOME")

# Alma OAI Harvest Dates
CATALOG_OAI_PUBLISH_INTERVAL = Variable.get("CATALOG_OAI_PUBLISH_INTERVAL")
CATALOG_HARVEST_FROM_DATE = Variable.get("CATALOG_PRE_PRODUCTION_HARVEST_FROM_DATE")
CATALOG_LAST_HARVEST_FROM_DATE = Variable.get("CATALOG_PRE_PRODUCTION_LAST_HARVEST_FROM_DATE")
S3_NAME_SPACE = '{{ data_interval_start.strftime("%Y-%m-%d_%H-%M-%S") }}'
DEFAULT_HARVEST_UNTIL_DATE = '{{ data_interval_start.strftime("%Y-%m-%dT%H:%M:%SZ") }}'
CATALOG_HARVEST_UNTIL_DATE = Variable.get("CATALOG_HARVEST_UNTIL_DATE", default_var=DEFAULT_HARVEST_UNTIL_DATE)

# Alma OAI Harvest Variables (besides Dates)
CATALOG_OAI_CONFIG = Variable.get("CATALOG_OAI_CONFIG", deserialize_json=True)
# {
#   "endpoint": "https://temple.alma.exlibrisgroup.com/view/oai/01TULI_INST/request",
#   "included_sets": ["blacklight"],
#   "md_prefix": "marc21"
# }
CATALOG_OAI_MD_PREFIX = CATALOG_OAI_CONFIG.get("md_prefix")
CATALOG_OAI_INCLUDED_SETS = CATALOG_OAI_CONFIG.get("included_sets")
CATALOG_OAI_ENDPOINT = CATALOG_OAI_CONFIG.get("endpoint")

CATALOG_OAI_BW_CONFIG = Variable.get("CATALOG_OAI_BW_CONFIG", deserialize_json=True)
# {
#   "endpoint": "https://temple.alma.exlibrisgroup.com/view/oai/01TULI_INST/request",
#   "included_sets": ["blacklight-bw"],
#   "md_prefix": "marc21"
# }
CATALOG_OAI_BW_MD_PREFIX = CATALOG_OAI_BW_CONFIG.get("md_prefix")
CATALOG_OAI_BW_INCLUDED_SETS = CATALOG_OAI_BW_CONFIG.get("included_sets")
CATALOG_OAI_BW_ENDPOINT = CATALOG_OAI_BW_CONFIG.get("endpoint")

# cob_index Indexer Library Variables
COB_INDEX_VERSION = Variable.get("PRE_PRODUCTION_COB_INDEX_VERSION")

# Get Solr URL & Collection Name for indexing info; error out if not entered
SOLR_WRITER = BaseHook.get_connection("SOLRCLOUD-WRITER")
SOLR_CLOUD = BaseHook.get_connection("SOLRCLOUD")

COLLECTION = Variable.get("CATALOG_PRE_PRODUCTION_SOLR_COLLECTION")

# Get S3 data bucket variables
AIRFLOW_S3 = BaseHook.get_connection("AIRFLOW_S3")
AIRFLOW_DATA_BUCKET = Variable.get("AIRFLOW_DATA_BUCKET")

# CREATE DAG
DEFAULT_ARGS = {
    "owner": "cob",
    "depends_on_past": False,
    "start_date": datetime(2018, 12, 13, 3),
    "on_failure_callback": tasks.execute_slackpostonfail,
    "retries": 0,
    "retry_delay": timedelta(minutes=10)
}

DAG = airflow.DAG(
    "catalog_pre_production_oai_harvest",
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    schedule_interval=timedelta(hours=int(CATALOG_OAI_PUBLISH_INTERVAL))
)

"""
CREATE TASKS
Tasks with all logic contained in a single operator can be declared here.
Tasks with custom logic are relegated to individual Python files.
"""


SAFETY_CHECK = PythonOperator(
    task_id="safety_check",
    python_callable=helpers.catalog_safety_check,
    dag=DAG
)


GET_NUM_SOLR_DOCS_PRE = task_solrgetnumdocs(
    DAG,
    COLLECTION,
    "get_num_solr_docs_pre",
    conn_id=SOLR_CLOUD.conn_id
    )

BW_OAI_HARVEST = PythonOperator(
    task_id='bw_oai_harvest',
    python_callable=harvest.oai_to_s3,
    op_kwargs={
        "access_id": AIRFLOW_S3.login,
        "access_secret": AIRFLOW_S3.password,
        "bucket_name": AIRFLOW_DATA_BUCKET,
        "harvest_from_date": None,
        "harvest_until_date": None,
        "metadata_prefix": CATALOG_OAI_BW_MD_PREFIX,
        "oai_endpoint": CATALOG_OAI_BW_ENDPOINT,
        "records_per_file": 10000,
        "included_sets": CATALOG_OAI_BW_INCLUDED_SETS,
        "timestamp": f"{ S3_NAME_SPACE }/bw"
    },
    dag=DAG
)

LIST_CATALOG_BW_S3_DATA = S3ListOperator(
    task_id="list_catalog_bw_s3_data",
    bucket=AIRFLOW_DATA_BUCKET,
    prefix=DAG.dag_id + f"/{ S3_NAME_SPACE }/bw/",
    delimiter="",
    aws_conn_id=AIRFLOW_S3.conn_id,
    dag=DAG
)

PREPARE_BOUNDWITHS = PythonOperator(
    task_id='prepare_boundwiths',
    python_callable=prepare_oai_boundwiths,
    op_kwargs={
        "AWS_ACCESS_KEY_ID": AIRFLOW_S3.login,
        "AWS_SECRET_ACCESS_KEY": AIRFLOW_S3.password,
        "BUCKET": AIRFLOW_DATA_BUCKET,
        "DEST_FOLDER": DAG.dag_id + f"/{ S3_NAME_SPACE }/lookup.tsv",
        "S3_KEYS": "{{ ti.xcom_pull(task_ids='list_catalog_bw_s3_data') }}",
        "SOURCE_FOLDER": DAG.dag_id + f"/{ S3_NAME_SPACE }/bw"
    },
    dag=DAG
)

OAI_HARVEST = PythonOperator(
    task_id='oai_harvest',
    python_callable=harvest.oai_to_s3,
    op_kwargs={
        "access_id": AIRFLOW_S3.login,
        "access_secret": AIRFLOW_S3.password,
        "bucket_name": AIRFLOW_DATA_BUCKET,
        "harvest_from_date": CATALOG_HARVEST_FROM_DATE,
        "harvest_until_date": CATALOG_HARVEST_UNTIL_DATE,
        "lookup_key": DAG.dag_id + f"/{ S3_NAME_SPACE }/lookup.tsv",
        "metadata_prefix": CATALOG_OAI_MD_PREFIX,
        "oai_endpoint": CATALOG_OAI_ENDPOINT,
        "parser": harvest.perform_xml_lookup_with_cache(),
        "records_per_file": 1000,
        "included_sets": CATALOG_OAI_INCLUDED_SETS,
        "timestamp": f"{ S3_NAME_SPACE }"
    },
    dag=DAG
)

INDEX_UPDATES_OAI_MARC = BashOperator(
    task_id="index_updates_oai_marc",
    bash_command=AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/ingest_marc.sh ",
    env={**os.environ, **{
        "AWS_ACCESS_KEY_ID": AIRFLOW_S3.login,
        "AWS_SECRET_ACCESS_KEY": AIRFLOW_S3.password,
        "BUCKET": AIRFLOW_DATA_BUCKET,
        "FOLDER": DAG.dag_id + f"/{ S3_NAME_SPACE }/new-updated",
        "GIT_BRANCH": COB_INDEX_VERSION,
        "HOME": AIRFLOW_USER_HOME,
        "LATEST_RELEASE": "false",
        "SOLR_AUTH_USER": SOLR_WRITER.login or "",
        "SOLR_AUTH_PASSWORD": SOLR_WRITER.password or "",
        "SOLR_URL": tasks.get_solr_url(SOLR_WRITER, COLLECTION),
        "ALMAOAI_LAST_HARVEST_FROM_DATE": CATALOG_LAST_HARVEST_FROM_DATE,
        "COMMAND": "ingest",
    }},
    dag=DAG
)

INDEX_DELETES_OAI_MARC = BashOperator(
    task_id="index_deletes_oai_marc",
    bash_command=AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/ingest_marc.sh ",
    env={**os.environ, **{
        "AWS_ACCESS_KEY_ID": AIRFLOW_S3.login,
        "AWS_SECRET_ACCESS_KEY": AIRFLOW_S3.password,
        "BUCKET": AIRFLOW_DATA_BUCKET,
        "FOLDER": DAG.dag_id + f"/{ S3_NAME_SPACE }/deleted",
        "GIT_BRANCH": COB_INDEX_VERSION,
        "HOME": AIRFLOW_USER_HOME,
        "LATEST_RELEASE": "false",
        "SOLR_AUTH_USER": SOLR_WRITER.login or "",
        "SOLR_AUTH_PASSWORD": SOLR_WRITER.password or "",
        "SOLR_URL": tasks.get_solr_url(SOLR_WRITER, COLLECTION),
        "COMMAND": "delete --suppress",
    }},
    dag=DAG
)

SOLR_COMMIT = SimpleHttpOperator(
    task_id='solr_commit',
    method='GET',
    http_conn_id=SOLR_WRITER.conn_id,
    endpoint= '/solr/' + COLLECTION + '/update?commit=true',
    dag=DAG
)

GET_NUM_SOLR_DOCS_POST = task_solrgetnumdocs(
    DAG,
    COLLECTION,
    "get_num_solr_docs_post",
    conn_id=SOLR_CLOUD.conn_id
)

UPDATE_DATE_VARIABLES = PythonOperator(
    task_id="update_date_variables",
    python_callable=update_variables,
    op_kwargs={
        "UPDATE": {
            "CATALOG_PRE_PRODUCTION_HARVEST_FROM_DATE": CATALOG_HARVEST_UNTIL_DATE,
            "CATALOG_PRE_PRODUCTION_LAST_HARVEST_FROM_DATE": CATALOG_HARVEST_FROM_DATE,
        }
    },
    dag=DAG
)

POST_SLACK = PythonOperator(
    task_id="slack_post_succ",
    python_callable=catalog_slackpostonsuccess,
    dag=DAG
)

# SET UP TASK DEPENDENCIES
GET_NUM_SOLR_DOCS_PRE.set_upstream(SAFETY_CHECK)
BW_OAI_HARVEST.set_upstream(GET_NUM_SOLR_DOCS_PRE)
LIST_CATALOG_BW_S3_DATA.set_upstream(BW_OAI_HARVEST)
PREPARE_BOUNDWITHS.set_upstream(LIST_CATALOG_BW_S3_DATA)
OAI_HARVEST.set_upstream(PREPARE_BOUNDWITHS)
INDEX_UPDATES_OAI_MARC.set_upstream(OAI_HARVEST)
INDEX_DELETES_OAI_MARC.set_upstream(INDEX_UPDATES_OAI_MARC)
SOLR_COMMIT.set_upstream(INDEX_DELETES_OAI_MARC)
GET_NUM_SOLR_DOCS_POST.set_upstream(SOLR_COMMIT)
UPDATE_DATE_VARIABLES.set_upstream(GET_NUM_SOLR_DOCS_POST)
POST_SLACK.set_upstream(UPDATE_DATE_VARIABLES)
