"""Airflow DAG to perform a partial index of tul_cob catalog from OAI into Pre Production Solr Collection."""
import os
import pendulum
import airflow

from datetime import timedelta
from airflow.timetables.trigger import CronTriggerTimetable
from tulflow import harvest, tasks
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from cob_datapipeline.notifiers import send_collection_notification
from cob_datapipeline.tasks.xml_parse import prepare_oai_boundwiths, update_variables
from cob_datapipeline.tasks.task_solr_get_num_docs import task_solrgetnumdocs
from cob_datapipeline import helpers
from airflow.providers.slack.notifications.slack import send_slack_notification

slackpostonsuccess = send_collection_notification(channel="blacklight_project")
slackpostonfail = send_slack_notification(channel="infra_alerts", username="airflow", text=":poop: Task failed: {{ dag.dag_id }} {{ ti.task_id }} {{ dag_run.logical_date }} {{ ti.log_url }}")

"""
INIT SYSTEMWIDE VARIABLES
check for existence of systemwide variables shared across tasks that can be
initialized here if not found (i.e. if this is a new installation)
"""

# Alma OAI Harvest Dates
AIRFLOW_HOME = "{{ var.value.AIRFLOW_HOME }}"
AIRFLOW_USER_HOME = "{{ var.value.AIRFLOW_USER_HOME }}"
CATALOG_HARVEST_FROM_DATE = "{{ var.value.CATALOG_PRE_PRODUCTION_HARVEST_FROM_DATE }}"
CATALOG_LAST_HARVEST_FROM_DATE = "{{ var.value.CATALOG_PRE_PRODUCTION_LAST_HARVEST_FROM_DATE }}"
S3_NAME_SPACE = '{{ logical_date.strftime("%Y-%m-%d_%H-%M-%S") }}'
DEFAULT_HARVEST_UNTIL_DATE = '{{ logical_date.strftime("%Y-%m-%dT%H:%M:%SZ") }}'
CATALOG_HARVEST_UNTIL_DATE = "{{ var.value.get('CATALOG_HARVEST_UNTIL_DATE', logical_date.strftime('%Y-%m-%dT%H:%M:%SZ')) }}"

# Alma OAI Harvest Variables (besides Dates)
# {
#   "endpoint": "https://temple.alma.exlibrisgroup.com/view/oai/01TULI_INST/request",
#   "included_sets": ["blacklight"],
#   "md_prefix": "marc21"
# }

# {
#   "endpoint": "https://temple.alma.exlibrisgroup.com/view/oai/01TULI_INST/request",
#   "included_sets": ["blacklight-bw"],
#   "md_prefix": "marc21"
# }
CATALOG_OAI_MD_PREFIX = "{{ var.json.CATALOG_OAI_CONFIG.md_prefix }}"
CATALOG_OAI_INCLUDED_SETS = "{{ var.json.CATALOG_OAI_CONFIG.included_sets }}"
CATALOG_OAI_ENDPOINT = "{{ var.json.CATALOG_OAI_CONFIG.endpoint }}"
CATALOG_OAI_BW_MD_PREFIX = "{{ var.json.CATALOG_OAI_BW_CONFIG.md_prefix }}"
CATALOG_OAI_BW_INCLUDED_SETS = "{{ var.json.CATALOG_OAI_BW_CONFIG.included_sets }}"
CATALOG_OAI_BW_ENDPOINT = "{{ var.json.CATALOG_OAI_BW_CONFIG.endpoint }}"

# cob_index Indexer Library Variables
COB_INDEX_VERSION = "{{ var.value.PRE_PRODUCTION_COB_INDEX_VERSION }}"

# Get Solr URL & Collection Name for indexing info; error out if not entered
COLLECTION = "{{ var.value.CATALOG_PRE_PRODUCTION_SOLR_COLLECTION }}"
SOLR_WRITER_URL = (
    tasks.get_solr_url_template("SOLRCLOUD-WRITER", COLLECTION)
)

# Get S3 data bucket variables
AIRFLOW_DATA_BUCKET = "{{ var.value.AIRFLOW_DATA_BUCKET }}"

# CREATE DAG
DEFAULT_ARGS = {
    "owner": "cob",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2018, 12, 13, tz="America/New_York"),
    "on_failure_callback": [slackpostonfail],
    "retries": 0,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=10)
}

DAG = airflow.DAG(
    "catalog_pre_production_oai_harvest_v2",
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    render_template_as_native_obj=True,
    schedule=CronTriggerTimetable("0 3/3 * * *", timezone="America/New_York")
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
    conn_id="SOLRCLOUD"
    )

BW_OAI_HARVEST = PythonOperator(
    task_id='bw_oai_harvest',
    python_callable=harvest.oai_to_s3,
    op_kwargs={
        "access_id": "{{ conn.AIRFLOW_S3.login }}",
        "access_secret": "{{ conn.AIRFLOW_S3.password }}",
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
    aws_conn_id="AIRFLOW_S3",
    dag=DAG
)

PREPARE_BOUNDWITHS = PythonOperator(
    task_id='prepare_boundwiths',
    python_callable=prepare_oai_boundwiths,
    op_kwargs={
        **helpers.airflow_s3_env(),
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
        **helpers.airflow_s3_access_kwargs(),
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

LIST_UPDATED_FILES = S3ListOperator(
        task_id="list_updated_files",
        bucket=AIRFLOW_DATA_BUCKET,
        prefix=DAG.dag_id + f"/{ S3_NAME_SPACE }/new-updated/",
        delimiter="",
        aws_conn_id="AIRFLOW_S3",
        trigger_rule="none_failed_min_one_success",
        )

INDEX_UPDATES_OAI_MARC = BashOperator(
    task_id="index_updates_oai_marc",
    bash_command=AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/ingest_marc.sh ",
    env={
        **helpers.airflow_s3_env(),
        **helpers.solr_auth_env(),
        "BUCKET": AIRFLOW_DATA_BUCKET,
        "GIT_BRANCH": COB_INDEX_VERSION,
        "HOME": AIRFLOW_USER_HOME,
        "LATEST_RELEASE": "false",
        "SOLR_URL": SOLR_WRITER_URL,
        "ALMAOAI_LAST_HARVEST_FROM_DATE": CATALOG_LAST_HARVEST_FROM_DATE,
        "COMMAND": "ingest",
        "DATA": '\'{{ ti.xcom_pull(task_ids="list_updated_files") | tojson }}\'',
    },
    trigger_rule="none_failed_min_one_success",
    execution_timeout=timedelta(hours=24),
    dag=DAG
)

LIST_DELETED_FILES = S3ListOperator(
        task_id="list_deleted_files",
        bucket=AIRFLOW_DATA_BUCKET,
        prefix=DAG.dag_id + f"/{ S3_NAME_SPACE }/deleted/",
        delimiter="",
        aws_conn_id="AIRFLOW_S3",
        trigger_rule="none_failed_min_one_success",
        )

INDEX_DELETES_OAI_MARC = BashOperator(
    task_id="index_deletes_oai_marc",
    bash_command=AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/ingest_marc.sh ",
    env={
        **helpers.airflow_s3_env(),
        **helpers.solr_auth_env(),
        "BUCKET": AIRFLOW_DATA_BUCKET,
        "GIT_BRANCH": COB_INDEX_VERSION,
        "HOME": AIRFLOW_USER_HOME,
        "LATEST_RELEASE": "false",
        "SOLR_URL": SOLR_WRITER_URL,
        "COMMAND": "delete --suppress",
        "DATA": '\'{{ ti.xcom_pull(task_ids="list_deleted_files") | tojson }}\'',
    },
    trigger_rule="none_failed_min_one_success",
    dag=DAG
)

SOLR_COMMIT = HttpOperator(
    task_id='solr_commit',
    method='GET',
    http_conn_id="SOLRCLOUD-WRITER",
    endpoint= '/solr/' + COLLECTION + '/update?commit=true',
    trigger_rule="none_failed_min_one_success",
    dag=DAG
)

GET_NUM_SOLR_DOCS_POST = task_solrgetnumdocs(
    DAG,
    COLLECTION,
    "get_num_solr_docs_post",
    conn_id="SOLRCLOUD"
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

CHOOSE_INDEXING_BRANCH = BranchPythonOperator(
        task_id="choose_indexing_branch",
        python_callable=helpers.choose_indexing_branch,
        dag=DAG)

NO_UPDATES_NO_DELETES_BRANCH = EmptyOperator(task_id = 'no_updates_no_deletes_branch', dag=DAG)
UPDATES_ONLY_BRANCH = EmptyOperator(task_id = 'updates_only_branch', dag=DAG)
DELETES_ONLY_BRANCH = EmptyOperator(task_id = 'deletes_only_branch', dag=DAG)
UPDATES_AND_DELETES_BRANCH = EmptyOperator(task_id = 'updates_and_deletes_branch', dag=DAG)
SUCCESS = EmptyOperator(
        task_id = 'success',
        on_success_callback=[slackpostonsuccess],
        trigger_rule="none_failed_min_one_success",
        dag=DAG)

# SET UP TASK DEPENDENCIES
GET_NUM_SOLR_DOCS_PRE.set_upstream(SAFETY_CHECK)
BW_OAI_HARVEST.set_upstream(GET_NUM_SOLR_DOCS_PRE)
LIST_CATALOG_BW_S3_DATA.set_upstream(BW_OAI_HARVEST)
PREPARE_BOUNDWITHS.set_upstream(LIST_CATALOG_BW_S3_DATA)
OAI_HARVEST.set_upstream(PREPARE_BOUNDWITHS)
CHOOSE_INDEXING_BRANCH.set_upstream(OAI_HARVEST)

# updates_only
(CHOOSE_INDEXING_BRANCH
 >> UPDATES_ONLY_BRANCH
 >> LIST_UPDATED_FILES
 >> INDEX_UPDATES_OAI_MARC
 >> SOLR_COMMIT
 >> GET_NUM_SOLR_DOCS_POST
 >> UPDATE_DATE_VARIABLES
 >> SUCCESS)

# deletes_only
(CHOOSE_INDEXING_BRANCH
 >> DELETES_ONLY_BRANCH
 >> LIST_DELETED_FILES
 >> INDEX_DELETES_OAI_MARC
 >> SOLR_COMMIT
 >> GET_NUM_SOLR_DOCS_POST
 >> UPDATE_DATE_VARIABLES
 >> SUCCESS)

# updates_and_deletes
(CHOOSE_INDEXING_BRANCH
 >> UPDATES_AND_DELETES_BRANCH
 >> LIST_UPDATED_FILES
 >> INDEX_UPDATES_OAI_MARC
 >> LIST_DELETED_FILES
 >> INDEX_DELETES_OAI_MARC
 >> SOLR_COMMIT
 >> GET_NUM_SOLR_DOCS_POST
 >> UPDATE_DATE_VARIABLES
 >> SUCCESS)

# no_updates_no_deletes
(CHOOSE_INDEXING_BRANCH
 >> NO_UPDATES_NO_DELETES_BRANCH
 >> SUCCESS)
