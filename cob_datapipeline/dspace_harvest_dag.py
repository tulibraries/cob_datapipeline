"""Airflow DAG to harvest DSpace electronic theses and dissertations"""
import os
import pendulum
import airflow

from datetime import timedelta
from tulflow import harvest
from cob_datapipeline import helpers
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from cob_datapipeline.operators.batch_s3_to_sftp_operator import BatchS3ToSFTPOperator
from airflow.providers.slack.notifications.slack import send_slack_notification

slackpostonfail = send_slack_notification(channel="infra_alerts", username="airflow", text=":poop: Task failed: {{ dag.dag_id }} {{ ti.task_id }} {{ dag_run.logical_date }} {{ ti.log_url }}")

"""
INIT SYSTEMWIDE VARIABLES
check for existence of systemwide variables shared across tasks that can be
initialized here if not found (i.e. if this is a new installation)
"""

AIRFLOW_HOME = "{{ var.value.AIRFLOW_HOME }}"
AIRFLOW_USER_HOME = "{{ var.value.AIRFLOW_USER_HOME }}"

# Get S3 data bucket variables
AIRFLOW_DATA_BUCKET = "{{ var.value.AIRFLOW_DATA_BUCKET }}"
S3_NAME_SPACE = '{{ logical_date.strftime("%Y-%m-%d_%H-%M-%S") }}'

# OAI Harvest Variables
DSPACE_HARVEST_FROM_DATE = "{{ var.value.DSPACE_HARVEST_FROM_DATE }}"
DEFAULT_HARVEST_UNTIL_DATE = '{{ logical_date.strftime("%Y-%m-%dT%H:%M:%SZ") }}'
DSPACE_HARVEST_UNTIL_DATE = "{{ var.value.get('DSPACE_HARVEST_UNTIL_DATE', logical_date.strftime('%Y-%m-%dT%H:%M:%SZ')) }}"
DSPACE_OAI_MD_PREFIX = "{{ var.json.DSPACE_OAI_CONFIG.md_prefix }}"
DSPACE_OAI_ENDPOINT = "{{ var.json.DSPACE_OAI_CONFIG.endpoint }}"
DSPACE_OAI_INCLUDED_SETS = "{{ var.json.DSPACE_OAI_CONFIG.get('included_sets', [var.json.DSPACE_OAI_CONFIG.setspec]) }}"

# CREATE DAG
DEFAULT_ARGS = {
    'owner': 'cob',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2018, 12, 13, tz="UTC"),
    "on_failure_callback": [slackpostonfail],
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

DAG = airflow.DAG(
    dag_id="dspace_harvest",
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    schedule=None
)

"""
CREATE TASKS
Tasks with all logic contained in a single operator can be declared here.
Tasks with custom logic are relegated to individual Python files.
"""

OAI_HARVEST = PythonOperator(
    task_id='oai_harvest',
    python_callable=harvest.oai_to_s3,
    op_kwargs={
        **helpers.airflow_s3_access_kwargs(),
        "bucket_name": AIRFLOW_DATA_BUCKET,
        "harvest_from_date": DSPACE_HARVEST_FROM_DATE,
        "harvest_until_date": DSPACE_HARVEST_UNTIL_DATE,
        "metadata_prefix": DSPACE_OAI_MD_PREFIX,
        "oai_endpoint": DSPACE_OAI_ENDPOINT,
        "included_sets": DSPACE_OAI_INCLUDED_SETS,
        "records_per_file": 1000,
        "timestamp": f"{ S3_NAME_SPACE }"
    },
    dag=DAG
)

CLEANUP_DATA = PythonOperator(
    task_id='cleanup_data',
    python_callable=helpers.cleanup_metadata,
    op_kwargs={
        "source_prefix": DAG.dag_id + "/" + S3_NAME_SPACE + "/new-updated",
        "destination_prefix": DAG.dag_id + "/" + S3_NAME_SPACE + "/cleaned",
        **helpers.airflow_s3_access_kwargs(),
        "bucket_name": AIRFLOW_DATA_BUCKET,
        "records_per_file": 1000,
        "timestamp": f"{ S3_NAME_SPACE }"
    },
    dag=DAG
)

XSL_TRANSFORM = BashOperator(
    task_id="xsl_transform",
    bash_command=AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/transform.sh ",
    env={
        **helpers.airflow_s3_env(),
        "BUCKET": AIRFLOW_DATA_BUCKET,
        "DAG_ID": DAG.dag_id,
        "DAG_TS": S3_NAME_SPACE,
        "DEST": "transformed",
        "HOME": AIRFLOW_USER_HOME,
        "SOURCE": "cleaned",
        "XSL_FILENAME": AIRFLOW_HOME + "/dags/cob_datapipeline/files/TEU_XOAItoMARCXML.xsl",
        "BATCH_TRANSFORM": AIRFLOW_HOME + "/dags/cob_datapipeline/files/batch_transform.xsl"
    },
    dag=DAG
)

LIST_S3_FILES = S3ListOperator(
    task_id="list_s3_files",
    bucket=AIRFLOW_DATA_BUCKET,
    prefix=DAG.dag_id + "/" + S3_NAME_SPACE + "/transformed/",
    aws_conn_id="AIRFLOW_S3",
    dag=DAG
)

S3_TO_SFTP = BatchS3ToSFTPOperator(
    task_id="s3_to_sftp",
    sftp_conn_id="DSPACESFTP",
    xcom_id="list_s3_files",
    sftp_base_path="production/",
    aws_conn_id="AIRFLOW_S3",
    s3_bucket=AIRFLOW_DATA_BUCKET,
    s3_prefix="dspace_harvest/" + S3_NAME_SPACE + "/transformed",
    dag=DAG
)

# SET UP TASK DEPENDENCIES
CLEANUP_DATA.set_upstream(OAI_HARVEST)
XSL_TRANSFORM.set_upstream(CLEANUP_DATA)
LIST_S3_FILES.set_upstream(XSL_TRANSFORM)
S3_TO_SFTP.set_upstream(LIST_S3_FILES)
