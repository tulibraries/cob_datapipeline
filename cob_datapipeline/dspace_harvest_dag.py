"""Airflow DAG to harvest DSpace electronic theses and dissertations"""
from datetime import datetime, timedelta
import os
from tulflow import harvest, tasks
from cob_datapipeline import helpers
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.s3_list import S3ListOperator
from cob_datapipeline.operators.batch_s3_to_sftp_operator import BatchS3ToSFTPOperator
import airflow

"""
INIT SYSTEMWIDE VARIABLES
check for existence of systemwide variables shared across tasks that can be
initialized here if not found (i.e. if this is a new installation)
"""

AIRFLOW_HOME = Variable.get("AIRFLOW_HOME")
AIRFLOW_USER_HOME = Variable.get("AIRFLOW_USER_HOME")

# Get S3 data bucket variables
AIRFLOW_S3 = BaseHook.get_connection("AIRFLOW_S3")
AIRFLOW_DATA_BUCKET = Variable.get("AIRFLOW_DATA_BUCKET")
S3_NAME_SPACE = '{{ execution_date.strftime("%Y-%m-%d_%H-%M-%S") }}'

# OAI Harvest Variables
DSPACE_HARVEST_FROM_DATE = Variable.get("DSPACE_HARVEST_FROM_DATE")
DEFAULT_HARVEST_UNTIL_DATE = '{{ execution_date.strftime("%Y-%m-%dT%H:%M:%SZ") }}'
DSPACE_HARVEST_UNTIL_DATE = Variable.get("DSPACE_HARVEST_UNTIL_DATE", default_var=DEFAULT_HARVEST_UNTIL_DATE)
DSPACE_OAI_CONFIG = Variable.get("DSPACE_OAI_CONFIG", deserialize_json=True)
DSPACE_OAI_MD_PREFIX = DSPACE_OAI_CONFIG.get("md_prefix")
DSPACE_OAI_ENDPOINT = DSPACE_OAI_CONFIG.get("endpoint")
DSPACE_OAI_INCLUDED_SETS = DSPACE_OAI_CONFIG.get("included_sets")

# CREATE DAG
DEFAULT_ARGS = {
    'owner': 'cob',
    'depends_on_past': False,
    'start_date': datetime(2019, 5, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': tasks.execute_slackpostonfail,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

DAG = airflow.DAG(
    dag_id="dspace_harvest",
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    schedule_interval=None
)

"""
CREATE TASKS
Tasks with all logic contained in a single operator can be declared here.
Tasks with custom logic are relegated to individual Python files.
"""

OAI_HARVEST = PythonOperator(
    task_id='oai_harvest',
    provide_context=True,
    python_callable=harvest.oai_to_s3,
    op_kwargs={
        "access_id": AIRFLOW_S3.login,
        "access_secret": AIRFLOW_S3.password,
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
    provide_context=True,
    python_callable=helpers.cleanup_metadata,
    op_kwargs={
        "source_prefix": DAG.dag_id + "/" + S3_NAME_SPACE + "/new-updated",
        "destination_prefix": DAG.dag_id + "/" + S3_NAME_SPACE + "/cleaned",
        "access_id": AIRFLOW_S3.login,
        "access_secret": AIRFLOW_S3.password,
        "bucket_name": AIRFLOW_DATA_BUCKET,
        "records_per_file": 1000,
        "timestamp": f"{ S3_NAME_SPACE }"
    },
    dag=DAG
)

XSL_TRANSFORM = BashOperator(
    task_id="xsl_transform",
    bash_command=AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/transform.sh ",
    env={**os.environ, **{
        "AWS_ACCESS_KEY_ID": AIRFLOW_S3.login,
        "AWS_SECRET_ACCESS_KEY": AIRFLOW_S3.password,
        "BUCKET": AIRFLOW_DATA_BUCKET,
        "DAG_ID": DAG.dag_id,
        "DAG_TS": S3_NAME_SPACE,
        "DEST": "transformed",
        "HOME": AIRFLOW_USER_HOME,
        "SOURCE": "cleaned",
        "XSL_FILENAME": AIRFLOW_HOME + "/dags/cob_datapipeline/files/TEU_XOAItoMARCXML.xsl",
        "BATCH_TRANSFORM": AIRFLOW_HOME + "/dags/cob_datapipeline/files/batch_transform.xsl"
    }},
    dag=DAG
)

LIST_S3_FILES = S3ListOperator(
    task_id="list_s3_files",
    bucket=AIRFLOW_DATA_BUCKET,
    prefix=DAG.dag_id + "/" + S3_NAME_SPACE + "/transformed/",
    aws_conn_id=AIRFLOW_S3.conn_id,
    dag=DAG
)

S3_TO_SFTP = BatchS3ToSFTPOperator(
    task_id="s3_to_sftp",
    provide_context=True,
    sftp_conn_id="DSPACESFTP",
    xcom_id="{{ ti.xcom_pull(task_ids='list_s3_files') }}",
    sftp_base_path="production/",
    s3_conn_id="AIRFLOW_S3",
    s3_bucket=AIRFLOW_DATA_BUCKET,
    s3_prefix="dspace_harvest/" + S3_NAME_SPACE + "/transformed",
    dag=DAG
)

# SET UP TASK DEPENDENCIES
CLEANUP_DATA.set_upstream(OAI_HARVEST)
XSL_TRANSFORM.set_upstream(CLEANUP_DATA)
LIST_S3_FILES.set_upstream(XSL_TRANSFORM)
S3_TO_SFTP.set_upstream(LIST_S3_FILES)
