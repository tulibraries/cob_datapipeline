"""Airflow DAG to harvest DSpace electronic theses and dissertations"""
from datetime import datetime, timedelta
from tulflow import harvest, tasks
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
import airflow

"""
INIT SYSTEMWIDE VARIABLES
check for existence of systemwide variables shared across tasks that can be
initialized here if not found (i.e. if this is a new installation)
"""

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
DSPACE_OAI_SETSPEC = DSPACE_OAI_CONFIG.get("setspec")

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
    'dspace_harvest',
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
        "setspec": DSPACE_OAI_SETSPEC,
        "records_per_file": 1000,
        "timestamp": f"{ S3_NAME_SPACE }"
    },
    dag=DAG
)
