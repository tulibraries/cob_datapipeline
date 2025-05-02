"""Airflow DAG to index GENCON Databases into Solr."""
from datetime import datetime, timedelta
import airflow
import os
import pendulum
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from tulflow import tasks
from airflow.providers.slack.notifications.slack import send_slack_notification
from airflow.models.connection import Connection
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.operators.python_operator import PythonOperator
import boto3

# slackpostonsuccess = send_collection_notification(channel="blacklight_project")
# slackpostonfail = send_slack_notification(channel="infra_alerts", username="airflow", text=":poop: Task failed: {{ dag.dag_id }} {{ ti.task_id }} {{ dag_run.logical_date }} {{ ti.log_url }}")

"""
INIT SYSTEMWIDE VARIABLES

check for existence of systemwide variables shared across tasks that can be
initialized here if not found (i.e. if this is a new installation) & defaults exist
"""

AIRFLOW_HOME = Variable.get("AIRFLOW_HOME")
AIRFLOW_USER_HOME = Variable.get("AIRFLOW_USER_HOME")

SCHEDULE_INTERVAL = Variable.get("GENCON_INDEX_SCHEDULE_INTERVAL")

# Get S3 data bucket variables
AIRFLOW_S3 = BaseHook.get_connection("AIRFLOW_S3")
AIRFLOW_DATA_BUCKET = Variable.get("AIRFLOW_DATA_BUCKET")

# Get Solr URL & Collection Name for indexing info; error out if not entered
SOLR_CONN = BaseHook.get_connection("SOLRCLOUD-WRITER")
SOLR_CONFIG = Variable.get("GENCON_SOLR_CONFIG", deserialize_json=True)
# {"configset": "gencon50-v3.0.1", "replication_factor": 4}
CONFIGSET = SOLR_CONFIG.get("configset")
ALIAS = CONFIGSET + "-prod"
REPLICATION_FACTOR = SOLR_CONFIG.get("replication_factor")

# CREATE DAG
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2018, 12, 13, tz="UTC"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

DAG = airflow.DAG(
    "gencon_index",
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

MAKE_TEMP_DIRECTORY = BashOperator(
    task_id="make_temp_directory",
    bash_command="mkdir -p /tmp/gencon",
    dag=DAG)

GET_CSV_DATA = BashOperator(
    task_id="get_csv_data",
    bash_command="aws s3 cp s3://tulib-gencon /tmp/gencon --recursive",
    retries=1,
    env={
       "AWS_ACCESS_KEY_ID": AIRFLOW_S3.login,
       "AWS_SECRET_ACCESS_KEY": AIRFLOW_S3.password,
       "BUCKET": AIRFLOW_DATA_BUCKET,
    },
    dag=DAG)

"""
GET_CSV_DATA = BashOperator(
    task_id="get_csv_data",
    bash_command="aws s3 cp s3://tulib-gencon /tmp/gencon --recursive",
    retries=1,
    env={
       "AWS_ACCESS_KEY_ID": AIRFLOW_S3.login,
       "AWS_SECRET_ACCESS_KEY": AIRFLOW_S3.password,
       "BUCKET": AIRFLOW_DATA_BUCKET,
    },
    dag=DAG)
GET_CSV_DATA = BashOperator(
    task_id="get_csv_data",
    bash_command="cp -pfr /opt/airflow/dags/cob_datapipeline/scripts/gencon_dags/csv-s3/* /tmp/gencon/",
    dag=DAG)

GET_CSV_DATA = PythonOperator(
    task_id="s3_to_local_python",
    python_callable=download_from_s3,
)
"""

INDEX_GENCON = BashOperator(
    task_id="index_gencon",
    bash_command="/opt/airflow/dags/cob_datapipeline/scripts/ingest_gencon.sh ",
    retries=1,
    env={
        "HOME": AIRFLOW_USER_HOME,
        "SOLR_AUTH_USER": SOLR_CONN.login if SOLR_CONN.login else "",
        "SOLR_AUTH_PASSWORD": SOLR_CONN.password if SOLR_CONN.password else "",
        "SOLR_WEB_URL": tasks.get_solr_url(SOLR_CONN, CONFIGSET),
    },
    dag=DAG
)

REMOVE_TEMP_DIRECTORY = BashOperator(
    task_id="remove_temp_directory",
    bash_command="rm -rf /tmp/gencon",
    dag=DAG)

GET_CSV_DATA.set_upstream(MAKE_TEMP_DIRECTORY)
INDEX_GENCON.set_upstream(GET_CSV_DATA)
REMOVE_TEMP_DIRECTORY.set_upstream(INDEX_GENCON)
