"""Airflow DAG to index GENCON Databases into Solr."""
from datetime import datetime, timedelta
import airflow
import os
import pendulum
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator
from tulflow import tasks
from airflow.providers.slack.notifications.slack import send_slack_notification
from airflow.models.connection import Connection
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator

"""
INIT SYSTEMWIDE VARIABLES

check for existence of systemwide variables shared across tasks that can be
initialized here if not found (i.e. if this is a new installation) & defaults exist
"""

AIRFLOW_HOME = Variable.get("AIRFLOW_HOME")
AIRFLOW_USER_HOME = Variable.get("AIRFLOW_USER_HOME")

SCHEDULE_INTERVAL = Variable.get("GENCON_INDEX_SCHEDULE_INTERVAL")

GENCON_INDEX_VERSION = Variable.get("GENCON_INDEX_VERSION")
GENCON_INDEX_PATH = Variable.get("GENCON_INDEX_PATH")
GENCON_TEMP_PATH = Variable.get("GENCON_TEMP_PATH")
GENCON_CSV_S3 = Variable.get("GENCON_CSV_S3")

# Get S3 data bucket variables
AIRFLOW_S3 = BaseHook.get_connection("AIRFLOW_S3")
AIRFLOW_DATA_BUCKET = Variable.get("AIRFLOW_DATA_BUCKET")

# Get Solr URL & Collection Name for indexing info; error out if not entered
SOLR_CONN = BaseHook.get_connection("SOLRCLOUD-WRITER")
SOLR_CONFIG = Variable.get("GENCON_SOLR_CONFIG", deserialize_json=True)
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


AIRFLOW_HOME = "/opt/airflow"
INDEX_GENCON = BashOperator(
    task_id="index_gencon",
    bash_command=AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/ingest_gencon.sh ",
    retries=1,
    env={
        "AWS_ACCESS_KEY_ID": AIRFLOW_S3.login,
        "AWS_SECRET_ACCESS_KEY": AIRFLOW_S3.password,
        "BUCKET": AIRFLOW_DATA_BUCKET,
        "GIT_BRANCH": GENCON_INDEX_VERSION,
        "HOME": AIRFLOW_USER_HOME,
        "GENCON_INDEX_PATH": GENCON_INDEX_PATH,
        "GENCON_TEMP_PATH": GENCON_TEMP_PATH,
        "GENCON_CSV_S3": GENCON_CSV_S3,
        "SOLR_AUTH_USER": SOLR_CONN.login if SOLR_CONN.login else "",
        "SOLR_AUTH_PASSWORD": SOLR_CONN.password if SOLR_CONN.password else "",
        "SOLR_WEB_URL": tasks.get_solr_url(SOLR_CONN, CONFIGSET),
    },
    dag=DAG
)
