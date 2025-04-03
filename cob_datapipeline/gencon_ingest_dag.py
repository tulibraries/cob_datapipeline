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

GET_CSV_DATA = BashOperator(
    task_id="get_csv_data",
    bash_command="cp -pr " + AIRFLOW_HOME + "/dags/gencon_datapipeline/scripts/gencon_dags/csv-s3/* " + AIRFLOW_HOME + "/dags/gencon_datapipeline/scripts/gencon_dags/csv/*",
    dag=DAG)

INDEX_DATABASES = BashOperator(
    task_id="index_gencon",
    bash_command=AIRFLOW_HOME + "/dags/gencon_datapipeline/scripts/ingest_csv.sh",
    retries=1,
    env={**os.environ, **{
        "HOME": AIRFLOW_USER_HOME,
        "SOLR_URL": tasks.get_solr_url(SOLR_CONN, CONFIGSET + "-{{ ti.xcom_pull(task_ids='set_collection_name') }}"),
        "SOLRCLOUD_PASSWORD": SOLR_CONN.password if SOLR_CONN.password else "",
        "SOLRCLOUD_USER": SOLR_CONN.login if SOLR_CONN.login else "",
    }},
    dag=DAG
)

INDEX_DATABASES.set_upstream(GET_CSV_DATA)
