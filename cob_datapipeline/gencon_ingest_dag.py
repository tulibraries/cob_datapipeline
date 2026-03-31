"""Airflow DAG to index GENCON Databases into Solr."""
import airflow
import os
import pendulum

from datetime import timedelta
from airflow.providers.standard.operators.bash import BashOperator

"""
INIT SYSTEMWIDE VARIABLES

check for existence of systemwide variables shared across tasks that can be
initialized here if not found (i.e. if this is a new installation) & defaults exist
"""

AIRFLOW_HOME = "{{ var.value.AIRFLOW_HOME }}"
AIRFLOW_USER_HOME = "{{ var.value.AIRFLOW_USER_HOME }}"

SCHEDULE = os.getenv("GENCON_INDEX_SCHEDULE") or os.getenv("GENCON_INDEX_SCHEDULE_INTERVAL")

GENCON_INDEX_BRANCH = "{{ var.value.GENCON_INDEX_BRANCH }}"
GENCON_TEMP_PATH = "{{ var.value.GENCON_TEMP_PATH }}"
GENCON_CSV_S3 = "{{ var.value.GENCON_CSV_S3 }}"

# Get S3 data bucket variables
AIRFLOW_DATA_BUCKET = "{{ var.value.AIRFLOW_DATA_BUCKET }}"

# Get Solr URL & Collection Name for indexing info; error out if not entered
CONFIGSET = "{{ var.json.GENCON_SOLR_CONFIG.configset }}"
SOLR_WEB_URL = (
    "{% set solr = conn.get('SOLRCLOUD-WRITER') %}"
    "{{ '' if solr.host.startswith('http') else 'http://' }}{{ solr.host }}"
    "{% if solr.port %}:{{ solr.port }}{% endif %}/solr/"
    + CONFIGSET
)

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
    schedule=SCHEDULE
)

"""
CREATE TASKS
Tasks with all logic contained in a single operator can be declared here.
Tasks with custom logic are relegated to individual Python files.
"""

INDEX_GENCON = BashOperator(
    task_id="index_gencon",
    bash_command=AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/ingest_gencon.sh ",
    retries=1,
    env={
        "AWS_ACCESS_KEY_ID": "{{ conn.AIRFLOW_S3.login }}",
        "AWS_SECRET_ACCESS_KEY": "{{ conn.AIRFLOW_S3.password }}",
        "BUCKET": AIRFLOW_DATA_BUCKET,
        "GIT_BRANCH": GENCON_INDEX_BRANCH,
        "HOME": AIRFLOW_USER_HOME,
        "GENCON_TEMP_PATH": GENCON_TEMP_PATH,
        "GENCON_CSV_S3": GENCON_CSV_S3,
        "SOLR_AUTH_USER": "{{ conn.get('SOLRCLOUD-WRITER').login or '' }}",
        "SOLR_AUTH_PASSWORD": "{{ conn.get('SOLRCLOUD-WRITER').password or '' }}",
        "SOLR_WEB_URL": SOLR_WEB_URL,
    },
    dag=DAG
)
