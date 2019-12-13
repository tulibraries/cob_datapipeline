"""Airflow DAG to index AZ Databases into Solr."""
from datetime import datetime, timedelta
import os
import airflow
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from tulflow import tasks
from cob_datapipeline.task_slack_posts import az_slackpostonsuccess, slackpostonfail
from cob_datapipeline.task_sc_get_num_docs import task_solrgetnumdocs

"""
INIT SYSTEMWIDE VARIABLES

check for existence of systemwide variables shared across tasks that can be
initialized here if not found (i.e. if this is a new installation) & defaults exist
"""

AIRFLOW_HOME = Variable.get("AIRFLOW_HOME")
AIRFLOW_USER_HOME = Variable.get("AIRFLOW_USER_HOME")
SCHEDULE_INTERVAL = Variable.get("AZ_INDEX_SCHEDULE_INTERVAL")

# Get Solr URL & Collection Name for indexing info; error out if not entered

SOLR_CONN = BaseHook.get_connection("SOLRCLOUD")
SOLR_CONFIG = Variable.get("AZ_SOLR_CONFIG", deserialize_json=True)
# {"configset": "tul_cob-az-2", "replication_factor": 2}
CONFIGSET = SOLR_CONFIG.get("configset")
ALIAS = CONFIGSET + "-stage"
REPLICATION_FACTOR = SOLR_CONFIG.get("replication_factor")

#Databases AZ Springshare creds
AZ_CLIENT_ID = Variable.get("AZ_CLIENT_ID")
AZ_CLIENT_SECRET = Variable.get("AZ_CLIENT_SECRET")
AZ_BRANCH = Variable.get("AZ_STAGE_BRANCH")

# CREATE DAG
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 5, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': slackpostonfail,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

DAG = airflow.DAG(
    'stage_sc_az_reindex',
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    schedule_interval=SCHEDULE_INTERVAL
)

"""
CREATE TASKS
Tasks with all logic contained in a single operator can be declared here.
Tasks with custom logic are relegated to individual Python files.
"""

SET_COLLECTION_NAME = PythonOperator(
    task_id='set_collection_name',
    python_callable=datetime.now().strftime,
    op_args=["%Y-%m-%d_%H-%M-%S"],
    dag=DAG
)

GET_NUM_SOLR_DOCS_PRE = task_solrgetnumdocs(
    DAG,
    ALIAS,
    'get_num_solr_docs_pre',
    conn_id=SOLR_CONN.conn_id
)

CREATE_COLLECTION = tasks.create_sc_collection(
    DAG,
    SOLR_CONN.conn_id,
    CONFIGSET + "-{{ ti.xcom_pull(task_ids='set_collection_name') }}",
    REPLICATION_FACTOR,
    CONFIGSET
)

INDEX_DATABASES = BashOperator(
    task_id='index_az',
    bash_command=AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/ingest_databases.sh ",
    env={**os.environ, **{
        "HOME": AIRFLOW_USER_HOME,
        "SOLR_AZ_URL": tasks.get_solr_url(SOLR_CONN, CONFIGSET + "-{{ ti.xcom_pull(task_ids='set_collection_name') }}"),
        "AZ_CLIENT_ID": AZ_CLIENT_ID,
        "AZ_CLIENT_SECRET": AZ_CLIENT_SECRET,
        "AZ_BRANCH": AZ_BRANCH,
        "SOLR_AUTH_PASSWORD": SOLR_CONN.password if SOLR_CONN.password else "",
        "SOLR_AUTH_USER": SOLR_CONN.login if SOLR_CONN.login else "",
    }},
    dag=DAG
)

GET_NUM_SOLR_DOCS_POST = task_solrgetnumdocs(
    DAG,
    CONFIGSET +"-{{ ti.xcom_pull(task_ids='set_collection_name') }}",
    'get_num_solr_docs_post',
    conn_id=SOLR_CONN.conn_id)

SOLR_ALIAS_SWAP = tasks.swap_sc_alias(
    DAG,
    SOLR_CONN.conn_id,
    CONFIGSET +"-{{ ti.xcom_pull(task_ids='set_collection_name') }}",
    ALIAS
)

POST_SLACK = PythonOperator(
    task_id='slack_post_succ',
    python_callable=az_slackpostonsuccess,
    provide_context=True,
    dag=DAG
)

# SET UP TASK DEPENDENCIES
SET_COLLECTION_NAME.set_upstream(GET_NUM_SOLR_DOCS_PRE)
CREATE_COLLECTION.set_upstream(SET_COLLECTION_NAME)
INDEX_DATABASES.set_upstream(CREATE_COLLECTION)
GET_NUM_SOLR_DOCS_POST.set_upstream(INDEX_DATABASES)
SOLR_ALIAS_SWAP.set_upstream(GET_NUM_SOLR_DOCS_POST)
POST_SLACK.set_upstream(SOLR_ALIAS_SWAP)
