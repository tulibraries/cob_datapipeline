# Airflow DAG to index Web Content into SolrCloud.
from datetime import datetime, timedelta
import airflow
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from cob_datapipeline.task_slackpost import task_web_content_slackpostonsuccess, task_slackpostonfail
from cob_datapipeline.task_sc_get_num_docs import task_solrgetnumdocs
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from tulflow.tasks import create_sc_collection, get_solr_url, swap_sc_alias

"""
INIT SYSTEMWIDE VARIABLES

check for existence of systemwide variables shared across tasks that can be
initialized here if not found (i.e. if this is a new installation) & defaults exist
"""

# Get Solr URL & Collection Name for indexing info; error out if not entered
SOLR_CONN = BaseHook.get_connection("SOLRCLOUD")
CONFIGSET = Variable.get("WEB_CONTENT_CONFIGSET")
REPLICATION_FACTOR = Variable.get("WEB_CONTENT_REPLICATION_FACTOR")
AIRFLOW_HOME = Variable.get("AIRFLOW_HOME")
WEB_CONTENT_CORE = airflow.models.Variable.get("WEB_CONTENT_CORE")
WEB_CONTENT_BRANCH = airflow.models.Variable.get("WEB_CONTENT_BRANCH")
WEB_CONTENT_BASIC_AUTH_USER = airflow.models.Variable.get("WEB_CONTENT_BASIC_AUTH_USER")
WEB_CONTENT_BASIC_AUTH_PASSWORD = airflow.models.Variable.get("WEB_CONTENT_BASIC_AUTH_PASSWORD")
WEB_CONTENT_BASE_URL = airflow.models.Variable.get("WEB_CONTENT_BASE_URL")

SCHEDULE_INTERVAL = airflow.models.Variable.get("WEB_CONTENT_SCHEDULE_INTERVAL")

# CREATE DAG
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 5, 28),
    'email': ['david.kinzer@temple.edu'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': task_slackpostonfail,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

DAG = airflow.DAG(
    'sc_web_content_reindex', default_args=DEFAULT_ARGS, catchup=False,
    max_active_runs=1, schedule_interval=SCHEDULE_INTERVAL
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
    CONFIGSET,
    'get_num_solr_docs_pre',
    conn_id=SOLR_CONN.conn_id
)

CREATE_COLLECTION = create_sc_collection(
    DAG,
    SOLR_CONN.conn_id,
    CONFIGSET + "-{{ ti.xcom_pull(task_ids='set_collection_name') }}",
    REPLICATION_FACTOR,
    CONFIGSET
)

INDEX_WEB_CONTENT = BashOperator(
    task_id='index_web_content',
    bash_command="/usr/local/airflow/dags/cob_datapipeline/scripts/ingest_web_content.sh ",
    env={
        "SOLR_WEB_URL": "{{ ti.xcom_pull(task_ids='set_collection_name') }}",
        "AIRFLOW_HOME": AIRFLOW_HOME,
        "WEB_CONTENT_BRANCH": WEB_CONTENT_BRANCH,
        "WEB_CONTENT_BASIC_AUTH_USER": WEB_CONTENT_BASIC_AUTH_USER,
        "WEB_CONTENT_BASIC_AUTH_PASSWORD": WEB_CONTENT_BASIC_AUTH_PASSWORD,
        "WEB_CONTENT_BASE_URL": WEB_CONTENT_BASE_URL,
        "SOLR_AUTH_USER": SOLR_CONN.login,
        "SOLR_AUTH_PASSWORD": SOLR_CONN.password,
    },
    dag=DAG
)

GET_NUM_SOLR_DOCS_POST = task_solrgetnumdocs(
    DAG,
    CONFIGSET +"-{{ ti.xcom_pull(task_ids='set_collection_name') }}",
    'get_num_solr_docs_post',
    conn_id=SOLR_CONN.conn_id
)

SOLR_ALIAS_SWAP = swap_sc_alias(
    DAG,
    SOLR_CONN.conn_id,
    CONFIGSET +"-{{ ti.xcom_pull(task_ids='set_collection_name') }}",
    CONFIGSET
)

POST_SLACK = PythonOperator(
    task_id='slack_post_succ',
    python_callable=task_web_content_slackpostonsuccess,
    provide_context=True,
    dag=DAG
)

# SET UP TASK DEPENDENCIES
CREATE_COLLECTION.set_upstream(SET_COLLECTION_NAME)
CREATE_COLLECTION.set_upstream(GET_NUM_SOLR_DOCS_PRE)
INDEX_WEB_CONTENT.set_upstream(CREATE_COLLECTION)
GET_NUM_SOLR_DOCS_POST.set_upstream(INDEX_WEB_CONTENT)
SOLR_ALIAS_SWAP.set_upstream(GET_NUM_SOLR_DOCS_POST)
POST_SLACK.set_upstream(SOLR_ALIAS_SWAP)
