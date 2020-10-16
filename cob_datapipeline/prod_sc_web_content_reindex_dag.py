# Airflow DAG to index Web Content into SolrCloud.
from datetime import datetime, timedelta
from tulflow import tasks
import airflow
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from cob_datapipeline.task_slack_posts import web_content_slackpostonsuccess
from cob_datapipeline.task_sc_get_num_docs import task_solrgetnumdocs
from cob_datapipeline.operators import\
        PushVariable, DeleteAliasListVariable, DeleteCollectionListVariable

"""
INIT SYSTEMWIDE VARIABLES

check for existence of systemwide variables shared across tasks that can be
initialized here if not found (i.e. if this is a new installation) & defaults exist
"""

AIRFLOW_HOME = Variable.get("AIRFLOW_HOME")
AIRFLOW_USER_HOME = Variable.get("AIRFLOW_USER_HOME")
SCHEDULE_INTERVAL = Variable.get("WEB_CONTENT_SCHEDULE_INTERVAL")

# Get Solr URL & Collection Name for indexing info; error out if not entered
SOLR_CONN = BaseHook.get_connection("SOLRCLOUD")
SOLR_CONFIG = Variable.get("WEB_CONTENT_SOLR_CONFIG", deserialize_json=True)
# {"configset": "tul_cob-web-2", "replication_factor": 2}
CONFIGSET = SOLR_CONFIG.get("configset")
ALIAS = CONFIGSET + "-prod"
REPLICATION_FACTOR = SOLR_CONFIG.get("replication_factor")
WEB_CONTENT_BRANCH = Variable.get("WEB_CONTENT_PROD_BRANCH")

# Manifold website creds
WEB_CONTENT_BASE_URL = Variable.get("WEB_CONTENT_PROD_BASE_URL")

# CREATE DAG
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 5, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': tasks.execute_slackpostonfail,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

DAG = airflow.DAG(
    'prod_sc_web_content_reindex',
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

INDEX_WEB_CONTENT = BashOperator(
    task_id='index_web_content',
    bash_command=AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/ingest_web_content.sh ",
    env={
        "HOME": AIRFLOW_USER_HOME,
        "SOLR_AUTH_PASSWORD": SOLR_CONN.password if SOLR_CONN.password else "",
        "SOLR_AUTH_USER": SOLR_CONN.login if SOLR_CONN.login else "",
        "SOLR_WEB_URL": tasks.get_solr_url(SOLR_CONN, CONFIGSET + "-{{ ti.xcom_pull(task_ids='set_collection_name') }}"),
        "WEB_CONTENT_BASE_URL": WEB_CONTENT_BASE_URL,
        "WEB_CONTENT_BRANCH": WEB_CONTENT_BRANCH
    },
    dag=DAG
)

GET_NUM_SOLR_DOCS_POST = task_solrgetnumdocs(
    DAG,
    CONFIGSET +"-{{ ti.xcom_pull(task_ids='set_collection_name') }}",
    'get_num_solr_docs_post',
    conn_id=SOLR_CONN.conn_id
)

SOLR_ALIAS_SWAP = tasks.swap_sc_alias(
    DAG,
    SOLR_CONN.conn_id,
    CONFIGSET +"-{{ ti.xcom_pull(task_ids='set_collection_name') }}",
    ALIAS
)

PUSH_ALIAS = PushVariable(
    task_id="push_alias",
    name="WEB_CONTENT_QA_ALIASES",
    value=ALIAS,
    dag=DAG)

DELETE_ALIAS = DeleteAliasListVariable(
    task_id="delete_aliases",
    solr_conn_id='SOLRCLOUD',
    list_variable="WEB_CONTENT_QA_ALIASES",
    skip_from_last=2,
    skip_included=[ALIAS],
    dag=DAG)

PUSH_COLLECTION = PushVariable(
    task_id="push_collection",
    name="WEB_CONTENT_QA_COLLECTIONS",
    value=CONFIGSET +"-{{ ti.xcom_pull(task_ids='set_collection_name') }}",
    dag=DAG)

DELETE_COLLECTIONS = DeleteCollectionListVariable(
    task_id="delete_collections",
    solr_conn_id='SOLRCLOUD',
    list_variable="WEB_CONTENT_QA_COLLECTIONS",
    skip_from_last=2,
    skip_included=[CONFIGSET +"-{{ ti.xcom_pull(task_ids='set_collection_name') }}"],
    dag=DAG)

POST_SLACK = PythonOperator(
    task_id='slack_post_succ',
    python_callable=web_content_slackpostonsuccess,
    provide_context=True,
    dag=DAG
)

# SET UP TASK DEPENDENCIES
SET_COLLECTION_NAME.set_upstream(GET_NUM_SOLR_DOCS_PRE)
CREATE_COLLECTION.set_upstream(SET_COLLECTION_NAME)
INDEX_WEB_CONTENT.set_upstream(CREATE_COLLECTION)
GET_NUM_SOLR_DOCS_POST.set_upstream(INDEX_WEB_CONTENT)
SOLR_ALIAS_SWAP.set_upstream(GET_NUM_SOLR_DOCS_POST)
PUSH_ALIAS.set_upstream(SOLR_ALIAS_SWAP)
DELETE_ALIAS.set_upstream(PUSH_ALIAS)
PUSH_COLLECTION.set_upstream(DELETE_ALIAS)
DELETE_COLLECTIONS.set_upstream(PUSH_COLLECTION)
POST_SLACK.set_upstream(DELETE_COLLECTIONS)
