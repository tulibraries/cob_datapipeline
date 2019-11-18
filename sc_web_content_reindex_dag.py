"""Generic Airflow DAG to index Web Content into SolrCloud."""
from datetime import datetime, timedelta
import airflow
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from cob_datapipeline.task_sc_get_num_docs import task_solrgetnumdocs
from cob_datapipeline.task_slackpost import task_web_content_slackpostonsuccess
from tulflow import tasks

"""
LOCAL FUNCTIONS
Functions / any code with processing logic should be elsewhere, tested, etc.
This is where to put functions that haven't been abstracted out yet.
"""

def require_dag_run(dag_run):
    """Simple function to check an environment has been configured."""
    if not "env" in dag_run.conf:
        raise Exception("Dag run must have env configured.")

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
WEB_CONTENT_SOLR_CONFIG = Variable.get("WEB_CONTENT_SOLR_CONFIG", deserialize_json=True)
# {"configset": "tul_cob-web-2", "replication_factor": 2}
CONFIGSET = WEB_CONTENT_SOLR_CONFIG.get("configset")
REPLICATION_FACTOR = WEB_CONTENT_SOLR_CONFIG.get("replication_factor")
WEB_CONTENT_BRANCH = Variable.get("WEB_CONTENT_BRANCH")

# Manifold website creds
WEB_CONTENT_BASIC_AUTH_USER = Variable.get("WEB_CONTENT_BASIC_AUTH_USER")
WEB_CONTENT_BASIC_AUTH_PASSWORD = Variable.get("WEB_CONTENT_BASIC_AUTH_PASSWORD")
WEB_CONTENT_BASE_URL = Variable.get("WEB_CONTENT_BASE_URL")

# CREATE DAG
DEFAULT_ARGS = {
    "owner": "cob",
    "depends_on_past": False,
    "start_date": datetime(2019, 5, 28),
    "on_failure_callback": tasks.execute_slackpostonfail,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

DAG = airflow.DAG(
    "sc_web_content_reindex",
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    schedule_interval=SCHEDULE_INTERVAL
)

"""
CREATE TASKS

Tasks with all logic contained in a single operator can be declared here.
Tasks with custom logic are relegated to individual Python files.
"""

# Single task to provide message about which controller triggered the DAG
REMOTE_TRIGGER_MESSAGE = BashOperator(
    task_id="remote_trigger_message",
    bash_command='echo "Remote trigger: \'{{ dag_run.conf["message"] }}\'"',
    dag=DAG,
)

REQUIRE_DAG_RUN = PythonOperator(
    task_id="require_dag_run",
    python_callable=require_dag_run,
    provide_context=True,
    dag=DAG,
)

SET_COLLECTION_NAME = PythonOperator(
    task_id="set_collection_name",
    python_callable=datetime.now().strftime,
    op_args=["%Y-%m-%d_%H-%M-%S"],
    dag=DAG
)

GET_NUM_SOLR_DOCS_PRE = task_solrgetnumdocs(
    DAG,
    CONFIGSET + "-{{ dag_run.conf.get('env') }}",
    "get_num_solr_docs_pre",
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
    task_id="index_web_content",
    bash_command=AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/ingest_web_content.sh ",
    env={
        "HOME": AIRFLOW_USER_HOME,
        "SOLR_AUTH_PASSWORD": SOLR_CONN.password,
        "SOLR_AUTH_USER": SOLR_CONN.login,
        "SOLR_WEB_URL": tasks.get_solr_url(SOLR_CONN, CONFIGSET + "-{{ ti.xcom_pull(task_ids='set_collection_name') }}"),
        "WEB_CONTENT_BASE_URL": WEB_CONTENT_BASE_URL,
        "WEB_CONTENT_BASIC_AUTH_PASSWORD": WEB_CONTENT_BASIC_AUTH_PASSWORD,
        "WEB_CONTENT_BASIC_AUTH_USER": WEB_CONTENT_BASIC_AUTH_USER,
        "WEB_CONTENT_BRANCH": WEB_CONTENT_BRANCH
    },
    dag=DAG
)

SOLR_ALIAS_SWAP = tasks.swap_sc_alias(
    DAG,
    SOLR_CONN.conn_id,
    CONFIGSET +"-{{ ti.xcom_pull(task_ids='set_collection_name') }}",
    CONFIGSET + "-{{ dag_run.conf.get('env') }}"
)

GET_NUM_SOLR_DOCS_POST = task_solrgetnumdocs(
    DAG,
    CONFIGSET +"-{{ ti.xcom_pull(task_ids='set_collection_name') }}",
    "get_num_solr_docs_post",
    conn_id=SOLR_CONN.conn_id
)

POST_SLACK = PythonOperator(
    task_id="slack_post_succ",
    python_callable=task_web_content_slackpostonsuccess,
    provide_context=True,
    dag=DAG
)

# SET UP TASK DEPENDENCIES
REQUIRE_DAG_RUN.set_upstream(REMOTE_TRIGGER_MESSAGE)
GET_NUM_SOLR_DOCS_PRE.set_upstream(REQUIRE_DAG_RUN)
SET_COLLECTION_NAME.set_upstream(GET_NUM_SOLR_DOCS_PRE)
CREATE_COLLECTION.set_upstream(SET_COLLECTION_NAME)
INDEX_WEB_CONTENT.set_upstream(CREATE_COLLECTION)
SOLR_ALIAS_SWAP.set_upstream(INDEX_WEB_CONTENT)
GET_NUM_SOLR_DOCS_POST.set_upstream(SOLR_ALIAS_SWAP)
POST_SLACK.set_upstream(GET_NUM_SOLR_DOCS_POST)
