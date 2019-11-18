"""Controller DAG to trigger sc_web_content_reindex for Stage environment:"""
from datetime import datetime
import airflow
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from tulflow.tasks import conditionally_trigger

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
ALIAS = CONFIGSET + "-stage"
REPLICATION_FACTOR = WEB_CONTENT_SOLR_CONFIG.get("replication_factor")
WEB_CONTENT_BRANCH = Variable.get("WEB_CONTENT_STAGE_BRANCH")

# Manifold website creds
WEB_CONTENT_BASIC_AUTH_USER = Variable.get("WEB_CONTENT_BASIC_AUTH_USER")
WEB_CONTENT_BASIC_AUTH_PASSWORD = Variable.get("WEB_CONTENT_BASIC_AUTH_PASSWORD")
WEB_CONTENT_BASE_URL = Variable.get("WEB_CONTENT_STAGE_BASE_URL")

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
    'stage_sc_web_content_reindex',
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
        "WEB_CONTENT_BASIC_AUTH_PASSWORD": WEB_CONTENT_BASIC_AUTH_PASSWORD,
        "WEB_CONTENT_BASIC_AUTH_USER": WEB_CONTENT_BASIC_AUTH_USER,
        "WEB_CONTENT_BRANCH": WEB_CONTENT_BRANCH
    },
    schedule_interval="@once",
)

# Define the single task in this controller DAG
STAGE_TRIGGER = TriggerDagRunOperator(
    task_id="stage_trigger",
    trigger_dag_id="sc_web_content_reindex",
    python_callable=conditionally_trigger,
    params={"condition_param": True,
            "message": "Triggering Stage Web Content Index DAG",
            "env": "stage"
           },
    dag=CONTROLLER_DAG
)
