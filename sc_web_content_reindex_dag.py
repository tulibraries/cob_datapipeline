# Airflow DAG to index Web Content into SolrCloud.
from datetime import datetime, timedelta
import airflow
from airflow.operators.python_operator import PythonOperator
from cob_datapipeline.task_ingest_web_content import ingest_web_content
from cob_datapipeline.task_ingest_databases import get_solr_url
from cob_datapipeline.task_slackpost import task_web_content_slackpostonsuccess, task_slackpostonfail
from cob_datapipeline.task_sc_get_num_docs import task_solrgetnumdocs
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from tulflow.tasks import create_sc_collection, swap_sc_alias

"""
INIT SYSTEMWIDE VARIABLES

check for existence of systemwide variables shared across tasks that can be
initialized here if not found (i.e. if this is a new installation) & defaults exist
"""

# Get Solr URL & Collection Name for indexing info; error out if not entered
SOLR_CONN = BaseHook.get_connection("SOLRCLOUD")
CONFIGSET = Variable.get("WEB_CONTENT_CONFIGSET")
REPLICATION_FACTOR = Variable.get("WEB_CONTENT_REPLICATION_FACTOR")
TIMESTAMP = "{{ execution_date.strftime('%Y-%m-%d_%H-%M-%S') }}"
COLLECTION = CONFIGSET + "-" + TIMESTAMP
SOLR_URL = get_solr_url(SOLR_CONN, COLLECTION)

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

GET_NUM_SOLR_DOCS_PRE = task_solrgetnumdocs(DAG, CONFIGSET, 'get_num_solr_docs_pre', conn_id=SOLR_CONN.conn_id)

CREATE_COLLECTION = create_sc_collection(DAG, SOLR_CONN.conn_id, COLLECTION, REPLICATION_FACTOR, CONFIGSET)

INGEST_WEB_CONTENT = ingest_web_content(dag=DAG, conn=SOLR_CONN, solr_url=SOLR_URL)

GET_NUM_SOLR_DOCS_POST = task_solrgetnumdocs(DAG, COLLECTION, 'get_num_solr_docs_post', conn_id=SOLR_CONN.conn_id)

SOLR_ALIAS_SWAP = swap_sc_alias(DAG, SOLR_CONN.conn_id, COLLECTION, CONFIGSET)

POST_SLACK = PythonOperator(
    task_id='slack_post_succ',
    python_callable=task_web_content_slackpostonsuccess,
    provide_context=True,
    dag=DAG
)

# SET UP TASK DEPENDENCIES
CREATE_COLLECTION.set_upstream(GET_NUM_SOLR_DOCS_PRE)
INGEST_WEB_CONTENT.set_upstream(CREATE_COLLECTION)
GET_NUM_SOLR_DOCS_POST.set_upstream(INGEST_WEB_CONTENT)
SOLR_ALIAS_SWAP.set_upstream(GET_NUM_SOLR_DOCS_POST)
POST_SLACK.set_upstream(SOLR_ALIAS_SWAP)
