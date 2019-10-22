# Airflow DAG to index AZ Databases into Solr.
from datetime import datetime, timedelta
import airflow
from airflow.operators.python_operator import PythonOperator
from cob_datapipeline.task_ingest_databases import ingest_databases
from cob_datapipeline.task_ingest_databases import get_solr_url
from cob_datapipeline.task_slackpost import task_az_slackpostonsuccess, task_slackpostonfail
from cob_datapipeline.task_sc_get_num_docs import task_solrgetnumdocs
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from tulflow.tasks import create_sc_collection, swap_sc_alias

"""
INIT SYSTEMWIDE VARIABLES

check for existence of systemwide variables shared across tasks that can be
initialized here if not found (i.e. if this is a new installation) & defaults exist
"""

AZ_INDEX_SCHEDULE_INTERVAL = airflow.models.Variable.get("AZ_INDEX_SCHEDULE_INTERVAL")

# Get Solr URL & Collection Name for indexing info; error out if not entered
SOLR_CONN = BaseHook.get_connection("SOLRCLOUD")
ALIAS = Variable.get("AZ_ALIAS")
CONFIGSET = Variable.get("AZ_CONFIGSET")
REPLICATION_FACTOR = Variable.get("AZ_REPLICATION_FACTOR")
TIMESTAMP = "{{ execution_date.strftime('%Y-%m-%d_%H-%M-%S') }}"
COLLECTION = CONFIGSET + "-" + TIMESTAMP
SOLR_URL = get_solr_url(SOLR_CONN, COLLECTION)

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

AZ_DAG = airflow.DAG(
    'sc_az_reindex', default_args=DEFAULT_ARGS, catchup=False,
    max_active_runs=1, schedule_interval=AZ_INDEX_SCHEDULE_INTERVAL
)

"""
CREATE TASKS
Tasks with all logic contained in a single operator can be declared here.
Tasks with custom logic are relegated to individual Python files.
"""

GET_NUM_SOLR_DOCS_PRE = task_solrgetnumdocs(AZ_DAG, ALIAS, 'get_num_solr_docs_pre', conn_id=SOLR_CONN.conn_id)

CREATE_COLLECTION = create_sc_collection(AZ_DAG, SOLR_CONN.conn_id, COLLECTION, REPLICATION_FACTOR, CONFIGSET)

INGEST_DATABASES = ingest_databases(dag=AZ_DAG, conn=SOLR_CONN, solr_url=SOLR_URL)

GET_NUM_SOLR_DOCS_POST = task_solrgetnumdocs(AZ_DAG, COLLECTION, 'get_num_solr_docs_post', conn_id=SOLR_CONN.conn_id)

SOLR_GENERIC_ALIAS_SWAP = swap_sc_alias(AZ_DAG, SOLR_CONN.conn_id, COLLECTION, ALIAS)
SOLR_CONFIGSET_ALIAS_SWAP = swap_sc_alias(AZ_DAG, SOLR_CONN.conn_id, COLLECTION, CONFIGSET)

POST_SLACK = PythonOperator(
    task_id='slack_post_succ',
    python_callable=task_az_slackpostonsuccess,
    provide_context=True,
    dag=AZ_DAG
)

# SET UP TASK DEPENDENCIES
CREATE_COLLECTION.set_upstream(GET_NUM_SOLR_DOCS_PRE)
INGEST_DATABASES.set_upstream(CREATE_COLLECTION)
GET_NUM_SOLR_DOCS_POST.set_upstream(INGEST_DATABASES)
SOLR_GENERIC_ALIAS_SWAP.set_upstream(GET_NUM_SOLR_DOCS_POST)
SOLR_CONFIGSET_ALIAS_SWAP.set_upstream(GET_NUM_SOLR_DOCS_POST)
POST_SLACK.set_upstream(SOLR_GENERIC_ALIAS_SWAP)
POST_SLACK.set_upstream(SOLR_CONFIGSET_ALIAS_SWAP)
