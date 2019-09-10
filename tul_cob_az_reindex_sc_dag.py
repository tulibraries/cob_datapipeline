"""Airflow DAG to index AZ Databases into Solr."""
from datetime import datetime, timedelta
import airflow
from airflow.operators.python_operator import PythonOperator
from cob_datapipeline.task_ingest_databases import ingest_databases
from cob_datapipeline.task_ingest_databases import get_solr_url
from cob_datapipeline.task_slackpost import task_az_slackpostonsuccess, task_slackpostonfail
from cob_datapipeline.task_sc_get_num_docs import task_solrgetnumdocs
from airflow.models import Variable
from airflow.hooks import BaseHook
from airflow.operators.http_operator import SimpleHttpOperator

#
# INIT SYSTEMWIDE VARIABLES
#
# check for existence of systemwide variables shared across tasks that can be
# initialized here if not found (i.e. if this is a new installation) & defaults exist
#

# Get Solr URL & Collection Name for indexing info; error out if not entered
SOLR_CONN = BaseHook.get_connection("SOLRCLOUD")
CONFIGSET = Variable.get("AZ_CONFIGSET")
COLLECTION = CONFIGSET + "-" + datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
SOLR_URL = get_solr_url(SOLR_CONN, COLLECTION)

AZ_INDEX_SCHEDULE_INTERVAL = airflow.models.Variable.get("AZ_INDEX_SCHEDULE_INTERVAL")
#
# CREATE DAG
#
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
    'tul_cob_az_sc_reindex', default_args=DEFAULT_ARGS, catchup=False,
    max_active_runs=1, schedule_interval=AZ_INDEX_SCHEDULE_INTERVAL
)

#
# CREATE TASKS
#
# Tasks with all logic contained in a single operator can be declared here.
# Tasks with custom logic are relegated to individual Python files.
#


get_num_solr_docs_pre = task_solrgetnumdocs(AZ_DAG, CONFIGSET, 'get_num_solr_docs_pre', conn_id=SOLR_CONN.conn_id)
CREATE_COLLECTION = SimpleHttpOperator(
   task_id="create_collection",
   method='GET',
   http_conn_id='SOLRCLOUD',
   endpoint="solr/admin/collections",
   data={
       "action": "CREATE",
       "name": COLLECTION,
       "numShards": "1",
       "replicationFactor": "3",
       "maxShardsPerNode": "1",
       "collection.configName": CONFIGSET
       },
   headers={},
   dag=AZ_DAG,
   log_response=True
)

ingest_databases_task = ingest_databases(dag=AZ_DAG, conn=SOLR_CONN, solr_az_url=SOLR_URL)
get_num_solr_docs_post = task_solrgetnumdocs(AZ_DAG, CONFIGSET, 'get_num_solr_docs_post', conn_id=SOLR_CONN.conn_id)
post_slack = PythonOperator(
    task_id='slack_post_succ',
    python_callable=task_az_slackpostonsuccess,
    provide_context=True,
    dag=AZ_DAG
)

#
# SET UP TASK DEPENDENCIES
#
CREATE_COLLECTION.set_upstream(get_num_solr_docs_pre)
ingest_databases_task.set_upstream(CREATE_COLLECTION)
get_num_solr_docs_post.set_upstream(ingest_databases_task)
post_slack.set_upstream(get_num_solr_docs_post)
