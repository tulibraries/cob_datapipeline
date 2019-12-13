"""Airflow DAG to index Web Content into Solr."""
from datetime import datetime, timedelta
import airflow
from airflow.operators.python_operator import PythonOperator
from cob_datapipeline.task_ingest_web_content import ingest_web_content
from cob_datapipeline.task_slack_posts import web_content_slackpostonsuccess, slackpostonfail
from cob_datapipeline.task_solrgetnumdocs import task_solrgetnumdocs

WEB_CONTENT_CORE = airflow.models.Variable.get("WEB_CONTENT_CORE")
SOLR_CONN = airflow.hooks.base_hook.BaseHook.get_connection("AIRFLOW_CONN_SOLR_LEADER")
WEB_CONTENT_SCHEDULE_INTERVAL = airflow.models.Variable.get("WEB_CONTENT_SCHEDULE_INTERVAL")
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
    'on_failure_callback': slackpostonfail,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

WEB_CONTENT_DAG = airflow.DAG(
    'tul_cob_web_content_index', default_args=DEFAULT_ARGS, catchup=False,
    max_active_runs=1, schedule_interval=WEB_CONTENT_SCHEDULE_INTERVAL
)

#
# CREATE TASKS
#
# Tasks with all logic contained in a single operator can be declared here.
# Tasks with custom logic are relegated to individual Python files.
#
get_num_solr_docs_pre = task_solrgetnumdocs(WEB_CONTENT_DAG, WEB_CONTENT_CORE, 'get_num_solr_docs_pre')
ingest_web_content_task = ingest_web_content(dag=WEB_CONTENT_DAG, conn=SOLR_CONN, delete=True)
get_num_solr_docs_post = task_solrgetnumdocs(WEB_CONTENT_DAG, WEB_CONTENT_CORE, 'get_num_solr_docs_post')
post_slack = PythonOperator(
    task_id='slack_post_succ',
    python_callable=web_content_slackpostonsuccess,
    provide_context=True,
    dag=WEB_CONTENT_DAG
)

#
# SET UP TASK DEPENDENCIES
#
ingest_web_content_task.set_upstream(get_num_solr_docs_pre)
get_num_solr_docs_post.set_upstream(ingest_web_content_task)
post_slack.set_upstream(get_num_solr_docs_post)
