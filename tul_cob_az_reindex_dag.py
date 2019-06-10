"""Airflow DAG to index AZ Databases into Solr."""
from datetime import datetime, timedelta
import airflow
from airflow.operators.python_operator import PythonOperator
from cob_datapipeline.task_gitpulltulcob import task_git_pull_tulcob
from cob_datapipeline.task_ingest_databases import get_database_docs, ingest_databases
from cob_datapipeline.task_slackpost import task_az_slackpostonsuccess, task_slackpostonfail
from cob_datapipeline.task_solrgetnumdocs import task_solrgetnumdocs

LATEST_RELEASE = airflow.models.Variable.get("GIT_PULL_TULCOB_LATEST_RELEASE")
GIT_REF = airflow.models.Variable.get("GIT_PULL_TULCOB_BRANCH_NAME")
AZ_CORE = airflow.models.Variable.get("AZ_CORE")
SOLR_CONN = airflow.hooks.base_hook.BaseHook.get_connection("AIRFLOW_CONN_SOLR_LEADER")

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
    'tul_cob_az_reindex', default_args=DEFAULT_ARGS, catchup=False,
    max_active_runs=1, schedule_interval=None
)

#
# CREATE TASKS
#
# Tasks with all logic contained in a single operator can be declared here.
# Tasks with custom logic are relegated to individual Python files.
#
get_num_solr_docs_pre = task_solrgetnumdocs(AZ_DAG, AZ_CORE, 'get_num_solr_docs_pre')
git_pull_tulcob_task = task_git_pull_tulcob(AZ_DAG, LATEST_RELEASE, GIT_REF)
get_database_docs_task = get_database_docs(AZ_DAG)
ingest_databases_task = ingest_databases(dag=AZ_DAG, conn=SOLR_CONN)
get_num_solr_docs_post = task_solrgetnumdocs(AZ_DAG, AZ_CORE, 'get_num_solr_docs_post')
post_slack = PythonOperator(
    task_id='slack_post_succ',
    python_callable=task_az_slackpostonsuccess,
    provide_context=True,
    dag=AZ_DAG
)

#
# SET UP TASK DEPENDENCIES
#
git_pull_tulcob_task.set_upstream(get_num_solr_docs_pre)
get_database_docs_task.set_upstream(git_pull_tulcob_task)
ingest_databases_task.set_upstream(get_database_docs_task)
get_num_solr_docs_post.set_upstream(ingest_databases_task)
post_slack.set_upstream(get_num_solr_docs_post)
