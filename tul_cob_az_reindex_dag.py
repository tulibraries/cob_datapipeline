import airflow
from airflow import utils, settings, DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from cob_datapipeline.task_gitpulltulcob import task_git_pull_tulcob
from cob_datapipeline.task_ingest_databases import get_database_docs, ingest_databases
from cob_datapipeline.task_slackpost import task_az_slackpostonsuccess, task_slackpostonfail
from cob_datapipeline.task_solrgetnumdocs import task_solrgetnumdocs
from cob_datapipeline.globals import GIT_PULL_TULCOB_LATEST_RELEASE, GIT_PULL_TULCOB_BRANCH_NAME, AZ_CORE, AIRFLOW_CONN_SOLR_LEADER

latest_release = GIT_PULL_TULCOB_LATEST_RELEASE
git_ref = GIT_PULL_TULCOB_BRANCH_NAME
solr_conn = AIRFLOW_CONN_SOLR_LEADER

#
# CREATE DAG
#
default_args = {
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

dag = DAG(
    'tul_cob_az_reindex', default_args=default_args, catchup=False,
    max_active_runs=1, schedule_interval=None
)

#
# CREATE TASKS
#
# Tasks with all logic contained in a single operator can be declared here.
# Tasks with custom logic are relegated to individual Python files.
#
get_num_solr_docs_pre = task_solrgetnumdocs(dag, AZ_CORE, 'get_num_solr_docs_pre')
git_pull_tulcob_task = task_git_pull_tulcob(dag, latest_release, git_ref)
get_database_docs_task = get_database_docs(dag)
ingest_databases_task = ingest_databases(dag=dag, conn=solr_conn)
get_num_solr_docs_post = task_solrgetnumdocs(dag, AZ_CORE, 'get_num_solr_docs_post')
post_slack = PythonOperator(
    task_id='slack_post_succ',
    python_callable=task_az_slackpostonsuccess,
    provide_context=True,
    dag=dag
)

#
# SET UP TASK DEPENDENCIES
#
git_pull_tulcob_task.set_upstream(get_num_solr_docs_pre)
get_database_docs_task.set_upstream(git_pull_tulcob_task)
ingest_databases_task.set_upstream(get_database_docs_task)
get_num_solr_docs_post.set_upstream(ingest_databases_task)
post_slack.set_upstream(get_num_solr_docs_post)
