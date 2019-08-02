from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from cob_datapipeline.task_almasftp import task_almasftp
from cob_datapipeline.task_ingestsftpmarc import ingest_sftpmarc
from cob_datapipeline.task_ingestmarc import ingest_marc
from cob_datapipeline.parsesftpdump import parse_sftpdump_dates, renamesftpfiles_onsuccess
from cob_datapipeline.task_gitpulltulcob import task_git_pull_tulcob
from cob_datapipeline.task_addxmlns import task_addxmlns
from cob_datapipeline.task_solrcommit import task_solrcommit
from cob_datapipeline.task_slackpost import task_slackpostonsuccess, task_slackpostonfail
from cob_datapipeline.task_solrgetnumdocs import task_solrgetnumdocs

# INIT SYSTEMWIDE VARIABLES
#
# check for existence of systemwide variables shared across tasks that can be
# initialized here if not found (i.e. if this is a new installation)
#
try:
    latest_release = Variable.get("GIT_PULL_TULCOB_LATEST_RELEASE")
except KeyError:
    Variable.set("GIT_PULL_TULCOB_LATEST_RELEASE", "false")
    latest_release = Variable.get("GIT_PULL_TULCOB_LATEST_RELEASE")

try:
    git_ref = Variable.get("GIT_PULL_TULCOB_BRANCH_NAME")
except KeyError:
    Variable.set("GIT_PULL_TULCOB_BRANCH_NAME", "qa")
    git_ref = Variable.get("GIT_PULL_TULCOB_BRANCH_NAME")

#
# CREATE DAG
#
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 12, 13),
    'email': ['tug76662@temple.edu'],
    'email_on_failure': True,
    'email_on_retry': True,
    'on_failure_callback': task_slackpostonfail,
    'retries': 0,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'tul_cob_reindex', default_args=default_args, catchup=False,
    max_active_runs=1, schedule_interval=None
)

#
# CREATE TASKS
#
# Tasks with all logic contained in a single operator can be declared here.
# Tasks with custom logic are relegated to individual Python files.
#
almasftp_task = task_almasftp(dag)
git_pull_tulcob_task = task_git_pull_tulcob(dag, latest_release, git_ref)
addxmlns_task = task_addxmlns(dag)

core_name = Variable.get("BLACKLIGHT_CORE_NAME")
# pause_replication = task_solr_replication(dag, core_name, "disable")
# resume_replication = task_solr_replication(dag, core_name, "enable")
solr_commit_postclear = task_solrcommit(dag, core_name, "solr_commit_postclear")
solr_commit_postindex = task_solrcommit(dag, core_name, "solr_commit_postindex")
get_num_solr_docs_pre = task_solrgetnumdocs(dag, core_name, 'get_num_solr_docs_pre')
get_num_solr_docs_post = task_solrgetnumdocs(dag, core_name, 'get_num_solr_docs_post')

ingestsftpmarc_task = ingest_sftpmarc(dag)
ingest_marc_boundwith = ingest_marc(dag, 'boundwith_merged.xml', 'ingest_boundwith_merged')

parse_sftpdump_dates = PythonOperator(
    task_id='parse_sftpdump',
    provide_context=True,
    python_callable=parse_sftpdump_dates,
    op_kwargs={},
    dag=dag)

solr_endpoint_update = '/solr/' + core_name + '/update'
clear_index = SimpleHttpOperator(
    task_id='clear_index',
    method='GET',
    http_conn_id='AIRFLOW_CONN_SOLR_LEADER',
    endpoint=solr_endpoint_update,
    data={"stream.body": "<delete><query>*:*</query></delete>"},
    headers={},
    dag=dag)

post_slack = PythonOperator(
    task_id='slack_post_succ',
    python_callable=task_slackpostonsuccess,
    provide_context=True,
    dag=dag
)

rename_sftpdump = PythonOperator(
    task_id='archive_sftpdump',
    python_callable=renamesftpfiles_onsuccess,
    provide_context=True,
    op_kwargs={},
    dag=dag
)

#
# SET UP TASK DEPENDENCIES
#
git_pull_tulcob_task.set_upstream(get_num_solr_docs_pre)
clear_index.set_upstream(get_num_solr_docs_pre)
almasftp_task.set_upstream(get_num_solr_docs_pre)
addxmlns_task.set_upstream(almasftp_task)
solr_commit_postclear.set_upstream(clear_index)
ingestsftpmarc_task.set_upstream(git_pull_tulcob_task)
ingestsftpmarc_task.set_upstream(addxmlns_task)
ingestsftpmarc_task.set_upstream(solr_commit_postclear)
parse_sftpdump_dates.set_upstream(ingestsftpmarc_task)
ingest_marc_boundwith.set_upstream(parse_sftpdump_dates)
solr_commit_postindex.set_upstream(ingest_marc_boundwith)
get_num_solr_docs_post.set_upstream(solr_commit_postindex)
rename_sftpdump.set_upstream(parse_sftpdump_dates)
post_slack.set_upstream(get_num_solr_docs_post)
post_slack.set_upstream(rename_sftpdump)
