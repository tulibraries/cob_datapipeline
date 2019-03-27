import airflow
from airflow import utils
from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from cob_datapipeline.task_almasftp import task_almasftp
from cob_datapipeline.task_ingestsftpmarc import ingest_sftpmarc
from cob_datapipeline.task_ingestmarc import ingest_marc
from cob_datapipeline.parsesftpdump import parse_sftpdump
from cob_datapipeline.task_gitpulltulcob import task_git_pull_tulcob
from cob_datapipeline.task_addxmlns import task_addxmlns
from cob_datapipeline.task_solr_replication import task_solr_replication
from cob_datapipeline.task_solrcommit import task_solrcommit
from cob_datapipeline.task_slackpost import task_slackpostonsuccess, task_slackpostonfail
from cob_datapipeline.task_solrgetnumdocs import task_solrgetnumdocs

core_name = Variable.get("BLACKLIGHT_CORE_NAME")
solr_endpoint_update = '/solr/' + core_name + '/update'

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
    'tul_cob_reindex', default_args=default_args, catchup=False, schedule_interval=None)


almasftp_task = task_almasftp(dag)
git_pull_tulcob_task = task_git_pull_tulcob(dag)
addxmlns_task = task_addxmlns(dag)

pause_replication = task_solr_replication(dag, core_name, "disable")
resume_replication = task_solr_replication(dag, core_name, "enable")
solr_commit_postclear = task_solrcommit(dag, core_name, "solr_commit_postclear")
solr_commit_postindex = task_solrcommit(dag, core_name, "solr_commit_postindex")
get_num_solr_docs_pre = task_solrgetnumdocs(dag, core_name, 'get_num_solr_docs_pre')
get_num_solr_docs_post = task_solrgetnumdocs(dag, core_name, 'get_num_solr_docs_post')

ingestsftpmarc_task = ingest_sftpmarc(dag)
ingest_marc_boundwith = ingest_marc(dag, 'boundwith_merged.xml', 'ingest_boundwith_merged')


parse_sftpdump = PythonOperator(
    task_id='parse_sftpdump',
    provide_context=True,
    python_callable=parse_sftpdump,
    op_kwargs={},
    dag=dag)

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

git_pull_tulcob_task.set_upstream(get_num_solr_docs_pre)
almasftp_task.set_upstream(git_pull_tulcob_task)
addxmlns_task.set_upstream(almasftp_task)
pause_replication.set_upstream(addxmlns_task)
clear_index.set_upstream(pause_replication)
solr_commit_postclear.set_upstream(clear_index)
ingestsftpmarc_task.set_upstream(solr_commit_postclear)
parse_sftpdump.set_upstream(ingestsftpmarc_task)
ingest_marc_boundwith.set_upstream(parse_sftpdump)
solr_commit_postindex.set_upstream(ingest_marc_boundwith)
get_num_solr_docs_post.set_upstream(solr_commit_postindex)
resume_replication.set_upstream(get_num_solr_docs_post)
post_slack.set_upstream(resume_replication)
