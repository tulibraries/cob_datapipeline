import airflow
from airflow import utils
from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from cob_datapipeline.task_ingestmarc import ingest_marc
from cob_datapipeline.processdeletes import process_deletes
from cob_datapipeline.almaoai_harvest import almaoai_harvest
from cob_datapipeline.renamemarcfiles import renamemarcfiles_onsuccess
from cob_datapipeline.task_solr_replication import task_solr_replication
from cob_datapipeline.task_solrcommit import task_solrcommit
from cob_datapipeline.task_slackpost import task_slackpostonsuccess, task_slackpostonfail
from cob_datapipeline.processtrajectlog import process_trajectlog
from cob_datapipeline.task_solrgetnumdocs import task_solrgetnumdocs

core_name = Variable.get("BLACKLIGHT_CORE_NAME")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 12, 13),
    'email': ['tug76662@temple.edu'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': task_slackpostonfail,
    'provide_context': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=10)
}

dag = DAG(
    'tul_cob', default_args=default_args, catchup=False, schedule_interval=timedelta(hours=6))


ingest_marc = ingest_marc(dag, 'oairecords.xml', 'ingest_marc')

pause_replication = task_solr_replication(dag, core_name, "disable")
resume_replication = task_solr_replication(dag, core_name, "enable")
solr_commit = task_solrcommit(dag, core_name, "solr_commit")

success_msg = "" #"{{ execution_date }} Task Instance: {{ ti }} DAG: {{ dag }} Task: {{ task }} DAG Run: {{ dag_run }}"
post_slack = PythonOperator(
    task_id='slack_post_succ',
    python_callable=task_slackpostonsuccess,
    provide_context=True,
    dag=dag
)

oaiharvest = PythonOperator(
    task_id='almaoai_harvest',
    python_callable=almaoai_harvest,
    dag=dag
)

do_deletes = PythonOperator(
    task_id='do_deletes',
    python_callable=process_deletes,
    op_kwargs={},
    dag=dag
)

rename_marc = PythonOperator(
    task_id='rename_marc',
    python_callable=renamemarcfiles_onsuccess,
    op_kwargs={},
    dag=dag
)

parse_traject = PythonOperator(
    task_id='process_trajectlog',
    python_callable=process_trajectlog,
    op_kwargs={},
    dag=dag
)

get_num_solr_docs_pre = task_solrgetnumdocs(dag, core_name, 'get_num_solr_docs_pre')
get_num_solr_docs_post = task_solrgetnumdocs(dag, core_name, 'get_num_solr_docs_post')

oaiharvest.set_upstream(get_num_solr_docs_pre)
pause_replication.set_upstream(oaiharvest)
ingest_marc.set_upstream(pause_replication)
do_deletes.set_upstream(ingest_marc)
solr_commit.set_upstream(do_deletes)
get_num_solr_docs_post.set_upstream(solr_commit)
rename_marc.set_upstream(get_num_solr_docs_post)
parse_traject.set_upstream(rename_marc)
post_slack.set_upstream(parse_traject)
resume_replication.set_upstream(post_slack)
