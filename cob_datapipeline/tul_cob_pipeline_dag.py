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
from cob_datapipeline.task_slack_posts import slackpostonsuccess, slackpostonfail
from cob_datapipeline.processtrajectlog import process_trajectlog
from cob_datapipeline.task_solrgetnumdocs import task_solrgetnumdocs

#
# INIT SYSTEMWIDE VARIABLES
#
# check for existence of systemwide variables shared across tasks that can be
# initialized here if not found (i.e. if this is a new installation)
#
try:
    oai_publish_interval = Variable.get("ALMA_OAI_PUBLISH_INTERVAL")
    dag_run_interval = timedelta(hours=int(oai_publish_interval))
except KeyError:
    Variable.set("ALMA_OAI_PUBLISH_INTERVAL", "6")
    dag_run_interval = timedelta(hours=6)

try:
    date_last_harvest = Variable.get("almaoai_last_harvest_date")
except KeyError:
    Variable.set("almaoai_last_harvest_date", datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'))

try:
    date_last_harvest = Variable.get("almaoai_last_harvest_from_date")
except KeyError:
    Variable.set("almaoai_last_harvest_from_date", datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'))


#
# CREATE DAG
#
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 12, 13, 3),
    'email': ['tug76662@temple.edu'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': slackpostonfail,
    'provide_context': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=10)
}

dag = DAG(
    'tul_cob', default_args=default_args, max_active_runs=1,
    catchup=False, schedule_interval=dag_run_interval
)

#
# CREATE TASKS
#
# Tasks with all logic contained in a single operator can be declared here.
# Tasks with custom logic are relegated to individual Python files.
#
marcfilename = 'oairecords.xml'
ingest_marc = ingest_marc(dag, marcfilename, 'ingest_marc')

core_name = Variable.get("BLACKLIGHT_CORE_NAME")
pause_replication = task_solr_replication(dag, core_name, "disable")
resume_replication = task_solr_replication(dag, core_name, "enable")
solr_commit = task_solrcommit(dag, core_name, "solr_commit")

post_slack = PythonOperator(
    task_id='slack_post_succ',
    python_callable=slackpostonsuccess,
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
    op_kwargs={'marcfilename': marcfilename},
    dag=dag
)

get_num_solr_docs_pre = task_solrgetnumdocs(dag, core_name, 'get_num_solr_docs_pre')
get_num_solr_docs_post = task_solrgetnumdocs(dag, core_name, 'get_num_solr_docs_post')

#
# SET UP TASK DEPENDENCIES
#
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