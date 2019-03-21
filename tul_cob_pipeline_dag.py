"""TUL_PARTIAL_INDEX: TU Libraries Airflow DAG for Partial Alma to Solr Indexing."""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from cob_datapipeline.task_ingestmarc import ingest_marc
from cob_datapipeline.task_processdeletes import process_deletes
from cob_datapipeline.almaoai_harvest import almaoai_harvest
from cob_datapipeline.renamemarcfiles import renamemarcfiles_onsuccess

CORE_NAME = Variable.get("BLACKLIGHT_CORE_NAME")

PARAM_ENDPOINT_REPLICATION = '/solr/' + CORE_NAME + '/replication'

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 12, 13),
    'email': ['tug76662@temple.edu'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=10),
}

OAI_INDEX_DAG = DAG('tul_cob', default_args=DEFAULT_ARGS, schedule_interval=timedelta(hours=6))


INGESTMARC_TASK = ingest_marc(OAI_INDEX_DAG)

#http://master_host:port/solr/CORE_NAME/replication?command=disablereplication
PAUSE_REPLICATION = SimpleHttpOperator(
    task_id='pause_replication',
    method='GET',
    http_conn_id='AIRFLOW_CONN_SOLR_LEADER',
    endpoint=PARAM_ENDPOINT_REPLICATION,
    data={"command": "disablereplication"},
    headers={},
    dag=OAI_INDEX_DAG)

#http://master_host:port/solr/CORE_NAME/replication?command=enablereplication
RESUME_REPLICATION = SimpleHttpOperator(
    task_id='resume_replication',
    method='GET',
    http_conn_id='AIRFLOW_CONN_SOLR_LEADER',
    endpoint=PARAM_ENDPOINT_REPLICATION,
    data={"command": "enablereplication"},
    trigger_rule="all_done",
    headers={},
    dag=OAI_INDEX_DAG)

OAIHARVEST_TASK = PythonOperator(
    task_id='almaoai_harvest',
    python_callable=almaoai_harvest,
    dag=OAI_INDEX_DAG
)

DODELETES_TASK = PythonOperator(
    task_id='do_deletes',
    provide_context=True,
    python_callable=process_deletes,
    op_kwargs={'CORE_NAME':CORE_NAME},
    dag=OAI_INDEX_DAG)

RENAMEMARC_TASK = PythonOperator(
    task_id='rename_marc',
    provide_context=True,
    python_callable=renamemarcfiles_onsuccess,
    op_kwargs={},
    dag=OAI_INDEX_DAG)


PAUSE_REPLICATION.set_upstream(OAIHARVEST_TASK)
INGESTMARC_TASK.set_upstream(PAUSE_REPLICATION)
DODELETES_TASK.set_upstream(INGESTMARC_TASK)
RENAMEMARC_TASK.set_upstream(DODELETES_TASK)
RESUME_REPLICATION.set_upstream(RENAMEMARC_TASK)
