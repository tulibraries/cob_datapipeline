import airflow
from airflow import utils
from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from cob_datapipeline.task_ingestmarc import ingest_marc
from cob_datapipeline.task_processdeletes import process_deletes
from cob_datapipeline.almaoai_harvest import almaoai_harvest
from cob_datapipeline.renamemarcfiles import renamemarcfiles_onsuccess

core_name = Variable.get("BLACKLIGHT_CORE_NAME");

param_endpoint_replication = '/solr/' + core_name + '/replication'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 12, 13),
    'email': ['tug76662@temple.edu'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'tul_cob', default_args=default_args, schedule_interval=timedelta(hours=6))


ingestmarc_task = ingest_marc(dag)

#http://master_host:port/solr/core_name/replication?command=disablereplication
pause_replication = SimpleHttpOperator(
    task_id='pause_replication',
    method='GET',
    http_conn_id='AIRFLOW_CONN_SOLR_LEADER',
    endpoint=param_endpoint_replication,
    data={"command": "disablereplication"},
    headers={},
    dag=dag)

#http://master_host:port/solr/core_name/replication?command=enablereplication
resume_replication = SimpleHttpOperator(
    task_id='resume_replication',
    method='GET',
    http_conn_id='AIRFLOW_CONN_SOLR_LEADER',
    endpoint=param_endpoint_replication,
    data={"command": "enablereplication"},
    headers={},
    dag=dag)

oaiharvest_task = PythonOperator(
     task_id='almaoai_harvest',
     python_callable=almaoai_harvest,
     dag=dag
)

dodeletes_task = PythonOperator(
    task_id='do_deletes',
    provide_context=True,
    python_callable=process_deletes,
    op_kwargs={'core_name':core_name},
    dag=dag)

renamemarc_task = PythonOperator(
    task_id='rename_marc',
    provide_context=True,
    python_callable=renamemarcfiles_onsuccess,
    op_kwargs={},
    dag=dag)


pause_replication.set_upstream(oaiharvest_task)
ingestmarc_task.set_upstream(pause_replication)
dodeletes_task.set_upstream(ingestmarc_task)
renamemarc_task.set_upstream(dodeletes_task)
resume_replication.set_upstream(renamemarc_task)
