import airflow
from airflow import utils
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.http_operator import SimpleHttpOperator
from tul_cob_pipeline.task_oaiharvest import oaiharvest
from tul_cob_pipeline.task_ingestmarc import ingest_marc
from tul_cob_pipeline.task_processdeletes import process_deletes

core_name = 'blacklight-core-dev'
param_endpoint = '/solr/' + core_name + '/replication'


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 9, 30),
    'email': ['tug76662@temple.edu'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(hours=1),
}

dag = DAG(
    'tul_cob', default_args=default_args, schedule_interval=timedelta(days=1))

#http://master_host:port/solr/core_name/replication?command=disablereplication
pause_replication = SimpleHttpOperator(
    task_id='pause_replication',
    method='GET',
    http_conn_id='AIRFLOW_CONN_SOLR_LEADER',
    endpoint=param_endpoint,
    data={"command": "disablereplication"},
    headers={},
    dag=dag)

#http://master_host:port/solr/core_name/replication?command=enablereplication
resume_replication = SimpleHttpOperator(
    task_id='resume_replication',
    method='GET',
    http_conn_id='AIRFLOW_CONN_SOLR_LEADER',
    endpoint=param_endpoint,
    data={"command": "enablereplication"},
    headers={},
    dag=dag)


oaiharvest_task = oaiharvest(dag)
ingestmarc_task = ingest_marc(dag)
dodeletes_task = process_deletes(dag)

pause_replication.set_upstream(oaiharvest_task)
ingestmarc_task.set_upstream(pause_replication)
dodeletes_task.set_upstream(ingestmarc_task)
resume_replication.set_upstream(dodeletes_task)
