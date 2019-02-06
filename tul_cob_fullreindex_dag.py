import airflow
from airflow import utils
from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from cob_datapipeline.task_almasftp import task_almasftp

core_name = Variable.get("BLACKLIGHT_CORE_NAME");

param_endpoint_replication = '/solr/' + core_name + '/replication'
param_endpoint_update = '/solr/' + core_name + 'update'

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
    'tul_cob_reindex', default_args=default_args, schedule_interval=None)


almasftp_task = task_almasftp(dag)

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
    trigger_rule="all_done",
    headers={},
    dag=dag)


clear_index = SimpleHttpOperator(
    task_id='clear_index',
    method='GET',
    http_conn_id='AIRFLOW_CONN_SOLR_LEADER',
    endpoint=param_endpoint_update,
    data={"stream.body": "<delete><query>*:*</query></delete>"},
    headers={},
    dag=dag)

solr_commit = SimpleHttpOperator(
    task_id='solr_commit',
    method='GET',
    http_conn_id='AIRFLOW_CONN_SOLR_LEADER',
    endpoint=param_endpoint_update,
    data={"stream.body": "<commit/>"},
    headers={},
    dag=dag)


clear_index.set_upstream(pause_replication)
solr_commit.set_upstream(clear_index)
almasftp_task.set_upstream(solr_commit)
resume_replication.set_upstream(almasftp_task)
