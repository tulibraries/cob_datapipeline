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


task_almasftp = task_almasftp(dag)

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


pause_replication.set_upstream(oaiharvest_task)
almasftp_task.set_upstream(pause_replication)
resume_replication.set_upstream(almasftp_task)
