"""TUL_FULL_INDEX: TU Libraries Airflow DAG for Full Alma to Solr Indexing."""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.http_operator import SimpleHttpOperator
from cob_datapipeline.task_almasftp import task_almasftp

CORE_NAME = Variable.get("BLACKLIGHT_CORE_NAME")

PARAM_ENDPOINT_REPLICATION = '/solr/' + CORE_NAME + '/replication'
PARAM_ENDPOINT_UPDATE = '/solr/' + CORE_NAME + 'update'

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

FTP_INDEX_DAG = DAG(
    'tul_cob_reindex', default_args=DEFAULT_ARGS, schedule_interval=None)


ALMASFTP_TASK = task_almasftp(FTP_INDEX_DAG)

#http://master_host:port/solr/core_name/replication?command=disablereplication
PAUSE_REPLICATION = SimpleHttpOperator(
    task_id='pause_replication',
    method='GET',
    http_conn_id='AIRFLOW_CONN_SOLR_LEADER',
    endpoint=PARAM_ENDPOINT_REPLICATION,
    data={"command": "disablereplication"},
    headers={},
    dag=FTP_INDEX_DAG)

#http://master_host:port/solr/core_name/replication?command=enablereplication
RESUME_REPLICATION = SimpleHttpOperator(
    task_id='resume_replication',
    method='GET',
    http_conn_id='AIRFLOW_CONN_SOLR_LEADER',
    endpoint=PARAM_ENDPOINT_REPLICATION,
    data={"command": "enablereplication"},
    trigger_rule="all_done",
    headers={},
    dag=FTP_INDEX_DAG)


CLEAR_INDEX = SimpleHttpOperator(
    task_id='clear_index',
    method='GET',
    http_conn_id='AIRFLOW_CONN_SOLR_LEADER',
    endpoint=PARAM_ENDPOINT_UPDATE,
    data={"stream.body": "<delete><query>*:*</query></delete>"},
    headers={},
    dag=FTP_INDEX_DAG)

SOLR_COMMIT = SimpleHttpOperator(
    task_id='solr_commit',
    method='GET',
    http_conn_id='AIRFLOW_CONN_SOLR_LEADER',
    endpoint=PARAM_ENDPOINT_UPDATE,
    data={"stream.body": "<commit/>"},
    headers={},
    dag=FTP_INDEX_DAG)


CLEAR_INDEX.set_upstream(PAUSE_REPLICATION)
SOLR_COMMIT.set_upstream(CLEAR_INDEX)
ALMASFTP_TASK.set_upstream(SOLR_COMMIT)
RESUME_REPLICATION.set_upstream(ALMASFTP_TASK)
