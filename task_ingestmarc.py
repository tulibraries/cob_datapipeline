from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from airflow import AirflowException
from airflow.hooks.base_hook import BaseHook
import os


def ingest_marc(dag, marcfilename, taskid):
    # http://162.216.18.86:8983/solr/blacklight-core
    conn = BaseHook.get_connection('AIRFLOW_CONN_SOLR_LEADER')
    solr_endpoint = 'http://' + conn.host + ':' + str(conn.port) + '/solr/' + Variable.get('BLACKLIGHT_CORE_NAME')
    infilename = Variable.get("AIRFLOW_DATA_DIR") + '/' + marcfilename
    ingest_command = Variable.get("AIRFLOW_HOME") + "/dags/cob_datapipeline/scripts/ingest_marc.sh"
    logfile = "{}/traject_log_{}.log".format(Variable.get("AIRFLOW_LOG_DIR"), marcfilename) 
    if not os.path.exists(logfile):
        with open(logfile, 'w'): pass

    if os.path.isfile(ingest_command):
        t1 = BashOperator(
            task_id=taskid,
            bash_command="source {} {} {}  2>&1  | tee {}".format(ingest_command, infilename, solr_endpoint, logfile) + ' ',
            dag=dag
        )
    else:
        raise Exception("Cannot locate {}".format(ingest_command))
    return t1
