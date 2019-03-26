from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from airflow import AirflowException
import os


def ingest_marc(dag, marcfilename, taskid):
    infilename = Variable.get("AIRFLOW_DATA_DIR") + '/' + marcfilename
    ingest_command = Variable.get("AIRFLOW_HOME") + "/dags/cob_datapipeline/scripts/ingest_marc.sh"
    if os.path.isfile(ingest_command):
        t1 = BashOperator(
            task_id=taskid,
            bash_command="source {} {}  2>&1  | tee {}/traject_log_{}.tmp ".format(ingest_command, infilename, Variable.get("AIRFLOW_DATA_DIR"), marcfilename) + ' ',
            dag=dag
        )

    else:
        raise Exception(str(os.path.isfile(ingest_command)) + " Cannot locate {}".format(ingest_command))
    return t1
