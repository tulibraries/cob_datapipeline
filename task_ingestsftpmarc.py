from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from airflow import AirflowException
import os


def ingest_sftpmarc(dag):
    ingest_command = Variable.get("AIRFLOW_HOME") + "/dags/cob_datapipeline/scripts/ingest_marc_multi.sh"
    marcfilepath = Variable.get("AIRFLOW_DATA_DIR") + "/sftpdump/"
    if os.path.isfile(ingest_command):
        t1 = BashOperator(
            task_id='ingest_sftp_marc',
            bash_command="source {} {}  2>&1  | tee {}/traject_log.tmp ".format(ingest_command, marcfilepath, Variable.get("AIRFLOW_DATA_DIR")) + ' ',
            dag=dag
        )

    else:
        raise Exception(str(os.path.isfile(ingest_command)) + " Cannot locate {}".format(ingest_command))
    return t1
