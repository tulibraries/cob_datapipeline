from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from airflow import AirflowException
import os


def task_addxmlns(dag):
    command = Variable.get("AIRFLOW_HOME") + "/dags/cob_datapipeline/scripts/addxmlns.sh"
    datadir = Variable.get("AIRFLOW_DATA_DIR") + "/sftpdump"
    if os.path.isfile(command):
        t1 = BashOperator(
            task_id='addxmlns',
            bash_command=command + ' ' + datadir + ' ',
            dag=dag
        )

    else:
        raise Exception(str(os.path.isfile(command)) + " Cannot locate {}".format(command))
    return t1
