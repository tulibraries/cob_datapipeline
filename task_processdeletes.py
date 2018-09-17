from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from airflow import AirflowException
import os

def process_deletes(dag):
    deletes_command = Variable.get("AIRFLOW_HOME") + "/dags/tul_cob_pipeline/scripts/process_deletes.sh"
    if os.path.isfile(deletes_command):
       t1 = BashOperator(
            task_id= 'process_deletes',
            bash_command=deletes_command+' ',
            dag=dag
       )
    else:
        raise Exception(str(os.path.isfile(ingest_command)) + " Cannot locate {}".format(deletes_command))
    return t1
