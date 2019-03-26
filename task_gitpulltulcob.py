from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from airflow import AirflowException
import os


def task_git_pull_tulcob(dag):
    command = Variable.get("AIRFLOW_HOME") + "/dags/cob_datapipeline/scripts/git_pull_tul_cob.sh"
    if os.path.isfile(command):
        t1 = BashOperator(
            task_id='git_pull_tulcob',
            bash_command=command + ' ',
            dag=dag
        )

    else:
        raise Exception(str(os.path.isfile(command)) + " Cannot locate {}".format(command))
    return t1
