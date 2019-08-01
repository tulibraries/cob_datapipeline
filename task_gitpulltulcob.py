"""Airflow Task to clone TUL Cob GitHub Repository for Transform Code."""
import os
import airflow
from airflow.operators.bash_operator import BashOperator

def task_git_pull_tulcob(dag, latest_release, git_ref):
    """Method that gathers required context then runs BashOperator for git pull."""
    airflow_home = airflow.models.Variable.get("AIRFLOW_HOME")
    command = airflow_home + "/dags/cob_datapipeline/scripts/git_pull_tul_cob.sh"

    if os.path.isfile(command):
        t1 = BashOperator(
            task_id='git_pull_tulcob',
            bash_command=command + ' {} {}'.format(latest_release, git_ref),
            dag=dag
        )

    else:
        raise Exception(str(os.path.isfile(command)) + " Cannot locate {}".format(command))
    return t1
