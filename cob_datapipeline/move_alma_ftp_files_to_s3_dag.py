"""Airflow DAG to index AZ Databases into Solr."""
from datetime import datetime, timedelta
import airflow
from airflow.models import Variable
from airflow.contrib.hooks.sftp_hook import SFTPHook
from airflow.operators.python_operator import PythonOperator
from tulflow import tasks

"""
INIT SYSTEMWIDE VARIABLES

check for existence of systemwide variables shared across tasks that can be
initialized here if not found (i.e. if this is a new installation) & defaults exist
"""

AIRFLOW_HOME = Variable.get("AIRFLOW_HOME")
AIRFLOW_USER_HOME = Variable.get("AIRFLOW_USER_HOME")
SCHEDULE_INTERVAL = Variable.get("FTP_TRANSFER_SCHEDULE_INTERVAL", "@daily")

FTP_CONNECTION_ID = "ALMA_SFTP"



# CREATE DAG
DEFAULT_ARGS = {
    "owner": "cob",
    "depends_on_past": False,
    "start_date": datetime(2019, 5, 28),
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": tasks.execute_slackpostonfail,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

DAG = airflow.DAG(
    "prod_cob_move_alma_sftp_to_s3",
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    schedule_interval=SCHEDULE_INTERVAL
)

"""
CREATE TASKS

Tasks with all logic contained in a single operator can be declared here.
Tasks with custom logic are relegated to individual Python files.
"""

def list_remote_directory(ssh_conn_id):
  hook = SFTPHook(ftp_conn_id=ssh_conn_id)
  ld = hook.list_directory(".")
  print(ld)
  return ld


LIST_FTP_FILES = PythonOperator(
    task_id="list_alma_sftp_files",
    python_callable=list_remote_directory,
    op_kwargs={"ssh_conn_id": FTP_CONNECTION_ID},
    dag=DAG
)




# re.compile(r'alma_bibs__(\d+_\d+)_new_\d+\.xml\.tar\.gz')

