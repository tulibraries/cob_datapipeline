"""Airflow DAG to index AZ Databases into Solr."""
from datetime import datetime, timedelta

import airflow
from airflow.contrib.hooks.sftp_hook import SFTPHook
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator


from cob_datapipeline.operators.batch_sftp_to_s3_operator import BatchSFTPToS3Operator
from cob_datapipeline.task_slack_posts import az_slackpostonsuccess

from tulflow import tasks

ALMA_SFTP_CONNECTION_ID = "ALMASFTP"

# CREATE DAG
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 5, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': tasks.execute_slackpostonfail,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

DAG = airflow.DAG(
    'prod_cob_move_alma_sftp_to_s3',
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    schedule_interval="@weekly"
)

def calculate_list_of_files_to_move(**context):
    sftp_conn = SFTPHook(ftp_conn_id=ALMA_SFTP_CONNECTION_ID)
    files_list = sftp_conn.list_directory("./")
    just_alma_bibs_files = [f for f in files_list if f.startswith("alma_bibs__20")]
    if just_alma_bibs_files:
        most_recent_date = max([int(f.split("_")[3]) for f in just_alma_bibs_files])
        context['task_instance'].xcom_push(key="most_recent_date", value=most_recent_date)
        return [f for f in files_list if f.startswith(f"alma_bibs__{most_recent_date}")]
    else:
        raise ValueError('No matching files were found on the alma sftp server')



GET_LIST_OF_FILES_TO_TRANSFER = PythonOperator(
    task_id='get_list_of_alma_sftp_files_to_transer',
    python_callable=calculate_list_of_files_to_move,
    provide_context=True,
    dag=DAG
)

MOVE_FILES_TO_S3 = BatchSFTPToS3Operator(
    task_id="move_file_to_s3",
    s3_bucket="tulib-airflow-dev",
    s3_prefix="almasftp/{{ ti.xcom_pull(task_ids='get_list_of_alma_sftp_files_to_transer', key='most_recent_date' )}}/",
    sftp_base_path="./",
    files_list_task_xcom_id='get_list_of_alma_sftp_files_to_transer',
    sftp_conn_id='ALMASFTP',
    s3_conn_id='AIRFLOW_S3',
    dag=DAG,
    provide_context=True,
)

GET_LIST_OF_FILES_TO_TRANSFER.set_downstream(MOVE_FILES_TO_S3)