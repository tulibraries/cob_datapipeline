"""Airflow DAG to move alma FTP files to S3 and then archive those files into ./backup folder."""
from datetime import datetime, timedelta
import logging
import pendulum
from tulflow import tasks
import airflow
from airflow.models import Variable
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.operators.python import PythonOperator
from cob_datapipeline.operators.batch_sftp_to_s3_operator import BatchSFTPToS3Operator
from cob_datapipeline.helpers import determine_most_recent_date
from cob_datapipeline.tasks.xml_parse import update_variables

ALMA_SFTP_CONNECTION_ID = "ALMASFTP"
S3_CONN_ID = "AIRFLOW_S3"
S3_BUCKET = Variable.get("AIRFLOW_DATA_BUCKET")

# CREATE DAG
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2018, 12, 13, tz="UTC"),
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": tasks.execute_slackpostonfail,
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

DAG = airflow.DAG(
    "catalog_move_alma_sftp_to_s3",
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    schedule_interval=None
)

def calculate_list_of_files_to_move(**context):
    sftp_conn = SFTPHook(ftp_conn_id=ALMA_SFTP_CONNECTION_ID)
    files_list = sftp_conn.list_directory("./")
    # Ignore an file that does not start with this prefix
    just_alma_bibs_files = [f for f in files_list if f.startswith("alma_bibs__20")]
    if just_alma_bibs_files:
        most_recent_date = determine_most_recent_date(just_alma_bibs_files)
        context["task_instance"].xcom_push(key="most_recent_date", value=most_recent_date)
        return [f for f in files_list if f.startswith(f"alma_bibs__{most_recent_date}")]
    else:
        raise ValueError("No matching files were found on the alma sftp server")

GET_LIST_OF_FILES_TO_TRANSFER = PythonOperator(
    task_id="get_list_of_alma_sftp_files_to_transer",
    python_callable=calculate_list_of_files_to_move,
    dag=DAG
)

MOVE_FILES_TO_S3 = BatchSFTPToS3Operator(
    task_id="move_file_to_s3",
    s3_bucket=S3_BUCKET,
    s3_prefix="almasftp/{{ ti.xcom_pull(task_ids='get_list_of_alma_sftp_files_to_transer', key='most_recent_date' )}}/",
    sftp_base_path="./",
    xcom_id="get_list_of_alma_sftp_files_to_transer",
    sftp_conn_id="ALMASFTP",
    s3_conn_id=S3_CONN_ID,
    dag=DAG,
)


def archive_files_in_sftp(**context):
    """Move sftp files into the archive folder"""
    sftp_conn = SFTPHook(ftp_conn_id=ALMA_SFTP_CONNECTION_ID)
    # Paramiko is the underlying package used for SSH/SFTP conns
    # the paramiko client exposes a lot more core SFTP functionality
    paramiko_conn = sftp_conn.get_conn()

    most_recent_date = context["task_instance"].xcom_pull(
        task_ids="get_list_of_alma_sftp_files_to_transer",
        key="most_recent_date")
    list_of_files = context["task_instance"].xcom_pull(
        task_ids="get_list_of_alma_sftp_files_to_transer")
    archive_path = "archive"

    if archive_path not in sftp_conn.list_directory("./"):
        sftp_conn.create_directory(path=f"./{archive_path}")
    elif str(most_recent_date) not in sftp_conn.list_directory(f"./{archive_path}"):
        sftp_conn.create_directory(f"./{archive_path}/{most_recent_date}")

    count = 0
    for filename in list_of_files:
        logging.info(f"Moving {filename} to {archive_path}/{most_recent_date}/{filename}")
        paramiko_conn.rename(f"{filename}", f"{archive_path}/{most_recent_date}/{filename}")
        count += 1
    return count

ARCHIVE_FILES_IN_SFTP = PythonOperator(
    task_id="archive_files_in_sftp",
    python_callable=archive_files_in_sftp,
    dag=DAG
)

UPDATE_VARIABLES = PythonOperator(
    task_id="update_variables",
    python_callable=update_variables,
    op_kwargs={
        "UPDATE": {
            "ALMASFTP_S3_ORIGINAL_DATA_NAMESPACE":
                "{{ ti.xcom_pull(task_ids='get_list_of_alma_sftp_files_to_transer', key='most_recent_date')}}",
        }
    },
    dag=DAG
)

def slackpostonsuccess(**context):
    """Send slack notification of successful DAG run"""
    most_recent_date = context["task_instance"].xcom_pull(
        task_ids="get_list_of_alma_sftp_files_to_transer",
        key="most_recent_date")
    count = context["task_instance"].xcom_pull(
        task_ids="archive_files_in_sftp")
    msg = f"{count} files moved from sftp to s3 in almasftp/{most_recent_date} and then archived on the sftp server in archive/{most_recent_date}"
    return tasks.execute_slackpostonsuccess(context, conn_id="COB_SLACK_WEBHOOK", message=msg)

SLACK_POST_SUCCESS = PythonOperator(
    task_id="success_slack_trigger",
    python_callable=slackpostonsuccess,
    dag=DAG
)

GET_LIST_OF_FILES_TO_TRANSFER.set_downstream(MOVE_FILES_TO_S3)
MOVE_FILES_TO_S3.set_downstream(ARCHIVE_FILES_IN_SFTP)
ARCHIVE_FILES_IN_SFTP.set_downstream(UPDATE_VARIABLES)
UPDATE_VARIABLES.set_downstream(SLACK_POST_SUCCESS)
