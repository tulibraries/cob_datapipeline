"""Airflow DAG to move bw files from alma-sftp to s3 and archives the bw ftp files."""
from datetime import datetime, timedelta
import logging
from tulflow import tasks
import airflow
from airflow.models import Variable
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.operators.python_operator import PythonOperator
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
    "start_date": datetime(2019, 5, 28),
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": tasks.execute_slackpostonfail,
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

DAG = airflow.DAG(
    "boundwith_move_alma_sftp_to_s3",
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    schedule_interval=None
)

def calculate_list_of_files_to_move(**context):
    sftp_conn = SFTPHook(ftp_conn_id=ALMA_SFTP_CONNECTION_ID)
    files_list = sftp_conn.list_directory("./")
    # Ignore an file that does not start with this prefix
    files = [f for f in files_list if f.startswith("alma_bibs__boundwith2_20")]
    if files:
        most_recent_date = determine_most_recent_date(files)
        context["task_instance"].xcom_push(key="most_recent__bw_date", value=most_recent_date)
        return [f for f in files_list if f.startswith(f"alma_bibs__boundwith2_{most_recent_date}")]
    else:
        raise ValueError("No matching bw files were found on the alma sftp server")

GET_LIST_OF_FILES_TO_TRANSFER = PythonOperator(
    task_id="get_list_of_alma_sftp_files_to_transer",
    python_callable=calculate_list_of_files_to_move,
    provide_context=True,
    dag=DAG
)

MOVE_FILES_TO_S3 = BatchSFTPToS3Operator(
    task_id="move_file_to_s3",
    s3_bucket=S3_BUCKET,
    s3_prefix="almasftp/bw/{{ ti.xcom_pull(task_ids='get_list_of_alma_sftp_files_to_transer', key='most_recent_date' )}}/",
    sftp_base_path="./",
    xcom_id="get_list_of_alma_sftp_files_to_transer",
    sftp_conn_id="ALMASFTP",
    s3_conn_id=S3_CONN_ID,
    dag=DAG,
    provide_context=True,
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
    archive_path = "bw-archive"

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
    provide_context=True,
    dag=DAG
)

UPDATE_VARIABLES = PythonOperator(
    task_id="update_variables",
    provide_context=True,
    python_callable=update_variables,
    op_kwargs={
        "UPDATE": {
            "ALMASFTP_S3_ORIGINAL_BW_DATA_NAMESPACE":
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
    msg = f"{count} files moved from sftp to s3 in almasftp/bw/{most_recent_date} and then archived on the sftp server in archive/{most_recent_date}"
    return tasks.execute_slackpostonsuccess(context, conn_id="COB_SLACK_WEBHOOK", message=msg)

SLACK_POST_SUCCESS = PythonOperator(
    task_id="success_slack_trigger",
    provide_context=True,
    python_callable=slackpostonsuccess,
    dag=DAG
)

GET_LIST_OF_FILES_TO_TRANSFER.set_downstream(MOVE_FILES_TO_S3)
MOVE_FILES_TO_S3.set_downstream(ARCHIVE_FILES_IN_SFTP)
ARCHIVE_FILES_IN_SFTP.set_downstream(UPDATE_VARIABLES)
UPDATE_VARIABLES.set_downstream(SLACK_POST_SUCCESS)
