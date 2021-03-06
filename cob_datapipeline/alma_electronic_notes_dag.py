"""Airflow DAG to harvest alma electronic notes"""
from datetime import datetime, timedelta
import os
from tulflow import tasks
import airflow
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.s3_to_sftp_operator import S3ToSFTPOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from cob_datapipeline.task_slack_posts import notes_slackpostonsuccess


"""
INIT SYSTEMWIDE VARIABLES

check for existence of systemwide variables shared across tasks that can be
initialized here if not found (i.e. if this is a new installation) & defaults exist
"""

AIRFLOW_HOME = Variable.get("AIRFLOW_HOME")

# cob_index Indexer Library Variables
GIT_BRANCH = Variable.get("CATALOG_QA_BRANCH")
LATEST_RELEASE = Variable.get("CATALOG_QA_LATEST_RELEASE")

# Get S3 data bucket variables
AIRFLOW_S3 = BaseHook.get_connection("AIRFLOW_S3")
AIRFLOW_DATA_BUCKET = Variable.get("AIRFLOW_DATA_BUCKET")

# CREATE DAG
DEFAULT_ARGS = {
    "owner": "cob",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    'start_date': datetime(2019, 5, 28),
    "on_failure_callback": tasks.execute_slackpostonfail,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

DAG = airflow.DAG(
    "alma_electronic_notes",
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    schedule_interval="@daily"
)

"""
CREATE TASKS

Tasks with all logic contained in a single operator can be declared here.
Tasks with custom logic are relegated to individual Python files.
"""

SET_DATETIME = PythonOperator(
    task_id="set_datetime",
    python_callable=datetime.now().strftime,
    op_args=["%Y-%m-%d_%H-%M-%S"],
    dag=DAG
)

HARVEST_NOTES = BashOperator(
    task_id='harvest_notes',
    bash_command=AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/harvest_notes.sh ",
    env={**os.environ, **{
        "ALMA_API_KEY": Variable.get("ALMA_API_KEY"),
        "GIT_BRANCH": GIT_BRANCH,
        "LATEST_RELEASE": str(LATEST_RELEASE),
        "BUCKET": Variable.get("AIRFLOW_DATA_BUCKET"),
        "AWS_ACCESS_KEY_ID": AIRFLOW_S3.login,
        "AWS_SECRET_ACCESS_KEY": AIRFLOW_S3.password,
        "DATETIME": "{{ ti.xcom_pull(task_ids='set_datetime') }}"
    }},
    dag=DAG
)

# TODO: Replace with S3ListOperator
NOTE_TYPES = [ "collection", "service" ]
S3_TO_SERVER_TASKS = {}

for note_type in NOTE_TYPES:
    S3_TO_SERVER_TASKS[note_type] = S3ToSFTPOperator(
    task_id='s3_to_server_' + note_type,
    sftp_conn_id="tul_cob",
    sftp_path="/tmp/" + note_type + "_notes.json",
    s3_conn_id="AIRFLOW_S3",
    s3_bucket=AIRFLOW_DATA_BUCKET,
    s3_key="electronic-notes/{{ ti.xcom_pull(task_ids='set_datetime') }}/" + note_type + "_notes.json",
    dag=DAG
)

S3_TO_SERVER_TASKS = list(S3_TO_SERVER_TASKS.values())

RELOAD_ELECTRONIC_COLLECTIONS_COMMAND = """
 sudo su - tul_cob bash -c \
 "cd /var/www/tul_cob && RAILS_ENV=production bundle exec rake reload_electronic_notes"
"""

RELOAD_ELECTRONIC_NOTES = SSHOperator(
    task_id="reload_electronic_notes",
    command=RELOAD_ELECTRONIC_COLLECTIONS_COMMAND,
    ssh_conn_id="tul_cob",
    dag=DAG
)

SLACK_POST_SUCCESS = PythonOperator(
    task_id="slack_post_success",
    python_callable=notes_slackpostonsuccess,
    provide_context=True,
    dag=DAG
)

# SET UP TASK DEPENDENCIES
SET_DATETIME >> HARVEST_NOTES
HARVEST_NOTES >> S3_TO_SERVER_TASKS
S3_TO_SERVER_TASKS >> RELOAD_ELECTRONIC_NOTES
RELOAD_ELECTRONIC_NOTES >> SLACK_POST_SUCCESS
