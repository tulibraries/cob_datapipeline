"""
A maintenance workflow that you can deploy into Airflow to periodically clean
out the task logs to avoid those getting too big.
airflow trigger_dag --conf '[curly-braces]"maxLogAgeInDays":30[curly-braces]' airflow-log-cleanup
--conf options:
    maxLogAgeInDays:<INT> - Optional
"""
import os
import logging
import jinja2
import pendulum

from airflow.sdk import DAG
from airflow.configuration import conf
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import timedelta
from airflow.providers.slack.notifications.slack import send_slack_notification

slackpostonfail = send_slack_notification(channel="infra_alerts", username="airflow", text=":poop: Task failed: {{ dag.dag_id }} {{ ti.task_id }} {{ logical_date }} {{ ti.log_url }}")


# airflow-log-cleanup
DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
START_DATE = pendulum.datetime(2025, 1, 1, tz="UTC")
BASE_LOG_FOLDER = conf.get("logging", "BASE_LOG_FOLDER").rstrip("/")
# How often to Run. @daily - Once a day at Midnight
SCHEDULE = "@daily"
# Who is listed as the owner of this DAG in the Airflow Web Server
DAG_OWNER_NAME = "maintenance"
# Length to retain the log files if not already provided in the conf. If this
# is set to 30, the job will remove those files that are 30 days old or older
DEFAULT_MAX_LOG_AGE_IN_DAYS = "{{ var.value.get('airflow_log_cleanup__max_log_age_in_days', 30) }}"
# Whether the job should delete the logs or not. Included if you want to
# temporarily avoid deleting the logs
ENABLE_DELETE = True
# The number of worker nodes you have in Airflow. Will attempt to run this
# process for however many workers there are so that each worker gets its
# logs cleared.
NUMBER_OF_WORKERS = 1
DIRECTORIES_TO_DELETE = [
    "boundwith_move_alma_sftp_to_s3",
    "catalog_full_reindex",
    "catalog_move_alma_sftp_to_s3",
    "catalog_pre_production_oai_harvest",
    "catalog_production_oai_harvest",
    "prod_az_reindex",
    "prod_web_content_reindex",
    "maintenance_airflow-log-cleanup",
]
ENABLE_DELETE_CHILD_LOG = "{{ var.value.get('airflow_log_cleanup__enable_delete_child_log', 'False') }}"
LOG_CLEANUP_PROCESS_LOCK_FILE = "/tmp/airflow_log_cleanup_worker_" + "{{params.directory}}" + ".lock"
logging.info("ENABLE_DELETE_CHILD_LOG  " + ENABLE_DELETE_CHILD_LOG)

if not BASE_LOG_FOLDER or BASE_LOG_FOLDER.strip() == "":
    raise ValueError(
        "BASE_LOG_FOLDER variable is empty in airflow.cfg. It can be found "
        "under the [logging] section in the cfg file. Kindly provide an "
        "appropriate directory path."
    )

CHILD_PROCESS_LOG_DIRECTORY = None
try:
    CHILD_PROCESS_LOG_DIRECTORY = conf.get(
        "scheduler", "CHILD_PROCESS_LOG_DIRECTORY"
    )
    if CHILD_PROCESS_LOG_DIRECTORY != ' ':
        DIRECTORIES_TO_DELETE.append(CHILD_PROCESS_LOG_DIRECTORY)
except Exception as e:
    logging.exception(
        "Could not obtain CHILD_PROCESS_LOG_DIRECTORY from " +
        "Airflow Configurations: " + str(e)
    )

default_args = {
    'owner': DAG_OWNER_NAME,
    'depends_on_past': False,
    'start_date': START_DATE,
    'on_failure_callback': [slackpostonfail],
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    schedule=SCHEDULE,
    start_date=START_DATE,
    template_undefined=jinja2.Undefined
)
if hasattr(dag, 'doc_md'):
    dag.doc_md = __doc__
if hasattr(dag, 'catchup'):
    dag.catchup = False

start = EmptyOperator(
    task_id='start',
    dag=dag)

log_cleanup = """

echo "Getting Configurations..."
BASE_LOG_FOLDER="{{params.base_log_folder}}/{{params.directory}}"
WORKER_SLEEP_TIME="{{params.sleep_time}}"
ENABLE_DELETE_CHILD_LOG="{{ var.value.get('airflow_log_cleanup__enable_delete_child_log', 'False') }}"
CHILD_PROCESS_LOG_DIRECTORY="{{ params.child_process_log_directory }}"

sleep ${WORKER_SLEEP_TIME}s

MAX_LOG_AGE_IN_DAYS="{{dag_run.conf.maxLogAgeInDays}}"
if [ "${MAX_LOG_AGE_IN_DAYS}" == "" ]; then
    echo "maxLogAgeInDays conf variable isn't included. Using Default '${DEFAULT_MAX_LOG_AGE_IN_DAYS}'."
    MAX_LOG_AGE_IN_DAYS="${DEFAULT_MAX_LOG_AGE_IN_DAYS}"
fi
ENABLE_DELETE=""" + str("true" if ENABLE_DELETE else "false") + """
echo "Finished Getting Configurations"
echo ""

echo "Configurations:"
echo "BASE_LOG_FOLDER:      '${BASE_LOG_FOLDER}'"
echo "MAX_LOG_AGE_IN_DAYS:  '${MAX_LOG_AGE_IN_DAYS}'"
echo "ENABLE_DELETE:        '${ENABLE_DELETE}'"
echo "ENABLE_DELETE_CHILD_LOG: '${ENABLE_DELETE_CHILD_LOG}'"

if [ "${BASE_LOG_FOLDER}" = "${CHILD_PROCESS_LOG_DIRECTORY}" ] && [ "${ENABLE_DELETE_CHILD_LOG,,}" != "true" ]; then
    echo "Skipping child process log cleanup."
    exit 0
fi

cleanup() {
    echo "Executing Find Statement: $1"
    FILES_MARKED_FOR_DELETE=`eval $1`
    echo "Process will be Deleting the following File(s)/Directory(s):"
    echo "${FILES_MARKED_FOR_DELETE}"
    echo "Process will be Deleting `echo "${FILES_MARKED_FOR_DELETE}" | \
    grep -v '^$' | wc -l` File(s)/Directory(s)"     \
    # "grep -v '^$'" - removes empty lines.
    # "wc -l" - Counts the number of lines
    echo ""
    if [ "${ENABLE_DELETE}" == "true" ];
    then
        if [ "${FILES_MARKED_FOR_DELETE}" != "" ];
        then
            echo "Executing Delete Statement: $2"
            eval $2
            DELETE_STMT_EXIT_CODE=$?
            if [ "${DELETE_STMT_EXIT_CODE}" != "0" ]; then
                echo "Delete process failed with exit code \
                    '${DELETE_STMT_EXIT_CODE}'"

                echo "Removing lock file..."
                rm -f """ + str(LOG_CLEANUP_PROCESS_LOCK_FILE) + """
                if [ "${REMOVE_LOCK_FILE_EXIT_CODE}" != "0" ]; then
                    echo "Error removing the lock file. \
                    Check file permissions.\
                    To re-run the DAG, ensure that the lock file has been \
                    deleted (""" + str(LOG_CLEANUP_PROCESS_LOCK_FILE) + """)."
                    exit ${REMOVE_LOCK_FILE_EXIT_CODE}
                fi
                exit ${DELETE_STMT_EXIT_CODE}
            fi
        else
            echo "WARN: No File(s)/Directory(s) to Delete"
        fi
    else
        echo "WARN: You're opted to skip deleting the File(s)/Directory(s)!!!"
    fi
}


if [ ! -f """ + str(LOG_CLEANUP_PROCESS_LOCK_FILE) + """ ]; then

    echo "Lock file not found on this node! \
    Creating it to prevent collisions..."
    touch """ + str(LOG_CLEANUP_PROCESS_LOCK_FILE) + """
    CREATE_LOCK_FILE_EXIT_CODE=$?
    if [ "${CREATE_LOCK_FILE_EXIT_CODE}" != "0" ]; then
        echo "Error creating the lock file. \
        Check if the airflow user can create files under tmp directory. \
        Exiting..."
        exit ${CREATE_LOCK_FILE_EXIT_CODE}
    fi

    echo ""
    echo "Running Cleanup Process..."

    FIND_STATEMENT="find ${BASE_LOG_FOLDER}/*/* -type f -mtime +${MAX_LOG_AGE_IN_DAYS}"
    DELETE_STMT="${FIND_STATEMENT} -exec rm -f {} \\;"

    cleanup "${FIND_STATEMENT}" "${DELETE_STMT}"
    CLEANUP_EXIT_CODE=$?

    FIND_STATEMENT="find ${BASE_LOG_FOLDER}/*/* -type d -empty"
    DELETE_STMT="${FIND_STATEMENT} -prune -exec rm -rf {} \\;"

    cleanup "${FIND_STATEMENT}" "${DELETE_STMT}"
    CLEANUP_EXIT_CODE=$?

    FIND_STATEMENT="find ${BASE_LOG_FOLDER}/* -type d -empty"
    DELETE_STMT="${FIND_STATEMENT} -prune -exec rm -rf {} \\;"

    cleanup "${FIND_STATEMENT}" "${DELETE_STMT}"
    CLEANUP_EXIT_CODE=$?

    echo "Finished Running Cleanup Process"

    echo "Deleting lock file..."
    rm -f """ + str(LOG_CLEANUP_PROCESS_LOCK_FILE) + """
    REMOVE_LOCK_FILE_EXIT_CODE=$?
    if [ "${REMOVE_LOCK_FILE_EXIT_CODE}" != "0" ]; then
        echo "Error removing the lock file. Check file permissions. To re-run the DAG, ensure that the lock file has been deleted (""" + str(LOG_CLEANUP_PROCESS_LOCK_FILE) + """)."
        exit ${REMOVE_LOCK_FILE_EXIT_CODE}
    fi

else
    echo "Another task is already deleting logs on this worker node. \
    Skipping it!"
    echo "If you believe you're receiving this message in error, kindly check \
    if """ + str(LOG_CLEANUP_PROCESS_LOCK_FILE) + """ exists and delete it."
    exit 0
fi

"""

for log_cleanup_id in range(1, NUMBER_OF_WORKERS + 1):

    for dir_id, directory in enumerate(DIRECTORIES_TO_DELETE):

        log_cleanup_op = BashOperator(
            task_id='log_cleanup_worker_num_' + str(log_cleanup_id) + '_dir_' + str(dir_id),
            bash_command=log_cleanup,
            params={
                "directory": str(directory),
                "sleep_time": int(log_cleanup_id)*3,
                "base_log_folder": str(BASE_LOG_FOLDER),
                "child_process_log_directory": str(CHILD_PROCESS_LOG_DIRECTORY or ""),
                },
            dag=dag)

        log_cleanup_op.set_upstream(start)
