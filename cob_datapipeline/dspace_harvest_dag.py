"""Airflow DAG to harvest DSpace electronic theses and dissertations"""
from datetime import datetime, timedelta
from tulflow import tasks
import airflow

# CREATE DAG
DEFAULT_ARGS = {
    'owner': 'cob',
    'depends_on_past': False,
    'start_date': datetime(2019, 5, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': tasks.execute_slackpostonfail,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

DAG = airflow.DAG(
    'dspace_harvest',
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    schedule_interval=None
)

"""
CREATE TASKS
Tasks with all logic contained in a single operator can be declared here.
Tasks with custom logic are relegated to individual Python files.
"""
