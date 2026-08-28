"""Module providing a function to remove old schedluer logs."""
from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),  # Adjust the start date as needed
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'maintenance_scheduler_log_cleanup',
    default_args=default_args,
    description='Remove folders older than 30 days from /var/lib/airflow/airflow-app/logs/scheduler',
    schedule='@daily',
    catchup=False,
) as dag:

    cleanup_old_folders = BashOperator(
        task_id='cleanup_old_folders',
        bash_command=(
            "set -e; "
            "LOG_DIR=/var/lib/airflow/airflow-app/logs/scheduler; "
            "find \"$LOG_DIR\" "
            "-mindepth 1 -maxdepth 1 -type d -mtime +30 -exec rm -rf {} +; "
            "if [ -L \"$LOG_DIR/latest\" ] && [ ! -e \"$LOG_DIR/latest\" ]; then "
            "rm \"$LOG_DIR/latest\"; "
            "fi"
        )
    )
