"""Module providing a function to remove old schedluer logs."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

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
    schedule_interval='@daily',  # Runs once a day
    catchup=False,
) as dag:

    cleanup_old_folders = BashOperator(
        task_id='cleanup_old_folders',
        bash_command=(
            "find /var/lib/airflow/airflow-app/logs/scheduler "
            "-mindepth 1 -maxdepth 1 -type d -mtime +30 -exec rm -rf {} +"
        )
    )
