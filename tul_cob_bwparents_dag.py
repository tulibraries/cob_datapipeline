from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from cob_datapipeline.task_slackpost import task_slackpostonsuccess, task_slackpostonfail
from cob_datapipeline.almaoai_harvest import boundwithparents_oai_harvest

# INIT SYSTEMWIDE VARIABLES
#
ALMA_API_KEY = Variable.get("ALMA_API_KEY")

# check for existence of systemwide variables shared across tasks that can be
# initialized here if not found (i.e. if this is a new installation)
#

#
# CREATE DAG
#
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 6, 6, 12, 0, 0),
    'email': ['tug76662@temple.edu'],
    'email_on_failure': True,
    'email_on_retry': True,
    'on_failure_callback': task_slackpostonfail,
    'retries': 0,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'tul_cob_boundwith_parents', default_args=default_args, catchup=False,
    max_active_runs=1, schedule_interval=timedelta(hours=24)
)

#
# CREATE TASKS
#
# Tasks with all logic contained in a single operator can be declared here.
# Tasks with custom logic are relegated to individual Python files.
#

post_slack = PythonOperator(
    task_id='slack_post_succ',
    python_callable=task_slackpostonsuccess,
    provide_context=True,
    dag=dag
)

fetch_children = PythonOperator(
    task_id='fetch_boundwith_parents',
    python_callable=boundwithparents_oai_harvest,
    provide_context=True,
    op_kwargs={'apikey': ALMA_API_KEY},
    dag=dag
)

#
# SET UP TASK DEPENDENCIES
#


post_slack.set_upstream(fetch_children)
