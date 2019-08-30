from airflow import DAG
from datetime import datetime, timedelta
from airflow import AirflowException
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from cob_datapipeline.task_slackpost import task_slackpostonsuccess, task_slackpostonfail
from cob_datapipeline.almaoai_harvest import boundwithparents_oai_harvest
from cob_datapipeline.boundwith_fetch import create_boundwith_children_itemized_set

# INIT SYSTEMWIDE VARIABLES
#
try:
    ALMA_API_KEY = Variable.get("ALMA_API_KEY")
except KeyError:
    raise AirflowException("Need to set ALMA_API_KEY")

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

fetch_parents = PythonOperator(
    task_id='fetch_boundwith_parents',
    python_callable=boundwithparents_oai_harvest,
    provide_context=True,
    op_kwargs={'apikey': ALMA_API_KEY},
    dag=dag
)

create_children = PythonOperator(
    task_id='create_boundwith_children_itemized_set',
    python_callable=create_boundwith_children_itemized_set,
    provide_context=True,
    op_kwargs={'apikey': ALMA_API_KEY},
    dag=dag
)

#
# SET UP TASK DEPENDENCIES
#

fetch_parents.set_upstream(create_children)
post_slack.set_upstream(fetch_parents)
