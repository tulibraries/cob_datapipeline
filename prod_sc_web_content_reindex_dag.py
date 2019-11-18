"""Controller DAG to trigger sc_web_content_reindex for Production environment:"""
from datetime import datetime
import airflow
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from tulflow.tasks import conditionally_trigger

"""
INIT SYSTEMWIDE VARIABLES
Check for existence of systemwide variables shared across tasks that can be
initialized here if not found (i.e. if this is a new installation)
"""

# Define the DAG
CONTROLLER_DAG = airflow.DAG(
    dag_id="trigger_prod_sc_web_content_reindex_dag",
    default_args={
        "owner": "cob",
        "start_date": datetime.utcnow(),
    },
    schedule_interval="@once",
)

# Define the single task in this controller DAG
PROD_TRIGGER = TriggerDagRunOperator(
    task_id="prod_trigger",
    trigger_dag_id="sc_web_content_reindex",
    python_callable=conditionally_trigger,
    params={"condition_param": True,
            "message": "Triggering Production Web Content Index DAG",
            "env": "prod"
           },
    dag=CONTROLLER_DAG
)
