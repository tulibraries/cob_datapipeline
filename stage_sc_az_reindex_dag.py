"""Controller DAG to trigger sc_az_reindex for Stage environment:"""
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
    dag_id="trigger_stage_sc_az_reindex_dag",
    default_args={
        "owner": "cob",
        "start_date": datetime.utcnow(),
    },
    schedule_interval="@once",
)

# Define the single task in this controller DAG
STAGE_TRIGGER = TriggerDagRunOperator(
    task_id="stage_trigger",
    trigger_dag_id="sc_az_reindex",
    python_callable=conditionally_trigger,
    params={"condition_param": True,
            "message": "Triggering Stage Database (AZ) Index DAG",
            "env": "stage"
           },
    dag=CONTROLLER_DAG
)
