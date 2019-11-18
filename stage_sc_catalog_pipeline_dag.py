"""Controller DAG to trigger sc_catalog_pipeline for Stage environment:"""
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
    dag_id="trigger_stage_sc_catalog_pipeline_dag",
    default_args={
        "owner": "cob",
        "start_date": datetime.utcnow(),
    },
    schedule_interval="@once",
)

# Define the single task in this controller DAG
STAGE_TRIGGER = TriggerDagRunOperator(
    task_id="stage_trigger",
    trigger_dag_id="sc_catalog_pipeline",
    python_callable=conditionally_trigger,
    params={"condition_param": True,
            "message": "Triggering Stage Catalog (TUL COB) OAI Partial Reindex DAG",
            "env": "stage"
           },
    dag=CONTROLLER_DAG
)
