"""Airflow Task to index AZ Database records into Solr."""
import os
import airflow
from airflow.operators.bash_operator import BashOperator

AIRFLOW_HOME = airflow.models.Variable.get("AIRFLOW_HOME")
AIRFLOW_DATA_DIR = airflow.models.Variable.get("AIRFLOW_DATA_DIR")
AIRFLOW_LOG_DIR = airflow.models.Variable.get("AIRFLOW_LOG_DIR")
AZ_CORE = airflow.models.Variable.get("AZ_CORE")
AZ_CLIENT_ID = airflow.models.Variable.get("AZ_CLIENT_ID")
AZ_CLIENT_SECRET = airflow.models.Variable.get("AZ_CLIENT_SECRET")

def ingest_databases(dag, conn, task_id="ingest_databases"):
    """Task for ingesting items

    Parameters:
        dag (airflow.models.Dag): The dag we will run this task in.
        conn (airflow.models.connection): Connection object representing solr we index to.
        task_id (str): A label for this task.

    Returns:
        t1 (airflow.models.Task): The represention of this task.
    """
    solr_url = 'http://' + conn.host + ':' + str(conn.port) + '/solr/' + AZ_CORE
    env = dict(os.environ)
    env.update({
        "SOLR_AZ_URL": solr_url,
        "AIRFLOW_HOME": AIRFLOW_HOME,
        "AIRFLOW_DATA_DIR": AIRFLOW_DATA_DIR,
        "AIRFLOW_LOG_DIR": AIRFLOW_LOG_DIR,
    })
    return BashOperator(
        task_id=task_id,
        bash_command=AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/ingest_databases.sh ",
        env=env,
        dag=dag
    )


def get_database_docs(dag, task_id="get_database_docs"):
    """Task for getting databases docs

    Parameters:
        dag (airflow.models.Dag): The dag we will run this task in.
        task_id (str): A label for this task.

    Returns:
        t2 (airflow.models.Task): The represention of this task.
    """
    env = dict(os.environ)
    env.update({
        "AZ_CLIENT_ID": AZ_CLIENT_ID,
        "AZ_CLIENT_SECRET": AZ_CLIENT_SECRET,
    })
    return BashOperator(
        task_id=task_id,
        bash_command=AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/get_database_docs.sh ",
        env=env,
        dag=dag,
    )
