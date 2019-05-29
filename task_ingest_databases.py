from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow import AirflowException
from cob_datapipeline.globals import AIRFLOW_DATA_DIR, AIRFLOW_LOG_DIR, AIRFLOW_HOME, AZ_CORE, AZ_CLIENT_ID, AZ_CLIENT_SECRET
import os

def ingest_databases(dag, conn, filename="", task_id="ingest_databases"):
    """Task for ingesting items

    Parameters:
        dag (airflow.models.Dag): The dag we will run this task in.
        conn (airflow.models.connection): A Connection object representing the solr url we will ingest to.
        filename (str): The name of the file that we will ingest from.
        task_id (str): A label for this task.

    Returns:
        t1 (airflow.models.Task): The represention of this task.
    """
    SOLR_URL = 'http://' + conn.host + ':' + str(conn.port) + '/solr/' + AZ_CORE
    env = dict(os.environ)
    env.update({
                "SOLR_AZ_URL": SOLR_URL,
                "AIRFLOW_HOME": AIRFLOW_HOME,
                "AIRFLOW_DATA_DIR": AIRFLOW_DATA_DIR,
                "AIRFLOW_LOG_DIR": AIRFLOW_LOG_DIR,
                })
    return BashOperator(
            task_id=task_id,
            bash_command= AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/ingest_databases.sh ",
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
            bash_command= AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/get_database_docs.sh ",
            env=env,
            dag=dag,
            )
