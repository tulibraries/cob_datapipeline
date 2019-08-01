"""Airflow Task to index Web Content records into Solr."""
import os
import airflow
from airflow.operators.bash_operator import BashOperator

AIRFLOW_HOME = airflow.models.Variable.get("AIRFLOW_HOME")
AIRFLOW_DATA_DIR = airflow.models.Variable.get("AIRFLOW_DATA_DIR")
AIRFLOW_LOG_DIR = airflow.models.Variable.get("AIRFLOW_LOG_DIR")
WEB_CONTENT_CORE = airflow.models.Variable.get("WEB_CONTENT_CORE")
WEB_CONTENT_BRANCH = airflow.models.Variable.get("WEB_CONTENT_BRANCH")
WEB_CONTENT_BASIC_AUTH_USER = airflow.models.Variable.get("WEB_CONTENT_BASIC_AUTH_USER")
WEB_CONTENT_BASIC_AUTH_PASSWORD = airflow.models.Variable.get("WEB_CONTENT_BASIC_AUTH_PASSWORD")
WEB_CONTENT_BASE_URL = airflow.models.Variable.get("WEB_CONTENT_BASE_URL")

def ingest_web_content(dag, conn, task_id="ingest_web_content"):
    """Task for ingesting items

    Parameters:
        dag (airflow.models.Dag): The dag we will run this task in.
        conn (airflow.models.connection): Connection object representing solr we index to.
        task_id (str): A label for this task.

    Returns:
        t1 (airflow.models.Task): The represention of this task.
    """
    solr_url = 'http://' + conn.host + ':' + str(conn.port) + '/solr/' + WEB_CONTENT_CORE
    env = dict(os.environ)
    env.update({
        "SOLR_WEB_URL": solr_url,
        "AIRFLOW_HOME": AIRFLOW_HOME,
        "AIRFLOW_DATA_DIR": AIRFLOW_DATA_DIR,
        "AIRFLOW_LOG_DIR": AIRFLOW_LOG_DIR,
        "WEB_CONTENT_BRANCH": WEB_CONTENT_BRANCH,
        "WEB_CONTENT_BASIC_AUTH_USER": WEB_CONTENT_BASIC_AUTH_USER,
        "WEB_CONTENT_BASIC_AUTH_PASSWORD": WEB_CONTENT_BASIC_AUTH_PASSWORD,
        "WEB_CONTENT_BASE_URL": WEB_CONTENT_BASE_URL
    })
    return BashOperator(
        task_id=task_id,
        bash_command=AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/ingest_web_content.sh ",
        env=env,
        dag=dag
    )
