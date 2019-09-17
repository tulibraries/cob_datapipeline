"""Airflow Task to index AZ Database records into Solr."""
import os
import airflow
import re
from airflow.operators.bash_operator import BashOperator

AIRFLOW_HOME = airflow.models.Variable.get("AIRFLOW_HOME")
AIRFLOW_DATA_DIR = airflow.models.Variable.get("AIRFLOW_DATA_DIR")
AIRFLOW_LOG_DIR = airflow.models.Variable.get("AIRFLOW_LOG_DIR")
AZ_CORE = airflow.models.Variable.get("AZ_CORE")
AZ_CLIENT_ID = airflow.models.Variable.get("AZ_CLIENT_ID")
AZ_CLIENT_SECRET = airflow.models.Variable.get("AZ_CLIENT_SECRET")
AZ_BRANCH = airflow.models.Variable.get("AZ_BRANCH")

def ingest_databases(dag, conn, task_id="ingest_databases", solr_url=None, delete=False):
    """Task for ingesting items

    Parameters:
        dag (airflow.models.Dag): The dag we will run this task in.
        conn (airflow.models.connection): Connection object representing solr we index to.
        task_id (str): A label for this task.
        solr_url (str): A URL that overrides the default SOLR_AZ_URL.
        delete (bool): Whether or not to delete collection before ingesting.

    Returns:
        t1 (airflow.models.Task): The represention of this task.
    """

    if solr_url:
        solr_az_url = solr_url
    else:
        solr_az_url = get_solr_url(conn, AZ_CORE)

    env = dict(os.environ)
    env.update({
        "SOLR_AZ_URL": solr_az_url,
        "AIRFLOW_HOME": AIRFLOW_HOME,
        "AIRFLOW_DATA_DIR": AIRFLOW_DATA_DIR,
        "AIRFLOW_LOG_DIR": AIRFLOW_LOG_DIR,
        "AZ_CLIENT_ID": AZ_CLIENT_ID,
        "AZ_CLIENT_SECRET": AZ_CLIENT_SECRET,
        "AZ_BRANCH": AZ_BRANCH,
    })

    if conn.login != None and conn.password != None:
        env.update({
            "SOLR_AUTH_USER": conn.login,
            "SOLR_AUTH_PASSWORD": conn.password,
            })

    if delete:
        env.update({ "AZ_DELETE_SWITCH": "--delete" })

    return BashOperator(
        task_id=task_id,
        bash_command=AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/ingest_databases.sh ",
        env=env,
        dag=dag
    )


def get_solr_url(conn, core):
    """  Generates a solr url from  passed in connection and core.

    Parameters:
        conn (airflow.models.connection): Connection object representing solr we index to.
        core (str)  The solr collection or configuration  to use.

    Returns:
        solr_url (str): A solr URL.

    """
    solr_url = conn.host

    if not re.match("^http", solr_url):
        solr_url = 'http://' + solr_url

    if conn.port:
        solr_url += ':' + str(conn.port)

    solr_url += '/solr/' + core

    return solr_url
