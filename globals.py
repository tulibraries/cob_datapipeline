import airflow
from airflow import utils, settings, DAG
from airflow.models import Variable, Connection
from airflow.hooks.base_hook import BaseHook

"""
Incapsolates access to global settings.
"""

try:
    AIRFLOW_HOME = Variable.get("AIRFLOW_HOME")
except:
    AIRFLOW_HOME = "/usr/local/airflow"

try:
    AIRFLOW_DATA_DIR = Variable.get("AIRFLOW_DATA_DIR")
except:
    AIRFLOW_DATA_DIR = "/usr/local/airflow/data"

try:
    AIRFLOW_LOG_DIR = Variable.get("AIRFLOW_DATA_DIR")
except:
    AIRFLOW_LOG_DIR = "/usr/local/airflow/logs"

try:
    AIRFLOW_CONN_SOLR_LEADER = BaseHook.get_connection('AIRFLOW_CONN_SOLR_LEADER')
except:
    AIRFLOW_CONN_SOLR_LEADER = Connection(
            conn_id="AIRFLOW_CONN_SOLR_LEADER",
            conn_type="http",
            host="host.docker.internal",
            port="8983",
            )
try:
    AZ_CORE = Variable.get("AZ_CORE")
except:

    AZ_CORE = "az-database"

try:
    AZ_CLIENT_ID = Variable.get("AZ_CLIENT_ID")
except:
    AZ_CLIENT_ID = "AZ_CLIENT_ID"

try:
    AZ_CLIENT_SECRET = Variable.get("AZ_CLIENT_SECRET")
except:
    AZ_CLIENT_SECRET = "AZ_CLIENT_SECRET"


try:
    GIT_PULL_TULCOB_LATEST_RELEASE = Variable.get("GIT_PULL_TULCOB_LATEST_RELEASE")
except:
    GIT_PULL_TULCOB_LATEST_RELEASE = "false"

try:
    GIT_PULL_TULCOB_BRANCH_NAME = Variable.get("GIT_PULL_TULCOB_BRANCH_NAME")
except:
    GIT_PULL_TULCOB_BRANCH_NAME = "qa"
