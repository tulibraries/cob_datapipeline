# PyTest Configuration file.
import json
import os
import shutil
import subprocess
import tempfile

TEST_AIRFLOW_HOME = tempfile.mkdtemp(prefix="cob_airflow_home_", dir="/tmp")

os.environ["AIRFLOW_HOME"] = TEST_AIRFLOW_HOME
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"

from airflow.models import Variable, Connection
from airflow import settings
from airflow.exceptions import AirflowNotFoundException
from airflow.settings import Session
from airflow.sdk.execution_time import context as sdk_context
from airflow.sdk.execution_time.comms import ErrorResponse
from airflow.sdk.exceptions import AirflowRuntimeError, ErrorType
from airflow.utils import db


def _sdk_test_get_variable(key, deserialize_json):
    variable = Session.query(Variable).filter(Variable.key == key).one_or_none()
    if variable is None or variable.val is None:
        raise AirflowRuntimeError(
            ErrorResponse(
                error=ErrorType.VARIABLE_NOT_FOUND,
                detail={"message": f"Variable {key} not found"},
            )
        )

    if deserialize_json:
        return json.loads(variable.val)

    return variable.val


def _sdk_test_set_variable(key, value, description=None, serialize_json=False):
    Variable.set(
        key,
        value,
        description=description,
        serialize_json=serialize_json,
    )


def _sdk_test_delete_variable(key):
    Variable.delete(key)


def _sdk_test_get_connection(conn_id):
    connection = Session.query(Connection).filter(Connection.conn_id == conn_id).one_or_none()
    if connection is None:
        raise AirflowNotFoundException(f"The conn_id `{conn_id}` isn't defined")

    return connection

settings.initialize()
db.initdb()

sdk_context._get_variable = _sdk_test_get_variable
sdk_context._set_variable = _sdk_test_set_variable
sdk_context._delete_variable = _sdk_test_delete_variable
sdk_context._get_connection = _sdk_test_get_connection

Session.query(Connection).delete()
Session.commit()
repo_dir = os.getcwd()

subprocess.run("mkdir -p dags/cob_datapipeline", shell=True)
subprocess.run("mkdir -p data", shell=True)
subprocess.run("mkdir -p logs", shell=True)
subprocess.run("cp *.py dags/cob_datapipeline", shell=True)
subprocess.run("cp -r configs dags/cob_datapipeline", shell=True)
subprocess.run("cp -r scripts dags/cob_datapipeline", shell=True)
Variable.set("AIRFLOW_HOME", repo_dir)
Variable.set("AIRFLOW_DATA_BUCKET", "test_bucket")
Variable.set("AIRFLOW_DATA_DIR", repo_dir + "/data")
Variable.set("AIRFLOW_LOG_DIR", repo_dir + "/logs")
Variable.set("AIRFLOW_USER_HOME", repo_dir)
Variable.set("ALMA_API_KEY", "key")
Variable.set("ALMAOAI_LAST_HARVEST_FROM_DATE", "none")
Variable.set("ALMAOAI_LAST_HARVEST_DATE", "none")
Variable.set("CATALOG_PROD_LAST_HARVEST_FROM_DATE", None)
Variable.set("CATALOG_PRE_PRODUCTION_HARVEST_FROM_DATE", None)
Variable.set("CATALOG_PRE_PRODUCTION_LAST_HARVEST_FROM_DATE", None)
Variable.set("ALMASFTP_HARVEST_PATH", repo_dir + "/data/sftpdump/")
Variable.set("ALMASFTP_HOST", "127.0.0.1")
Variable.set("ALMASFTP_PORT", 9229)
Variable.set("ALMASFTP_S3_ORIGINAL_DATA_NAMESPACE", "2020060800")
Variable.set("ALMASFTP_S3_ORIGINAL_BW_DATA_NAMESPACE", "2020060800")
Variable.set("ALMASFTP_S3_PREFIX", "almasftp")
Variable.set("ALMASFTP_USER", "almasftp")
Variable.set("ALMASFTP_PASSWD", "password")
Variable.set("ALMASFTP_HARVEST_RAW_DATE", "none")
Variable.set("AZ_BRANCH", "AZ_BRANCH")
Variable.set("AZ_PROD_BRANCH", "AZ_BRANCH")
Variable.set("AZ_QA_BRANCH", "AZ_BRANCH")
Variable.set("AZ_CLIENT_ID", "AZ_CLIENT_ID")
Variable.set("AZ_CLIENT_SECRET", "AZ_CLIENT_SECRET")
Variable.set("AZ_DELETE_SWITCH", "--delete")
Variable.set("AZ_INDEX_SCHEDULE_INTERVAL", "@weekly")
Variable.set("AZ_SOLR_CONFIG", {"configset": "tul_cob-az-0", "replication_factor": 4}, serialize_json=True)
Variable.set("AZ_SOLR_CONFIG_QA", {"configset": "tul_cob-az-0", "replication_factor": 4}, serialize_json=True)
Variable.set("PROD_COB_INDEX_VERSION", "CATALOG_BRANCH")
Variable.set("CATALOG_PROD_LATEST_RELEASE", "False")
Variable.set("CATALOG_PRODUCTION_SOLR_COLLECTION", "foo")
Variable.set("PRE_PRODUCTION_COB_INDEX_VERSION", "CATALOG_BRANCH")
Variable.set("CATALOG_QA_LATEST_RELEASE", "False")
Variable.set("CATALOG_PRE_PRODUCTION_SOLR_CONFIG", {"configset": "tul_cob-catalog-0", "replication_factor": 4}, serialize_json=True)
Variable.set("CATALOG_PRE_PRODUCTION_SOLR_COLLECTION", "FOO")
Variable.set("DSPACE_HARVEST_FROM_DATE", "DSPACE_PROD_HARVEST_FROM_DATE")
Variable.set("DSPACE_HARVEST_UNTIL_DATE", "2020-01-01T00:00:00Z")
Variable.set("DSPACE_OAI_CONFIG", {"endpoint": "foobar", "md_prefix": "xoai", "setspec": "col_20.500.12613_11"}, serialize_json=True)
Variable.set("SOLR_AUTH_USER", "SOLR_AUTH_USER")
Variable.set("SOLR_AUTH_PASSWORD", "SOLR_AUTH_PASSWORD")
Variable.set("WEB_CONTENT_BASE_URL", "WEB_CONTENT_BASE_URL")
Variable.set("WEB_CONTENT_PROD_BASE_URL", "WEB_CONTENT_BASE_URL")
Variable.set("WEB_CONTENT_QA_BASE_URL", "WEB_CONTENT_BASE_URL")
Variable.set("WEB_CONTENT_QA_BASIC_AUTH_USER", "WEB_CONTENT_QA_BASIC_AUTH_USER")
Variable.set("WEB_CONTENT_QA_BASIC_AUTH_PASSWORD", "WEB_CONTENT_QA_BASIC_AUTH_PASSWORD")
Variable.set("WEB_CONTENT_READ_TIMEOUT", "WEB_CONTENT_READ_TIMEOUT")
Variable.set("WEB_CONTENT_BRANCH", "WEB_CONTENT_BRANCH")
Variable.set("WEB_CONTENT_PROD_BRANCH", "WEB_CONTENT_BRANCH")
Variable.set("WEB_CONTENT_QA_BRANCH", "WEB_CONTENT_BRANCH")
Variable.set("WEB_CONTENT_CONFIGSET", "tul_cob-web-1")
Variable.set("WEB_CONTENT_CORE", "WEB_CONTENT_CORE")
Variable.set("WEB_CONTENT_SOLR_CONFIG", {"configset": "tul_cob-web-2", "replication_factor": 4}, serialize_json=True)
Variable.set("WEB_CONTENT_SCHEDULE_INTERVAL", "@weekly")
Variable.set("GENCON_INDEX_SCHEDULE", "@monthly")
Variable.set("GENCON_INDEX_BRANCH", "main")
Variable.set("GENCON_TEMP_PATH", "/tmp/gencon")
Variable.set("GENCON_CSV_S3", "s3://tulib-airflow-prod/gencon")
Variable.set("GENCON_SOLR_CONFIG", {"configset": "gencon50-v3.0.1", "replication_factor": 4}, serialize_json=True)
Variable.set("CATALOG_OAI_PUBLISH_INTERVAL", 0)
Variable.set("CATALOG_PROD_HARVEST_FROM_DATE", "CATALOG_PROD_HARVEST_FROM_DATE")
Variable.set("CATALOG_HARVEST_UNTIL_DATE", "2020-01-01T00:00:00Z")
Variable.set("CATALOG_OAI_CONFIG", {"endpoint": "https://temple.alma.exlibrisgroup.com/view/oai/01TULI_INST/request", "included_sets": ["blacklight"], "md_prefix": ""}, serialize_json=True)
Variable.set("CATALOG_OAI_BW_CONFIG", {"endpoint": "https://temple.alma.exlibrisgroup.com/view/oai/01TULI_INST/request", "included_sets": ["blacklight-bw"], "md_prefix": "marc21"}, serialize_json=True)
Variable.set("CATALOG_PROD_BRANCH", "CATALOG_PROD_BRANCH")
Variable.set("CATALOG_QA_BRANCH", "CATALOG_QA_BRANCH")
Variable.set("CATALOG_PROD_LATEST_RELEASE", "CATALOG_PROD_LATEST_RELEASE")
Variable.set("CATALOG_QA_LATEST_RELEASE", "CATALOG_QA_LATEST_RELEASE")
Variable.set("CATALOG_FULL_REINDEX_SOLR_CONFIG", {"configset": "tul_cob-catalog-2", "replication_factor": 4}, serialize_json=True)


slack = Connection(
    conn_id="AIRFLOW_CONN_SLACK_WEBHOOK",
    conn_type="http",
    host="http://127.0.0.1/services",
    port=""
)

solrcloud = Connection(
    conn_id="SOLRCLOUD",
    conn_type="http",
    host="http://127.0.0.1",
    port="8983",
    login="puppy",
    password="chow"
)
solrcloud_writer = Connection(
    conn_id="SOLRCLOUD-WRITER",
    conn_type="http",
    host="http://127.0.0.1",
    port="8983",
    login="puppy",
    password="chow"
)
aws = Connection(
    conn_id="AIRFLOW_S3",
    conn_type="aws",
    login="puppy",
    password="chow"
)

library_website = Connection(
    conn_id="library_website",
    conn_type="http",
    host="http://127.0.0.2",
    schema="http",
)

airflow_session = Session()
airflow_session.add(slack)
airflow_session.add(solrcloud)
airflow_session.add(solrcloud_writer)
airflow_session.add(library_website)
airflow_session.add(aws)
airflow_session.commit()


def pytest_sessionfinish():
    """
    Called after whole test run finished, right before
    returning the exit status to the system.
    """
    subprocess.run("rm -rf dags", shell=True)
    subprocess.run("rm -rf data", shell=True)
    subprocess.run("rm -rf logs", shell=True)
    shutil.rmtree(TEST_AIRFLOW_HOME, ignore_errors=True)
