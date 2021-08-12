"""Airflow DAG to perform a full re-index tul_cob catalog into Production SolrCloud Collection."""
from datetime import datetime, timedelta
from tulflow import tasks
import airflow
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.providers.amazon.aws.operators.s3_list.S3ListOperator import S3ListOperator
from cob_datapipeline.tasks.xml_parse import prepare_boundwiths, prepare_alma_data, update_variables
from cob_datapipeline.tasks.task_solr_get_num_docs import task_solrgetnumdocs
from cob_datapipeline.tasks.task_slack_posts import catalog_slackpostonsuccess
from cob_datapipeline.operators import\
        PushVariable, DeleteCollectionListVariable
from cob_datapipeline import helpers
"""
INIT SYSTEMWIDE VARIABLES

Check for existence of systemwide variables shared across tasks that can be
initialized here if not found (i.e. if this is a new installation)
"""

AIRFLOW_HOME = Variable.get("AIRFLOW_HOME")
AIRFLOW_USER_HOME = Variable.get("AIRFLOW_USER_HOME")


# Get Solr URL & Collection Name for indexing info; error out if not entered
SOLR_WRITER = BaseHook.get_connection("SOLRCLOUD-WRITER")
SOLR_CLOUD = BaseHook.get_connection("SOLRCLOUD")
CATALOG_SOLR_CONFIG = Variable.get("CATALOG_PRE_PRODUCTION_SOLR_CONFIG", deserialize_json=True)
# {"configset": "tul_cob-catalog-0", "replication_factor": 4}
CONFIGSET = CATALOG_SOLR_CONFIG.get("configset")
COB_INDEX_VERSION = Variable.get("PRE_PRODUCTION_COB_INDEX_VERSION")
COLLECTION_NAME = helpers.catalog_collection_name(
        configset=CONFIGSET,
        cob_index_version=COB_INDEX_VERSION)
REPLICATION_FACTOR = CATALOG_SOLR_CONFIG.get("replication_factor")

# Used to check the current number of
PROD_COLLECTION_NAME = Variable.get("CATALOG_PRODUCTION_SOLR_COLLECTION")

# Don't think we ever want to actually use this. Remove?
LATEST_RELEASE = bool(Variable.get("CATALOG_PROD_LATEST_RELEASE"))

# Get S3 data bucket variables
AIRFLOW_S3 = BaseHook.get_connection("AIRFLOW_S3")
AIRFLOW_DATA_BUCKET = Variable.get("AIRFLOW_DATA_BUCKET")
ALMASFTP_S3_PREFIX = Variable.get("ALMASFTP_S3_PREFIX")
# Namespace of the data transferred by the almasftp dag
ALMASFTP_S3_ORIGINAL_DATA_NAMESPACE = Variable.get("ALMASFTP_S3_ORIGINAL_DATA_NAMESPACE")

CATALOG_PRE_PRODUCTION_HARVEST_FROM_DATE = (datetime.strptime(ALMASFTP_S3_ORIGINAL_DATA_NAMESPACE, '%Y%m%d%H') - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")

# CREATE DAG
DEFAULT_ARGS = {
    "owner": "cob",
    "depends_on_past": False,
    "start_date": datetime(2018, 12, 13),
    "on_failure_callback": tasks.execute_slackpostonfail,
    "retries": 0,
    "retry_delay": timedelta(minutes=10),
}

DAG = airflow.DAG(
    "catalog_full_reindex",
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    schedule_interval=None
)

"""
CREATE TASKS
Tasks with all logic contained in a single operator can be declared here.
Tasks with custom logic are relegated to individual Python files.
"""

SAFETY_CHECK = PythonOperator(
    task_id="safety_check",
    python_callable=helpers.catalog_safety_check,
    dag=DAG
)

VERIFY_PROD_COLLECTION = SimpleHttpOperator(
    task_id="verify_prod_collection",
    http_conn_id='http_tul_cob',
    method='GET',
    endpoint='/okcomputer/solr/' + PROD_COLLECTION_NAME,
    log_response=True,
    retries=2,
    dag=DAG,
)

SET_S3_NAMESPACE = PythonOperator(
    task_id="set_s3_namespace",
    python_callable=datetime.now().strftime,
    op_args=["%Y-%m-%d_%H-%M-%S"],
    dag=DAG
)

LIST_ALMA_S3_DATA = S3ListOperator(
    task_id="list_alma_s3_data",
    bucket=AIRFLOW_DATA_BUCKET,
    prefix=ALMASFTP_S3_PREFIX + "/" + ALMASFTP_S3_ORIGINAL_DATA_NAMESPACE + "/alma_bibs__",
    delimiter="/",
    aws_conn_id=AIRFLOW_S3.conn_id,
    dag=DAG
)

LIST_BOUNDWITH_S3_DATA = S3ListOperator(
    task_id="list_boundwith_s3_data",
    bucket=AIRFLOW_DATA_BUCKET,
    prefix=ALMASFTP_S3_PREFIX + "/alma_bibs__boundwith",
    delimiter="/",
    aws_conn_id=AIRFLOW_S3.conn_id,
    dag=DAG
)

PREPARE_BOUNDWITHS = PythonOperator(
    task_id="prepare_boundwiths",
    provide_context=True,
    python_callable=prepare_boundwiths,
    op_kwargs={
        "AWS_ACCESS_KEY_ID": AIRFLOW_S3.login,
        "AWS_SECRET_ACCESS_KEY": AIRFLOW_S3.password,
        "BUCKET": AIRFLOW_DATA_BUCKET,
        "DEST_FOLDER": ALMASFTP_S3_PREFIX + "/" + DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_s3_namespace') }}/lookup.tsv",
        "S3_KEYS": "{{ ti.xcom_pull(task_ids='list_boundwith_s3_data') }}",
        "SOURCE_FOLDER": ALMASFTP_S3_PREFIX + "/alma_bibs__boundwith"
    },
    dag=DAG
)

PREPARE_ALMA_DATA = PythonOperator(
    task_id="prepare_alma_data",
    python_callable=prepare_alma_data,
    op_kwargs={
        "AWS_ACCESS_KEY_ID": AIRFLOW_S3.login,
        "AWS_SECRET_ACCESS_KEY": AIRFLOW_S3.password,
        "BUCKET": AIRFLOW_DATA_BUCKET,
        "DEST_PREFIX": ALMASFTP_S3_PREFIX + "/" + DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_s3_namespace') }}",
        "LOOKUP_KEY": ALMASFTP_S3_PREFIX + "/" + DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_s3_namespace') }}/lookup.tsv",
        "S3_KEYS": "{{ ti.xcom_pull(task_ids='list_alma_s3_data') }}",
        "SOURCE_PREFIX": ALMASFTP_S3_PREFIX + "/" + ALMASFTP_S3_ORIGINAL_DATA_NAMESPACE + "/alma_bibs__",
        "SOURCE_SUFFIX": ".tar.gz"
    },
    dag=DAG
)

CREATE_COLLECTION = PythonOperator(
    task_id="create_collection",
    python_callable=helpers.catalog_create_missing_collection,
    provide_context=True,
    dag=DAG,
    op_kwargs={
        "conn": SOLR_CLOUD,
        "collection": COLLECTION_NAME,
        "replication_factor": REPLICATION_FACTOR,
        "configset": CONFIGSET,
        }
)

PUSH_COLLECTION = PushVariable(
    task_id="push_collection",
    name="CATALOG_COLLECTIONS",
    value=COLLECTION_NAME,
    dag=DAG)

DELETE_COLLECTIONS = DeleteCollectionListVariable(
    task_id="delete_collections",
    solr_conn_id='SOLRCLOUD',
    list_variable="CATALOG_COLLECTIONS",
    skip_from_last=3,
    skip_included=[COLLECTION_NAME, PROD_COLLECTION_NAME],
    dag=DAG)

GET_NUM_SOLR_DOCS_PRE = task_solrgetnumdocs(
    DAG,
    COLLECTION_NAME,
    "get_num_solr_docs_pre",
    conn_id=SOLR_CLOUD.conn_id
)

INDEX_SFTP_MARC = BashOperator(
    task_id="index_sftp_marc",
    bash_command=AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/ingest_marc.sh ",
    env={
        "AWS_ACCESS_KEY_ID": AIRFLOW_S3.login,
        "AWS_SECRET_ACCESS_KEY": AIRFLOW_S3.password,
        "BUCKET": AIRFLOW_DATA_BUCKET,
        "FOLDER": ALMASFTP_S3_PREFIX + "/" + DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_s3_namespace') }}/alma_bibs__",
        "GIT_BRANCH": COB_INDEX_VERSION,
        "HOME": AIRFLOW_USER_HOME,
        "LATEST_RELEASE": str(LATEST_RELEASE),
        "SOLR_AUTH_USER": SOLR_WRITER.login or "",
        "SOLR_AUTH_PASSWORD": SOLR_WRITER.password or "",
        "SOLR_URL": tasks.get_solr_url(SOLR_WRITER, COLLECTION_NAME),
        "TRAJECT_FULL_REINDEX": "yes",
    },
    dag=DAG
)

SOLR_COMMIT = SimpleHttpOperator(
    task_id="solr_commit",
    method="GET",
    http_conn_id=SOLR_CLOUD.conn_id,
    endpoint= "/solr/" + COLLECTION_NAME + "/update?commit=true",
    dag=DAG
)

UPDATE_DATE_VARIABLES = PythonOperator(
    task_id="update_variables",
    provide_context=True,
    python_callable=update_variables,
    op_kwargs={
        "UPDATE": {
            "CATALOG_PRE_PRODUCTION_SOLR_COLLECTION": COLLECTION_NAME,
            "CATALOG_PRE_PRODUCTION_HARVEST_FROM_DATE": CATALOG_PRE_PRODUCTION_HARVEST_FROM_DATE
        }
    },
    dag=DAG
)

GET_NUM_SOLR_DOCS_POST = task_solrgetnumdocs(
    DAG,
    COLLECTION_NAME,
    "get_num_solr_docs_post",
    conn_id=SOLR_CLOUD.conn_id
)

GET_NUM_SOLR_DOCS_CURRENT_PROD = task_solrgetnumdocs(
    DAG,
    PROD_COLLECTION_NAME,
    "get_num_solr_docs_current_prod",
    conn_id=SOLR_CLOUD.conn_id
)

POST_SLACK = PythonOperator(
    task_id="slack_post_succ",
    python_callable=catalog_slackpostonsuccess,
    provide_context=True,
    dag=DAG
)

# SET UP TASK DEPENDENCIES

SET_S3_NAMESPACE.set_upstream(SAFETY_CHECK)
SET_S3_NAMESPACE.set_upstream(VERIFY_PROD_COLLECTION)
LIST_ALMA_S3_DATA.set_upstream(SET_S3_NAMESPACE)
LIST_BOUNDWITH_S3_DATA.set_upstream(SET_S3_NAMESPACE)
PREPARE_BOUNDWITHS.set_upstream(LIST_BOUNDWITH_S3_DATA)
PREPARE_ALMA_DATA.set_upstream(LIST_ALMA_S3_DATA)
PREPARE_ALMA_DATA.set_upstream(PREPARE_BOUNDWITHS)
CREATE_COLLECTION.set_upstream(PREPARE_ALMA_DATA)
CREATE_COLLECTION.set_upstream(PREPARE_BOUNDWITHS)
PUSH_COLLECTION.set_upstream(CREATE_COLLECTION)
DELETE_COLLECTIONS.set_upstream(PUSH_COLLECTION)
GET_NUM_SOLR_DOCS_PRE.set_upstream(DELETE_COLLECTIONS)
INDEX_SFTP_MARC.set_upstream(GET_NUM_SOLR_DOCS_PRE)
SOLR_COMMIT.set_upstream(INDEX_SFTP_MARC)
UPDATE_DATE_VARIABLES.set_upstream(SOLR_COMMIT)
GET_NUM_SOLR_DOCS_POST.set_upstream(UPDATE_DATE_VARIABLES)
POST_SLACK.set_upstream(GET_NUM_SOLR_DOCS_POST)
POST_SLACK.set_upstream(GET_NUM_SOLR_DOCS_CURRENT_PROD)
