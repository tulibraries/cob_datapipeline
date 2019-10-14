"""Airflow DAG to perform a full re-index tul_cob catalog into Solr."""
from datetime import datetime, timedelta
import airflow
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.s3_list_operator import S3ListOperator
from cob_datapipeline.sc_xml_parse import prepare_boundwiths, prepare_alma_data
from cob_datapipeline.task_sc_get_num_docs import task_solrgetnumdocs
from cob_datapipeline.task_slackpost import task_catalog_slackpostonsuccess, task_slackpostonfail
from tulflow.tasks import create_sc_collection, get_solr_url, swap_sc_alias



"""
INIT SYSTEMWIDE VARIABLES
Check for existence of systemwide variables shared across tasks that can be
initialized here if not found (i.e. if this is a new installation)
"""

AIRFLOW_HOME = Variable.get("AIRFLOW_HOME")
AIRFLOW_USER_HOME = Variable.get("AIRFLOW_USER_HOME")

# cob_index Indexer Library Variables
GIT_BRANCH = Variable.get("GIT_PULL_TULCOB_BRANCH_NAME")
LATEST_RELEASE = Variable.get("GIT_PULL_TULCOB_LATEST_RELEASE")

# Get Solr URL & Collection Name for indexing info; error out if not entered
SOLR_CONN = BaseHook.get_connection("SOLRCLOUD")
CONFIGSET = Variable.get("CATALOG_CONFIGSET")
REPLICATION_FACTOR = Variable.get("CATALOG_REPLICATION_FACTOR")
TIMESTAMP = "{{ execution_date.strftime('%Y-%m-%d_%H-%M-%S') }}"
COLLECTION = CONFIGSET + "-" + TIMESTAMP
SOLR_URL = get_solr_url(SOLR_CONN, COLLECTION)
ALIAS = CONFIGSET

# Get S3 data bucket variables
AIRFLOW_S3 = BaseHook.get_connection("AIRFLOW_S3")
AIRFLOW_DATA_BUCKET = Variable.get("AIRFLOW_DATA_BUCKET")
ALMASFTP_S3_PREFIX = Variable.get("ALMASFTP_S3_PREFIX")

# CREATE DAG
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2018, 12, 13),
    "on_failure_callback": task_slackpostonfail,
    "retries": 0,
    "retry_delay": timedelta(minutes=10),
}

DAG = airflow.DAG(
    "sc_catalog_full_reindex",
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

GET_NUM_SOLR_DOCS_PRE = task_solrgetnumdocs(
    DAG,
    ALIAS,
    "get_num_solr_docs_pre",
    conn_id=SOLR_CONN.conn_id
)

LIST_ALMA_S3_DATA = S3ListOperator(
    task_id='list_alma_s3_data',
    bucket=AIRFLOW_DATA_BUCKET,
    prefix=ALMASFTP_S3_PREFIX + "/alma_bibs__",
    delimiter='/',
    aws_conn_id=AIRFLOW_S3.conn_id,
    dag=DAG
)

PREPARE_BOUNDWITHS = PythonOperator(
    task_id='prepare_boundwiths',
    provide_context=True,
    python_callable=prepare_boundwiths,
    op_kwargs={
        "AWS_ACCESS_KEY_ID": AIRFLOW_S3.login,
        "AWS_SECRET_ACCESS_KEY": AIRFLOW_S3.password,
        "BUCKET": AIRFLOW_DATA_BUCKET,
        "DEST_FOLDER": ALMASFTP_S3_PREFIX + "/" + DAG.dag_id + "/" + TIMESTAMP + "/lookup.tsv",
        "S3_KEYS": "{{ ti.xcom_pull(task_ids='list_alma_s3_data') }}",
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
        "DEST_PREFIX": ALMASFTP_S3_PREFIX + "/" + DAG.dag_id + "/" + TIMESTAMP,
        "LOOKUP_KEY": ALMASFTP_S3_PREFIX + "/" + DAG.dag_id + "/" + TIMESTAMP + "/lookup.tsv",
        "S3_KEYS": "{{ ti.xcom_pull(task_ids='list_alma_s3_data') }}",
        "SOURCE_PREFIX": ALMASFTP_S3_PREFIX + "/" + "alma_bibs__",
        "SOURCE_SUFFIX": ".tar.gz"
    },
    dag=DAG
)

SC_CREATE_COLLECTION = create_sc_collection(
    DAG,
    SOLR_CONN.conn_id,
    COLLECTION,
    REPLICATION_FACTOR,
    CONFIGSET
)

SC_INDEX_SFTP_MARC = BashOperator(
    task_id="index_sftp_marc",
    bash_command=AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/sc_ingest_marc.sh ",
    env={
        "AWS_ACCESS_KEY_ID": AIRFLOW_S3.login,
        "AWS_SECRET_ACCESS_KEY": AIRFLOW_S3.password,
        "BUCKET": AIRFLOW_DATA_BUCKET,
        "DATA_IN": "{{ ti.xcom_pull(task_ids='list_alma_s3_data') }}",
        "FOLDER": ALMASFTP_S3_PREFIX + "/" + DAG.dag_id + "/" + TIMESTAMP,
        "GIT_BRANCH": GIT_BRANCH,
        "HOME": AIRFLOW_USER_HOME,
        "LATEST_RELEASE": LATEST_RELEASE,
        "SOLR_AUTH_USER": SOLR_CONN.login,
        "SOLR_AUTH_PASSWORD": SOLR_CONN.password,
        "SOLR_URL": SOLR_URL
    },
    dag=DAG
)

ARCHIVE_S3_DATA = BashOperator(
    task_id="archive_s3_data",
    bash_command=AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/sc_archive_s3_data.sh ",
    env={
        "AIRFLOW_HOME": AIRFLOW_HOME,
        "AWS_ACCESS_KEY_ID": AIRFLOW_S3.login,
        "AWS_SECRET_ACCESS_KEY": AIRFLOW_S3.password,
        "BUCKET": AIRFLOW_DATA_BUCKET,
        "DEST_FOLDER": ALMASFTP_S3_PREFIX + "/archived/" + TIMESTAMP,
        "SOURCE_FOLDER": ALMASFTP_S3_PREFIX
    },
    dag=DAG
)

SOLR_ALIAS_SWAP = swap_sc_alias(
    DAG,
    SOLR_CONN.conn_id,
    COLLECTION,
    ALIAS
)

GET_NUM_SOLR_DOCS_POST = task_solrgetnumdocs(
    DAG,
    ALIAS,
    "get_num_solr_docs_post",
    conn_id=SOLR_CONN.conn_id
)

POST_SLACK = PythonOperator(
    task_id='slack_post_succ',
    python_callable=task_catalog_slackpostonsuccess,
    provide_context=True,
    dag=DAG
)

# SET UP TASK DEPENDENCIES
LIST_ALMA_S3_DATA.set_upstream(GET_NUM_SOLR_DOCS_PRE)
PREPARE_BOUNDWITHS.set_upstream(LIST_ALMA_S3_DATA)
PREPARE_ALMA_DATA.set_upstream(PREPARE_BOUNDWITHS)
SC_CREATE_COLLECTION.set_upstream(PREPARE_ALMA_DATA)
SC_INDEX_SFTP_MARC.set_upstream(SC_CREATE_COLLECTION)
ARCHIVE_S3_DATA.set_upstream(SC_INDEX_SFTP_MARC)
SOLR_ALIAS_SWAP.set_upstream(ARCHIVE_S3_DATA)
GET_NUM_SOLR_DOCS_POST.set_upstream(SOLR_ALIAS_SWAP)
POST_SLACK.set_upstream(GET_NUM_SOLR_DOCS_POST)
