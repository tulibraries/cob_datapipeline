"""Airflow DAG to perform a full re-index tul_cob catalog into Stage SolrCloud Collection."""
from datetime import datetime, timedelta
import airflow
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.s3_list_operator import S3ListOperator
from cob_datapipeline.sc_xml_parse import prepare_boundwiths, prepare_alma_data
from cob_datapipeline.task_sc_get_num_docs import task_solrgetnumdocs
from cob_datapipeline.task_slack_posts import catalog_slackpostonsuccess
from tulflow import tasks

"""
INIT SYSTEMWIDE VARIABLES

Check for existence of systemwide variables shared across tasks that can be
initialized here if not found (i.e. if this is a new installation)
"""

AIRFLOW_HOME = Variable.get("AIRFLOW_HOME")
AIRFLOW_USER_HOME = Variable.get("AIRFLOW_USER_HOME")

# Get Solr URL & Collection Name for indexing info; error out if not entered
SOLR_CONN = BaseHook.get_connection("SOLRCLOUD-WRITER")
CATALOG_SOLR_CONFIG = Variable.get("CATALOG_SOLR_CONFIG", deserialize_json=True)
# {"configset": "tul_cob-catalog-0", "replication_factor": 2}
CONFIGSET = CATALOG_SOLR_CONFIG.get("configset")
ALIAS = CONFIGSET + "-stage"
REPLICATION_FACTOR = CATALOG_SOLR_CONFIG.get("replication_factor")

# cob_index Indexer Library Variables
GIT_BRANCH = Variable.get("CATALOG_STAGE_BRANCH")
LATEST_RELEASE = Variable.get("CATALOG_STAGE_LATEST_RELEASE")

# Get S3 data bucket variables
AIRFLOW_S3 = BaseHook.get_connection("AIRFLOW_S3")
AIRFLOW_DATA_BUCKET = Variable.get("AIRFLOW_DATA_BUCKET")
ALMASFTP_S3_PREFIX = Variable.get("ALMASFTP_S3_PREFIX")

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
    "stage_sc_catalog_full_reindex",
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

SET_COLLECTION_NAME = PythonOperator(
    task_id="set_collection_name",
    python_callable=datetime.now().strftime,
    op_args=["%Y-%m-%d_%H-%M-%S"],
    dag=DAG
)

GET_NUM_SOLR_DOCS_PRE = task_solrgetnumdocs(
    DAG,
    ALIAS,
    "get_num_solr_docs_pre",
    conn_id=SOLR_CONN.conn_id
)

LIST_ALMA_S3_DATA = S3ListOperator(
    task_id="list_alma_s3_data",
    bucket=AIRFLOW_DATA_BUCKET,
    prefix=ALMASFTP_S3_PREFIX + "/alma_bibs__",
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
        "DEST_FOLDER": ALMASFTP_S3_PREFIX + "/" + DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/lookup.tsv",
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
        "DEST_PREFIX": ALMASFTP_S3_PREFIX + "/" + DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}",
        "LOOKUP_KEY": ALMASFTP_S3_PREFIX + "/" + DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/lookup.tsv",
        "S3_KEYS": "{{ ti.xcom_pull(task_ids='list_alma_s3_data') }}",
        "SOURCE_PREFIX": ALMASFTP_S3_PREFIX + "/alma_bibs__",
        "SOURCE_SUFFIX": ".tar.gz"
    },
    dag=DAG
)

CREATE_COLLECTION = tasks.create_sc_collection(
    DAG,
    SOLR_CONN.conn_id,
    CONFIGSET + "-{{ ti.xcom_pull(task_ids='set_collection_name') }}",
    REPLICATION_FACTOR,
    CONFIGSET
)

INDEX_SFTP_MARC = BashOperator(
    task_id="index_sftp_marc",
    bash_command=AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/sc_ingest_marc.sh ",
    env={
        "AWS_ACCESS_KEY_ID": AIRFLOW_S3.login,
        "AWS_SECRET_ACCESS_KEY": AIRFLOW_S3.password,
        "BUCKET": AIRFLOW_DATA_BUCKET,
        "FOLDER": ALMASFTP_S3_PREFIX + "/" + DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/alma_bibs__",
        "GIT_BRANCH": GIT_BRANCH,
        "HOME": AIRFLOW_USER_HOME,
        "LATEST_RELEASE": str(LATEST_RELEASE),
        "SOLR_AUTH_USER": SOLR_CONN.login or "",
        "SOLR_AUTH_PASSWORD": SOLR_CONN.password or "",
        "SOLR_URL": tasks.get_solr_url(SOLR_CONN, CONFIGSET + "-{{ ti.xcom_pull(task_ids='set_collection_name') }}"),
        "TRAJECT_FULL_REINDEX": "yes",
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
        "DATA_IN": "{{ ti.xcom_pull(task_ids='list_alma_s3_data') }}",
        "DEST_FOLDER": ALMASFTP_S3_PREFIX + "/archived/{{ ti.xcom_pull(task_ids='set_collection_name') }}",
        "HOME": AIRFLOW_USER_HOME,
        "SOURCE_FOLDER": ALMASFTP_S3_PREFIX
    },
    dag=DAG
)

SOLR_ALIAS_SWAP = tasks.swap_sc_alias(
    DAG,
    SOLR_CONN.conn_id,
    CONFIGSET +"-{{ ti.xcom_pull(task_ids='set_collection_name') }}",
    CONFIGSET + "-stage"
)

GET_NUM_SOLR_DOCS_POST = task_solrgetnumdocs(
    DAG,
    CONFIGSET +"-{{ ti.xcom_pull(task_ids='set_collection_name') }}",
    "get_num_solr_docs_post",
    conn_id=SOLR_CONN.conn_id
)

POST_SLACK = PythonOperator(
    task_id="slack_post_succ",
    python_callable=catalog_slackpostonsuccess,
    provide_context=True,
    dag=DAG
)

# SET UP TASK DEPENDENCIES
GET_NUM_SOLR_DOCS_PRE.set_upstream(SET_COLLECTION_NAME)
LIST_ALMA_S3_DATA.set_upstream(GET_NUM_SOLR_DOCS_PRE)
PREPARE_BOUNDWITHS.set_upstream(LIST_ALMA_S3_DATA)
PREPARE_ALMA_DATA.set_upstream(PREPARE_BOUNDWITHS)
CREATE_COLLECTION.set_upstream(PREPARE_ALMA_DATA)
INDEX_SFTP_MARC.set_upstream(CREATE_COLLECTION)
ARCHIVE_S3_DATA.set_upstream(INDEX_SFTP_MARC)
SOLR_ALIAS_SWAP.set_upstream(ARCHIVE_S3_DATA)
GET_NUM_SOLR_DOCS_POST.set_upstream(SOLR_ALIAS_SWAP)
POST_SLACK.set_upstream(GET_NUM_SOLR_DOCS_POST)
