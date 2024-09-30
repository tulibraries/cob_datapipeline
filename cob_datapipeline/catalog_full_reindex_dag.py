"""Airflow DAG to perform a full re-index tul_cob catalog into Production SolrCloud Collection."""
from datetime import datetime, timedelta
from tulflow import tasks
import airflow
import pendulum
from airflow.decorators import task_group
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from cob_datapipeline.notifiers import send_collection_notification
from cob_datapipeline.tasks import xml_parse
from cob_datapipeline.tasks.task_solr_get_num_docs import task_solrgetnumdocs
from cob_datapipeline.operators import\
        PushVariable, DeleteCollectionListVariable
from cob_datapipeline import helpers
from airflow.providers.slack.notifications.slack import send_slack_notification

slackpostonsuccess = send_collection_notification(channel="blacklight_project")
slackpostonfail = send_slack_notification(channel="infra_alerts", username="airflow", text=":poop: Task failed: {{ dag.dag_id }} {{ ti.task_id }} {{ dag_run.logical_date }} {{ ti.log_url }}")

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

# Used to break up indexing into multiple processes.
# Also used as a multiplier of the "prepare_alma_data" task group.
CATALOG_INDEXING_MULTIPLIER = Variable.get("CATALOG_INDEXING_MULTIPLIER", 3)

# Don't think we ever want to actually use this. Remove?
LATEST_RELEASE = bool(Variable.get("CATALOG_PROD_LATEST_RELEASE"))

# Get S3 data bucket variables
AIRFLOW_S3 = BaseHook.get_connection("AIRFLOW_S3")
AIRFLOW_DATA_BUCKET = Variable.get("AIRFLOW_DATA_BUCKET")
ALMASFTP_S3_PREFIX = Variable.get("ALMASFTP_S3_PREFIX")
# Namespace of the data transferred by the almasftp dag
ALMASFTP_S3_ORIGINAL_DATA_NAMESPACE = Variable.get("ALMASFTP_S3_ORIGINAL_DATA_NAMESPACE")
ALMASFTP_S3_ORIGINAL_BW_DATA_NAMESPACE = Variable.get("ALMASFTP_S3_ORIGINAL_BW_DATA_NAMESPACE")

CATALOG_PRE_PRODUCTION_HARVEST_FROM_DATE = (datetime.strptime(ALMASFTP_S3_ORIGINAL_DATA_NAMESPACE, '%Y%m%d%H') - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")

# CREATE DAG
DEFAULT_ARGS = {
    "owner": "cob",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2018, 12, 13, tz="UTC"),
    "on_failure_callback": [slackpostonfail],
    "retries": 0,
    "retry_delay": timedelta(minutes=10),
}

DAG = airflow.DAG(
    "catalog_full_reindex",
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    schedule=None
)

# Function to split a list into group of lists
def split_list(a_list, n):
    if n == 0:
        return []

     # Calculate the group size and remainder
    group_size = len(a_list) // n
    remainder = len(a_list) % n

    # Create the groups
    groups = []
    start = 0
    for i in range(n):
        # Distribute the remainder across the first few groups
        end = start + group_size + (1 if i < remainder else 0)
        groups.append(a_list[start:end])
        start = end

    return groups

with DAG as dag:
    """
    CREATE TASKS
    Tasks with all logic contained in a single operator can be declared here.
    Tasks with custom logic are relegated to individual Python files.
    """

    SAFETY_CHECK = PythonOperator(
        task_id="safety_check",
        python_callable=helpers.catalog_safety_check,
        retries=3,
    )

    VERIFY_PROD_COLLECTION = HttpOperator(
        task_id="verify_prod_collection",
        http_conn_id='http_tul_cob',
        method='GET',
        endpoint='/okcomputer/solr/' + PROD_COLLECTION_NAME,
        log_response=True,
        retries=3,
    )

    SET_S3_NAMESPACE = BashOperator(
        task_id="set_s3_namespace",
        bash_command='echo ' + "{{ logical_date.strftime('%Y-%m-%d_%H-%M-%S') }}",
    )

    def split_list_task(**kwargs):
        ti = kwargs["ti"]
        a_list = ti.xcom_pull("list_alma_s3_data.get_list")
        groups_count = CATALOG_INDEXING_MULTIPLIER
        return list(split_list(a_list, groups_count))


    @task_group
    def list_s3_files(prefix):
        GET_LIST = S3ListOperator(
                task_id="get_list",
                bucket=AIRFLOW_DATA_BUCKET,
                prefix=prefix,
                delimiter="/",
                aws_conn_id=AIRFLOW_S3.conn_id,
                )
        SPLIT_LIST = PythonOperator(
                    task_id="split_list",
                    python_callable=split_list_task,
                    provide_context=True,
                )
        GET_LIST >> SPLIT_LIST

    LIST_ALMA_S3_DATA = list_s3_files.override(group_id='list_alma_s3_data')(
            prefix=ALMASFTP_S3_PREFIX + "/" + ALMASFTP_S3_ORIGINAL_DATA_NAMESPACE + "/alma_bibs__",
            )

    LIST_BOUNDWITH_S3_DATA = S3ListOperator(
        task_id="list_boundwith_s3_data",
        bucket=AIRFLOW_DATA_BUCKET,
        prefix=ALMASFTP_S3_PREFIX + "/bw/" + ALMASFTP_S3_ORIGINAL_BW_DATA_NAMESPACE + "/alma_bibs__boundwith",
        delimiter="/",
        aws_conn_id=AIRFLOW_S3.conn_id,
    )


    PREPARE_BOUNDWITHS = PythonOperator(
        task_id=f"prepare_boundwiths",
        python_callable=xml_parse.prepare_boundwiths,
        op_kwargs={
            "AWS_ACCESS_KEY_ID": AIRFLOW_S3.login,
            "AWS_SECRET_ACCESS_KEY": AIRFLOW_S3.password,
            "BUCKET": AIRFLOW_DATA_BUCKET,
            "DEST_FOLDER": ALMASFTP_S3_PREFIX + "/" + DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_s3_namespace') }}/lookup.tsv",
            "S3_KEYS": "{{ ti.xcom_pull(task_ids='list_boundwith_s3_data') }}",
            "SOURCE_FOLDER": ALMASFTP_S3_PREFIX + "/bw/" + ALMASFTP_S3_ORIGINAL_BW_DATA_NAMESPACE + "/alma_bibs__boundwith"
        },
        )

    @task_group()
    def prepare_alma_data():
        for index in range(CATALOG_INDEXING_MULTIPLIER):
            PREPARE_ALMA_DATA = PythonOperator(
                task_id=f"prepare_alma_data_{index}",
                python_callable=xml_parse.prepare_alma_data,
                op_kwargs={
                    "AWS_ACCESS_KEY_ID": AIRFLOW_S3.login,
                    "AWS_SECRET_ACCESS_KEY": AIRFLOW_S3.password,
                    "BUCKET": AIRFLOW_DATA_BUCKET,
                    "DEST_PREFIX": ALMASFTP_S3_PREFIX + "/" + DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_s3_namespace') }}",
                    "LOOKUP_KEY": ALMASFTP_S3_PREFIX + "/" + DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_s3_namespace') }}/lookup.tsv",
                    "S3_KEYS": "{{ ti.xcom_pull(task_ids='list_alma_s3_data.split_list')[" + str(index) + "] }}",
                    "SOURCE_PREFIX": ALMASFTP_S3_PREFIX + "/" + ALMASFTP_S3_ORIGINAL_DATA_NAMESPACE + "/alma_bibs__",
                    "SOURCE_SUFFIX": ".tar.gz"
                },
            )

    PREPARE_ALMA_DATA = prepare_alma_data()

    CREATE_COLLECTION = PythonOperator(
        task_id="create_collection",
        python_callable=helpers.catalog_create_missing_collection,
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
    )

    DELETE_COLLECTIONS = DeleteCollectionListVariable(
        task_id="delete_collections",
        solr_conn_id='SOLRCLOUD',
        list_variable="CATALOG_COLLECTIONS",
        skip_from_last=3,
        skip_included=[COLLECTION_NAME, PROD_COLLECTION_NAME],
    )

    GET_NUM_SOLR_DOCS_PRE = task_solrgetnumdocs(
        DAG,
        COLLECTION_NAME,
        "get_num_solr_docs_pre",
        conn_id=SOLR_CLOUD.conn_id
    )

    LIST_S3_MARC_FILES = list_s3_files.override(group_id="list_s3_marc_files")(
            prefix=ALMASFTP_S3_PREFIX + "/" + DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_s3_namespace') }}/alma_bibs__",
            )


    @task_group()
    def index_sftp_marc():
        for index in range(CATALOG_INDEXING_MULTIPLIER):
            INDEX_SFTP_MARC = BashOperator(
                task_id=f"index_sftp_marc_{index}",
                bash_command=AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/ingest_marc.sh ",
                env={
                    "AWS_ACCESS_KEY_ID": AIRFLOW_S3.login,
                    "AWS_SECRET_ACCESS_KEY": AIRFLOW_S3.password,
                    "BUCKET": AIRFLOW_DATA_BUCKET,
                    "DATA": "{{ ti.xcom_pull(task_ids='list_s3_marc_files.split_list')[" + str(index) + "] | tojson }}",
                    "GIT_BRANCH": COB_INDEX_VERSION,
                    "HOME": AIRFLOW_USER_HOME,
                    "LATEST_RELEASE": str(LATEST_RELEASE),
                    "SOLR_AUTH_USER": SOLR_WRITER.login or "",
                    "SOLR_AUTH_PASSWORD": SOLR_WRITER.password or "",
                    "SOLR_URL": tasks.get_solr_url(SOLR_WRITER, COLLECTION_NAME),
                    "TRAJECT_FULL_REINDEX": "yes",
                },
            )

    INDEX_SFTP_MARC = index_sftp_marc()

    SOLR_COMMIT = HttpOperator(
        task_id="solr_commit",
        method="GET",
        http_conn_id=SOLR_CLOUD.conn_id,
        endpoint= "/solr/" + COLLECTION_NAME + "/update?commit=true",
    )

    UPDATE_DATE_VARIABLES = PythonOperator(
        task_id="update_variables",
        python_callable=xml_parse.update_variables,
        op_kwargs={
            "UPDATE": {
                "CATALOG_PRE_PRODUCTION_SOLR_COLLECTION": COLLECTION_NAME,
                "CATALOG_PRE_PRODUCTION_HARVEST_FROM_DATE": CATALOG_PRE_PRODUCTION_HARVEST_FROM_DATE
            }
        },
    )

    GET_NUM_SOLR_DOCS_POST = task_solrgetnumdocs(
        DAG,
        COLLECTION_NAME,
        "get_num_solr_docs_post",
        conn_id=SOLR_CLOUD.conn_id
    )

    SLACK_SUCCESS_POST = EmptyOperator(
            task_id="slack_success_post",
            dag=DAG,
            on_success_callback=[slackpostonsuccess],
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
    LIST_S3_MARC_FILES.set_upstream(GET_NUM_SOLR_DOCS_PRE)
    INDEX_SFTP_MARC.set_upstream(LIST_S3_MARC_FILES)
    SOLR_COMMIT.set_upstream(INDEX_SFTP_MARC)
    UPDATE_DATE_VARIABLES.set_upstream(SOLR_COMMIT)
    GET_NUM_SOLR_DOCS_POST.set_upstream(UPDATE_DATE_VARIABLES)
    SLACK_SUCCESS_POST.set_upstream(GET_NUM_SOLR_DOCS_POST)
