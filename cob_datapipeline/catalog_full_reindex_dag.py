"""Airflow DAG to perform a full re-index tul_cob catalog into Production SolrCloud Collection."""
import airflow
import pendulum

from datetime import timedelta
from airflow.sdk import task_group, Connection, Variable
from tulflow import tasks
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
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

AIRFLOW_HOME = "{{ var.value.AIRFLOW_HOME }}"
AIRFLOW_USER_HOME = "{{ var.value.AIRFLOW_USER_HOME }}"

# Get Solr URL & Collection Name for indexing info; error out if not entered
CONFIGSET = "{{ var.json.CATALOG_PRE_PRODUCTION_SOLR_CONFIG.configset }}"
COB_INDEX_VERSION = "{{ var.value.PRE_PRODUCTION_COB_INDEX_VERSION }}"
COLLECTION_NAME = helpers.catalog_collection_name(
        configset=CONFIGSET,
        cob_index_version=COB_INDEX_VERSION)
REPLICATION_FACTOR = "{{ var.json.CATALOG_PRE_PRODUCTION_SOLR_CONFIG.replication_factor }}"

# Used to check the current number of
PROD_COLLECTION_NAME = "{{ var.value.CATALOG_PRODUCTION_SOLR_COLLECTION }}"

# Used to break up indexing into multiple processes.
# Also used as a multiplier of the "prepare_alma_data" task group.
CATALOG_INDEXING_MULTIPLIER = int(Variable.get("CATALOG_INDEXING_MULTIPLIER", default="3"))

# Don't think we ever want to actually use this. Remove?
LATEST_RELEASE = "{{ var.value.CATALOG_PROD_LATEST_RELEASE }}"

# Get S3 data bucket variables
AIRFLOW_DATA_BUCKET = "{{ var.value.AIRFLOW_DATA_BUCKET }}"
ALMASFTP_S3_PREFIX = "{{ var.value.ALMASFTP_S3_PREFIX }}"
# Namespace of the data transferred by the almasftp dag
ALMASFTP_S3_ORIGINAL_DATA_NAMESPACE = "{{ var.value.ALMASFTP_S3_ORIGINAL_DATA_NAMESPACE }}"
ALMASFTP_S3_ORIGINAL_BW_DATA_NAMESPACE = "{{ var.value.ALMASFTP_S3_ORIGINAL_BW_DATA_NAMESPACE }}"

CATALOG_PRE_PRODUCTION_HARVEST_FROM_DATE = (
    "{{ "
    "(macros.datetime.strptime(var.value.get('ALMASFTP_S3_ORIGINAL_DATA_NAMESPACE', '2020060800'), '%Y%m%d%H') "
    "- macros.timedelta(hours=24)).strftime('%Y-%m-%dT%H:%M:%SZ') "
    "}}"
)
SOLR_WRITER_URL = (
    tasks.get_solr_url_template("SOLRCLOUD-WRITER", COLLECTION_NAME)
)


def create_collection_runtime(**kwargs):
    conn = Connection.get("SOLRCLOUD")
    replication_factor = kwargs.get("replication_factor")
    if isinstance(replication_factor, str) and replication_factor.isdigit():
        replication_factor = int(replication_factor)

    helpers.catalog_create_missing_collection(
        conn=conn,
        collection=kwargs.get("collection"),
        replication_factor=replication_factor,
        configset=kwargs.get("configset"),
    )


def delete_collections_runtime(**kwargs):
    DeleteCollectionListVariable(
        task_id="delete_collections_runtime",
        solr_conn_id=kwargs.get("solr_conn_id"),
        list_variable=kwargs.get("list_variable"),
        skip_from_last=kwargs.get("skip_from_last"),
        skip_included=kwargs.get("skip_included"),
    ).execute()

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
        task = ti.task
        task_group = task.task_group
        group_name = task_group.group_id
        print(f"The group_name is {group_name}")

        a_list = ti.xcom_pull(f"{group_name}.get_list")
        groups_count = CATALOG_INDEXING_MULTIPLIER
        return list(split_list(a_list, groups_count))


    @task_group
    def list_s3_files(prefix):
        GET_LIST = S3ListOperator(
                task_id="get_list",
                bucket=AIRFLOW_DATA_BUCKET,
                prefix=prefix,
                delimiter="/",
                aws_conn_id="AIRFLOW_S3",
                )
        SPLIT_LIST = PythonOperator(
                    task_id="split_list",
                    python_callable=split_list_task,
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
        aws_conn_id="AIRFLOW_S3",
    )


    PREPARE_BOUNDWITHS = PythonOperator(
        task_id=f"prepare_boundwiths",
        python_callable=xml_parse.prepare_boundwiths,
        execution_timeout=timedelta(minutes=30),
        retries=3,
        op_kwargs={
            **helpers.airflow_s3_env(),
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
                retries=3,
                op_kwargs={
                    **helpers.airflow_s3_env(),
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
        python_callable=create_collection_runtime,
        op_kwargs={
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

    DELETE_COLLECTIONS = PythonOperator(
        task_id="delete_collections",
        python_callable=delete_collections_runtime,
        op_kwargs={
            "solr_conn_id": "SOLRCLOUD",
            "list_variable": "CATALOG_COLLECTIONS",
            "skip_from_last": 3,
            "skip_included": [COLLECTION_NAME, PROD_COLLECTION_NAME],
        },
    )

    GET_NUM_SOLR_DOCS_PRE = task_solrgetnumdocs(
        DAG,
        COLLECTION_NAME,
        "get_num_solr_docs_pre",
        conn_id="SOLRCLOUD"
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
                retries=3,
                env={
                    **helpers.airflow_s3_env(),
                    **helpers.solr_auth_env(),
                    "BUCKET": AIRFLOW_DATA_BUCKET,
                    "DATA": "{{ ti.xcom_pull(task_ids='list_s3_marc_files.split_list')[" + str(index) + "] | tojson }}",
                    "GIT_BRANCH": COB_INDEX_VERSION,
                    "HOME": AIRFLOW_USER_HOME,
                    "LATEST_RELEASE": LATEST_RELEASE,
                    "SOLR_URL": SOLR_WRITER_URL,
                    "TRAJECT_FULL_REINDEX": "yes",
                },
            )

    INDEX_SFTP_MARC = index_sftp_marc()

    SOLR_COMMIT = HttpOperator(
        task_id="solr_commit",
        method="GET",
        http_conn_id="SOLRCLOUD",
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
        conn_id="SOLRCLOUD"
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
