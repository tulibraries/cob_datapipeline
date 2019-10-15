# Airflow DAG to index tul_cob catalog into Solr.
from datetime import datetime, timedelta
import airflow
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from cob_datapipeline.sc_almasftp_fetch import almasftp_sc_fetch
from cob_datapipeline.task_ingestsftpmarc import ingest_sftpmarc
from cob_datapipeline.task_ingest_databases import get_solr_url
from cob_datapipeline.sc_parse_sftpdump_dates import parse_sftpdump_dates
from cob_datapipeline.task_addxmlns import task_addxmlns
from cob_datapipeline.task_solrcommit import task_solrcommit
from cob_datapipeline.task_slackpost import task_catalog_slackpostonsuccess, task_slackpostonfail
from cob_datapipeline.task_sc_get_num_docs import task_solrgetnumdocs
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from tulflow.tasks import create_sc_collection, swap_sc_alias

"""
INIT SYSTEMWIDE VARIABLES
Check for existence of systemwide variables shared across tasks that can be
initialized here if not found (i.e. if this is a new installation)
"""

AIRFLOW_HOME = Variable.get("AIRFLOW_HOME")
GIT_BRANCH = Variable.get("GIT_PULL_TULCOB_BRANCH_NAME")
LATEST_RELEASE = Variable.get("GIT_PULL_TULCOB_LATEST_RELEASE")
ALMAOAI_LAST_HARVEST_FROM_DATE = Variable.get("ALMAOAI_LAST_HARVEST_FROM_DATE")
ALMAOAI_LAST_HARVEST_DATE = Variable.get("ALMAOAI_LAST_HARVEST_DATE")
ALMASFTP_HARVEST_RAW_DATE = Variable.get("ALMASFTP_HARVEST_RAW_DATE")

# Get Solr URL & Collection Name for indexing info; error out if not entered
SOLR_CONN = BaseHook.get_connection("SOLRCLOUD")
CONFIGSET = Variable.get("CATALOG_CONFIGSET")
REPLICATION_FACTOR = Variable.get("CATALOG_REPLICATION_FACTOR")
TIMESTAMP = "{{ execution_date.strftime('%Y-%m-%d_%H-%M-%S') }}"
COLLECTION = CONFIGSET + "-" + TIMESTAMP
SOLR_URL = get_solr_url(SOLR_CONN, COLLECTION)
ALIAS = CONFIGSET

# Get sftp variables
ALMASFTP_HOST = Variable.get("ALMASFTP_HOST")
ALMASFTP_PORT = Variable.get("ALMASFTP_PORT")
ALMASFTP_USER = Variable.get("ALMASFTP_USER")
ALMASFTP_PASSWD = Variable.get("ALMASFTP_PASSWD")
ALMASFTP_PATH = "/incoming"
ALMASFTP_HARVEST_PATH = Variable.get("AIRFLOW_DATA_DIR") + "/sftpdump"

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
    "sc_catalog_full_reindex", default_args=DEFAULT_ARGS, catchup=False,
    max_active_runs=1, schedule_interval=None
)

"""
CREATE TASKS
Tasks with all logic contained in a single operator can be declared here.
Tasks with custom logic are relegated to individual Python files.
"""

GET_NUM_SOLR_DOCS_PRE = task_solrgetnumdocs(DAG, CONFIGSET, "get_num_solr_docs_pre", conn_id=SOLR_CONN.conn_id)
ALMA_SFTP = PythonOperator(
    task_id="alma_sftp",
    python_callable=almasftp_sc_fetch,
    op_kwargs={
        "host": ALMASFTP_HOST,
        "port": ALMASFTP_PORT,
        "user": ALMASFTP_USER,
        "passwd": ALMASFTP_PASSWD,
        "remotepath": ALMASFTP_PATH,
        "localpath": ALMASFTP_HARVEST_PATH
     },
    dag=DAG
)

SC_CREATE_COLLECTION = create_sc_collection(DAG, SOLR_CONN.conn_id, COLLECTION, REPLICATION_FACTOR, CONFIGSET)

SC_ADD_XML_NAMESPACES = BashOperator(
    task_id="sc_add_xml_namespaces",
    bash_command=AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/sc_add_xml_namespaces.sh ",
    env={
        "ALMASFTP_HARVEST_PATH": ALMASFTP_HARVEST_PATH
     },
    dag=DAG
)

SC_INGEST_SFTP_MARC = BashOperator(
    task_id="sc_ingest_sftp_marc",
    bash_command=AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/sc_ingest_marc_multi.sh ",
    env={
        "HOME": AIRFLOW_HOME,
        "SOLR_URL": SOLR_URL,
        "ALMASFTP_HARVEST_PATH": ALMASFTP_HARVEST_PATH,
        "DATA_IN": "alma_bib__*.xml",
        "GIT_BRANCH": GIT_BRANCH,
        "LATEST_RELEASE": LATEST_RELEASE,
     },
    dag=DAG
)

SC_PARSE_SFTPDUMP_DATES = PythonOperator(
    task_id="sc_parse_sftpdump_dates",
    provide_context=True,
    python_callable=parse_sftpdump_dates,
    op_kwargs={
        "INGEST_COMMAND": AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/ingest_marc_multi.sh",
        "ALMASFTP_HARVEST_PATH": ALMASFTP_HARVEST_PATH
    },
    dag=DAG
)

SC_INGEST_MARC_BOUNDWITH = BashOperator(
    task_id="sc_ingest_marc_boundwith",
    bash_command=AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/sc_ingest_marc_multi.sh ",
    env={
        "HOME": AIRFLOW_HOME,
        "SOLR_URL": SOLR_URL,
        "ALMASFTP_HARVEST_PATH": ALMASFTP_HARVEST_PATH,
        "DATA_IN": "boundwith_merged.xml",
        "GIT_BRANCH": GIT_BRANCH,
        "LATEST_RELEASE": LATEST_RELEASE,
     },
    dag=DAG
)

SOLR_ALIAS_SWAP = swap_sc_alias(DAG, SOLR_CONN.conn_id, COLLECTION, ALIAS)

GET_NUM_SOLR_DOCS_POST = task_solrgetnumdocs(DAG, ALIAS, "get_num_solr_docs_post", conn_id=SOLR_CONN.conn_id)

ARCHIVE_SFTPDUMP = BashOperator(
    task_id="archive_sftpdump",
    bash_command=AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/sc_sftp_cleanup.sh ",
    env={
        "ALMASFTP_HARVEST_PATH": ALMASFTP_HARVEST_PATH,
        "ALMASFTP_HARVEST_RAW_DATE": Variable.get("ALMASFTP_HARVEST_RAW_DATE")
     },
    dag=DAG
)

POST_SLACK = PythonOperator(
   task_id='slack_post_succ',
   python_callable=task_catalog_slackpostonsuccess,
   provide_context=True,
   dag=DAG
)

# SET UP TASK DEPENDENCIES
ALMA_SFTP.set_upstream(GET_NUM_SOLR_DOCS_PRE)
SC_CREATE_COLLECTION.set_upstream(ALMA_SFTP)
SC_ADD_XML_NAMESPACES.set_upstream(SC_CREATE_COLLECTION)
SC_INGEST_SFTP_MARC.set_upstream(SC_ADD_XML_NAMESPACES)
SC_PARSE_SFTPDUMP_DATES.set_upstream(SC_INGEST_SFTP_MARC)
SC_INGEST_MARC_BOUNDWITH.set_upstream(SC_PARSE_SFTPDUMP_DATES)
SOLR_ALIAS_SWAP.set_upstream(SC_INGEST_MARC_BOUNDWITH)
GET_NUM_SOLR_DOCS_POST.set_upstream(SOLR_ALIAS_SWAP)
ARCHIVE_SFTPDUMP.set_upstream(SC_PARSE_SFTPDUMP_DATES)
POST_SLACK.set_upstream(GET_NUM_SOLR_DOCS_POST)
POST_SLACK.set_upstream(ARCHIVE_SFTPDUMP)
