"""Airflow DAG to index tul_cob catalog into Solr."""
from datetime import datetime, timedelta
import airflow
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from cob_datapipeline.almasftp_sc_fetch import almasftp_sc_fetch
from cob_datapipeline.task_ingestsftpmarc import ingest_sftpmarc
from cob_datapipeline.task_ingest_databases import get_solr_url
from cob_datapipeline.sc_parsesftpdump import parse_sftpdump_dates
from cob_datapipeline.task_addxmlns import task_addxmlns
from cob_datapipeline.task_solrcommit import task_solrcommit
from cob_datapipeline.task_slackpost import task_catalog_slackpostonsuccess, task_slackpostonfail
from cob_datapipeline.task_sc_get_num_docs import task_solrgetnumdocs
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from tulflow.tasks import create_sc_collection, swap_sc_alias

# INIT SYSTEMWIDE VARIABLES
#
# check for existence of systemwide variables shared across tasks that can be
# initialized here if not found (i.e. if this is a new installation)
#
AIRFLOW_HOME = Variable.get("AIRFLOW_HOME")
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

# Get sftp variables
ALMASFTP_HOST = Variable.get("ALMASFTP_HOST")
ALMASFTP_PORT = Variable.get("ALMASFTP_PORT")
ALMASFTP_USER = Variable.get("ALMASFTP_USER")
ALMASFTP_PASSWD = Variable.get("ALMASFTP_PASSWD")
ALMASFTP_PATH = "/incoming"
ALMASFTP_HARVEST_PATH = Variable.get("AIRFLOW_DATA_DIR") + "/sftpdump"
AIRFLOW_CONN_ALMASFTP = BaseHook.get_connection("AIRFLOW_CONN_ALMASFTP")

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
    "catalog_full_reindex_sc", default_args=DEFAULT_ARGS, catchup=False,
    max_active_runs=1, schedule_interval=None
)

#
# CREATE TASKS
#
# Tasks with all logic contained in a single operator can be declared here.
# Tasks with custom logic are relegated to individual Python files.
#
get_num_solr_docs_pre = task_solrgetnumdocs(DAG, CONFIGSET, "get_num_solr_docs_pre", conn_id=SOLR_CONN.conn_id)
almasftp_task = PythonOperator(
    task_id="almasftp",
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
git_pull_catalog_sc_task = BashOperator(
    task_id="git_pull_catalog_sc",
    bash_command=AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/git_pull_catalog_sc.sh ",
    env={
        "LATEST_RELEASE": LATEST_RELEASE,
        "GIT_BRANCH": GIT_BRANCH,
        "HOME": AIRFLOW_HOME
     },
    dag=DAG
)
create_sc_collection = create_sc_collection(DAG, SOLR_CONN.conn_id, COLLECTION, REPLICATION_FACTOR, CONFIGSET)
addxmlns_task = BashOperator(
    task_id="addxmlns",
    bash_command=AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/sc_addxmlns.sh ",
    env={
        "ALMASFTP_HARVEST_PATH": ALMASFTP_HARVEST_PATH
     },
    dag=DAG
)
ingestsftpmarc_task = BashOperator(
    task_id="ingest_sftp_marc",
    bash_command=AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/sc_ingest_marc_multi.sh ",
    env={
        "HOME": AIRFLOW_HOME,
        "ALMASFTP_HARVEST_PATH": ALMASFTP_HARVEST_PATH,
        "SOLR_URL": SOLR_URL
     },
    dag=DAG
)
parse_sftpdump_dates = PythonOperator(
    task_id="parse_sftpdump",
    provide_context=True,
    python_callable=parse_sftpdump_dates,
    op_kwargs={
        "INGEST_COMMAND": AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/ingest_marc_multi.sh",
        "ALMASFTP_HARVEST_PATH": ALMASFTP_HARVEST_PATH
    },
    dag=DAG
)
ingest_marc_boundwith = BashOperator(
    task_id="ingest_boundwith_merged",
    bash_command=AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/sc_ingest_marc.sh ",
    env={
        "HOME": AIRFLOW_HOME,
        "SOLR_URL": SOLR_URL,
        "ALMASFTP_HARVEST_PATH": ALMASFTP_HARVEST_PATH,
        "DATA_IN": "boundwith_merged.xml",
        "ALMAOAI_LAST_HARVEST_FROM_DATE": Variable.get("almaoai_last_harvest_from_date")
     },
    dag=DAG
)
SOLR_ALIAS_SWAP = swap_sc_alias(DAG, SOLR_CONN.conn_id, COLLECTION, ALIAS)
get_num_solr_docs_post = task_solrgetnumdocs(DAG, ALIAS, "get_num_solr_docs_post", conn_id=SOLR_CONN.conn_id)
rename_sftpdump = BashOperator(
    task_id="archive_sftpdump",
    bash_command=AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/sc_sftp_cleanup.sh ",
    env={
        "ALMASFTP_HARVEST_PATH": ALMASFTP_HARVEST_PATH,
        "ALMASFTP_HARVEST_RAW_DATE": Variable.get("ALMASFTP_HARVEST_RAW_DATE")
     },
    dag=DAG
)
post_slack = PythonOperator(
   task_id='slack_post_succ',
   python_callable=task_catalog_slackpostonsuccess,
   provide_context=True,
   dag=DAG
)

# SET UP TASK DEPENDENCIES
git_pull_catalog_sc_task.set_upstream(get_num_solr_docs_pre)
almasftp_task.set_upstream(get_num_solr_docs_pre)
create_sc_collection.set_upstream(almasftp_task)
addxmlns_task.set_upstream(create_sc_collection)
ingestsftpmarc_task.set_upstream(git_pull_catalog_sc_task)
ingestsftpmarc_task.set_upstream(addxmlns_task)
parse_sftpdump_dates.set_upstream(ingestsftpmarc_task)
ingest_marc_boundwith.set_upstream(parse_sftpdump_dates)
SOLR_ALIAS_SWAP.set_upstream(ingest_marc_boundwith)
get_num_solr_docs_post.set_upstream(SOLR_ALIAS_SWAP)
rename_sftpdump.set_upstream(parse_sftpdump_dates)
post_slack.set_upstream(get_num_solr_docs_post)
post_slack.set_upstream(rename_sftpdump)
