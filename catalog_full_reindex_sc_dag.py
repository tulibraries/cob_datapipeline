"""Airflow DAG to index tul_cob catalog into Solr."""
from datetime import datetime, timedelta
import airflow
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from cob_datapipeline.almasftp_sc_fetch import almasftp_sc_fetch
from cob_datapipeline.task_ingestsftpmarc import ingest_sftpmarc
from cob_datapipeline.task_ingestmarc import ingest_marc
from cob_datapipeline.task_ingest_databases import get_solr_url
from cob_datapipeline.parsesftpdump import parse_sftpdump_dates, renamesftpfiles_onsuccess
from cob_datapipeline.task_addxmlns import task_addxmlns
from cob_datapipeline.task_solrcommit import task_solrcommit
from cob_datapipeline.task_slackpost import task_slackpostonsuccess, task_slackpostonfail
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

# Get sftp variables
ALMASFTP_HOST = Variable.get("ALMASFTP_HOST")
ALMASFTP_PORT = Variable.get("ALMASFTP_PORT")
ALMASFTP_USER = Variable.get("ALMASFTP_USER")
ALMASFTP_PASSWD = Variable.get("ALMASFTP_PASSWD")
ALMASFTP_PATH = "/incoming"
ALMASFTP_HARVEST_PATH = Variable.get("AIRFLOW_DATA_DIR") + "/sftpdump"
AIRFLOW_CONN_ALMASFTP = BaseHook.get_connection("AIRFLOW_CONN_ALMASFTP")

#
# CREATE DAG
#
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
addxmlns_task = task_addxmlns(DAG)

get_num_solr_docs_post = task_solrgetnumdocs(DAG, CONFIGSET, "get_num_solr_docs_post", conn_id=SOLR_CONN.conn_id)

# ingestsftpmarc_task = ingest_sftpmarc(DAG)
# ingest_marc_boundwith = ingest_marc(DAG, "boundwith_merged.xml", "ingest_boundwith_merged")

# parse_sftpdump_dates = PythonOperator(
#     task_id="parse_sftpdump",
#     provide_context=True,
#     python_callable=parse_sftpdump_dates,
#     op_kwargs={},
#     DAG=DAG)
#
# solr_endpoint_update = "/solr/" + CONFIGSET + "/update"
# clear_index = SimpleHttpOperator(
#     task_id="clear_index",
#     method="GET",
#     http_conn_id="AIRFLOW_CONN_SOLR_LEADER",
#     endpoint=solr_endpoint_update,
#     data={"stream.body": "<delete><query>*:*</query></delete>"},
#     headers={},
#     DAG=DAG)
#
# post_slack = PythonOperator(
#     task_id="slack_post_succ",
#     python_callable=task_slackpostonsuccess,
#     provide_context=True,
#     DAG=DAG
# )
#
# rename_sftpdump = PythonOperator(
#     task_id="archive_sftpdump",
#     python_callable=renamesftpfiles_onsuccess,
#     provide_context=True,
#     op_kwargs={},
#     DAG=DAG
# )

#
# SET UP TASK DEPENDENCIES
#
git_pull_catalog_sc_task.set_upstream(get_num_solr_docs_pre)
almasftp_task.set_upstream(get_num_solr_docs_pre)
create_sc_collection.set_upstream(almasftp_task)
addxmlns_task.set_upstream(create_sc_collection)
# ingestsftpmarc_task.set_upstream(git_pull_tulcob_task)
# ingestsftpmarc_task.set_upstream(addxmlns_task)
# ingestsftpmarc_task.set_upstream(solr_commit_postclear)
# parse_sftpdump_dates.set_upstream(ingestsftpmarc_task)
# ingest_marc_boundwith.set_upstream(parse_sftpdump_dates)
# get_num_solr_docs_post.set_upstream(solr_commit_postindex)
# rename_sftpdump.set_upstream(parse_sftpdump_dates)
# post_slack.set_upstream(get_num_solr_docs_post)
# post_slack.set_upstream(rename_sftpdump)
