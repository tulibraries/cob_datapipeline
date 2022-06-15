"""Airflow Task to Post other Task Success / Failure on Slack."""
import json
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from tulflow import tasks


def slackpostonsuccess(**context):
    """Task Method to Post Successful TUL Cob Index DAG Completion on Slack."""
    num_recs = Variable.get('almaoai_last_num_oai_update_recs')
    num_dels = Variable.get('almaoai_last_num_oai_delete_recs')
    num_rejects = Variable.get('traject_num_rejected')

    ti = context.get('task_instance')
    logurl = ti.log_url
    dagid = ti.dag_id
    date = context.get('data_interval_start')
    response_pre = ti.xcom_pull(task_ids='get_num_solr_docs_pre')
    ndocs_pre = json.loads(response_pre)["response"]["numFound"]
    response_post = ti.xcom_pull(task_ids='get_num_solr_docs_post')
    ndocs_post = json.loads(response_post)["response"]["numFound"]
    msg = """{} DAG {} success:
    {} update records ingested,
    {} rejected, and
    {} deleted.
    We started with {} docs and ended with {} docs. {}""".format(
        date,
        dagid,
        num_recs,
        num_rejects,
        num_dels,
        ndocs_pre,
        ndocs_post,
        logurl
    )
    return tasks.execute_slackpostonsuccess(context, conn_id="COB_SLACK_WEBHOOK", message=msg)


def az_slackpostonsuccess(dag, **context):
    """Task Method to Post Successful Databases AZ DAG Completion on Slack."""
    ti = context.get('task_instance')
    logurl = ti.log_url
    dagid = ti.dag_id
    date = context.get('data_interval_start')
    response_pre = ti.xcom_pull(task_ids='get_num_solr_docs_pre')
    ndocs_pre = json.loads(response_pre)["response"]["numFound"]
    response_post = ti.xcom_pull(task_ids='get_num_solr_docs_post')
    ndocs_post = json.loads(response_post)["response"]["numFound"]

    msg = """{date} DAG {dagid} success:
    We started with {ndocs_pre} docs and ended with {ndocs_post} docs.
    {logurl}""".format(
        date=date,
        dagid=dagid,
        ndocs_pre=ndocs_pre,
        ndocs_post=ndocs_post,
        logurl=logurl
    )

    return tasks.execute_slackpostonsuccess(context, conn_id="COB_SLACK_WEBHOOK", message=msg)


web_content_slackpostonsuccess = az_slackpostonsuccess
catalog_slackpostonsuccess = az_slackpostonsuccess

def notes_slackpostonsuccess(**context):
    """Task Method to Post successful Alma Notes dag completion on Slack."""
    return tasks.execute_slackpostonsuccess(context, conn_id="COB_SLACK_WEBHOOK")


def full_reindex_slack_post_on_success(**context):
    """Task Method to Post Successful Full Reindex DAG Completion on Slack."""
    ti = context.get('task_instance')
    logurl = ti.log_url
    dagid = ti.dag_id
    date = context.get('data_interval_start')
    response_pre = ti.xcom_pull(task_ids='get_num_solr_docs_pre')
    ndocs_pre = json.loads(response_pre)["response"]["numFound"]
    response_post = ti.xcom_pull(task_ids='get_num_solr_docs_post')
    ndocs_post = json.loads(response_post)["response"]["numFound"]
    response_current_prod = ti.xcom_pull(task_ids='get_num_solr_docs_current_prod')
    ndocs_current_prod = json.loads(response_current_prod)["response"]["numFound"]

    msg = """{date} DAG {dagid} success:
    We started with {ndocs_pre} docs and ended with {ndocs_post} docs.
    The current Production Solr Collection has {ndocs_current_prod}
    {logurl}""".format(
        date=date,
        dagid=dagid,
        ndocs_pre=ndocs_pre,
        ndocs_post=ndocs_post,
        ndocs_current_prod=ndocs_current_prod,
        logurl=logurl
    )

    return tasks.execute_slackpostonsuccess(context, conn_id="COB_SLACK_WEBHOOK", message=msg)

def slackpostonfail(context):
    """Task Method to Post Failed Task on Aggregator Slack."""
    msg = None
    return tasks.execute_slackpostonfail(context)
