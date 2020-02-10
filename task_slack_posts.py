"""Airflow Task to Post other Task Success / Failure on Slack."""
import json
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from tulflow import tasks


def slackpostonsuccess(**context):
    """Task Method to Post Successful TUL Cob Index DAG Completion on Slack."""
    num_recs = Variable.get('almaoai_last_num_oai_update_recs')
    num_dels = Variable.get('almaoai_last_num_oai_delete_recs')
    num_rejects = Variable.get('traject_num_rejected')

    ti = context.get('task_instance')
    logurl = ti.log_url
    dagid = ti.dag_id
    date = context.get('execution_date')
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
    date = context.get('execution_date')
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


def slackpostonfail(context):
    """Task Method to Post Failed Task on Aggregator Slack."""
    msg = None
    return tasks.execute_slackpostonfail(context, conn_id="COB_SLACK_WEBHOOK", message=msg)