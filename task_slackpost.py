"""Airflow Task to Post other Task Success / Failure on Slack."""
import json
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator


def task_slackpostonsuccess(dag, **context):
    """Task Method to Post Successful TUL Cob Index DAG Completion on Slack."""
    num_recs = Variable.get('almaoai_last_num_oai_update_recs')
    num_dels = Variable.get('almaoai_last_num_oai_delete_recs')
    num_rejects = Variable.get('traject_num_rejected')

    ti = context.get('task_instance')
    logurl = ti.log_url
    dagid = ti.dag_id
    date = context.get('execution_date')

    try:
        response_pre = ti.xcom_pull(task_ids='get_num_solr_docs_pre')
        ndocs_pre = json.loads(response_pre)["response"]["numFound"]
    except Exception:
        print("Error grabbing solr num docs pre from xcom")
        ndocs_pre = 0
    try:
        response_post = ti.xcom_pull(task_ids='get_num_solr_docs_post')
        ndocs_post = json.loads(response_post)["response"]["numFound"]
    except Exception:
        print("Error grabbing solr num docs post from xcom")
        ndocs_post = 0

    # adds_up = "Aw HELL no!" (unused)
    # if (int(ndocs_pre) + (int(num_recs) - int(num_dels) - int(num_rejects))) == int(ndocs_post):
    #     adds_up = "Heck YES it does!"

    message = "{} DAG {} success: {} update records ingested, {} rejected, and {} deleted. We started with {} docs and ended with {} docs. {}".format(date, dagid, num_recs, num_rejects, num_dels, ndocs_pre, ndocs_post, logurl)

    return task_generic_slackpostsuccess(dag, message).execute(context=context)

def task_az_slackpostonsuccess(dag, **context):
    """Task Method to Post Successful Databases AZ DAG Completion on Slack."""
    ti = context.get('task_instance')
    logurl = ti.log_url
    dagid = ti.dag_id
    date = context.get('execution_date')

    try:
        response_pre = ti.xcom_pull(task_ids='get_num_solr_docs_pre')
        ndocs_pre = json.loads(response_pre)["response"]["numFound"]
    except Exception:
        print("Error grabbing solr num docs pre from xcom")
        ndocs_pre = 0
    try:
        response_post = ti.xcom_pull(task_ids='get_num_solr_docs_post')
        ndocs_post = json.loads(response_post)["response"]["numFound"]
    except Exception:
        print("Error grabbing solr num docs post from xcom")
        ndocs_post = 0

    message = "{date} DAG {dagid} success: We started with {ndocs_pre} docs and ended with {ndocs_post} docs. {logurl}".format(date=date, dagid=dagid, ndocs_pre=ndocs_pre, ndocs_post=ndocs_post, logurl=logurl)

    return task_generic_slackpostsuccess(dag, message).execute(context=context)

task_web_content_slackpostonsuccess = task_az_slackpostonsuccess
task_catalog_slackpostonsuccess = task_az_slackpostonsuccess


def task_slackpostonfail(context):
    """Task Method to Post Failed DAG or Task Completion on Slack."""
    conn = BaseHook.get_connection('AIRFLOW_CONN_SLACK_WEBHOOK')
    ti = context.get('task_instance')
    logurl = ti.log_url
    taskid = ti.task_id
    dagid = ti.dag_id
    date = context.get('execution_date')

    slack_post = SlackWebhookOperator(
        task_id='slackpostonfail',
        http_conn_id='AIRFLOW_CONN_SLACK_WEBHOOK',
        webhook_token=conn.password,
        message=":poop: Task failed: {} {} {} {}".format(dagid, taskid, date, logurl),
        username='airflow',
        dag=context.get('dag'))

    return slack_post.execute(context=context)


def task_generic_slackpostsuccess(dag, message='Oh, happy day!'):
    """Task Method to Post Successful DAG or Task Completion on Slack."""
    conn = BaseHook.get_connection('AIRFLOW_CONN_SLACK_WEBHOOK')

    return SlackWebhookOperator(
        task_id="slackpostonsuccess",
        http_conn_id='AIRFLOW_CONN_SLACK_WEBHOOK',
        webhook_token=conn.password,
        message=':partygritty: ' + message,
        username='airflow',
        trigger_rule="all_success",
        dag=dag)
