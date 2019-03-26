from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
import json


def task_slackpostonsuccess(dag, **context):
    conn = BaseHook.get_connection('AIRFLOW_CONN_SLACK_WEBHOOK')
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
    except Exception as e:
        print("Error grabbing solr num docs pre from xcom")
        ndocs_pre = 0
    try:
        response_post = ti.xcom_pull(task_ids='get_num_solr_docs_post')
        ndocs_post = json.loads(response_post)["response"]["numFound"]
    except Exception as e:
        print("Error grabbing solr num docs post from xcom")
        ndocs_post = 0

    adds_up = "Aw HELL no!"
    if (int(ndocs_pre) + (int(num_recs) - int(num_dels) - int(num_rejects))) == int(ndocs_post):
        adds_up = "Heck YES it does!"

    message = ":partygritty: {} DAG {} success: {} update records ingested, {} rejected, and {} deleted. We started with {} docs and ended with {} docs. {}".format(date, dagid, num_recs, num_rejects, num_dels, ndocs_pre, ndocs_post, logurl)
    slack_post = SlackWebhookOperator(
        task_id="slackpostonsuccess",
        http_conn_id='AIRFLOW_CONN_SLACK_WEBHOOK',
        webhook_token=conn.password,
        message=message,
        username='airflow',
        trigger_rule="all_success",
        dag=dag)

    return slack_post.execute(context=context)


def task_slackpostonfail(context):
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
