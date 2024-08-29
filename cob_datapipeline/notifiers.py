import json
from airflow.providers.slack.notifications.slack import send_slack_notification


def send_collection_notification(channel, username="airflow"):
    """
    Solr collections specific slack success notifier.
    Expects a 'get_num_solr_docs_pre' and a 'get_num_solr_docs_post' tasks
    to be defined. Also, expects those tasks to return Solr response
    with numFound attribute.
    """

    def slackpostonsuccess_collections(context):
        ti = context.get("task_instance")
        logurl = ti.log_url
        dagid = ti.dag_id
        date = context.get("logical_date")

        try:
            response_pre = ti.xcom_pull(task_ids="get_num_solr_docs_pre")
            ndocs_pre = json.loads(response_pre)["response"]["numFound"]
        except Exception as e:
            print("Error grabbing solr num docs from get_num_solr_docs_pre task xcom.", e)
            ndocs_pre = 0

        try:
            response_post = ti.xcom_pull(task_ids="get_num_solr_docs_post")
            ndocs_post = json.loads(response_post)["response"]["numFound"]
        except Exception:
            print("Error grabbing solr num docs from get_num_solr_docs_post task xcom.", e)
            ndocs_post = 0

        message = f":partygritty: {date} DAG {dagid} success: We started with {ndocs_pre} docs and ended with {ndocs_post} docs. {logurl}"
        notifier = send_slack_notification(channel=channel, username=username, text=message)

        return notifier.notify({})

    return slackpostonsuccess_collections
