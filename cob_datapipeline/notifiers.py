import logging
import json
from airflow.providers.slack.notifications.slack import send_slack_notification


def send_collection_notification(channel, username="airflow"):
    """
    Solr collections specific slack success notifier.
    """

    def slackpostonsuccess_collections(context):
        ti = context["ti"]
        logurl = ti.log_url
        dagid = ti.dag_id
        date = context["logical_date"]

        def get_num_docs(task_id):
            try:
                response = ti.xcom_pull(task_ids=task_id)
                if isinstance(response, str):
                    response = json.loads(response)
                return response["response"]["numFound"]
            except Exception as e:
                logging.warning(
                    "Error grabbing solr num docs from %s task xcom: %s",
                    task_id,
                    e,
                )
                return 0

        ndocs_pre = get_num_docs("get_num_solr_docs_pre")
        ndocs_post = get_num_docs("get_num_solr_docs_post")

        message = (
            f":partygritty: {date} DAG {dagid} success: "
            f"We started with {ndocs_pre} docs and ended with {ndocs_post} docs. {logurl}"
        )

        notifier = send_slack_notification(
            channel=channel,
            username=username,
            text=message
        )

        return notifier.notify({})

    return slackpostonsuccess_collections
