from airflow.operators.http_operator import SimpleHttpOperator


def task_solrcommit(dag, core_name, taskid):
    param_endpoint_update = '/solr/' + core_name + '/update'

    t1 = SimpleHttpOperator(
        task_id=taskid,
        method='GET',
        http_conn_id='AIRFLOW_CONN_SOLR_LEADER',
        endpoint=param_endpoint_update,
        data={"stream.body": "<commit/>"},
        xcom_push=True,
        headers={},
        dag=dag)

    return t1
