from airflow.operators.http_operator import SimpleHttpOperator


def task_solrgetnumdocs(dag, core_name, taskid):
    solr_endpoint_select = '/solr/' + core_name + '/select'

    t1 = SimpleHttpOperator(
        task_id=taskid,
        method='GET',
        http_conn_id='AIRFLOW_CONN_SOLR_LEADER',
        endpoint=solr_endpoint_select,
        data={"defType": "edismax", "facet": "false", "indent": "on", "q": "*:*", "wt": "json", "rows": "0"},
        headers={},
        xcom_push=True,
        dag=dag)

    return t1
