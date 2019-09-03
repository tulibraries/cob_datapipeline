from airflow.operators.http_operator import SimpleHttpOperator


def task_solrgetnumdocs(dag, alias_name, taskid, conn_id=conn_id):
    solr_endpoint_select = '/solr/' + alias_name + '/select'

    SimpleHttpOperator(
        task_id=taskid,
        method='GET',
        http_conn_id=conn_id,
        endpoint=solr_endpoint_select,
        data={"defType": "edismax", "facet": "false", "indent": "on", "q": "*:*", "wt": "json", "rows": "0"},
        headers={},
        xcom_push=True,
        dag=dag)

