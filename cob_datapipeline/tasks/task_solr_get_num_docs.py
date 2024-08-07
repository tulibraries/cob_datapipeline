from airflow.providers.http.operators.http import HttpOperator

def task_solrgetnumdocs(dag, alias_name, taskid, conn_id):
    """Task to get the number of solr documents"""

    solr_endpoint_select = '/solr/' + alias_name + '/select'

    return HttpOperator(
        task_id=taskid,
        method='GET',
        http_conn_id=conn_id,
        endpoint=solr_endpoint_select,
        data={"defType": "edismax",
              "facet": "false",
              "indent": "on",
              "q": "*:*",
              "wt": "json",
              "rows": "0"
             },
        headers={},
        dag=dag)
