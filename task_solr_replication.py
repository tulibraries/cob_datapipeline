from airflow.operators.http_operator import SimpleHttpOperator

#http://master_host:port/solr/core_name/replication?command=enablereplication

def task_solr_replication(dag, core_name, command):
    param_endpoint_replication = '/solr/' + core_name + '/replication'

    if command is 'enable':
        taskid = 'resume_replication'
        command = "enablereplication"
        trigrule = "all_done"
    elif command is 'disable':
        taskid = 'pause_replication'
        command = "disablereplication"
        trigrule = "all_success"
    else:
        raise Exception("Invalid argument command={}".format(command))

    solr_replication = SimpleHttpOperator(
        task_id=taskid,
        method='GET',
        http_conn_id='AIRFLOW_CONN_SOLR_LEADER',
        endpoint=param_endpoint_replication,
        data={"command": command},
        trigger_rule=trigrule,
        xcom_push=True,
        dag=dag)

    return solr_replication


# #http://master_host:port/solr/core_name/replication?command=disablereplication
# pause_replication = SimpleHttpOperator(
#     task_id='pause_replication',
#     method='GET',
#     http_conn_id='AIRFLOW_CONN_SOLR_LEADER',
#     endpoint=param_endpoint_replication,
#     data={"command": "disablereplication"},
#     headers={},
#     dag=dag)
#
# #http://master_host:port/solr/core_name/replication?command=enablereplication
# resume_replication = SimpleHttpOperator(
#     task_id='resume_replication',
#     method='GET',
#     http_conn_id='AIRFLOW_CONN_SOLR_LEADER',
#     endpoint=param_endpoint_replication,
#     data={"command": "enablereplication"},
#     trigger_rule="all_done",
#     headers={},
#     dag=dag)
