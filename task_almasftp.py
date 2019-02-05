from airflow import DAG
from airflow import AirflowException
from airflow.operators.python_operator import PythonOperator

def task_almasftp(dag):
    create_command = "./almasftp.py"
    if os.path.exists(create_command):
       t1 = PythonOperator(
            task_id= 'almasftp',
            python_callable=almasftp_fetch,
            dag=dag
       )
    else:
        raise Exception("Cannot locate {}".format(create_command))
    return t1
