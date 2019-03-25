from airflow import DAG
from airflow import AirflowException
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
import os
from cob_datapipeline.almasftp_fetch import almasftp_fetch

def task_almasftp(dag):
    Variable.set("almafullreindex_inprogress", True)
    t1 = PythonOperator(
        task_id='almasftp',
        python_callable=almasftp_fetch,
        dag=dag
    )
    return t1
