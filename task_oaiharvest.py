from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow import AirflowException
from tul_cob_pipeline.almaoai_harvest import almaoai_harvest

def oaiharvest(dag):
   t1 = PythonOperator(
        task_id='almaoai_harvest',
        python_callable=almaoai_harvest,
        dag=dag
   )
   return t1
