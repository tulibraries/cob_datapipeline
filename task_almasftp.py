"""TU Libraries Airflow Task for Alma SFTP Server Harvest."""

from airflow.operators.python_operator import PythonOperator
from cob_datapipeline.almasftp_fetch import almasftp_fetch

def task_almasftp(dag):
    """Method to kick off almasftp.py script for Alma SFTP Record Harvest."""
    #pylint: disable=missing-docstring,invalid-name, C0301

    t1 = PythonOperator(
        task_id='almasftp',
        python_callable=almasftp_fetch,
        dag=dag
    )
    return t1
