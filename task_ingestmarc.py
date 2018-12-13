from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from airflow import AirflowException
import os
import datetime
import time

def ingest_marc(dag):
    outfile = None
    deletedfile = None
    date_current_harvest = datetime.datetime.now()
    date = Variable.get("almaoai_last_harvest_date")
    outfilename = Variable.get("AIRFLOW_DATA_DIR") + '/oairecords.xml'
    ingest_command = Variable.get("AIRFLOW_HOME") + "/dags/cob_datapipeline/scripts/ingest_marc.sh"
    if os.path.isfile(ingest_command):
       t1 = BashOperator(
            task_id= 'ingest_marc',
            bash_command="{} {}".format(ingest_command, outfilename)+' ',
            dag=dag
       )

    else:
        raise Exception(str(os.path.isfile(ingest_command)) + " Cannot locate {}".format(ingest_command))
    return t1
