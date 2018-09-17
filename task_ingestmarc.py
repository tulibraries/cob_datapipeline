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
    outfilename = Variable.get("AIRFLOW_HOME") + '/oairecords.xml'
    ingest_command = Variable.get("AIRFLOW_HOME") + "/dags/tul_cob_pipeline/scripts/ingest_marc.sh"
    if os.path.isfile(ingest_command):
       t1 = BashOperator(
            task_id= 'ingest_marc',
            bash_command="{} {}".format(ingest_command, outfilename)+' ',
            dag=dag
       )

       if os.path.isfile(outfilename):
           os.rename(outfilename, Variable.get("AIRFLOW_HOME") + '/oairecords-{}-{}.xml'.format(date,time.time()))
       deletedfilename = Variable.get("AIRFLOW_HOME") + '/oairecords_deleted.xml'
       if os.path.isfile(deletedfilename):
           os.rename(deletedfilename, Variable.get("AIRFLOW_HOME") + '/oairecords_deleted-{}-{}.xml'.format(date,time.time()))
    else:
        raise Exception(str(os.path.isfile(ingest_command)) + " Cannot locate {}".format(ingest_command))
    return t1
