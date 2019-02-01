from airflow import DAG
from airflow.models import Variable
from airflow import AirflowException
import os
import datetime
import time

def renamemarcfiles_onsuccess(ds, **kwargs):
    outfile = None
    deletedfile = None
    date_current_harvest = datetime.datetime.now()
    date = Variable.get("almaoai_last_harvest_date")

    outfilename = Variable.get("AIRFLOW_DATA_DIR") + '/oairecords.xml'
    if os.path.isfile(outfilename):
        os.rename(outfilename, Variable.get("AIRFLOW_DATA_DIR") + '/oairecords-{}-{}.xml'.format(date,time.time()))
        
    deletedfilename = Variable.get("AIRFLOW_DATA_DIR") + '/oairecords_deleted.xml'
    if os.path.isfile(deletedfilename):
        os.rename(deletedfilename, Variable.get("AIRFLOW_DATA_DIR") + '/oairecords_deleted-{}-{}.xml'.format(date,time.time()))
