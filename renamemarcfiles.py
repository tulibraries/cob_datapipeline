from airflow import DAG
from airflow.models import Variable
from airflow import AirflowException
import os
import datetime
import time


def renamemarcfiles_onsuccess(ds, **kwargs):
    date = Variable.get("almaoai_last_harvest_date")

    filelist = ['oairecords.xml','oairecords_deleted.xml', 'boundwith_children.xml',
                'boundwith_parents.xml', 'boundwith_merged.xml']

    for file in filelist:
        outfilename = Variable.get("AIRFLOW_DATA_DIR") + '/' + file
        if os.path.isfile(outfilename):
            tokens = file.split('.')
            os.rename(outfilename, Variable.get("AIRFLOW_DATA_DIR") + '/{}-{}-{}.xml'.format(tokens[0], date, time.time()))
