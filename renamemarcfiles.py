"""TU Libraries Airflow Script for Renaming OAI Harvest Files after Done."""

import os
import time
from airflow.models import Variable

#pylint: disable=missing-docstring,invalid-name, C0301
def renamemarcfiles_onsuccess():
    """Method to Rename OAI Harvest Records Files after Processing."""
    date = Variable.get("almaoai_last_harvest_date")

    outfilename = Variable.get("AIRFLOW_DATA_DIR") + '/oairecords.xml'
    if os.path.isfile(outfilename):
        os.rename(outfilename, Variable.get("AIRFLOW_DATA_DIR") + '/oairecords-{}-{}.xml'.format(date, time.time()))

    deletedfilename = Variable.get("AIRFLOW_DATA_DIR") + '/oairecords_deleted.xml'
    if os.path.isfile(deletedfilename):
        os.rename(deletedfilename, Variable.get("AIRFLOW_DATA_DIR") + '/oairecords_deleted-{}-{}.xml'.format(date, time.time()))
