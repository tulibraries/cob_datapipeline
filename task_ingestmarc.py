"""TU Libraries Airflow Task for Alma to Solr Record Creates & Updates."""

import os
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable

def ingest_marc(dag):
    """Method to Index (create and update) records from Alma into Solr."""
    #pylint: disable=missing-docstring,invalid-name, C0301

    outfilename = Variable.get("AIRFLOW_DATA_DIR") + '/oairecords.xml'
    ingest_command = Variable.get("AIRFLOW_HOME") + "/dags/cob_datapipeline/scripts/ingest_marc.sh"
    if os.path.isfile(ingest_command):
        t1 = BashOperator(
            task_id='ingest_marc',
            bash_command="{} {}".format(ingest_command, outfilename)+' ',
            dag=dag
            )
    else:
        raise Exception(str(os.path.isfile(ingest_command)) + " Cannot locate {}".format(ingest_command))
    return t1
