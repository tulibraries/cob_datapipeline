import os
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable


def boundwith_process(dag):
    ingest_command = Variable.get("AIRFLOW_HOME") + "/dags/cob_datapipeline/scripts/boundwith_process.rb"
    childrenmarcfilepath = Variable.get("AIRFLOW_DATA_DIR") + '/boundwith_children.xml'
    parentsmarcfilepath = Variable.get("AIRFLOW_DATA_DIR") + '/boundwith_parents.xml'
    outfilepath = Variable.get("AIRFLOW_DATA_DIR") + '/boundwith_merged.xml'

    if os.path.isfile(ingest_command):
        t1 = BashOperator(
            task_id='boundwith_process',
            bash_command="ruby {} {} {} {} ".format(ingest_command, parentsmarcfilepath,
                                                    childrenmarcfilepath, outfilepath) + ' ',
            dag=dag
        )
    else:
        raise Exception(str(os.path.isfile(ingest_command)) +
                        " Cannot locate {}".format(ingest_command))
    return t1
