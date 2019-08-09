import os
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook



def ingest_boundwith(dag):
    # http://162.216.18.86:8983/solr/blacklight-core
    conn = BaseHook.get_connection('AIRFLOW_CONN_SOLR_LEADER')
    solr_endpoint = 'http://' + conn.host + ':' + str(conn.port) + '/solr/' + Variable.get('BLACKLIGHT_CORE_NAME')
    ingest_command = Variable.get("AIRFLOW_HOME") + "/dags/cob_datapipeline/scripts/ingest_marc.sh"
    marcfilepath = Variable.get("AIRFLOW_DATA_DIR") + "/boundwith_merged.xml"
    harvest_from_date = Variable.get("almaoai_last_harvest_from_date")

    if os.path.isfile(ingest_command):
        t1 = BashOperator(
            task_id='ingest_boundwith',
            bash_command="source {} {} {} {}".format(ingest_command, marcfilepath, solr_endpoint, harvest_from_date) + ' ',
            dag=dag
        )
    else:
        raise Exception(str(os.path.isfile(ingest_command)) + " Cannot locate {}".format(ingest_command))
    return t1
