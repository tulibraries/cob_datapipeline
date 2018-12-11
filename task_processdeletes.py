from airflow.operators.http_operator import SimpleHttpOperator
from airflow.models import Variable
from airflow import AirflowException
import os
import xmltodict

from airflow.exceptions import AirflowException
from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

#http://127.0.0.1:8983/solr/blacklight-core-dev/update?stream.body=<delete><query>id:(991036925416203811 OR 991036926096903811)</query></delete>&commit=true
#http://127.0.0.1:8983/solr/blacklight-core-dev/update?stream.body=%3Cdelete%3E%3Cquery%3Erecord_update_date:[0%20TO%20%222018-09-18%2014:35:16%22]%20AND%20id:991036281919703811%3C/query%3E%3C/delete%3E&commit=true

def process_deletes(ds, **kwargs):
    with open('oairecords_deleted.xml') as fd:
        doc = xmltodict.parse(fd.read())
        for record in doc['collection']['record']:
            # oai:alma.01TULI_INST:991000011889703811
            id = record['ns0:header']['ns0:identifier'].split(':')[2]
            #2018-09-21T20:21:12Z
            date = record['ns0:header']['ns0:datestamp']

            param_endpoint_update_delete = '/solr/' + kwargs.get('core_name') + '/update' + '?stream.body=<delete><query>record_update_date:[0 TO {}] AND id:{}</query></delete>&commit=true'.format(date,id)
            print(id)

            task_id='delete_{}'.format(id)
            method='GET'
            http_conn_id='AIRFLOW_CONN_SOLR_LEADER'
            data={"command": 'delete_{}'.format(id)}
            http = HttpHook(method, http_conn_id)
            response = http.run(param_endpoint_update_delete, data, {}, {})
            # if self.response_check:
            #     if not self.response_check(response):
            #         raise AirflowException("Response check returned False.")
            # if self.xcom_push_flag:
            print(response.text)

    #commit  Commits may be issued explicitly with a <commit/> message,

    #rollback
