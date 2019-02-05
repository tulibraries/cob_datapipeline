from airflow.operators.http_operator import SimpleHttpOperator
from airflow.models import Variable
from airflow import AirflowException
import os
import xmltodict
import xml
from collections import defaultdict

from airflow.exceptions import AirflowException
from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

#http://127.0.0.1:8983/solr/blacklight-core-dev/update?stream.body=<delete><query>id:(991036925416203811 OR 991036926096903811)</query></delete>&commit=true
#http://127.0.0.1:8983/solr/blacklight-core-dev/update?stream.body=%3Cdelete%3E%3Cquery%3Erecord_update_date:[0%20TO%20%222018-09-18%2014:35:16%22]%20AND%20id:991036281919703811%3C/query%3E%3C/delete%3E&commit=true

def process_deletes(ds, **kwargs):
    num_deleted = -1
    deletes_fname = Variable.get("AIRFLOW_DATA_DIR") + 'oairecords_deleted.xml'
    with open(deletes_fname) as fd:
        doc = None
        try:
            #xmltodict returns a list if there are multiple items but not if there is only 1
            #how is anything supposed to work in these conditions?
            #adding a custom lambda to force it to behave consistently
            doc = xmltodict.parse(fd.read(), dict_constructor=lambda *args, **kwargs: defaultdict(list, *args, **kwargs))
        except xml.parsers.expat.ExpatError:
            print('No delete records retrieved, bailing.')

        #handle the case when there is no record because we got no deletes
        if doc is not None and 'record' in doc['collection']:
            for record in doc['collection']['record']:
                # oai:alma.01TULI_INST:991000011889703811
                if 'ns0:header' in record:
                    #print(record)
                    #print(type(record))
                    header = record['ns0:header']
                    if 'ns0:identifier' in header:
                        if len(header['ns0:identifier'].split(':')) > 2:
                            id = header['ns0:identifier'].split(':')[2]
                            #2018-09-21T20:21:12Z
                            if 'ns0:datestamp' in header:
                                date = header['ns0:datestamp']

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
                                num_deleted += 1
                            else:
                                print('Bad datestamp {}'.format(header))
                        else:
                            print('Bad identifier {}'.format(header['ns0:identifier']))
                else:
                    print('No header found {}'.format(record))
        else:
            print('No record found {}'.format(doc))
    #commit  Commits may be issued explicitly with a <commit/> message,

    #rollback
    return num_deleted
