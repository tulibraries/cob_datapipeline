from airflow.models import Variable
from airflow import AirflowException
import os
import re

#example traject results to parse:
#INFO finished Traject::Indexer#process: 203 records in 2.419 seconds; 83.9 records/second overall.
#ERROR Traject::Indexer#process returning 'false' due to 67 skipped records.


def process_trajectlog(ds, **kwargs):
    num_skipped = 0
    num_ingested = 0
    skipstr = "ERROR Traject::Indexer#process returning 'false' due to "
    finishstr = "INFO finished Traject::Indexer#process: "
    trajectlog_fname = Variable.get("AIRFLOW_DATA_DIR") + '/traject_log.tmp'
    with open(trajectlog_fname) as fd:
        try:
            for line in fd:
                if line.find(skipstr) > 0:
                    m = re.search(r"ERROR Traject::Indexer#process returning 'false' due to (.*) skipped records.", line)
                    if m is not None:
                        num_skipped = m.group(1)
                        print(num_skipped)
                        Variable.set("traject_num_rejected", num_skipped)
                    else:
                        print(line)
                # this is redundant with the info from the oaiharvest
                # elif line.find(finishstr) > 0:
                #     m = re.search(r'INFO finished Traject::Indexer#process: (.*)', line)
                #     if m != None:
                #         num_ingested = m.group(1)
                #         print(num_ingested)
                #     else:
                #         print( line )
            os.remove(trajectlog_fname) #contents are also in the ingest_marc task so we don't need this anymore
        except Exception as e:
            print('Error parsing log {} bailing {}.'.format(trajectlog_fname, str(e)))
