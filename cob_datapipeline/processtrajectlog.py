from airflow.models import Variable
from airflow import AirflowException
import time
import os
import re

#example traject results to parse:
#INFO finished Traject::Indexer#process: 203 records in 2.419 seconds; 83.9 records/second overall.
#ERROR Traject::Indexer#process returning 'false' due to 67 skipped records.


def process_trajectlog(ds, **kwargs):
    num_skipped = 0
    num_ingested = 0
    num_solr_errors = 0
    num_severe_errors = 0
    skipstr = "ERROR Traject::Indexer#process returning 'false' due to "
    batcherrstr = "Error in Solr batch add. Will retry documents individually at performance penalty"
    errstr = "Could not add record"
    finishstr = "INFO finished Traject::Indexer#process: "
    trajectlog_fname = '{}/traject_log_{}.log'.format(Variable.get("AIRFLOW_LOG_DIR"), kwargs['marcfilename'])
    print('Parsing {}'.format(trajectlog_fname))
    with open(trajectlog_fname) as fd:
        try:
            for line in fd:
                if line.find(errstr) > 0:
                    m = re.search(r"Solr error response: (.*): {", line)
                    if m is not None:
                        solr_return_code = m.group(1)
                        num_solr_errors += 1
                        if solr_return_code != '409':
                            num_severe_errors += 1
                        m = re.search(r"source_id:(.*) output_id:(.*)>:", line)
                        if m is not None:
                            print("Skipping ID: {}".format(m.group(1)))
                elif line.find(skipstr) > 0:
                    m = re.search(r"ERROR Traject::Indexer#process returning 'false' due to (.*) skipped records.", line)
                    if m is not None:
                        num_skipped = m.group(1)
                        #Variable.set("traject_num_rejected", num_skipped)
                    else:
                        print(line)
        except Exception as e:
            print('Error parsing log {} bailing {}.'.format(trajectlog_fname, str(e)))

    print("Num severe errors: {}".format(num_severe_errors))
    print("Num solr errors: {}".format(num_solr_errors))
    print("Num skipped: {}".format(num_skipped))
    Variable.set("traject_num_rejected", num_solr_errors)
    if os.path.isfile(trajectlog_fname):
        os.rename(trajectlog_fname, '{}-{}.log'.format(trajectlog_fname, time.time()))
