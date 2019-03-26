from airflow.models import Variable
from airflow.exceptions import *
import os
import subprocess
import time
import re


def parse_sftpdump(ds, **kwargs):
    UTC_DATESTAMP_FSTR = '%Y-%m-%dT%H:%M:%SZ'
    marcfile_prefix = 'alma_bibs__'
    marcfile_extension = 'xml'
    ingest_command = Variable.get("AIRFLOW_HOME") + "/dags/cob_datapipeline/scripts/ingest_marc_multi.sh"
    marcfilepath = Variable.get("AIRFLOW_DATA_DIR") + "/sftpdump/"
    date = None
    if os.path.isfile(ingest_command):
        marcdircontents = os.listdir(marcfilepath)
        marcfileregex = re.compile(marcfile_prefix + ".*\." + marcfile_extension + "$")
        #parse filename for datestamp e.g. alma_bibs__2018041306_6863237930003811_new_61.xml
        datestampregex = re.compile(r"alma_bibs__(.*)_(.*).xml")
        for infilename in marcdircontents:
            if marcfileregex.search(infilename):
                m = datestampregex.search(infilename)
                if m is not None:
                    datestamp = m.group(1)
                    #  MAYBE TRY TO PARSE THESE OUT IN THE FUTURE?
                    Variable.set("almaoai_last_num_oai_update_recs", 0)
                    Variable.set("almaoai_last_num_oai_delete_recs", 0)
                    Variable.set("almaoai_last_harvest_date", 0)
                    break
        date = time.strptime(datestamp[:8], "%Y%m%d")
        Variable.set("almaoai_last_harvest_date", time.strftime(UTC_DATESTAMP_FSTR, date))
    else:
        raise Exception(str(os.path.isfile(ingest_command)) + " Cannot locate {}".format(ingest_command))
