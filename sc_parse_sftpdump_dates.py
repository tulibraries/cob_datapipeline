"""
Task to loop through the files downloaded from sftp and parse out the date
they were generated by Alma.
"""
import os
import time
import re
from airflow.models import Variable

UTC_DATESTAMP_FSTR = "%Y-%m-%dT%H:%M:%SZ"
MARCFILE_PREFIX = "alma_bibs__"
MARCFILE_EXTENSION = "xml"


# Parse filename for datestamp. (e.g. alma_bibs__2018041306_6863237930003811_new_61.xml)
def get_dump_date(marcdircontents):
    """Gets dump date, parses filenames in the directory and returns the first date it finds."""
    datestamp = None
    datestampregex = re.compile(r"alma_bibs__(.*)_(.*).xml")
    marcfileregex = re.compile(MARCFILE_PREFIX + r".*\." + MARCFILE_EXTENSION + "$")
    for infilename in marcdircontents:
        if marcfileregex.search(infilename):
            rxresults = datestampregex.search(infilename)
            if rxresults is not None:
                datestamp = rxresults.group(1)
                break
    return datestamp


def parse_sftpdump_dates(**kwargs):
    """Assigns date variables and resets oai variables for full reindex."""
    ingest_command = kwargs.get("INGEST_COMMAND")
    marcfilepath = kwargs.get("ALMASFTP_HARVEST_PATH")
    date = None

    if os.path.isfile(ingest_command):
        marcdircontents = os.listdir(marcfilepath)
        datestamp = get_dump_date(marcdircontents)
        if datestamp is not None:
            #  MAYBE TRY TO PARSE THESE OUT IN THE FUTURE?
            Variable.set("ALMASFTP_HARVEST_RAW_DATE", datestamp)
            Variable.set("almaoai_last_num_oai_update_recs", 0)
            Variable.set("almaoai_last_num_oai_delete_recs", 0)
            date = time.strptime(datestamp[:8], "%Y%m%d")
            Variable.set("ALMAOAI_LAST_HARVEST_DATE", time.strftime(UTC_DATESTAMP_FSTR, date))
            Variable.set("ALMAOAI_LAST_HARVEST_FROM_DATE", time.strftime(UTC_DATESTAMP_FSTR, date))
        else:
            raise Exception("Cannot find a datestamp in {}".format(marcfilepath))
    else:
        raise Exception(str(os.path.isfile(ingest_command)) +
                        " Cannot locate {}".format(ingest_command))
