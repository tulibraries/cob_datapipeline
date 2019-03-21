"""TU Libraries Airflow Script for Harvesting data from Alma SFTP Server."""

import os
import sys
from pexpect import spawn, EOF, TIMEOUT
from airflow import AirflowException

def almasftp_fetch():
    #pylint: disable=invalid-name, C0301
    """Method to Retrieve / Harvest Records from Alma SFTP Server."""
    host = "23.92.21.64"
    port = os.environ["ALMASFTP_PORT"]
    user = "almasftp"
    passwd = os.environ["ALMASFTP_PASSWD"]
    remotepath = "/incoming"

    file_prefix = "alma_bibs__"
    file_extension = ".xml.tar.gz"

    p = spawn("sftp -P %s %s@%s"%(port, user, host))
    p.logfile = sys.stdout
    try:
        p.expect("(?i)password:")
        x = p.sendline(passwd)
        x = p.expect(["Permission denied", "sftp>"])
        if not x:
            print(" Permission denied for password: ")
            print(password)
            p.kill(0)
        else:
            x = p.sendline("cd " + remotepath)
            x = p.expect("sftp>")
            x = p.sendline("mget " + file_prefix + "*" + file_extension)
            x = p.expect("sftp>")
            x = p.isalive()
            p.close()
    except EOF:
        print(str(p))
        print("Transfer failed: EOF.")
        raise AirflowException("Transfer failed: EOF.")
    except TIMEOUT:
        print(str(p))
        print("Transfer failed: TIMEOUT.")
        raise AirflowException("Transfer failed: TIMEOUT.")
    except:
        raise AirflowException("Transfer failed.")
