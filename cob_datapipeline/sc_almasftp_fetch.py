from airflow import AirflowException
from pexpect import *
import sys
import os
import logging

def almasftp_sc_fetch(**kwargs):
    host = kwargs.get("host")
    port = kwargs.get("port")
    user = kwargs.get("user")
    passwd = kwargs.get("passwd")
    remotepath = kwargs.get("remotepath")
    localpath = kwargs.get("localpath")

    if not os.path.exists(localpath):
        os.makedirs(localpath)

    file_prefix = "alma_bibs__"
    file_extension = ".xml.tar.gz"

    sftpcmd = "sftp -P {port} -o StrictHostKeyChecking=no {user}@{host}".format(port=port, user=user, host=host)
    logging.info(sftpcmd)
    # pexpect defaults to a bytestream for logging which airflow can"t handle, so force it to speak unicode
    p = spawn(sftpcmd, encoding="utf-8")

    try:
        p.expect("(?i)password:")
        x = p.sendline(passwd)
        x = p.expect(["Permission denied", "sftp>"])
        if not x:
            logging.error("Permission denied for password:")
            p.kill(0)
        else:
            p.logfile = sys.stdout
            x = p.sendline("cd " + remotepath)
            x = p.expect("sftp>")
            x = p.sendline("lcd " + localpath)
            x = p.expect("sftp>")
            x = p.sendline("mget " + file_prefix + "*" + file_extension)
            x = p.expect("sftp>", timeout=720)
            x = p.isalive()
            p.close()
            retval = p.exitstatus
    except EOF:
        logging.error(str(p))
        logging.error("Transfer failed: EOF.")
        raise AirflowException("Transfer failed: EOF.")
    except TIMEOUT:
        logging.error(str(p))
        logging.error("Transfer failed: TIMEOUT.")
        raise AirflowException("Transfer failed: TIMEOUT.")
    except Exception as e:
        raise AirflowException("Transfer failed: {}.".format(str(e)))
