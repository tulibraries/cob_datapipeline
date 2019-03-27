from airflow import AirflowException
from airflow.models import Variable
from pexpect import *
import sys
import os

# class DisableLogger():
#     def __enter__(self):
#         logging.disable(logging.CRITICAL)
#
#     def __exit__(self, a, b, c):
#         logging.disable(logging.NOTSET)
# with DisableLogger():


def almasftp_fetch():
    host = Variable.get('ALMASFTP_HOST')
    port = Variable.get('ALMASFTP_PORT')
    user = Variable.get('ALMASFTP_USER')
    passwd = Variable.get('ALMASFTP_PASSWD')
    remotepath = '/incoming'
    localpath = Variable.get("AIRFLOW_DATA_DIR") + "/sftpdump"

    file_prefix = 'alma_bibs__'
    file_extension = '.xml.tar.gz'

    p = spawn('sftp -P {} {}@{}'.format(port, user, host))
    # p.logfile = sys.stdout

    try:
        p.expect('(?i)password:')
        x = p.sendline(passwd)
        x = p.expect(['Permission denied', 'sftp>'])
        if not x:
            print('Permission denied for password:')
            print(password)
            p.kill(0)
        else:
            x = p.sendline('cd ' + remotepath)
            x = p.expect('sftp>')
            x = p.sendline('lcd ' + localpath)
            x = p.expect('sftp>')
            x = p.sendline('mget ' + file_prefix + '*' + file_extension)
            x = p.expect('sftp>', timeout=360)
            x = p.isalive()
            x = p.close()
            retval = p.exitstatus
    except EOF:
        print(str(p))
        print('Transfer failed: EOF.')
        raise AirflowException('Transfer failed: EOF.')
    except TIMEOUT:
        print(str(p))
        print('Transfer failed: TIMEOUT.')
        raise AirflowException('Transfer failed: TIMEOUT.')
    except Exception as e:
        raise AirflowException('Transfer failed: {}.'.format(e.message))
