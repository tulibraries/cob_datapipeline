from airflow import AirflowException
from airflow.models import Variable
from pexpect import *
import sys
import os


def almasftp_fetch():
    host = Variable.get('ALMASFTP_HOST')
    port = Variable.get('ALMASFTP_PORT')
    user = Variable.get('ALMASFTP_USER')
    passwd = Variable.get('ALMASFTP_PASSWD')
    remotepath = '/incoming'
    localpath = Variable.get("AIRFLOW_DATA_DIR") + "/sftpdump"

    if not os.path.exists(localpath):
        os.makedirs(localpath)

    file_prefix = 'alma_bibs__'
    file_extension = '.xml.tar.gz'

    sftpcmd = 'sftp -P {} {}@{}'.format(port, user, host)
    print(sftpcmd)
    # pexpect defaults to a bytestream for logging which airflow can't handle, so force it to speak unicode
    p = spawn(sftpcmd, encoding='utf-8')

    try:
        p.expect('(?i)password:')
        x = p.sendline(passwd)
        x = p.expect(['Permission denied', 'sftp>'])
        if not x:
            print('Permission denied for password:')
            print(passwd)
            p.kill(0)
        else:
            p.logfile = sys.stdout
            x = p.sendline('cd ' + remotepath)
            x = p.expect('sftp>')
            x = p.sendline('lcd ' + localpath)
            x = p.expect('sftp>')
            x = p.sendline('mget ' + file_prefix + '*' + file_extension)
            x = p.expect('sftp>', timeout=720)
            x = p.isalive()
            p.close()
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
        raise AirflowException('Transfer failed: {}.'.format(str(e)))
