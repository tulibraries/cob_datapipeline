from airflow.models.baseoperator import BaseOperator, BaseOperatorLink
from airflow.contrib.operators.sftp_to_s3_operator import SFTPToS3Operator
from airflow.utils.decorators import apply_defaults


class BatchSFTPToS3Operator(SFTPToS3Operator):
    
    template_fields = ('s3_prefix','s3_key', 'sftp_path')

    @apply_defaults
    def __init__(self,
                 s3_bucket,
                 s3_prefix,
                 sftp_base_path="./",
                 xcom_id="",
                 sftp_conn_id='ssh_default',
                 s3_conn_id='aws_default',
                 *args,
                 **kwargs):
        super(BatchSFTPToS3Operator, self).__init__(s3_bucket=s3_bucket, s3_key=None, sftp_path=None, *args, **kwargs)
        self.sftp_conn_id = sftp_conn_id
        self.xcom_id = xcom_id
        self.sftp_base_path= sftp_base_path
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.s3_conn_id = s3_conn_id

    def execute(self, context):
        count = 0
        sftp_files_list = context['task_instance'].xcom_pull(task_ids=self.xcom_id)
        self.log.info("The files list is %s", sftp_files_list)
        for f in sftp_files_list:
            self.log.info("Sending %s to s3", f)
            count += 1
            
            self.s3_key = f"{self.s3_prefix}{f}"
            self.sftp_path = f"{self.sftp_base_path}/{f}"
            
            super(BatchSFTPToS3Operator, self).execute(context)
            self.log.info("Sent to s3://%s/%s", self.s3_bucket, self.s3_key)
        
        self.log.info(f"Total Files transfered: {count}")
        
####

