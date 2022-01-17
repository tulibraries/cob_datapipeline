from airflow.providers.amazon.aws.transfers.sftp_to_s3 import SFTPToS3Operator
from airflow.utils.decorators import apply_defaults


class BatchSFTPToS3Operator(SFTPToS3Operator):
    """
    This operator enables the transferring a batch of files from a SFTP server to
    Amazon S3. It expects a previous task to push the list of files to be transferred to XCOM.
    :param sftp_conn_id: The sftp connection id. The name or identifier for
        establishing a connection to the SFTP server.
    :type sftp_conn_id: str
    :param sftp_base_path: The sftp remote path where the batch of files can be found.
    :type sftp_path: str
    :param s3_conn_id: The s3 connection id. The name or identifier for
        establishing a connection to S3
    :type s3_conn_id: str
    :param s3_bucket: The targeted s3 bucket. This is the S3 bucket to where
        the file is uploaded.
    :type s3_bucket: str
    :param s3_prefix: The prefix that will be used to generate full s3 key path for each file in
        the batch.
    :type s3_prefix: str
    :param xcom_id: Id of the task which pushed the list of files to be transferred to xcom.
        Pulls the default xcom value for that ID.
    :type xcom_id: str
    """

    template_fields = ('s3_prefix','s3_key', 'sftp_path')

    @apply_defaults
    def __init__(self,
                 s3_bucket,
                 s3_prefix,
                 sftp_base_path="./",
                 xcom_id="",
                 sftp_conn_id='ssh_default',
                 s3_conn_id='aws_default',
                 provide_context=True,
                 **kwargs):
        super(BatchSFTPToS3Operator, self).__init__(s3_bucket=s3_bucket, s3_key=None, sftp_path=None, **kwargs)
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
