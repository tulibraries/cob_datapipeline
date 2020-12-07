from airflow.contrib.operators.s3_to_sftp_operator import S3ToSFTPOperator
from airflow.utils.decorators import apply_defaults


class BatchS3ToSFTPOperator(S3ToSFTPOperator):
    """
    This operator enables the transferring a batch of files from Amazon S3 a SFTP server.
    It expects a previous task to push the list of files to be transferred to XCOM.
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
                 *args,
                 **kwargs):
        super(BatchS3ToSFTPOperator, self).__init__(s3_bucket=s3_bucket, s3_key=None, sftp_path=None, *args, **kwargs)
        self.sftp_conn_id = sftp_conn_id
        self.xcom_id = xcom_id
        self.sftp_base_path = sftp_base_path
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.s3_conn_id = s3_conn_id

    def execute(self, context):
        count = 0
        s3_files_list = context['task_instance'].xcom_pull(task_ids=self.xcom_id)
        self.log.info("The files list is %s", s3_files_list)
        for file in s3_files_list:
            self.log.info("Sending %s from s3", file)
            count += 1
            self.s3_key = f"{self.s3_prefix}{file}"
            self.sftp_path = f"{self.sftp_base_path}/{file}"

            super(BatchS3ToSFTPOperator, self).execute(context)
            self.log.info("Sent to sftp: %s/%s", self.s3_base_bath, self.s3_key)

        self.log.info(f"Total Files transfered: {count}")