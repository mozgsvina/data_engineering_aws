from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend
from airflow.exceptions import AirflowException


class StageToRedshiftOperator(BaseOperator):
    template_fields = ("s3_key",)
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}'
    """


    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 aws_credentials_id="aws_credentials",
                 table="",
                 create_table_sql="",
                 json_format="auto",
                 s3_bucket="airflow-bucket-anya",
                 s3_key="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table = table
        self.create_table_sql = create_table_sql
        self.aws_credentials_id = aws_credentials_id
        self.json_format = json_format


    def execute(self, context):
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection(self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        try:
            self.log.info("Creating staging destination Redshift table")
            redshift.run(self.create_table_sql)
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.table))
            self.log.info("Copying data from S3 to Redshift")
            # s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
            s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"


            formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                aws_connection.login,
                aws_connection.password,
                self.json_format
            )
            redshift.run(formatted_sql)

        except Exception as e:
            self.log.error(f"Error occurred during data copy: {str(e)}")
            raise AirflowException("Data copy from S3 to Redshift failed")






