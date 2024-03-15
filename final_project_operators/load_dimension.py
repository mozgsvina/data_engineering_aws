from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import final_project_sql_statements
from airflow.secrets.metastore import MetastoreBackend
from airflow.exceptions import AirflowException


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="redshift",
                aws_credentials_id="aws_credentials",
                table="",
                sql_create="",
                sql_insert="",
                truncate_table=False,
                *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.sql_create = sql_create
        self.sql_insert = sql_insert
        self.truncate_table = truncate_table

    def execute(self, context):
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection(self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        try:

            self.log.info(f"Creating dimension table if not exists {self.table}")
            redshift.run(self.sql_create)

            if self.truncate_table:
                self.log.info(f"Truncating table {self.table}")
                redshift.run("TRUNCATE TABLE {}".format(self.table))

            # self.log.info(f"Deleting table if exists: {self.table}")
            # redshift.run("DROP TABLE IF EXISTS {}".format(self.table))

            self.log.info(f"Inserting data into table {self.table}")
            redshift.run(self.sql_insert)
            
            self.log.info(f"Dimension load complete for table {self.table}")

        except Exception as e:
            self.log.error(f"Error occurred during dimension load for table {self.table}: {str(e)}")
            raise AirflowException(f"Dimension load failed for table {self.table}")
        

