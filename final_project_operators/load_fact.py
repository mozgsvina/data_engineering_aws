from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend
from udacity.common import final_project_sql_statements
from airflow.exceptions import AirflowException


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="redshift",
                aws_credentials_id="aws_credentials",
                *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection(self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        try:
            self.log.info("Creating fact table")
            redshift.run(final_project_sql_statements.SqlQueries.songplay_table_create)
            self.log.info("Clearing table for testing purposes")
            redshift.run("DELETE FROM songplay;")
            self.log.info("Inserting data into fact table")
            redshift.run(final_project_sql_statements.SqlQueries.songplay_table_insert)
        except Exception as e:
            raise AirflowException(f"Error executing SQL queries: {str(e)}")

