from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="redshift",
                aws_credentials_id="aws_credentials",
                test_cases={},
                *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.test_cases = test_cases

    def execute(self, context):
        metastoreBackend = MetastoreBackend()
        aws_connection = metastoreBackend.get_connection(self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for sql, expected_val in self.test_cases.items():
            res_records = redshift.get_records(sql)
            if res_records and res_records[0] and len(res_records[0]) > 0:
                    count_value = res_records[0][0]
                    if count_value != expected_val:
                        raise ValueError(f"Data quality check failed for {sql} test. Expected {expected_val}, got {count_value}")
            else:
                raise ValueError(f"Data quality check failed. Expected {expected_val}, got no count value.")
