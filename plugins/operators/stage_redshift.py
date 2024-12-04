from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.secrets.metastore import MetastoreBackend
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = "#358140"

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        s3_path="",
        table_name="",
        *args,
        **kwargs,
    ):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.conn_id = redshift_conn_id
        self.s3_path = s3_path
        self.table_name = table_name
        self.copy_sql = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            FORMAT AS JSON 'auto'
            REGION 'us-east-1'
        """

    def execute(self, context):
        metastoreBackend = MetastoreBackend()
        aws_connection = metastoreBackend.get_connection("aws_credentials")
        redshift_hook = PostgresHook(postgres_conn_id=self.conn_id)

        self.log.info("Copying data from S3 to Redshift.")
        formatted_sql = self.copy_sql.format(
            self.table_name,
            self.s3_path,
            aws_connection.login,
            aws_connection.password,
        )
        redshift_hook.run(formatted_sql)
        self.log.info("Copied data from S3 to Redshift.")
