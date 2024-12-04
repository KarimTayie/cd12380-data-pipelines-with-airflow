from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries


class LoadFactOperator(BaseOperator):

    ui_color = "#F98866"

    @apply_defaults
    def __init__(self, conn_id="", *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.conn_id)

        redshift_hook.run(SqlQueries.songplay_table_insert)
        self.log.info("Inserted data into songplay table.")
