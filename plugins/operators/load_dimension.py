from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries


class LoadDimensionOperator(BaseOperator):

    ui_color = "#80BD9E"

    @apply_defaults
    def __init__(self, conn_id="", dimension="", mode="append", *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.dimension = dimension
        self.mode = mode.lower()
        self._queries_dict = {
            "user": SqlQueries.user_table_insert,
            "song": SqlQueries.song_table_insert,
            "artist": SqlQueries.artist_table_insert,
            "time": SqlQueries.time_table_insert,
        }

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.conn_id)
        
        if self.mode == "delete_load":
            self.log.info(f"Truncating {self.dimension} table before loading data.")
            redshift_hook.run(f"TRUNCATE TABLE {self.dimension};")

        redshift_hook.run(self._queries_dict[self.dimension])
        self.log.info(f"Inserted data into {self.dimension} table.")
