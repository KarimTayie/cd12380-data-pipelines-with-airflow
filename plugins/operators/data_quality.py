from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = "#89DA59"

    @apply_defaults
    def __init__(
        self,
        conn_id="",
        test_queries=None,
        expected_results=None,
        *args,
        **kwargs,
    ):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.test_queries = test_queries
        self.expected_results = expected_results

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.conn_id)
        
        if not self.test_queries or not self.expected_results:
            raise ValueError("test_queries and expected_results must be provided.")
        
        if len(self.test_queries) != len(self.expected_results):
            raise ValueError("test_queries and expected_results must have the same length.")
        
        for test_query, expected_result in zip(self.test_queries, self.expected_results):
            records = redshift_hook.get_records(test_query)
            if expected_result != records[0][0]:
                raise ValueError(f"Data quality check failed. Expected {expected_result}, but got {records[0][0]}.")
        
        self.log.info("Data quality check passed.")
