from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class SetupDatabaseOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 dict_of_queries: dict = None,
                 *args,
                 **kwargs):

        super(SetupDatabaseOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.dict_of_queries = dict_of_queries if dict_of_queries else {}

    def execute(self, context):
        """
        Runs a batch of queries provided in the dictionarr format query_name:query_content

        This operator ir normally used for set up databases, such as table and relationship creation

        If multiple statements are contained in the query variable, they all are run separately
        """
        self.log.info(f"Running setup {self.dict_of_queries}")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for query_name, query in self.dict_of_queries.items():
            self.log.info(f"Running Query: {query_name}")
            if isinstance(query, str):
                redshift.run(query)
                self.log.info(f"Running: {query}")
            elif isinstance(query, list):
                for query_entry in query:
                    self.log.info(f"Running: {query_entry}")
                    redshift.run(query_entry)
            else:
                continue
            self.log.info(f"Finished Running Query {query_name}")
