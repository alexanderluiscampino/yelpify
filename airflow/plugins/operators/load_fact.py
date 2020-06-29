from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 target_table="",
                 create_table_query="",
                 insert_table_query="",
                 append_only=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.create_table_query = create_table_query
        self.insert_table_query = insert_table_query
        self.append_only = append_only

    def execute(self, context):
        self.log.info("Creating Redshift Connection")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Redshift Connection Established")

        self.log.info(
            f"Creating Target Table If Not Exists: {self.target_table}"
        )
        redshift.run(self.create_table_query)

        if not self.append_only:
            self.log.warning(f"Deleting Data From: {self.target_table}")
            redshift.run(f"DELETE FROM {self.target_table}")

        self.log.info(
            f"Inserting data into: {self.target_table}"
        )

        redshift.run(self.insert_table_query.format(
            TABLE_NAME=self.target_table
        ))

        self.log.info(f"Finished Inserting Data Into: {self.target_table}")
