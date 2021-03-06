from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 list_of_tables=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.list_of_tables = list_of_tables if list_of_tables else []

    def execute(self, context):
        """
        Runs data quality checks on the tables created

        Checks Run:
            length of records: there must be at least one record per table
        """
        check_pass = True
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(
            f"Initiating data quality checks for: {self.list_of_tables}")

        for table in self.list_of_tables:
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            self.log.info(records)
            if len(records) < 1 or len(records[0]) < 1:
                check_pass = False
            else:
                num_records = records[0][0]

                if num_records == 0:
                    check_pass = False

            if not check_pass:
                message = f"Failed Data Quality Check For: {table}"
                self.log.error(message)
                raise ValueError(message)

            self.log.info(
                f"{table} has passed data quality check with {num_records} records")
