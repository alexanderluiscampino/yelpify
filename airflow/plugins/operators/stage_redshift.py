import datetime
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    copy_sql = """
        COPY public.{TABLE}
        FROM '{S3_PATH}'
        ACCESS_KEY_ID '{ACCESS_ID}'
        SECRET_ACCESS_KEY '{ACCESS_KEY}'
        format as json '{JSON_PARSER_PATH}'
    """

    @apply_defaults
    def __init__(self,
                 # Define operators params (with defaults)
                 redshift_conn_id="",
                 aws_credentials_id="",
                 target_table="",
                 create_table_query="",
                 s3_bucket="",
                 s3_path="",
                 json_path="",
                 use_partitioned_data=False,
                 execution_date="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.target_table = target_table
        self.create_table_query = create_table_query
        self.s3_path = s3_path
        self.json_path = json_path
        self.execution_date = kwargs.get('execution_date')
        self.use_partitioned_data = use_partitioned_data

    def execute(self, context):
        self.log.info("Creating Redshift Connection")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Redshift Connection Established")

        self.log.info(
            f"Creating Target Table If Not Exists: {self.target_table}"
        )
        redshift.run(self.create_table_query)

        self.log.warning("Deleting Data From Table: {self.target_table}")
        redshift.run(f"DELETE FROM {self.target_table}")

        # Backfill a specific date
        if self.use_partitioned_data == 'true' and self.execution_date:
            data_s3_path = "{BASE_PATH}/{YEAR}/{MONTH}".format(
                BASE_PATH=self.s3_path,
                YEAR=self.execution_date.strftime("%Y"),
                MONTH=self.execution_date.strftime("%d")
            )
        else:
            data_s3_path = self.s3_path

        self.log.info(f"Loading Data From: {data_s3_path}")

        if not self.json_path:
            s3_json_path = "auto"
        else:
            s3_json_path = self.json_path

        self.log.info(f"Using the JSON Path Parser: {s3_json_path}")

        credentials = AwsHook(self.aws_credentials_id).get_credentials()
        staging_sql = StageToRedshiftOperator.copy_sql.format(
            TABLE=self.target_table,
            S3_PATH=data_s3_path,
            ACCESS_ID=credentials.access_key,
            ACCESS_KEY=credentials.secret_key,
            JSON_PARSER_PATH=s3_json_path
        )

        self.log.info("Initiating Data Loading")
        redshift.run(staging_sql)
        self.log.info("Redshift COPY Finished")
