import datetime
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    copy_parquet = """
        COPY {TABLE}
        FROM '{S3_URI}'
        IAM_ROLE  '{IAM_ROLE}'
        FORMAT AS PARQUET;
    """

    copy_csv = """
        COPY {TABLE}
        FROM '{S3_URI}'
        ACCESS_KEY_ID '{ACCESS_ID}'
        SECRET_ACCESS_KEY '{ACCESS_KEY}'
        CSV
        IGNOREHEADER 1;
    """

    copy_json = """
        COPY public.{TABLE}
        FROM '{S3_URI}'
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
                 iam_role="",
                 create_table_query="",
                 s3_bucket="",
                 s3_path="",
                 json_path="",
                 use_partitioned_data=False,
                 truncate=False,
                 data_type='parquet',
                 execution_date='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.target_table = target_table
        self.iam_role = iam_role
        self.create_table_query = create_table_query
        self.s3_path = s3_path
        self.json_path = json_path
        self.truncate = truncate
        self.data_type = data_type
        self.execution_date = kwargs.get('execution_date')
        self.use_partitioned_data = use_partitioned_data
        self.log.info(kwargs)

    def execute(self, context):
        """
        Module used to load specific data from S3 into Redshift
        It can load PARQUET, JSON and CSV format files.
        Accepts data partitioned by dates, such as
            year=2010/month=11/day=2
        Ideally for using backfil functionalities
        """
        credentials = AwsHook(self.aws_credentials_id).get_credentials()
        self.log.info("Creating Redshift Connection")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Redshift Connection Established")

        if self.create_table_query:
            self.log.info(
                f"Creating Target Table If Not Exists: {self.target_table}"
            )
            redshift.run(self.create_table_query)

        if self.truncate:
            self.log.warning("Deleting Data From Table: {self.target_table}")
            redshift.run(f"DELETE FROM {self.target_table}")

        # Backfill a specific date
        if self.use_partitioned_data == 'true' and self.execution_date:
            data_s3_path = "{BASE_PATH}".format(
                BASE_PATH=self.s3_path,
                YEAR=self.execution_date.strftime("%Y"),
                MONTH=self.execution_date.strftime("%m"),
                DAY=self.execution_date.strftime("%d")
            )
        else:
            self.log.info(f"Execution date: {self.execution_date}")
            data_s3_path = self.s3_path

        self.log.info(f"Loading Data From: {data_s3_path}")

        if self.data_type == 'json':
            if not self.json_path:
                s3_json_path = "auto"
            else:
                s3_json_path = self.json_path
                self.log.info(f"Using the JSON Path Parser: {s3_json_path}")

            staging_sql = StageToRedshiftOperator.copy_sql.format(
                TABLE=self.target_table,
                S3_URI=data_s3_path,
                ACCESS_ID=credentials.access_key,
                ACCESS_KEY=credentials.secret_key,
                JSON_PARSER_PATH=s3_json_path
            )
        elif self.data_type == 'parquet':
            staging_sql = StageToRedshiftOperator.copy_parquet.format(
                TABLE=self.target_table,
                S3_URI=data_s3_path,
                IAM_ROLE=self.iam_role
            )
        elif self.data_type == 'csv':
            staging_sql = StageToRedshiftOperator.copy_csv.format(
                TABLE=self.target_table,
                S3_URI=data_s3_path,
                ACCESS_ID=credentials.access_key,
                ACCESS_KEY=credentials.secret_key
            )

        self.log.info("Initiating Data Loading")
        redshift.run(staging_sql)
        self.log.info("Redshift COPY Finished")
