import json
import logging
import os
from enum import Enum, unique
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators import (DataQualityOperator,
                               LoadOperator, StageToRedshiftOperator,
                               PostgresOperator, SetupDatabaseOperator)

from airflow.operators.dummy_operator import DummyOperator
from helpers import SqlQueries


@unique
class TableType(Enum):
    FACT = 'fact'
    DIM = 'dim'
    STAGE = 'staging'


@unique
class Table(Enum):
    BUSINESS = 'business'
    CITY = 'city'
    REVIEW = 'review'
    TIP = 'tip'
    USERS = 'users'
    STOCK = 'stock'

    def get_data_type(self):
        if self == self.STOCK:
            return 'csv'
        else:
            return 'parquet'

    def get_insert_query(self, table_type: TableType):
        attribute = f"{self.get_table_name(table_type)}_insert"
        return getattr(SqlQueries, attribute).value

    def get_table_name(self, table_type: TableType):
        return f"{self.value}_{table_type.value}"

    def get_s3_path(self, partitioned=False):
        if not partitioned:
            if self == self.STOCK:
                return "s3://yelp-customer-reviews/stock-data/chipotle.csv"
            else:
                return f"s3://yelp-customer-reviews/processed/{self.value}/".replace('users', 'user')

        else:
            path = f"s3://yelp-customer-reviews/data-lake/{self.value}".replace(
                'users', 'user')
            return path + "/pyear={YEAR}/pmonth={MONTH}/pday={DAY}"


PARTITIONED = False
DROP_ALL = True

staging_tables = [table.get_table_name(TableType.STAGE) for table in Table]
fact_tables = [table.get_table_name(TableType.FACT) for table in Table]
dim_tables = [Table.REVIEW.get_table_name(TableType.DIM)]
list_of_tables = fact_tables + dim_tables

delete_tables = staging_tables+list_of_tables if DROP_ALL else staging_tables
delete_statements = {
    "drop_table": [SqlQueries.drop_table_statement.value.format(TABLE_NAME=table) for table in delete_tables]
}
delete_statements = {}
setup_database_dict = {
    query.name: query.value for query in SqlQueries if ('create' in query.name)
}
foreign_keys_setup = {
    SqlQueries.setup_foreign_keys.name: SqlQueries.setup_foreign_keys.value
}

default_args = {
    'owner': 'alexandrec',
    'start_date': datetime(20, 1, 1),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
    'email_on_retry': False
}

dag = DAG('yelpify',
          default_args=default_args,
          catchup=False,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'  # @daily
          )

start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag
)

drop_existing_tables = SetupDatabaseOperator(
    task_id='Drop_existing_tables',
    dict_of_queries=delete_statements,
    dag=dag
)

setup_database = SetupDatabaseOperator(
    task_id='Setup_database',
    dict_of_queries=setup_database_dict,
    dag=dag
)

stage_business_to_redshift = StageToRedshiftOperator(
    task_id='Stage_business_data',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    iam_role='arn:aws:iam::500349149336:role/dwhRole',
    target_table=Table.BUSINESS.get_table_name(TableType.STAGE),
    s3_path=Table.BUSINESS.get_s3_path(PARTITIONED),
    use_partitioned_data=PARTITIONED,
    data_type=Table.BUSINESS.get_data_type(),
    provide_context=True
)

stage_review_to_redshift = StageToRedshiftOperator(
    task_id='Stage_review_data',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    iam_role='arn:aws:iam::500349149336:role/dwhRole',
    target_table=Table.REVIEW.get_table_name(TableType.STAGE),
    s3_path=Table.REVIEW.get_s3_path(PARTITIONED),
    json_path=None,
    use_partitioned_data=PARTITIONED,
    data_type=Table.REVIEW.get_data_type(),
    provide_context=True

)

stage_users_to_redshift = StageToRedshiftOperator(
    task_id='Stage_users_data',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    iam_role='arn:aws:iam::500349149336:role/dwhRole',
    target_table=Table.USERS.get_table_name(TableType.STAGE),
    s3_path=Table.USERS.get_s3_path(PARTITIONED),
    use_partitioned_data=PARTITIONED,
    data_type=Table.USERS.get_data_type(),
    provide_context=True,
    dag=dag
)

stage_tip_to_redshift = StageToRedshiftOperator(
    task_id='Stage_tip_data',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    iam_role='arn:aws:iam::500349149336:role/dwhRole',
    target_table=Table.TIP.get_table_name(TableType.STAGE),
    s3_path=Table.TIP.get_s3_path(PARTITIONED),
    use_partitioned_data=PARTITIONED,
    data_type=Table.TIP.get_data_type(),
    provide_context=True,
    dag=dag
)

stage_stock_to_redshift = StageToRedshiftOperator(
    task_id='Stage_stock_data',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    target_table=Table.STOCK.get_table_name(TableType.STAGE),
    s3_path=Table.STOCK.get_s3_path(PARTITIONED),
    use_partitioned_data=PARTITIONED,
    data_type=Table.STOCK.get_data_type(),
    provide_context=True,
    dag=dag
)


process_tip_fact = LoadOperator(
    task_id='Process_tip_fact',
    redshift_conn_id="redshift",
    target_table=Table.TIP.get_table_name(TableType.FACT),
    insert_table_query=Table.TIP.get_insert_query(TableType.FACT),
    dag=dag,
    provide_context=True
)

process_business_fact = LoadOperator(
    task_id='Process_business_fact',
    redshift_conn_id="redshift",
    target_table=Table.BUSINESS.get_table_name(TableType.FACT),
    insert_table_query=Table.BUSINESS.get_insert_query(TableType.FACT),
    dag=dag
)

process_city_fact = LoadOperator(
    task_id='Process_city_fact',
    redshift_conn_id="redshift",
    target_table=Table.CITY.get_table_name(TableType.FACT),
    insert_table_query=Table.CITY.get_insert_query(TableType.FACT),
    dag=dag
)

process_stock_fact = LoadOperator(
    task_id='Process_stock_fact',
    redshift_conn_id="redshift",
    target_table=Table.STOCK.get_table_name(TableType.FACT),
    insert_table_query=Table.STOCK.get_insert_query(TableType.FACT),
    dag=dag
)

process_review_fact = LoadOperator(
    task_id='Process_review_fact',
    redshift_conn_id="redshift",
    target_table=Table.REVIEW.get_table_name(TableType.FACT),
    insert_table_query=Table.REVIEW.get_insert_query(TableType.FACT),
    dag=dag
)

process_users_fact = LoadOperator(
    task_id='Process_users_fact',
    redshift_conn_id="redshift",
    target_table=Table.USERS.get_table_name(TableType.FACT),
    insert_table_query=Table.USERS.get_insert_query(TableType.FACT),
    dag=dag
)

process_review_dim = LoadOperator(
    task_id='Process_review_dim',
    redshift_conn_id="redshift",
    target_table=Table.REVIEW.get_table_name(TableType.DIM),
    insert_table_query=Table.REVIEW.get_insert_query(TableType.DIM),
    dag=dag
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    list_of_tables=list_of_tables,
    dag=dag
)

setup_foreign_key = SetupDatabaseOperator(
    task_id='Setup_foreign_keys',
    dict_of_queries=foreign_keys_setup,
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> drop_existing_tables >> setup_database

setup_database >> stage_business_to_redshift
setup_database >> stage_review_to_redshift
setup_database >> stage_users_to_redshift
setup_database >> stage_tip_to_redshift
setup_database >> stage_stock_to_redshift


stage_business_to_redshift >> process_city_fact
stage_review_to_redshift >> process_review_dim
stage_users_to_redshift >> process_users_fact
stage_tip_to_redshift >> process_tip_fact
stage_stock_to_redshift >> process_stock_fact

process_city_fact >> process_business_fact
process_business_fact >> process_stock_fact
process_business_fact >> process_review_dim
process_business_fact >> process_tip_fact
process_users_fact >> process_tip_fact
process_review_dim >> process_review_fact


process_tip_fact >> run_quality_checks
process_stock_fact >> run_quality_checks
process_review_fact >> run_quality_checks

run_quality_checks >> setup_foreign_key >> end_operator
