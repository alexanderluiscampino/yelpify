import json
import logging
import os
from enum import Enum, unique
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators import (DataQualityOperator, LoadDimensionOperator,
                               LoadFactOperator, StageToRedshiftOperator,
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

    def get_table_name(self, table_type: TableType):
        return f"{self.name}_{table_type.value}"

    def get_partitions(self):
        return {
            self.USERS: {'YEAR': 2004, 'MONTH': 10, 'DAY': 12},
            self.REVIEW: {'YEAR': 2005, 'MONTH': 3, 'DAY': 3},
            self.TIP: {'YEAR': 2009, 'MONTH': 12, 'DAY': 15}
        }.get(self)

    def get_s3_path(self):
        if self == self.BUSINESS:
            return "s3://yelp-customer-reviews/processed/business/"
        elif self == self.STOCK:
            return "s3://yelp-customer-reviews/stock-data/cmg.us.txt"
        else:
            path = f"s3://yelp-customer-reviews/data-lake/{self.value}".replace(
                'users', 'user')
            path = path + "/pyear={YEAR}/pmonth={MONTH}/pday={DAY}"
            return path.format(**self.get_partitions())


default_args = {
    'owner': 'alexandrec',
    'start_date': datetime(20, 1, 1),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
    'email_on_retry': False
}

dag = DAG('sparkify',
          default_args=default_args,
          catchup=False,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
          )

setup_database_dict = {}
setup_database_dict = {
    query.name: query.value for query in SqlQueries if ('create' in query.name)

}
# setup_database_dict[SqlQueries.setup_foreign_keys.name] = SqlQueries.setup_foreign_keys.value


start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag
)
setup_database = SetupDatabaseOperator(
    task_id='Setup_database',
    list_of_queries=setup_database_dict,
    dag=dag
)

stage_business_to_redshift = StageToRedshiftOperator(
    task_id='Stage_business_data',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    iam_role='arn:aws:iam::500349149336:role/dwhRole',
    target_table=Table.BUSINESS.get_table_name(TableType.STAGE),
    s3_path=Table.BUSINESS.get_s3_path(),
    json_path=None,
    use_partitioned_data=False,
    data_type=Table.BUSINESS.get_data_type(),
    provide_context=True

)

stage_review_to_redshift = DummyOperator(
    task_id='Stage_review_data',
    dag=dag
)

stage_users_to_redshift = DummyOperator(
    task_id='Stage_users_data',
    dag=dag
)

stage_tip_to_redshift = DummyOperator(
    task_id='Stage_tip_data',
    dag=dag
)

stage_stock_to_redshift = DummyOperator(
    task_id='Stage_stock_data',
    dag=dag
)


process_tip_fact = DummyOperator(
    task_id='Process_tip_fact',
    dag=dag
)

process_business_fact = DummyOperator(
    task_id='Process_business_fact',
    dag=dag
)

process_city_fact = DummyOperator(
    task_id='Process_city_fact',
    dag=dag
)

process_stock_fact = DummyOperator(
    task_id='Process_stock_fact',
    dag=dag
)

process_review_fact = DummyOperator(
    task_id='Process_review_fact',
    dag=dag
)

process_users_fact = DummyOperator(
    task_id='Process_users_fact',
    dag=dag
)

process_review_dim = DummyOperator(
    task_id='Process_review_dim',
    dag=dag
)

run_quality_checks = DummyOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

# stage_data = StageToRedshiftOperator(
#     task_id='',
#     dag=dag
# )

# load_fact = LoadFactOperator(
#     task_id='',
#     dag=dag
# )

# load_dimension = LoadDimensionOperator(
#     task_id='',
#     dag=dag
# )


# run_quality = DataQualityOperator(
#     task_id='',
#     dag=dag
# )

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> setup_database
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

run_quality_checks >> end_operator
