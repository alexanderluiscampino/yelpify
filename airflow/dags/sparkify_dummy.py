import json
import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators import (DataQualityOperator, LoadDimensionOperator,
                               LoadFactOperator, StageToRedshiftOperator)
from airflow.operators import (StageToRedshiftOperator)
from airflow.operators.dummy_operator import DummyOperator
from helpers import SqlQueries

default_args = {
    'owner': 'alexandrec',
    'start_date': datetime(20, 1, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
    'email_on_retry': False
}

dag = DAG('sparkify',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
          )

start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag
)

stage_business_to_redshift = DummyOperator(
    task_id='Stage_business_data',
    dag=dag
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

start_operator >> stage_business_to_redshift
start_operator >> stage_review_to_redshift
start_operator >> stage_users_to_redshift
start_operator >> stage_tip_to_redshift
start_operator >> stage_stock_to_redshift

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
