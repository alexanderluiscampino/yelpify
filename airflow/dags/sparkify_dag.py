import json
import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
# from airflow.operators import (DataQualityOperator, LoadDimensionOperator,
#                                LoadFactOperator, StageToRedshiftOperator)
from airflow.operators import (StageToRedshiftOperator)
from airflow.operators.dummy_operator import DummyOperator
from helpers import SqlQueries

# Load DAG Variables
# events_bucket_name = Variable.get("EVENTS_DATA_BUCKET_NAME")
# events_key = Variable.get("EVENTS_DATA_KEY")
# events_jsonpath = Variable.get("EVENTS_JSONPATH")
# events_partitioned = Variable.get("EVENTS_DATA_PARTITIONED")

# songs_bucket_name = Variable.get("SONGS_DATA_BUCKET_NAME")
# songs_key = Variable.get("SONGS_DATA_KEY")
# songs_jsonpath = Variable.get("SONGS_JSONPATH")
# songs_partitioned = Variable.get("SONGS_DATA_PARTITIONED")

# foo = Variable.get("INSERT_MODE_DIM")
# foo = Variable.get("INSERT_MODE_FACT")


# songsplay_table_name = Variable.get("SONGPLAYS_TABLE")
# songs_table_name = Variable.get("SONGS_TABLE")
# artists_table_name = Variable.get("ARTISTS_TABLE")
# users_table_name = Variable.get("USERS_TABLE")
# time_table_name = Variable.get("TIME_TABLE")


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

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    target_table=Table.STAGING_EVENTS.name,
    create_table_query=Table.STAGING_EVENTS.get_query('create'),
    s3_bucket=events_bucket_name,
    s3_key=events_key,
    json_path=events_jsonpath,
    use_partitioned_data=events_partitioned,
    execution_date="{{ ds }}"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)
