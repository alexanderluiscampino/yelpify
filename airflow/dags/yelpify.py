from enum import Enum, unique
import boto3
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


@unique
class Table(Enum):
    ARTISTS = 'artists'
    SONGPLAYS = 'songplays'
    SONGS = 'songs'
    STAGING_EVENTS = 'staging_events'
    STAGING_SONGS = 'staging_songs'
    TIME = 'time'
    USERS = 'users'

    def get_query(self, query_type: str):
        return getattr(SqlQueries, f"{self.value}_table_{query_type}", None)


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
          description='Load and transform data in Redshift with Airflow',
          schedule_interval=None  # '0 * * * *'
          )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    target_table=Table.STAGING_EVENTS.value,
    create_table_query=Table.STAGING_EVENTS.get_query('create'),
    s3_path=Variable.get("EVENTS_DATA_PATH"),
    json_path=Variable.get("EVENTS_JSONPATH", default_var=None),
    use_partitioned_data=Variable.get(
        "EVENTS_DATA_PARTITIONED", default_var=False),
    provide_context=False
)


stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    target_table=Table.STAGING_SONGS.value,
    create_table_query=Table.STAGING_SONGS.get_query('create'),
    s3_path=Variable.get("SONGS_DATA_PATH"),
    json_path=Variable.get("SONGS_JSONPATH", default_var=None),
    use_partitioned_data=Variable.get(
        "SONGS_DATA_PARTITIONED", default_var=False),
    provide_context=False
)


fact_table_mode = True if Variable.get(
    "INSERT_MODE_FACT", default_var=False) == "append" else False
dimensional_table_mode = True if Variable.get(
    "INSERT_MODE_DIM", default_var=False) == "append" else False


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    target_table=Table.SONGPLAYS.value,
    create_table_query=Table.SONGPLAYS.get_query('create'),
    insert_table_query=Table.SONGPLAYS.get_query('insert'),
    append_only=fact_table_mode,
    provide_context=False
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    target_table=Table.USERS.value,
    create_table_query=Table.USERS.get_query('create'),
    insert_table_query=Table.USERS.get_query('insert'),
    append_only=fact_table_mode,
    provide_context=False

)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    target_table=Table.SONGS.value,
    create_table_query=Table.SONGS.get_query('create'),
    insert_table_query=Table.SONGS.get_query('insert'),
    append_only=fact_table_mode,
    provide_context=False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    target_table=Table.ARTISTS.value,
    create_table_query=Table.ARTISTS.get_query('create'),
    insert_table_query=Table.ARTISTS.get_query('insert'),
    append_only=fact_table_mode,
    provide_context=False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    target_table=Table.TIME.value,
    create_table_query=Table.TIME.get_query('create'),
    insert_table_query=Table.TIME.get_query('insert'),
    append_only=fact_table_mode,
    provide_context=False
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    list_of_tables=[entry.value for entry in Table]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# Task dependencies
start_operator >> stage_events_to_redshift >> load_songplays_table
start_operator >> stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
