import os
import logging
import configparser


from lib.spark_util import create_spark_session
from lib.s3_util import create_bucket
from src.song import process_song_data
from src.log import process_log_data

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


def load_config_file(filepath: str):
    config = configparser.ConfigParser()
    config.read(filepath)
    return config


def setup_aws_env():
    config = load_config_file('./aws-config.cfg')
    os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']
    os.environ['AWS_DEFAULT_REGION'] = config['AWS']['AWS_DEFAULT_REGION']


def get_raw_data_location(dry_run: bool = False):
    """
    Gets raw data location, depending on the dry_run parameter
    Returns just a small amount of data in case of dry run, used
    in testing setting

    Keyword Arguments:
        dry_run {bool} -- Users choice of it is testing or no (default: {False})

    Returns:
        {tuple} -- Raw data paths
    """
    raw_data_bucket_name = "udacity-dend"

    if not dry_run:
        song_data_path = f"s3a://{raw_data_bucket_name}/song_data/*/*/*"
        log_data_path = f"s3a://{raw_data_bucket_name}/log_data/*/*"
    else:
        song_data_path = f"s3a://{raw_data_bucket_name}/song_data/A/A/A"
        log_data_path = f"s3a://{raw_data_bucket_name}/log_data/2018/11"

    return song_data_path, log_data_path


def setup_output(output_bucket_name: str, bucket_exists: bool = True):
    """
    Setups output bucket, if it does not exist

    Arguments:
        output_bucket_name {str} -- Output bucket name

    Keyword Arguments:
        bucket_exists {bool} -- User choice if bucket exists or not (default: {True})
    """
    if not bucket_exists:
        logger.info(f"Creating Bucket: `{output_bucket_name}`")
        create_bucket(output_bucket_name)


def run_sparkify_etl(output_bucket_name: str, song_data_path: str, log_data_path: str):
    """
    Run complete Sparkify ETL processing the Raw Songs and Log data and transforming it
    to a Star Schema data model, with 4 Dimension tables and 1 main table
        Main Table:
            - Songs
        Dimensional Tables:
            - Artists
            - Users
            - Time
            - Songsplay
    Data is written to S3 in the Parquet format, partioned by key parameters for performance

    Arguments:
        output_bucket_name {str} -- Output Bucket Name
        song_data_path {str} --  S3 Path to Raw Song Data
        log_data_path {str} -- S3 Path to Raw Log Data
    """

    

    spark = create_spark_session()

    logger.info(f"Running Sparkigy ETL.\n \
                  Writting output to `{output_bucket_name}`"
                )
    logger.info("Processing Song Data")
    songs, artists = process_song_data(
        spark, song_data_path, output_bucket_name)
    logger.info("Processing Log Data")
    process_log_data(
        spark, log_data_path, output_bucket_name, songs, artists)
    logger.info("Sparkify ETL is completed")


if __name__ == "__main__":
    APP = 'sparkify'
    STAGE = 'dev'
    setup_aws_env()
    output_bucket_name = f'{APP}-{STAGE}'
    setup_output(output_bucket_name, bucket_exists=True)

    song_data_path, log_data_path = get_raw_data_location(dry_run=True)
    run_sparkify_etl(output_bucket_name, song_data_path, log_data_path)
