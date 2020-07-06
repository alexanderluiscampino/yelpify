import os
import logging
import configparser
import s3fs


from lib.spark_util import create_spark_session
from lib.s3_util import create_bucket
from src.business import Business
from src.review import Review
from src.tip import Tip
from src.user import User

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


setup_aws_env()
s3 = s3fs.S3FileSystem(anon=False)


def load_config_file(filepath: str):
    config = configparser.ConfigParser()
    config.read(filepath)
    return config


def setup_aws_env():
    config = load_config_file('./aws-config.cfg')
    os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']
    os.environ['AWS_DEFAULT_REGION'] = config['AWS']['AWS_DEFAULT_REGION']


def get_raw_data_location(bucket_name: str,  dry_run: bool = True):
    """
    Gets raw data location, depending on the dry_run parameter
    Returns just a small amount of data in case of dry run, used
    in testing setting


    Returns:
        {dict} -- Raw data paths
    """

    root_path = 'raw' if not dry_run else 'raw-test'

    dataset_uris_dict = {}
    for entry in s3.ls(f"{bucket_name}/{root_path}"):
        dataset_uris_dict[Path(entry).stem.split('_')[-1]] = f"s3://{entry}"
    dataset_uris_dict

    return dataset_uris_dict


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


def run_yelpify_etl(bucket_name: str, dataset_uris_dict: dict):
    """
    Run complete yelpify ETL pre-processing the data to S3 data lake
    Data is written to S3 in the Parquet format, partioned by key parameters for performance

    Arguments:
        bucket_name {str} -- Bucket Name
        dataset_uris_dict {str} -- Location of raw data in S3
    """
    processed_uri = f's3://{bucket_name}/processed'
    data_lake_uri = f's3://{bucket_name}/data-lake'

    for Handler in (Business, Review, Tip, User):
        dataset_handler = Handler(dataset_uris_dict)
        logger.info(f"Processing dataset: {dataset_handler.name}")
        dataset_handler.process()
        logger.info(f"Writing to S3 (non-partitioned): {dataset_handler.name}")
        dataset_handler.write_to_s3(processed_uri, partitioned=False)
        logger.info(f"Applying partition columns to: {dataset_handler.name}")
        dataset_handler.apply_partitioning()
        logger.info(f"Writing to S3 (partitioned): {dataset_handler.name}")
        dataset_handler.write_to_s3(data_lake_uri, partitioned=True)
        logger.info(f"Done processing: {dataset_handler.name}")


if __name__ == "__main__":
    APP = 'yelpify'
    TEST = True
    bucket_name = 'yelp-customer-reviews'
    dataset_uris_dict = get_raw_data_location(bucket_name, dry_run=TEST)
    run_yelpify_etl(bucket_name, dataset_uris_dict)
