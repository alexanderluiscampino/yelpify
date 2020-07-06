import logging
import boto3
import os
from botocore.exceptions import ClientError
import s3fs
from pathlib import Path

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = s3fs.S3FileSystem(anon=False)



def move_directory_to_s3(local_directory: str, bucket_name: str, root_prefix: str, filetype: str):
    path = Path(local_directory)

    for index, entry in enumerate(path.rglob(f'*.{filetype}')):
        local_path = str(entry)
        object_key = local_path.replace(str(path), root_prefix)
        s3_uri = f's3://{bucket_name}/{object_key}'
        print(s3_uri)
        s3.put(str(entry), s3_uri)


def create_bucket(bucket_name: str, region: str = os.getenv('AWS_DEFAULT_REGION'), acl="private"):
    """
    Creates a bucket name in the specified region

    Boto3 will make use of the set environment variables or assumed role.

    Args:
        bucket_name {str} -- Bucket to create
        region {str} -- String region to create bucket in, defaults to AWS_DEFAULT_REGION in env variable
        acl {str} -- Canned access control list to apply to the bucket. 'public-read'
            makes sure everything posted is publicly readable
    Returns:
        {str} -- bucket_name
    """

    assert region, "No region was defined"

    # Create bucket
    try:
        s3_client = boto3.client('s3', region_name=region)
        location = {'LocationConstraint': region}
        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration=location
        )

        logger.info(f"Bucket `{bucket_name}` Created")
    except ClientError as e:
        logging.exception("Could not create bucket")
        raise e

    return bucket_name
