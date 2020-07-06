import logging
import os
from pathlib import Path
from shutil import rmtree

from lib.s3_util import move_directory_to_s3, s3
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, dayofmonth, month, to_timestamp, udf,
                                   year)
from pyspark.sql.types import DateType, IntegerType, StringType, TimestampType

logger = logging.getLogger()


class SparkDF(object):
    """
    Utility class to handle common operation related to Spark Dataframes
    """

    def __init__(self, filepath: str):
        self.spark = self.create_spark_session()
        self.df = self._load_json_data(filepath)

    def create_spark_session(self):
        """Create a Spark session"""
        os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:2.7.2 pyspark-shell'

        spark = SparkSession \
            .builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
            .getOrCreate()
        return spark

    def _load_json_data(self, filepath: str):
        """
        Load JSON data from S3 to a Dataframe

        Returns:
            Spark Dataframe -- Spark dataframe with contents of JSON files
        """
        try:
            logger.info(f"Loading file: {filepath}")
            return self.spark.read.json(filepath)
        except Exception as e:
            if "No FileSystem for scheme: s3" in str(e):
                logger.warning("Switching to slow S3a loading method")
                filepath = filepath.replace("s3://", "s3a://")
                return self.spark.read.json(filepath)
            else:
                raise e

        return

    def subset_df(self, columns: list, option: str):
        if option == 'keep':
            self.df = self.df.select(*columns)
        elif option == 'drop':
            self.df = self.df.drop(*columns)

    def _write_to_parquet(self, s3_output_path: str, mode: str = 'overwrite', partitions: list = []):
        """
        Writes Spark Dataframe to S3 in the Parquet Format

        Arguments:
            s3_output_path {str} -- Output path in S3

        Keyword Arguments:
            mode {str} -- Writing mode (default: {'overwrite'})
            partitions {list} -- List of field to partition the data by (default: {[]})

        Raises:
            e: Raises any error thrown by the write.parqet method from the Spark dataframe
        """
        local_temp_dir = Path('./temp')
        os.makedirs(local_temp_dir, exist_ok=True)
        bucket_name, root_prefix, _ = s3.split_path(s3_output_path)

        try:
            logger.info(s3_output_path)
            self.df.write.parquet(
                str(local_temp_dir),
                mode=mode,
                partitionBy=partitions
            )
            move_directory_to_s3(
                local_temp_dir, bucket_name, root_prefix, 'parquet')
            rmtree(local_temp_dir)
        except Exception as e:
            if "No FileSystem for scheme: s3" in str(e):
                logger.warning("Switching to slow S3 output method")
                s3_output_path = s3_output_path.replace("s3://", "s3a://")
                self.df.write.parquet(
                    s3_output_path,
                    mode=mode,
                    partitionBy=partitions
                )
            else:
                raise e
