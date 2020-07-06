import logging

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    TimestampType
)
from pyspark.sql.functions import col
from lib.spark_util import DerivativeDF, RawDF

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def process_song_data(spark, s3_raw_data_path: str, output_bucket_name: str):
    """
    Handles the creation of the central dimensional table based on the raw song data

    Tables created:
        -- Song table
        -- Artist table

    Arguments:
        spark {SparkSession} -- Spark Session
        s3_raw_data_path {str} -- S3 path of raw songs data JSON files
        output_bucket_name {str} -- Output bucket name
    """

    def get_song_schema():
        """
        Returns song spark dataframe schema with correct data types

        Returns:
            {StructType} -- Song schema
        """
        return StructType([
            StructField("artist_id", StringType(), False),
            StructField("artist_latitude", StringType(), True),
            StructField("artist_longitude", StringType(), True),
            StructField("artist_location", StringType(), True),
            StructField("artist_name", StringType(), False),
            StructField("song_id", StringType(), False),
            StructField("title", StringType(), False),
            StructField("duration", DoubleType(), False),
            StructField("year", IntegerType(), False)
        ])

    def create_song_table(song_raw, output_bucket_name: str):
        """
        Create sogs pyspark dataframe

        Arguments:
            log_raw {DerivativeDF} -- Log helping class for pyspak dtaframes
            output_bucket_name {str} -- Output in S3 location

        Returns:
            {DerivativeDF} -- songs derivate dataframe
        """
        songs = DerivativeDF(song_raw.df.select(
            "song_id", "title", "artist_id", "year", "duration"))
        songs._write_to_parquet(
            s3_output_path=f"s3://{output_bucket_name}/songs",
            partitions=["year", "artist_id"]
        )

        return songs

    def create_artists_table(song_raw, output_bucket_name: str):
        """
        Create artists pyspark dataframe

        Arguments:
            log_raw {DerivativeDF} -- Log helping class for pyspak dtaframes
            output_bucket_name {str} -- Output in S3 location

        Returns:
            {DerivativeDF} -- artists derivate dataframe
        """
        artists = DerivativeDF(song_raw.df
                               .select(
                                   "artist_id",
                                   col("artist_name").alias("name"),
                                   col("artist_location").alias("location"),
                                   col("artist_latitude").alias("latitude"),
                                   col("artist_longitude").alias("longitude"))
                               .distinct()
                               )
        artists._write_to_parquet(
            s3_output_path=f"s3://{output_bucket_name}/artists",
            partitions=["artist_id"]
        )

        return artists

    logger.info(f"Reading and Processing `{s3_raw_data_path}`")
    song_raw = RawDF(spark, s3_raw_data_path, get_song_schema())
    logger.info("Processing and writting `songs` data")
    songs = create_song_table(song_raw, output_bucket_name)
    logger.info("Processing and writting `artists` data")
    artists = create_artists_table(song_raw, output_bucket_name)

    return songs, artists
