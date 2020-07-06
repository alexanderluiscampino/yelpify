import logging
from datetime import datetime

from pyspark.sql import Window
from pyspark.sql.functions import (udf,
                                   col,
                                   year,
                                   month,
                                   dayofmonth,
                                   hour,
                                   weekofyear,
                                   date_format,
                                   dayofweek,
                                   max,
                                   monotonically_increasing_id
                                   )

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    TimestampType
)

from lib.spark_util import DerivativeDF, RawDF


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def process_log_data(spark, s3_raw_data_path: str, output_bucket_name: str, songs: DerivativeDF, artists: DerivativeDF):
    """
    Processes log data creating the dimnensionl tables associated with it

    It will utilize the Songs and Artists Derivative DF to access their data in the form of a Spark DF
    and join with the log data to create the necessary dimensional tables.

    The output of this fuction is the following 3 tables, in S3, in the parquet format:
        - Users Table
        - Time Table
        - SongsPlay Table

    Arguments:
        spark {SparkSession} -- Spark Session
        s3_raw_data_path {str} -- Location of raw log data in S3
        output_bucket_name {str} -- Output bucket for processed data
        songs {DerivativeDF} -- Songs derivative DF
        artists {DerivativeDF} -- Artists derivative DF

    Returns:
        {DerivativeDF} -- users, time, songsplay
    """

    def get_log_schema():
        """
        Returns log spark dataframe schema with correct data types

        Returns:
            {StructType} -- Log schema
        """
        return StructType([
            StructField("artist", StringType(), True),
            StructField("auth", StringType(), False),
            StructField("firstName", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("itemInSession", IntegerType(), False),
            StructField("lastName", StringType(), True),
            StructField("length", DoubleType(), True),
            StructField("level", StringType(), False),
            StructField("location", StringType(), True),
            StructField("method", StringType(), False),
            StructField("page", StringType(), False),
            StructField("registration", DoubleType(), True),
            StructField("sessionId", IntegerType(), False),
            StructField("song", StringType(), True),
            StructField("status", IntegerType(), False),
            StructField("ts", DoubleType(), False),
            StructField("userAgent", StringType(), True),
            StructField("userId", StringType(), True)
        ])

    get_datetime = udf(
        lambda x: datetime.fromtimestamp(x / 1000).replace(microsecond=0),
        TimestampType()
    )

    def create_users_table(log_raw, output_bucket_name: str):
        """
        Create users pyspark dataframe

        Arguments:
            log_raw {DerivativeDF} -- Log helping class for pyspak dtaframes
            output_bucket_name {str} -- Output in S3 location

        Returns:
            {DerivativeDF} -- users derivate dataframe
        """
        users = DerivativeDF(log_raw.df
                             .withColumn("max_ts_user", max("ts").over(Window.partitionBy("userID")))
                             .filter(
                                 (col("ts") == col("max_ts_user")) &
                                 (col("userID") != "") &
                                 (col("userID").isNotNull())
                             )
                             .select(
                                 col("userID").alias("user_id"),
                                 col("firstName").alias("first_name"),
                                 col("lastName").alias("last_name"),
                                 "gender",
                                 "level"
                             )
                             )
        users._write_to_parquet(
            s3_output_path=f"s3://{output_bucket_name}/users",
            partitions=["level"]
        )

        return users

    def create_time_table(log_raw, output_bucket_name: str):
        """
        Create time pyspark dataframe

        Arguments:
            log_raw {DerivativeDF} -- Log helping class for pyspak dtaframes
            output_bucket_name {str} -- Output in S3 location

        Returns:
            {DerivativeDF} -- time derivate dataframe
        """
        time = DerivativeDF(log_raw.df
                            .withColumn("hour", hour("start_time"))
                            .withColumn("day", dayofmonth("start_time"))
                            .withColumn("week", weekofyear("start_time"))
                            .withColumn("month", month("start_time"))
                            .withColumn("year", year("start_time"))
                            .withColumn("weekday", dayofweek("start_time"))
                            .select("start_time", "hour", "day", "week", "month", "year", "weekday")
                            .distinct()
                            )

        time._write_to_parquet(
            s3_output_path=f"s3://{output_bucket_name}/time",
            partitions=["year", "month"]
        )

        return time

    def create_songsplay_table(log_raw, songs, artists, output_bucket_name: str):
        """
        Create sogsplay pyspark dataframe

        Arguments:
            log_raw {DerivativeDF} -- Log helping class for pyspak dtaframes
            output_bucket_name {str} -- Output in S3 location

        Returns:
            {DerivativeDF} -- songsplay derivate dataframe
        """
        songs_temp = (
            songs.df
            .join(artists.df, "artist_id", "full")
            .select("song_id", "title", "artist_id", "name", "duration")
        )

        songplays_temp = log_raw.df.join(
            songs_temp,
            [
                log_raw.df.song == songs_temp.title,
                log_raw.df.artist == songs_temp.name,
                log_raw.df.length == songs_temp.duration
            ],
            "left"
        )

        songplays = DerivativeDF(songplays_temp
                                 .join(time.df, "start_time", "left")
                                 .select(
                                     "start_time",
                                     col("userId").alias("user_id"),
                                     "level",
                                     "song_id",
                                     "artist_id",
                                     col("sessionId").alias("session_id"),
                                     "location",
                                     col("userAgent").alias("user_agent"),
                                     "year",
                                     "month"
                                 )
                                 .withColumn("songplay_id", monotonically_increasing_id())
                                 )

        songplays._write_to_parquet(
            s3_output_path=f"s3://{output_bucket_name}/songplays",
            partitions=["year", "month"]
        )

        return songplays

    logger.info(f"Reading and Processing `{s3_raw_data_path}`")
    log_raw = RawDF(spark, s3_raw_data_path, get_log_schema())
    log_raw.df = log_raw.df.filter(col("page") == "NextSong")
    log_raw.df = log_raw.df.withColumn("start_time", get_datetime("ts"))

    logger.info("Processing and writting `users` data")
    users = create_users_table(log_raw, output_bucket_name)
    logger.info("Processing and writting `time` data")
    time = create_time_table(log_raw, output_bucket_name)
    logger.info("Processing and writting `songplays` data")
    songplays = create_songsplay_table(
        log_raw, songs, artists, output_bucket_name)

    return users, time, songplays
