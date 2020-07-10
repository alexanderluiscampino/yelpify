from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, month, dayofmonth, year
from pyspark.sql.types import StringType, IntegerType, TimestampType, DateType
from pyspark.sql.functions import udf

import logging
logger = logging.getLogger()


class User(SparkDF):

    def __init__(self, dataset_uris_dict: dict):
        super().__init__(dataset_uris_dict[self.name])

    @property
    def name(self):
        return 'user'

    def get_partitions(self):
        return ['pyear', 'pmonth', 'pday']

    def process(self):
        self.subset_df([
            'friends',
                       'compliment_cool',
                       'compliment_cute',
                       'compliment_funny',
                       'compliment_hot',
                       'compliment_list',
                       'compliment_more',
                       'compliment_note',
                       'compliment_photos',
                       'compliment_plain',
                       'compliment_profile',
                       'compliment_writer',
                       'cool',
                       'elite',
                       'fans',
                       'funny',
                       'useful'], option='drop')

    def apply_partitioning(self):
        self.df = (self.df.
                   select(
                       '*',
                       to_timestamp(
                           col('yelping_since'), 'yyyy-MM-dd HH:mm:ss').alias('yelping_since_dt')
                   )
                   )
        self.df = (self.df
                   .withColumn("pmonth", month("yelping_since_dt"))
                   .withColumn("pyear", year("yelping_since_dt"))
                   .withColumn("pday", dayofmonth("yelping_since_dt"))
                   .select('*')
                   )
        self.subset_df(['yelping_since_dt'], option='drop')

    def write_to_s3(self, s3_path: str, partitioned: bool = False):
        if partitioned:
            partitions = self.get_partitions()
        else:
            partitions = []

        s3_path = f"{s3_path}/{self.name}"
        self._write_to_parquet(s3_path, partitions=partitions)
