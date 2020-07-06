from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, month, dayofmonth, year
from pyspark.sql.types import StringType, IntegerType, TimestampType, DateType
from pyspark.sql.functions import udf

import logging
logger = logging.getLogger()


class Tip(SparkDF):

    def __init__(self, dataset_uris_dict: dict):
        super().__init__(dataset_uris_dict[self.name])

    @property
    def name(self):
        return 'tip'

    def get_partitions(self):
        return ['pyear', 'pmonth', 'pday']

    def process(self):
        self.df = (self.df.
                   select(
                       '*',
                       to_timestamp(
                           col('date'), 'yyyy-MM-dd HH:mm:ss').alias('dt')
                   )
                   )
        self.subset_df(['date'], option='drop')

    def apply_partitioning(self):
        self.df = (self.df
                   .withColumn("pmonth", month("dt"))
                   .withColumn("pyear", year("dt"))
                   .withColumn("pday", dayofmonth("dt"))
                   .select('*')
                   )

    def write_to_s3(self, s3_path: str, partitioned: bool = False):
        if partitioned:
            partitions = self.get_partitions()
        else:
            partitions = []

        s3_path = f"{s3_path}/{self.name}"
        self._write_to_parquet(s3_path, partitions=partitions)
