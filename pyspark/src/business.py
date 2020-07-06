from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, month, dayofmonth, year
from pyspark.sql.types import StringType, IntegerType, TimestampType, DateType
from pyspark.sql.functions import udf

import logging
logger = logging.getLogger()


class Business(SparkDF):

    def __init__(self, dataset_uris_dict: dict):
        super().__init__(dataset_uris_dict[self.name])

    @property
    def name(self):
        return 'business'

    def get_partitions(self):
        return ['pstate', 'pcity']

    def process(self):
        columns_to_keep = [
            'business_id',
            'name',
            'categories',
            'state',
            'city',
            'address',
            'postal_code',
            'review_count',
            'stars'
        ]
        self.subset_df(columns_to_keep, option='keep')

    def apply_partitioning(self):
        self.df = (self.df
                   .select('*',
                           col("state").alias("pstate"),
                           col("city").alias("pcity")
                           )
                   )

    def write_to_s3(self, s3_path: str, partitioned: bool = False):
        if partitioned:
            partitions = self.get_partitions()
        else:
            partitions = []

        s3_path = f"{s3_path}/{self.name}"
        self._write_to_parquet(s3_path, partitions=partitions)
