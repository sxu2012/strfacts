'''
Test Data Analytics:
'''

import os
from pyspark.sql import SparkSession


def test_local_analytics():

    spark = SparkSession.builder.master('local[*]')\
            .appName('test_data_analytics').getOrCreate()
    # Read reviews parquet file into data frame reviews_df
    reviews_df = spark.read.format("parquet")\
            .load("c:/sb/strfacts/analyticsdata")
    assert reviews_df.count() > 0

def test_azure_analytics():

    # Read reviews parquet file into data frame reviews_df
    reviews_df = spark.read.format("parquet")\
            .load("/mnt/blob1/analyticsdata")
    assert reviews_df.count() > 0
