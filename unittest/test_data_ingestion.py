'''
Test Data Ingestion:
'''

import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def test_local_ingestion():
    assert os.path.exists("c:/sb/strfacts/cleandata")

    # define schema for reviews.csv.gz
    reviews_schema = StructType([
        StructField('location', StringType(), True),
        StructField('listing_id', StringType(), True),
        #StructField('id',         IntegerType(),True),
        StructField('date',       StringType(),True),
        #StructField('reviewer_id', IntegerType(),True),
        StructField('reviewer_name', StringType(),True),
        StructField('comments', StringType(), True)])

    spark = SparkSession.builder.master('local[*]')\
            .appName('test_data_ingestion').getOrCreate()
    # Read reviews parquet file into data frame reviews_df
    reviews_df = spark.read.format("parquet")\
            .schema(reviews_schema)\
            .load("c:/sb/strfacts/cleandata")
    assert reviews_df.count() > 0


def test_azure_ingestion():
    assert os.path.exists("/dbfs/mnt/blob1/cleandata")

    # define schema for reviews.csv.gz
    reviews_schema = StructType([
        StructField('location', StringType(), True),
        StructField('listing_id', StringType(), True),
        #StructField('id',         IntegerType(),True),
        StructField('date',       StringType(),True),
        #StructField('reviewer_id', IntegerType(),True),
        StructField('reviewer_name', StringType(),True),
        StructField('comments', StringType(), True)])

    # Read reviews parquet file into data frame reviews_df
    reviews_df = spark.read.format("parquet")\
            .schema(reviews_schema)\
            .load("/dbfs/mnt/blob1/cleandata")
    assert reviews_df.count() > 0
