#!/usr/bin/env python
# coding: utf-8

## Converted from: PySpark Jupyter Notebook on Azure Databricks
## Extract the top most frequent used words in reviews for each listing

'''
Data Analytics:
    Extract the top most frequent used words in reviews for each property

    Input: Files from Azure Blob or local file system in csv zipped format
    Output: Cleaned and transformed in parquet format with partition
'''

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.functions as F
from pyspark.sql.window import Window

def analytics(datapath):

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
            .load(datapath+'cleandata')

    # reviews_df.show(5)

    # group by list_id, concat all comments
    reviews_df.createOrReplaceTempView("reviews")
    query = """
        SELECT listing_id, CONCAT_WS(' ', collect_list(comments)) as all_comments, first(location) as area
        FROM reviews
        GROUP BY listing_id
        """
    tmpdf = spark.sql(query)
    #tmpdf.show(2)

    #regex remove all nonalphanumeric characters, split by " ",explode words
    df1 = tmpdf.select(tmpdf.listing_id,tmpdf.area,\
            F.explode(\
            F.split(\
            F.regexp_replace(tmpdf.all_comments,"[^a-zA-Z0-9 -]","")," "))\
            .alias("words"))

    #df1.show(10)

    # count words
    df2 = df1.groupBy(df1.listing_id, df1.area, df1.words)\
            .agg(F.count(df1.words).alias("cnt"))

    #df2.show(5)
    # create row_number within each window partition, order by cnt
    windowSpec  = Window.partitionBy(df2.listing_id).orderBy(df2.cnt)
    df3 = df2.withColumn("row_number",F.row_number().over(windowSpec))
    #df3.show(3)

    # Find the top 5 most frequent used words in the comments
    df4 = df3.select("*").where(df3.row_number<=5)\
            .select(df3.listing_id, df3.area, df3.words)\
            .groupBy(df3.listing_id, df3.area)\
            .agg(F.collect_list(df3.words)\
            .alias("frequentw"))\
            .select(df3.listing_id, df3.area,\
            F.array_join("frequentw", ",")\
            .alias("most_frequent_used_words"))

    #df4.show(10)

    # write the analytics data to parquet file with patition
    analyticsdata = datapath+'analyticsdata'
    df4.write.partitionBy("area").mode("append").parquet(analyticsdata)
    return

# main entry point
if __name__=="__main__":
    if len(sys.argv) >= 2:
        # for running in local Windows 11
        # usage: data_analytics.py local
        spark = SparkSession.builder.master('local[*]').appName('data_ingestion').getOrCreate()
        analytics("c:/sb/strfacts/")
    else:
        # default to run on Azure Databricks
        # mount_blob()
        analytics("/mnt/blob1/")
