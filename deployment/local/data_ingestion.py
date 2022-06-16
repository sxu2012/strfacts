#!/usr/bin/env python
# coding: utf-8

## Converted from: PySpark Jupyter Notebook on Azure Databricks
## Read data from storage, Clean / Transform the data, then persist data
## in parquet format with partition

'''
Data Ingestion:
    Read data from storage, Clean / Transform the data, then persist data
    in parquet format with partition

    Input: Files from Azure Blob or local file system in csv zipped format
    Output: Cleaned and transformed in parquet format with partition
'''

import sys
import os
from pyspark.sql import SparkSession
import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType




#parse csv file
def parse_csv(line:str, location):
    f =line.split(',')
    if len(f) < 6:
        # "NONE" partition holds the invalid data
        #return ("NONE", None, None, None, None, None, None)
        return ("NONE", None, None, None, None)
    else:
        # join the comments together into one string
        comments = ''
        for words in f[5:]:
            comments = comments +' '+words
        return(location,f[0],f[2],f[4],comments)
#
# input: fs: '/dbfs' for Databricks; '' for local
#        datapath: '/mnt/blobs/' or 'c:/.../' 
#
def ingestion(fs, datapath):
    # Free data is available quaterly, but you can pay to get monthly data. 
    # For now we will use free data
    # Calculate date for the last quater data.
    today = datetime.datetime.now()
    if today.month in [1, 2, 3]:
        dirnames = ["{}-{}".format(today.year-1, i) for i in ['10','11','12']]
    elif today.month in [4, 5, 6]:
        dirnames = ["{}-{}".format(today.year, i) for i in ['01','02','03']]
    elif today.month in [7, 8, 9]:
        dirnames = ["{}-{}".format(today.year, i) for i in ['04','05','06']]
    else:
        dirnames = ["{}-{}".format(today.year, i) for i in ['07','08','09']]
    print(dirnames)

    ## Read all csv files of last quarter, folders in format yyyy-mm
    ## builds rdd
    reviews_rdd = None
    # for local spark-submit
    if fs == '':
        spark = SparkSession.builder.master('local[*]').appName('data_ingestion').getOrCreate()
    for dn in os.listdir(fs+datapath+'indata'):
        # read the files in this folder if dn is last quarter
        if dn in dirnames:
            # for this project, we are only interested in the reviews.csv.gz file
            filenames = os.listdir(fs+datapath+'indata'+'/'+dn)
            for fn in filenames:
                a = len(fn)
                b = len("reviews.csv.gz")
                # hawaii.reviews.csv.gz; location = hawaii
                if a > b and fn[a-b:] == "reviews.csv.gz":
                    location = fn[:a-b-1]
                    print(datapath+'indata'+'/'+dn+'/'+fn)
                    raw_rdd=spark.sparkContext.textFile(datapath+'indata'+'/'+dn+'/'+fn)
                    header = raw_rdd.first() #get the first row to a variable
                    #remove the header, then clean the rest rows with parse_csv()
                    clean_rdd = raw_rdd.filter(lambda row:row != header).map(lambda row: parse_csv(row, location))
                    if reviews_rdd is None:
                        reviews_rdd = clean_rdd
                    else:
                        union_rdd = reviews_rdd.union(clean_rdd)
                        reviews_rdd = union_rdd

    # define schema for reviews.csv.gz
    reviews_schema = StructType([
        StructField('location', StringType(), True),
        StructField('listing_id', StringType(), True),
        #StructField('id',         IntegerType(),True),
        StructField('date',       StringType(),True),
        #StructField('reviewer_id', IntegerType(),True),
        StructField('reviewer_name', StringType(),True),
        StructField('comments', StringType(), True)])

    # create data frame from RDD with schema
    reviews_df = spark.createDataFrame(reviews_rdd, schema=reviews_schema)

    # Append to the parquet files with place name as partition
    cleandata = datapath+'cleandata'
    reviews_df.write.partitionBy("location").mode("append").parquet(cleandata)

# main entry point
if __name__=="__main__":
    if len(sys.argv) >= 2:
        # for running in local Windows 11
        # data_ingestion.py local
        ingestion("", "c:/sb/strfacts/")
    else:
        # default to run on Azure Databricks
        # mount_blob()
        ingestion("/dbfs", "/mnt/blob1/")
