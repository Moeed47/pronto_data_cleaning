#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb  2 05:11:29 2022

@author: moeed
"""

from google.cloud import storage

from pyspark.sql.types import StructField, StructType, StringType, LongType
from pyspark.sql import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import isnan, when, count, col
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from datetime import date
from datetime import datetime
from pyspark.sql.functions import year
from pyspark.sql.functions import month
from pyspark.sql.functions import dayofmonth
from pyspark.sql.functions import regexp_replace



spark = SparkSession.builder.appName("pronto-pyspark").getOrCreate()
chicago_taxi_df = (spark.read.format('bigquery').option('table', "bigquery-public-data.chicago_taxi_trips.taxi_trips").load())


chicago_taxi_df = chicago_taxi_df.na.fill("Unspecified",subset=["company"])


# Drop Null values from the selected columns
chicago_taxi_df = chicago_taxi_df.na.drop(how="any")
#chicago_taxi_df = chicago_taxi_df.na.drop(subset=["trip_end_timestamp","trip_seconds","trip_miles","fare","tips","pickup_latitude","pickup_longitude","pickup_location","dropoff_latitude","dropoff_longitude","dropoff_location"])

#Add date columns from timestamp
chicago_taxi_df = chicago_taxi_df.withColumn('trip_start_date', chicago_taxi_df['trip_start_timestamp'].cast('date')) \
    .withColumn('trip_end_date', chicago_taxi_df['trip_end_timestamp'].cast('date'))

#Add Extract Day, Month and Year from start timestamp column
chicago_taxi_df = chicago_taxi_df.withColumn("trip_start_day", dayofmonth(chicago_taxi_df["trip_start_date"])) \
    .withColumn("trip_start_month", month(chicago_taxi_df["trip_start_date"])).withColumn("trip_start_year", year(chicago_taxi_df["trip_start_date"]))


#Add Extract Day, Month and Year from End timestamp column
chicago_taxi_df = chicago_taxi_df.withColumn("trip_end_day", dayofmonth(chicago_taxi_df["trip_end_date"])) \
    .withColumn("trip_end_month", month(chicago_taxi_df["trip_end_date"])).withColumn("trip_end_year", year(chicago_taxi_df["trip_end_date"]))

# Adding additional conditions
chicago_taxi_df=chicago_taxi_df.where(chicago_taxi_df.trip_seconds>10)
chicago_taxi_df=chicago_taxi_df.where(chicago_taxi_df.trip_miles>0)



#splitting dataframe for testing and training
train_chicago_taxi_df, test_chicago_taxi_df = chicago_taxi_df.randomSplit(weights=[0.8,0.2])

#Writing complete dataframe and train and text dataframe to parquet
chicago_taxi_df.write.parquet("gs://pronto-bucket/pronto-chicago-taxi_trips/full_cleaned_chicago_taxi_trips.parquet",mode='overwrite')
train_chicago_taxi_df.write.parquet("gs://pronto-bucket/pronto-chicago-taxi_trips/train_chicago_taxi_trips.parquet",mode='overwrite')
test_chicago_taxi_df.write.parquet("gs://pronto-bucket/pronto-chicago-taxi_trips/test_chicago_taxi_trips.parquet",mode='overwrite')

