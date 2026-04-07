from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import * # col, from_json, split, when, avg
from pyspark.sql.types import * # StructType, StructField
import os
from datetime import datetime, date
spark = SparkSession.builder \
    .appName("Ingestion") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://10.140.0.4:9001") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "12345678") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()



# 1. Read the data
df = spark.read.parquet("hdfs://namenode:9000/data/yellow_tripdata_2025.parquet")
df_lookup = spark.read.option('inferSchema', True).option('header', True).csv("hdfs://namenode:9000/data/taxi_zone_lookup.csv")

print("### Collected raw data")

bronze_taxi_path = "s3a://bronze/default/yellow_taxi.parquet"
bronze_lookup_path = "s3a://bronze/default/yellow_taxi_lookup.csv"

print("### Writing to bronze")


df.write \
    .mode("overwrite") \
    .parquet(bronze_taxi_path)


df_lookup.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(bronze_lookup_path)
