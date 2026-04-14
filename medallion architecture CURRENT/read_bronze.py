from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import * # col, from_json, split, when, avg
from pyspark.sql.types import * # StructType, StructField
import os
from datetime import datetime, date
import configparser

config = configparser.ConfigParser()
config.read("/home/ubuntu/scripts/config.ini")

# MinIO configs
endpoint = config["minio"]["endpoint"]
access_key = config["minio"]["access_key"]
secret_key = config["minio"]["secret_key"]

spark = SparkSession.builder \
    .appName("ReadBronze") \
    .config("spark.hadoop.fs.s3a.endpoint", endpoint) \
    .config("spark.hadoop.fs.s3a.access.key", access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()




bronze_taxi_path = "s3a://bronze/raw/yellow_taxi.parquet"
bronze_lookup_path = "s3a://bronze/raw/yellow_taxi_lookup.csv"

# Read the Parquet data
df = spark.read.parquet(bronze_taxi_path)

# Read the CSV data (ensure you keep the header and schema inference)
df_lookup = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(bronze_lookup_path)
