from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import * # col, from_json, split, when, avg
from pyspark.sql.types import * # StructType, StructField
import os
from datetime import datetime, date
spark = SparkSession.builder \
    .appName("BronzeLayer") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://10.140.0.4:9001") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "12345678") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()


# temporarily using s3a storage instead of wget
df = spark.read.parquet("hdfs://namenode:9000/data/bronze/")
#df = spark.read.parquet("s3a://bronze/yellow_tripdata_2025.parquet")
#df = spark.read.parquet("hdfs://instance-20260312-012701:9000/data/bronze/")
# df = spark.read.parquet('../test_folder/yellow_tripdata_2025-01.parquet',
#                         '../test_folder/yellow_tripdata_2025-02.parquet',
#                         '../test_folder/yellow_tripdata_2025-03.parquet',
#                         '../test_folder/yellow_tripdata_2025-04.parquet',
#                         '../test_folder/yellow_tripdata_2025-05.parquet',
#                         '../test_folder/yellow_tripdata_2025-06.parquet',
#                         '../test_folder/yellow_tripdata_2025-07.parquet',
#                         '../test_folder/yellow_tripdata_2025-08.parquet',
#                         '../test_folder/yellow_tripdata_2025-10.parquet',
#                         '../test_folder/yellow_tripdata_2025-11.parquet')
#df_parquet = spark.read.parquet("hdfs://instance-20260312-012701:9000/data/yellow_tripdata_2025-01.parquet")
#df_parquet = spark.read.parquet("../test_folder/yellow_tripdata_2025-01.parquet")
print("### Collected raw data")


print("### Writing to bronze")
df.write.mode("overwrite").parquet("s3a://bronze/yellow_tripdata_2025.parquet")