from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import * # col, from_json, split, when, avg
from pyspark.sql.types import * # StructType, StructField
import os
from datetime import datetime, date
spark = SparkSession.builder \
    .appName("demo") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()


# temporarily using s3a storage instead of wget
df = spark.read.parquet("hdfs://instance-20260312-012701:9000/data/bronze/")
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