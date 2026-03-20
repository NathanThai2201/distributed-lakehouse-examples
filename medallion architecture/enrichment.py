from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import * # col, from_json, split, when, avg
from pyspark.sql.types import * # StructType, StructField
import os
from datetime import datetime, date
spark = SparkSession.builder.appName("demo").getOrCreate()





df_parquet = spark.read.parquet("./test_folder/yellow_tripdata_2025-01.parquet")

#df_parquet = spark.read.parquet("hdfs://instance-20260312-012701:9000/data/yellow_tripdata_2025-01.parquet")
df_parquet.printSchema()
df_parquet.limit(15).show()


df = df_parquet.withColumn(
    "trip_duration_minutes",
    expr("timestampdiff(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime)")
)

df.show()