from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import * # col, from_json, split, when, avg
from pyspark.sql.types import * # StructType, StructField
import os
from datetime import datetime, date
spark = SparkSession.builder.appName("demo").getOrCreate()




df_parquet = spark.read.parquet("test_folder/yellow_tripdata_2025-01.parquet")
df_lookup = spark.read.option('inferSchema', True).option('header',True).csv("test_folder/taxi_zone_lookup.csv")
#df_parquet = spark.read.parquet("hdfs://namenode:9000/data/yellow_tripdata_2025.parquet")
df_parquet.printSchema()
df_parquet.limit(15).show()
df_lookup.printSchema()
df_lookup.limit(15).show()



# Alias lookup table twice
pu_lookup = df_lookup.alias("pu")
do_lookup = df_lookup.alias("do")

# Join
df_joined = (
    df_parquet
    .join(pu_lookup, col("PULocationID") == col("pu.LocationID"), "left")
    .join(do_lookup, col("DOLocationID") == col("do.LocationID"), "left")
    .select(
        df_parquet["*"],  # only original columns
        col("pu.Borough").alias("PUBorough"),
        col("pu.Zone").alias("PUZone"),
        col("pu.service_zone").alias("PUservice_zone"),
        col("do.Borough").alias("DOBorough"),
        col("do.Zone").alias("DOZone"),
        col("do.service_zone").alias("DOservice_zone"),
    )
)

df_joined.show()