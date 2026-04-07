from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import * # col, from_json, split, when, avg
from pyspark.sql.types import * # StructType, StructField
import os
from datetime import datetime, date
spark = SparkSession.builder \
    .appName("GoldLayer") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://10.140.0.4:9001") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "12345678") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# classic minIO
# df_parquet = spark.read.parquet("s3a://silver/yellow_tripdata_2025.parquet")

# Read from Silver Iceberg
df = spark.read.table("silver_catalog.default.yellow_taxi")

print("### Reading silver data, aggregating")
#df_parquet.printSchema()
#df_parquet.limit(15).show()





# --- 1. PRE-CALCULATION BASE ---
# We calculate core metrics needed across multiple tables first
df_base = df.withColumn(
    "trip_duration_minutes",
    expr("timestampdiff(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime)")
).withColumn(
    "pickup_hour", hour("tpep_pickup_datetime")
).withColumn(
    "pickup_weekday", dayofweek("tpep_pickup_datetime")
)

# --- 2. TABLE: TIP ANALYSIS ---
# Combining tip_ratio, tipped, and high_tip into one table
print("### Creating Tip Analysis Table")
tip_df = df_base.select(
    "VendorID", "tpep_pickup_datetime", # Keys for joining if needed
    col("tip_amount"),
    col("fare_amount"),
    round(when(col("fare_amount") > 0, col("tip_amount") / col("fare_amount")).otherwise(0), 2).alias("tip_ratio"),
    (col("tip_amount") > 0).alias("is_tipped"),
    (when(col("fare_amount") > 0, col("tip_amount") / col("fare_amount")).otherwise(0) > 1).alias("is_extreme_tip")
)
tip_df.writeTo("gold_catalog.default.taxi_tips").createOrReplace()

# --- 3. TABLE: TRIP PERFORMANCE ---
# Duration, Speed, and Efficiency
print("### Creating Trip Performance Table")
performance_df = df_base.select(
    "VendorID", "tpep_pickup_datetime", "trip_distance",
    "trip_duration_minutes",
    round(when(col("trip_duration_minutes") > 0, 
               col("trip_distance") / (col("trip_duration_minutes") / 60)).otherwise(0), 2).alias("avg_speed_mph"),
    round(when(col("trip_duration_minutes") > 0, 
               col("total_amount") / col("trip_duration_minutes")).otherwise(0), 2).alias("revenue_per_minute")
)
performance_df.writeTo("gold_catalog.default.taxi_performance").createOrReplace()

# --- 4. TABLE: FINANCIAL BREAKDOWN ---
# Fees, costs per mile, and passenger costs
print("### Creating Financials Table")
financials_df = df_base.withColumn(
    "total_fees",
    round(col("extra") + col("mta_tax") + col("tolls_amount") + 
          col("improvement_surcharge") + col("congestion_surcharge") + 
          col("Airport_fee") + col("cbd_congestion_fee"), 2)
).select(
    "VendorID", "tpep_pickup_datetime",
    "total_fees",
    "total_amount",
    round(when(col("trip_distance") > 0, col("total_amount") / col("trip_distance")).otherwise(0), 2).alias("cost_per_mile"),
    round(when(col("passenger_count") > 0, col("total_amount") / col("passenger_count")).otherwise(0), 2).alias("fare_per_passenger")
)
financials_df.writeTo("gold_catalog.default.taxi_financials").createOrReplace()

# --- 5. TABLE: TRIP CLASSIFICATION & QUALITY ---
# Flags and Categories
print("### Creating Classifications Table")
class_df = df_base.select(
    "VendorID", "tpep_pickup_datetime",
    (col("pickup_hour").between(7, 9) | col("pickup_hour").between(16, 19)).alias("is_peak_hour"),
    col("pickup_weekday").isin([1, 7]).alias("is_weekend"),
    when(col("trip_distance") < 2, "short")
        .when(col("trip_distance") < 10, "medium")
        .otherwise("long").alias("trip_type"),
    (col("payment_type") == 1).alias("is_card_payment"),
    ((col("trip_distance") <= 0) | (col("fare_amount") <= 0) | (col("trip_duration_minutes") <= 0)).alias("is_suspicious")
)
class_df.writeTo("gold_catalog.default.taxi_classifications").createOrReplace()

print("### Writing to gold")

# classic minIO
# df.write.mode("overwrite").parquet("s3a://gold/yellow_tripdata_2025.parquet")

print("### Gold Layer Tables Created Successfully")