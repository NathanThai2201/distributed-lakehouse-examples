from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import * # col, from_json, split, when, avg
from pyspark.sql.types import * # StructType, StructField
import os
from datetime import datetime, date
spark = SparkSession.builder \
    .appName("SilverLayer") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://10.140.0.4:9001") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "12345678") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Classic minIO
# df = spark.read.parquet("s3a://bronze/yellow_tripdata_2025.parquet")







bronze_taxi_path = "s3a://bronze/default/yellow_taxi.parquet"
bronze_lookup_path = "s3a://bronze/default/yellow_taxi_lookup.csv"

# Read the Parquet data
df = spark.read.parquet(bronze_taxi_path)

# Read the CSV data (ensure you keep the header and schema inference)
df_lookup = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(bronze_lookup_path)






print("### Reading bronze data, cleaning")
#df.printSchema()
#df.limit(20).show()
#print("original: ",df.count())

df = df.dropna('all')

# df.limit(20).show()
# print("dropped NULLs: ",df.count())

df = df.dropDuplicates()

#df.limit(20).show()
# print("dropped all duplicates: ",df.count())

# df = df.fillna('None',subset=['originating_base_num'])


# filtering bad values
df = df.filter(
    (col("fare_amount") >= 0) &
    (col("tip_amount") >= 0) &
    (col("total_amount") >= 0) &
    (col("trip_distance") >= 0) &
    (col("tolls_amount") >= 0) &
    (col("extra") >= 0) &
    (col("mta_tax") >= 0) &
    (col("improvement_surcharge") >= 0)
)
#df.sort(col('tolls').asc()).show() 


df = df.withColumn(
    "trip_duration_minutes",
    (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60
)

df = df.filter(col("trip_duration_minutes") > 0)




# joining tables of tripdata and location lookup
# Alias lookup table twice
pu_lookup = df_lookup.alias("pu")
do_lookup = df_lookup.alias("do")

# Join
df_joined = (
    df
    .join(pu_lookup, col("PULocationID") == col("pu.LocationID"), "left")
    .join(do_lookup, col("DOLocationID") == col("do.LocationID"), "left")
    .select(
        df["*"],  # only original columns
        col("pu.Borough").alias("PUBorough"),
        col("pu.Zone").alias("PUZone"),
        col("pu.service_zone").alias("PUservice_zone"),
        col("do.Borough").alias("DOBorough"),
        col("do.Zone").alias("DOZone"),
        col("do.service_zone").alias("DOservice_zone"),
    )
)
print("### Writing to silver")



# classic minIO
# df.write.mode("overwrite").parquet("s3a://silver/yellow_tripdata_2025.parquet")


# Write the cleaned data
df_joined.writeTo("silver_catalog.default.yellow_taxi").createOrReplace()