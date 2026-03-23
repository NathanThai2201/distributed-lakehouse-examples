from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import * # col, from_json, split, when, avg
from pyspark.sql.types import * # StructType, StructField
import os
from datetime import datetime, date
spark = SparkSession.builder.appName("demo").getOrCreate()




df= spark.read.parquet("../test_folder/yellow_tripdata_2025-01.parquet")

#df_parquet = spark.read.parquet("hdfs://instance-20260312-012701:9000/data/yellow_tripdata_2025-01.parquet")
#df.printSchema()
#df.limit(20).show()
#print("original: ",df.count())

df = df.dropna('all')

# df.limit(20).show()
# print("dropped NULLs: ",df.count())

df = df.dropDuplicates()



#df.limit(20).show()
# print("dropped all duplicates: ",df.count())

df = df.fillna('None',subset=['originating_base_num']).show()


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
    expr("timestampdiff(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime)")
)

df = df.filter(col("trip_duration_minutes") > 0)
