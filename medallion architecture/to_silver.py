from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import * # col, from_json, split, when, avg
from pyspark.sql.types import * # StructType, StructField
import os
from datetime import datetime, date
spark = SparkSession.builder.appName("demo").getOrCreate()




df= spark.read.parquet("./test_folder/yellow_tripdata_2025-01.parquet")

#df_parquet = spark.read.parquet("hdfs://instance-20260312-012701:9000/data/yellow_tripdata_2025-01.parquet")
#df.printSchema()
#df.limit(20).show()
#print("original: ",df.count())

df = df.dropna('all')

# df.limit(20).show()
# print("dropped NULLs: ",df.count())

df.dropDuplicates()


df.limit(20).show()
# print("dropped all duplicates: ",df.count())

#df.fillna('None',subset=['originating_base_num']).show()

#df.sort(col('tolls').asc()).show() 
