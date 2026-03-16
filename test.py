from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import * # col, from_json, split, when, avg
from pyspark.sql.types import * # StructType, StructField
import os
from datetime import datetime, date
import pandas as pd
spark = SparkSession.builder.appName("demo").getOrCreate()





df = spark.read.parquet('./test_folder/yellow_tripdata_2025-01.parquet',
                        './test_folder/yellow_tripdata_2025-02.parquet',
                        './test_folder/yellow_tripdata_2025-03.parquet',
                        './test_folder/yellow_tripdata_2025-04.parquet',
                        './test_folder/yellow_tripdata_2025-05.parquet',
                        './test_folder/yellow_tripdata_2025-06.parquet',
                        './test_folder/yellow_tripdata_2025-07.parquet',
                        './test_folder/yellow_tripdata_2025-08.parquet',
                        './test_folder/yellow_tripdata_2025-10.parquet',
                        './test_folder/yellow_tripdata_2025-11.parquet') # inferschema, header and multiline do not help.
# df = spark.read.parquet('nyctripdata')
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
