from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import * # col, from_json, split, when, avg
from pyspark.sql.types import * # StructType, StructField
import os
from datetime import datetime, date


from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
import configparser

config = configparser.ConfigParser()
config.read("/home/ubuntu/scripts/config.ini")

# MinIO configs
endpoint = config["minio"]["endpoint"]
access_key = config["minio"]["access_key"]
secret_key = config["minio"]["secret_key"]

spark = SparkSession.builder \
    .appName("LearningGold") \
    .config("spark.hadoop.fs.s3a.endpoint", endpoint) \
    .config("spark.hadoop.fs.s3a.access.key", access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()


df_tips = spark.read.table("gold_catalog.default.taxi_tips")
df_performance = spark.read.table("gold_catalog.default.taxi_performance")
df_financials = spark.read.table("gold_catalog.default.taxi_financials")
df_class = spark.read.table("gold_catalog.default.taxi_classifications")



print("### Reading gold data:")


df_tips.show()
df_performance.show()
df_financials.show()
df_class.show()

print("### SparkML Learning")


assembler = VectorAssembler(
    inputCols=["trip_distance", "trip_duration_minutes"],
    outputCol="features"
)

df_ml = assembler.transform(df_performance)

lr = LinearRegression(featuresCol="features", labelCol="total_amount")
model = lr.fit(df_ml)

predictions = model.transform(df_ml)

predictions.select(
    "trip_distance",
    "trip_duration_minutes",
    "total_amount",
    "prediction"
).show()
