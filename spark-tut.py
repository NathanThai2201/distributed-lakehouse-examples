from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import * # col, from_json, split, when, avg
from pyspark.sql.types import * # StructType, StructField
import os
from datetime import datetime, date
import pandas as pd
spark = SparkSession.builder.appName("demo").getOrCreate()


# Normal Examples:

def normal_examples1():
     # data frame example
    df = spark.createDataFrame(
        [
            ("sue", 32),
            ("li", 3),
            ("bob", 75),
            ("heo", 13),
        ],
        ["first_name", "age"],
    )
    print("DF")
    df.show()

    # add column that assignns life stage
    df1 = df.withColumn(
        "life_stage",
        when(col("age") < 13, "child")
        .when(col("age").between(13, 19), "teenager")
        .otherwise("adult"),
    )
    print("DF1")
    df1.show()

    # filter out child
    df1.where(col("life_stage").isin(["teenager", "adult"])).show()

    # group by life stages and calculate average
    df1.groupBy("life_stage").avg().show()


    # sql query:
    spark.sql("select avg(age) from {df1}", df1=df1).show()


    # same group by life stages and calculate average but done through SQL 
    spark.sql("select life_stage, avg(age) from {df1} group by life_stage", df1=df1).show()



    '''
        SPARK SQL
    '''
    print("SQL EXAMPLES")

    # spark.sql("DROP TABLE IF EXISTS some_people")
    # df1.write.saveAsTable("some_people")
    df1.write.mode("overwrite").saveAsTable("some_people")

    spark.sql("select * from some_people").show()

    # Insert values
    spark.sql("INSERT INTO some_people VALUES ('frank', 4, 'child')")
    spark.sql("select * from some_people").show()


    # get teenagers
    spark.sql("select * from some_people where life_stage='teenager'").show()




    '''
        RDD example: Pyspark's resilient distributed dataset with (Unstructured data)
    '''

    text_file = spark.sparkContext.textFile("some_text.txt")

    counts = (
        text_file.flatMap(lambda line: line.split(" "))
        .map(lambda word: (word, 1))
        .reduceByKey(lambda a, b: a + b)
    )

    counts.collect()

    for i in counts:
        print(i)

# Kafka Stream example:

def with_normalized_names(df, schema):
    parsed_df = (
        df.withColumn("json_data", from_json(col("value").cast("string"), schema))
        .withColumn("student_name", col("json_data.student_name"))
        .withColumn("graduation_year", col("json_data.graduation_year"))
        .withColumn("major", col("json_data.major"))
        .drop(col("json_data"))
        .drop(col("value"))
    )
    split_col = split(parsed_df["student_name"], "XX")
    return (
        parsed_df.withColumn("first_name", split_col.getItem(0))
        .withColumn("last_name", split_col.getItem(1))
        .drop("student_name")
    )

def perform_available_now_update():
    checkpointPath = "data/tmp_students_checkpoint/"
    path = "data/tmp_students"
    return df.transform(lambda df: with_normalized_names(df)).writeStream.trigger(
        availableNow=True
    ).format("parquet").option("checkpointLocation", checkpointPath).start(path)

def kafka_example():


    # df = spark.createDataFrame(
    #     [
    #         ("someXXperson","2023","math"),
    #         ("liXXyao","2025","physics"),
    #     ],
    #     ["student_name","graduation_year","major"]
    # )

    # df.show()


    #example kafka stream to data frame
    #     {"student_name":"someXXperson", "graduation_year":"2023", "major":"math"},
    #     {"student_name":"liXXyao", "graduation_year":"2025", "major":"physics"}]

    #     df = (
    #     spark.readStream.format("kafka")
    #     .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
    #     .option("subscribe", subscribeTopic)
    #     .load()
    # )
    
    schema = StructType([
    StructField("student_name", StringType()),
    StructField("graduation_year", StringType()),
    StructField("major", StringType()),
    ])


    with_normalized_names(df,schema)
    perform_available_now_update()


if __name__ == "__main__":

   normal_examples1()