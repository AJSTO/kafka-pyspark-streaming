import os
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import col, asc, desc
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import pandas as pd
spark_version = '3.3.2'
SUBMIT_ARGS = f'--packages ' \
              f'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,' \
              f'org.apache.kafka:kafka-clients:2.8.1 ' \
              f'pyspark-shell'
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS


spark = SparkSession \
    .builder \
    .appName("SSKafka") \
    .config('spark.jars.packages', './app/spark-sql-kafka-0-10_2.13-3.3.2.jar') \
    .getOrCreate()


kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "buses_v2") \
    .option("startingOffsets", "earliest") \
    .load()


schema = StructType(
    [
        StructField("Lines", StringType()),
        StructField("Lon", StringType()),
        StructField("VechicleNumber", StringType()),
        StructField("Time", StringType()),
        StructField("Lat", StringType()),
        StructField("Brigade", StringType()),
    ]
)
#dataframe = kafka_df.selectExpr("CAST(value AS STRING) as json").select(from_json(col("json").cast("string"), schema))
#dataframe = kafka_df.select(from_json(col("value").cast("string"), schema).alias("value"))

query = kafka_df.writeStream \
    .format('console') \
    .option('path', './costam') \
    .option("checkpointLocation", "./checkpoint") \
    .start().awaitTermination()
'''
def func(df, batch_id):
    my_df = df.selectExpr("CAST(value AS STRING) as json")
    schema = StructType(
        [
            StructField("Lines", StringType(), True),
            StructField("Lon", StringType(), True),
            StructField("VechicleNumber", StringType(), True),
            StructField("Time", StringType(), True),
            StructField("Lat", StringType(), True),
            StructField("Brigade", StringType(), True),
        ]
    )
    dataframe = my_df.select(from_json(col("json").cast("string"), schema))

    dataframe.write.format("csv"). \
        mode("append"). \
        save('./xd')
    print("batch_id : ", batch_id, dataframe.show(truncate=False))

query = kafka_df.writeStream \
    .foreachBatch(func) \
    .option("checkpointLocation", "./checkpoint") \
    .trigger(processingTime="1 minutes") \
    .start().awaitTermination()
'''