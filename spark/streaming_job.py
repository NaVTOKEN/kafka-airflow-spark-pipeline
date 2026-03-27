
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("UberStream").getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "uber_trips") \
    .load()

# Convert value
from pyspark.sql.functions import from_json
from pyspark.sql.types import *

schema = StructType([
    StructField("trip_id", IntegerType()),
    StructField("city", StringType()),
    StructField("fare", IntegerType()),
    StructField("status", StringType())
])

json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Example metric: cancelled trips
result = json_df.filter(col("status") == "cancelled")

query = result.writeStream \
    .format("parquet") \
    .option("path", "output/uber_stream") \
    .option("checkpointLocation", "checkpoint/") \
    .start()

query.awaitTermination()
