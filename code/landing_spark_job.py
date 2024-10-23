from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, avg, sum as spark_sum, to_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType

import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Kafka Spark Streaming") \
    .master("local[4]") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .getOrCreate()

# Define schema for JSON data (adapting from data.json structure)
schema = StructType([
    StructField("device_id", StringType(), True),
    StructField("Device ID", StringType(), True),
    StructField("hr", IntegerType(), True),
    StructField("Heart Rate", IntegerType(), True),
    StructField("steps", IntegerType(), True),
    StructField("Step Count", IntegerType(), True),
    StructField("Sleeping State", StringType(), True),
    StructField("state", StringType(), True),
    StructField("date", StringType(), True),  # Date is stored as a string in JSON
    StructField("timestamp", DoubleType(), True)
])

# Read data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS")) \
    .option("subscribe", os.getenv("KAFKA_TOPIC_NAME")) \
    .option("startingOffsets", os.getenv("KAFKA_TOPIC_OFFSET_STRATEGY")) \
    .load()

# Convert Kafka value from bytes to string
df = df.selectExpr("CAST(value AS STRING) as json")

# Parse the JSON data and apply the schema
df = df.select(from_json(col("json"), schema).alias("data")).select("data.*")

# Normalize the data (handle inconsistent formats and missing values)
df_normalized = df.withColumn(
    "heart_rate", when(col("Heart Rate").isNotNull(), col("Heart Rate")).otherwise(col("hr"))
).withColumn(
    "step_count", when(col("Step Count").isNotNull(), col("Step Count")).otherwise(col("steps"))
).withColumn(
    "sleeping_state", when(col("Sleeping State").isNotNull(), col("Sleeping State")).otherwise(col("state"))
).withColumn(
    "device_id", when(col("Device ID").isNotNull(), col("Device ID")).otherwise(col("device_id"))
).withColumn(
    "event_time", when(col("timestamp").isNotNull(), col("timestamp").cast(TimestampType())).otherwise(to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss.SSS"))
)

# Add a watermark on the event time (e.g., 10 minutes late)
df_normalized_with_watermark = df_normalized.withWatermark("event_time", "1 millisecond")

# Write the output to Parquet files with append mode
query = df_normalized_with_watermark.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", os.getenv("RAW_OUTPUT_PARQUET_PATH")) \
    .option("checkpointLocation", os.getenv("SPARK_CHKPOINT_DIR")) \
    .start()
    
# Wait for the termination of the query
query.awaitTermination()
