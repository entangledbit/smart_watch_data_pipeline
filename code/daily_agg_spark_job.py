
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum as spark_sum, to_date, col

import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Smartwatch Daily Aggregates Calculation") \
    .master("local[4]") \
    .getOrCreate()

# Path to the processed smartwatch data
processed_data_path = os.getenv("RAW_OUTPUT_PARQUET_PATH")

# Path to the user profile JSON file
user_profiles_path = os.getenv("USER_PROFILES_JSON")

# Read the processed smartwatch data from the Parquet file
df_smartwatch = spark.read.parquet(processed_data_path)

# Read the user profiles from the JSON file
df_user_profiles = spark.read \
    .option("multiline", "true") \
    .json(user_profiles_path)

# Join the processed smartwatch data with user profiles on device_id
df_joined = df_smartwatch.join(df_user_profiles, "device_id")

# Extract the date from the event_time for daily aggregation
df_joined = df_joined.withColumn("event_date", to_date(col("event_time")))

# Perform daily aggregates for each user (total steps, average heart rate)
df_daily_aggregates = df_joined.groupBy("user_id", "event_date") \
    .agg(
        spark_sum("step_count").alias("total_steps"),
        avg("heart_rate").alias("avg_heart_rate")
    )

# Define PostgreSQL connection properties
postgres_url = os.getenv("POSTGRES_URL")
postgres_properties = {
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "driver": os.getenv("POSTGRES_DRIVER")
}

# Write the aggregated results to a PostgreSQL table
# This assumes that the 'daily_aggregates' table exists and is partitioned by event_date.
df_daily_aggregates.write \
    .mode("overwrite") \
    .jdbc(url=postgres_url, table=os.getenv("DAILY_AGG_TBL_NAME"), properties=postgres_properties)

print("Daily aggregates have been calculated and written to PostgreSQL.")

