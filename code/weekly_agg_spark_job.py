from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum as spark_sum, weekofyear, col

import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Smartwatch Weekly Aggregates Calculation") \
    .master("local[4]") \
    .getOrCreate()

# Define PostgreSQL connection properties
postgres_url = os.getenv("POSTGRES_URL")
postgres_properties = {
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "driver": os.getenv("POSTGRES_DRIVER")
}

# Load daily aggregates from PostgreSQL (assuming 'daily_aggregates' table exists)
df_daily_aggregates = spark.read.jdbc(
    url=postgres_url,
    table=os.getenv("DAILY_AGG_TBL_NAME"),
    properties=postgres_properties
)

# Extract the week from the event_date for weekly aggregation
df_daily_aggregates = df_daily_aggregates.withColumn("event_week", weekofyear(col("event_date")))

# Perform weekly aggregates for each user (sum of daily total steps, average of daily average heart rate)
df_weekly_aggregates = df_daily_aggregates.groupBy("user_id", "event_week") \
    .agg(
        spark_sum("total_steps").alias("weekly_total_steps"),
        avg("avg_heart_rate").alias("weekly_avg_heart_rate")
    )

# Write the weekly aggregated results to a PostgreSQL table
# This assumes that the 'weekly_aggregates' table exists and is partitioned by event_week.
df_weekly_aggregates.write \
    .mode("overwrite") \
    .jdbc(url=postgres_url, table=os.getenv("WEEKLY_AGG_TBL_NAME"), properties=postgres_properties)

print("Weekly aggregates have been calculated and written to PostgreSQL.")
