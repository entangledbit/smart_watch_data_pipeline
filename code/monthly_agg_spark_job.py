from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum as spark_sum, col, date_format

import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Smartwatch Monthly Aggregates Calculation") \
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

# Extract the month from the event_date for monthly aggregation
df_daily_aggregates = df_daily_aggregates.withColumn("event_month", date_format(col("event_date"), "yyyy-MM"))

# Perform monthly aggregates for each user (sum of daily total steps, average of daily average heart rate)
df_monthly_aggregates = df_daily_aggregates.groupBy("user_id", "event_month") \
    .agg(
        spark_sum("total_steps").alias("monthly_total_steps"),
        avg("avg_heart_rate").alias("monthly_avg_heart_rate")
    )

# Write the monthly aggregated results to a PostgreSQL table
# This assumes that the 'monthly_aggregates' table exists and is partitioned by event_month.
df_monthly_aggregates.write \
    .mode("overwrite") \
    .jdbc(url=postgres_url, table=os.getenv("MONTHLY_AGG_TBL_NAME"), properties=postgres_properties)

print("Monthly aggregates have been calculated and written to PostgreSQL.")
