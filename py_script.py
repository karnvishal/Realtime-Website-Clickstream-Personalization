#!/usr/bin/env python3
"""
PySpark batch script for clickstream analysis on Google Cloud Dataproc.
This script reads from a bronze BigQuery table and generates Silver (sessionized)
and Gold (aggregated KPI) tables.
"""
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window


GCP_PROJECT_ID = "testproject-404708"
BIGQUERY_DATASET = "clickstream_dataset"
BRONZE_TABLE = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.clickstream_event_v3"


SILVER_SESSIONS_TABLE = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.clickstream_sessions_silver"
GOLD_TRENDING_PRODUCTS_TABLE = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.trending_products_gold"
GOLD_CONVERSION_FUNNEL_TABLE = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.conversion_funnel_gold"
GOLD_USER_LTV_TABLE = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.user_ltv_gold"
GOLD_DEVICE_TRENDS_TABLE = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.device_trends_gold"


TEMP_BUCKET = "checkpoint_temp_bucket"

def main():
    """Main function to run the Spark batch job."""

    spark = SparkSession.builder \
        .appName("ClickstreamEnhancedETL-Dataproc") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0") \
        .getOrCreate()

    print("Spark Session initialized for batch processing.")

    
    df_bronze = spark.read.format("bigquery").option("table", BRONZE_TABLE).load()
    df_processed = df_bronze.withColumn(
        "event_timestamp", (col("event_timestamp") / 1000).cast(TimestampType())
    ).withColumn(
        "product_price", col("product_details.price").cast(DoubleType())
    )
    df_processed.cache()
    print("Bronze layer data loaded and processed.")

    
    df_silver = (df_processed
        .groupBy(col("user_id"), col("session_id"), col("demographics"))
        .agg(
            count(when(col("event_type") == "page_view", 1)).alias("page_views"),
            count(when(col("event_type") == "add_to_cart", 1)).alias("adds_to_cart"),
            count(when(col("event_type") == "purchase", 1)).alias("purchases"),
            coalesce(sum(when(col("event_type") == "purchase", col("product_price"))), lit(0)).alias("total_session_value"),
            min("event_timestamp").alias("session_start"),
            max("event_timestamp").alias("session_end"),
            first("device").alias("device"),
            first("browser").alias("browser"),
            first("os").alias("os")
        )
        .withColumn("session_duration_seconds", unix_timestamp(col("session_end")) - unix_timestamp(col("session_start")))
    )
    (df_silver.write.format("bigquery")
        .option("table", SILVER_SESSIONS_TABLE).option("temporaryGcsBucket", TEMP_BUCKET).mode("overwrite").save())
    print(f"Silver layer data written to {SILVER_SESSIONS_TABLE}.")

    
    df_gold_trending = (df_processed.filter(col("event_type") == "purchase")
        .groupBy(window(col("event_timestamp"), "10 minutes", "5 minutes"), col("product_details.id").alias("product_id"), col("product_details.name").alias("product_name"))
        .agg(count("*").alias("total_purchases"), sum("product_price").alias("total_revenue"))
        .select(col("window.end").alias("window_timestamp"), "product_id", "product_name", "total_purchases", "total_revenue"))
    (df_gold_trending.write.format("bigquery")
        .option("table", GOLD_TRENDING_PRODUCTS_TABLE).option("temporaryGcsBucket", TEMP_BUCKET).mode("overwrite").save())
    print(f"Trending products data written to {GOLD_TRENDING_PRODUCTS_TABLE}.")

    
    df_funnel = df_processed.groupBy("event_type").count().sort(desc("count"))
    (df_funnel.write.format("bigquery")
        .option("table", GOLD_CONVERSION_FUNNEL_TABLE).option("temporaryGcsBucket", TEMP_BUCKET).mode("overwrite").save())
    print(f"Conversion funnel data written to {GOLD_CONVERSION_FUNNEL_TABLE}.")

    
    df_ltv = (df_processed.filter(col("event_type") == "purchase")
        .groupBy("user_id")
        .agg(sum("product_price").alias("lifetime_value"), count("*").alias("total_purchases"))
        .sort(desc("lifetime_value")))
    (df_ltv.write.format("bigquery")
        .option("table", GOLD_USER_LTV_TABLE).option("temporaryGcsBucket", TEMP_BUCKET).mode("overwrite").save())
    print(f"User LTV data written to {GOLD_USER_LTV_TABLE}.")

    
    df_device_trends = (df_processed
        .groupBy("device", "browser", "os")
        .agg(countDistinct("session_id").alias("total_sessions"), count(when(col("event_type") == "purchase", 1)).alias("total_purchases"))
        .sort(desc("total_sessions")))
    (df_device_trends.write.format("bigquery")
        .option("table", GOLD_DEVICE_TRENDS_TABLE).option("temporaryGcsBucket", TEMP_BUCKET).mode("overwrite").save())
    print(f"Device trends data written to {GOLD_DEVICE_TRENDS_TABLE}.")

    df_processed.unpersist() 
    spark.stop()
    print("Spark session stopped. Job finished successfully.")

if __name__ == "__main__":
    main()

