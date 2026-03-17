"""PySpark Real Scenario: Sales Pipeline

This script describes a simplified end-to-end pipeline:
  1) Read raw sales events
  2) Filter and enrich
  3) Write a clean sales fact table

Run with:
    spark-submit real_scenario_sales_pipeline.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, to_date


def main():
    spark = SparkSession.builder.appName("SalesPipelineExample").getOrCreate()

    # Raw data: sales events captured from an e-commerce platform
    # Columns: event_id, user_id, event_ts, product_id, quantity, price, currency
    raw_path = "data/input/sales_events.json"
    raw_df = spark.read.json(raw_path)

    # 1) Apply basic quality filters
    clean_df = (
        raw_df
        .filter(col("user_id").isNotNull())
        .filter(col("quantity") > 0)
        .filter(col("price") >= 0)
    )

    # 2) Derive useful columns
    enriched_df = (
        clean_df
        .withColumn("event_date", to_date(col("event_ts")))
        .withColumn("total_amount", col("quantity") * col("price"))
        .withColumn("currency", expr("upper(currency)"))
    )

    # 3) Write to a parquet fact table (partitioned by event_date)
    output_path = "data/output/sales_fact"
    (
        enriched_df
        .write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(output_path)
    )

    spark.stop()


if __name__ == "__main__":
    main()
