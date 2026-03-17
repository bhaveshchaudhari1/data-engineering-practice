"""PySpark Transformations Example

Goal:
- Read a CSV file of user events
- Apply a series of transformations: type casting, filtering, and deriving new columns

Run with:
    spark-submit transform_example.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, when


def main():
    spark = SparkSession.builder.appName("TransformationsExample").getOrCreate()

    # Example input schema:
    # user_id (string), event_type (string), event_ts (string), value (string)
    input_path = "data/input/user_events.csv"

    df = spark.read.option("header", True).csv(input_path)

    # 1. Type cast and clean
    df_clean = (
        df
        .withColumn("user_id", col("user_id").cast("int"))
        .withColumn("event_ts", to_timestamp(col("event_ts"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("value", col("value").cast("double"))
    )

    # 2. Filter invalid rows
    df_valid = df_clean.filter((col("user_id").isNotNull()) & (col("event_ts").isNotNull()))

    # 3. Derive a new column
    df_final = df_valid.withColumn(
        "event_type_category",
        when(col("event_type") == "click", "engagement")
        .when(col("event_type") == "purchase", "revenue")
        .otherwise("other")
    )

    # Show a sample
    df_final.show(20, truncate=False)

    # Optional: write to parquet
    output_path = "data/output/user_events_transformed"
    df_final.write.mode("overwrite").parquet(output_path)

    spark.stop()


if __name__ == "__main__":
    main()
