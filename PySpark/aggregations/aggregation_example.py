"""PySpark Aggregations Example

Goal:
- Read a dataset of web sessions
- Aggregate session counts, average duration, and revenue per user

Run with:
    spark-submit aggregation_example.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, countDistinct
from pyspark.sql.functions import sum as spark_sum


def main():
    spark = SparkSession.builder.appName("AggregationsExample").getOrCreate()

    # Example input schema:
    # user_id (int), session_id (string), duration_sec (int), revenue (double), country (string)
    input_path = "data/input/web_sessions.parquet"

    df = spark.read.parquet(input_path)

    # Aggregation per user
    user_stats = (
        df.groupBy("user_id")
        .agg(
            countDistinct("session_id").alias("session_count"),
            avg(col("duration_sec")).alias("avg_session_duration_sec"),
            spark_sum(col("revenue")).alias("total_revenue"),
        )
        .orderBy(col("total_revenue").desc())
    )

    user_stats.show(20, truncate=False)

    output_path = "data/output/user_aggregates"
    user_stats.write.mode("overwrite").parquet(output_path)

    spark.stop()


if __name__ == "__main__":
    main()
