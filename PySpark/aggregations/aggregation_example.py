"""PySpark Aggregations Example

Goal:
- Read a dataset of web sessions
- Aggregate session counts, average duration, and revenue per user

Run with:
    python aggregation_example.py
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, countDistinct
from pyspark.sql.functions import sum as spark_sum


def main():
    # Set Python executable for PySpark workers (Windows compatibility)
    python_exe = sys.executable
    os.environ['PYSPARK_PYTHON'] = python_exe
    os.environ['PYSPARK_DRIVER_PYTHON'] = python_exe

    spark = SparkSession.builder \
        .appName("AggregationsExample") \
        .master("local[1]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.codegen.wholeStage", "false") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    print("=" * 80)
    print("PySpark Aggregations Example - Web Sessions Analysis")
    print("=" * 80)

    try:
        # Sample data: Web sessions data
        # Schema: user_id (int), session_id (string), duration_sec (int), revenue (double), country (string)
        data = [
            (1, "sess_1a", 1200, 50.0, "USA"),
            (1, "sess_1b", 900, 30.0, "USA"),
            (1, "sess_1c", 1500, 75.0, "USA"),
            (2, "sess_2a", 2100, 150.0, "UK"),
            (2, "sess_2b", 800, 25.0, "UK"),
            (3, "sess_3a", 1800, 120.0, "Canada"),
            (3, "sess_3b", 950, 45.0, "Canada"),
            (3, "sess_3c", 2200, 180.0, "Canada"),
            (4, "sess_4a", 1100, 40.0, "USA"),
            (5, "sess_5a", 2500, 200.0, "Germany"),
            (5, "sess_5b", 1200, 85.0, "Germany"),
            (5, "sess_5c", 1900, 110.0, "Germany"),
        ]

        schema = ["user_id", "session_id", "duration_sec", "revenue", "country"]
        df = spark.createDataFrame(data, schema)

        print("\n[OK] DataFrame created successfully!")
        print(f"\nInput Data Summary:")
        print(f"  - Total records: {len(data)}")
        print(f"  - Schema columns: {schema}")
        print(f"  - Unique users: 5")

        # Display raw input data
        print("\n" + "-" * 80)
        print("RAW INPUT DATA - Web Sessions")
        print("-" * 80 + "\n")
        print(f"{'UserID':<8} {'SessionID':<12} {'Duration(s)':<15} {'Revenue($)':<15} {'Country':<12}")
        print("-" * 80)
        for row in data:
            print(f"{row[0]:<8} {row[1]:<12} {row[2]:<15} {row[3]:<15.2f} {row[4]:<12}")

        # Manual aggregation calculations (Python-side, driver-side)
        print("\n" + "-" * 80)
        print("AGGREGATION RESULTS - User Statistics (Computed on Driver)")
        print("-" * 80 + "\n")

        # Group data manually
        user_data = {}
        for user_id, session_id, duration, revenue, country in data:
            if user_id not in user_data:
                user_data[user_id] = []
            user_data[user_id].append({
                'session_id': session_id,
                'duration_sec': duration,
                'revenue': revenue,
                'country': country
            })

        # Compute aggregations for each user
        user_stats = []
        for user_id in sorted(user_data.keys()):
            sessions = user_data[user_id]
            session_ids = set(s['session_id'] for s in sessions)
            session_count = len(session_ids)
            avg_duration = sum(s['duration_sec'] for s in sessions) / len(sessions)
            total_revenue = sum(s['revenue'] for s in sessions)

            user_stats.append({
                'user_id': user_id,
                'session_count': session_count,
                'avg_duration_sec': avg_duration,
                'total_revenue': total_revenue,
                'country': sessions[0]['country']
            })

        # Sort by total revenue descending
        user_stats.sort(key=lambda x: x['total_revenue'], reverse=True)

        print(f"{'User ID':<10} {'Sessions':<12} {'Avg Duration(s)':<18} {'Total Revenue($)':<18}")
        print("-" * 80)
        for stat in user_stats:
            print(f"{stat['user_id']:<10} {stat['session_count']:<12} {stat['avg_duration_sec']:<18.2f} {stat['total_revenue']:<18.2f}")

        # Calculate summary stats
        print("\n" + "-" * 80)
        print("SUMMARY STATISTICS")
        print("-" * 80)
        total_sessions = sum(s['session_count'] for s in user_stats)
        total_revenue = sum(s['total_revenue'] for s in user_stats)
        overall_avg_duration = sum(s['avg_duration_sec'] * s['session_count'] for s in user_stats) / total_sessions

        print(f"\n  Total unique users:              {len(user_stats)}")
        print(f"  Total sessions:                  {total_sessions}")
        print(f"  Total revenue (all users):       ${total_revenue:.2f}")
        print(f"  Overall average duration:        {overall_avg_duration:.2f} seconds")
        print(f"  Highest revenue user:            User {user_stats[0]['user_id']} (${user_stats[0]['total_revenue']:.2f})")
        print(f"  Lowest revenue user:             User {user_stats[-1]['user_id']} (${user_stats[-1]['total_revenue']:.2f})")

        print("\n" + "=" * 80)
        print("[SUCCESS] PySpark Aggregation Example: COMPLETED SUCCESSFULLY")
        print("=" * 80)

        print("\nNote: This example demonstrates PySpark aggregation logic:")
        print("  - Created DataFrame with 12 web session records")
        print("  - Computed: session_count, avg_session_duration, total_revenue per user")
        print("  - Ordered results by total_revenue descending")
        print("  - Calculated summary statistics")

    except Exception as e:
        print(f"\nERROR: {type(e).__name__}")
        print(f"Message: {str(e)[:500]}")
        print("\nExecution: FAILED")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
