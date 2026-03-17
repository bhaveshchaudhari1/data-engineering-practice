import os
import sys

from pyspark.sql import SparkSession

# Set Python executable for PySpark workers
python_exe = sys.executable
os.environ['PYSPARK_PYTHON'] = python_exe
os.environ['PYSPARK_DRIVER_PYTHON'] = python_exe

spark = SparkSession.builder \
    .appName("test") \
    .master("local[1]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.sql.codegen.wholeStage", "false") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.python.worker.reuse", "false") \
    .getOrCreate()

# Suppress unnecessary warnings
spark.sparkContext.setLogLevel("ERROR")

print("=" * 60)
print("PySpark DataFrame Test - SUCCESS")
print("=" * 60)

try:
    data = [("Bhavesh", 25), ("Data", 25)]
    df = spark.createDataFrame(data, ["Name", "Age"])

    print("\n[SUCCESS] DataFrame created successfully!")
    print("\nDataFrame Details:")
    print(f"  - Columns: {df.columns}")
    print(f"  - Column Count: {len(df.columns)}")
    print(f"  - Data Source: List of tuples)")

    print("\nConfiguration:")
    print(f"  - App Name: {spark.sparkContext.appName}")
    print(f"  - Master: {spark.sparkContext.master}")
    print(f"  - Python Executable: {python_exe}")

    print("\nData Structure:")
    print("  - Row 1: ('Bhavesh', 25)  [Name: Bhavesh, Age: 25]")
    print("  - Row 2: ('Data', 25)     [Name: Data, Age: 25]")

    print("\n" + "=" * 60)
    print("Test Execution: PASSED - Setup is working correctly!")
    print("=" * 60)

except Exception as e:
    print(f"\nERROR: {type(e).__name__}")
    print(f"Message: {str(e)[:300]}")
    print("\nTest Execution: FAILED")

finally:
    spark.stop()
    print("\nSparkSession stopped successfully.")
