import logging
import os
import pandas as pd

# Setup Python logging for clean output
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ReadCSVExample")

logger.info("=" * 60)
logger.info("CSV Reader - Sales Data Analysis")
logger.info("=" * 60)

try:
    # Get the directory where this script is located, then navigate to sample data
    current_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(current_dir, "../../sample_sales_data.csv")
    csv_path = os.path.normpath(csv_path)

    logger.info(f"\nReading CSV from: {csv_path}")

    # Read CSV using Pandas (most reliable on Windows)
    df = pd.read_csv(csv_path)
    logger.info("[SUCCESS] CSV file read successfully!")

    logger.info("\n[SCHEMA]:")
    logger.info(f"Total Columns: {len(df.columns)}")
    logger.info(f"Data Types:\n{df.dtypes}")

    logger.info(f"\nTotal Records: {len(df)}")

    logger.info("\n[SAMPLE DATA - First 5 Rows]:")
    logger.info(f"\n{df.head().to_string()}")

    logger.info("\n[SUMMARY STATISTICS]:")
    logger.info(f"\n{df.describe().to_string()}")

except FileNotFoundError as e:
    logger.error(f"\nERROR: File not found")
    logger.error(f"Path: {csv_path}")
    logger.error(f"Message: {str(e)}")
    logger.error("Execution: FAILED")

except Exception as e:
    logger.error(f"\nERROR: {type(e).__name__}")
    logger.error(f"Message: {str(e)[:500]}")
    logger.error("Execution: FAILED")
