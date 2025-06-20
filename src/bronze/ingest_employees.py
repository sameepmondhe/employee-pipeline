"""
ingest_employees.py

This module ingests raw employee data from a CSV file and writes it to the Bronze Delta table.

Steps:
- Reads raw data from the specified CSV file
- Infers schema and loads data into a Spark DataFrame
- Writes the DataFrame to the Bronze Delta table for downstream processing

Logging is used to track the number of records ingested and the output location.
"""

from src.utils.spark_helpers import get_spark
import os
import logging

# Path to the raw employee data file
DATA_PATH = os.path.abspath("data/employees.txt")
# Path to the Bronze Delta table
BRONZE_PATH = "/dbfs/tmp/bronze/employees"

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger("bronze_ingest")

def main():
    spark = get_spark("BronzeIngest")
    logger.info(f"Reading raw data from {DATA_PATH}")
    # Read the raw CSV data with header and infer schema
    df = spark.read.option("header", True).option("inferSchema", True).csv(DATA_PATH)
    record_count = df.count()
    logger.info(f"Number of records ingested: {record_count}")
    # Write the ingested data to the Bronze Delta table
    df.write.format("delta").mode("overwrite").save(BRONZE_PATH)
    logger.info(f"Bronze table written to {BRONZE_PATH}")

if __name__ == "__main__":
    main()
