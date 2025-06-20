"""
curate_employees.py

This module reads validated employee data from the Silver Delta table, aggregates it, and writes curated results to the Gold Delta table.

Steps:
- Reads validated data from the Silver Delta table
- Aggregates average salary by department
- Writes the aggregated results to the Gold Delta table

Logging is used to track the number of records at each stage and the output location.
"""

from src.utils.spark_helpers import get_spark
from pyspark.sql.functions import avg
import logging

# Path to the Silver Delta table (input)
SILVER_PATH = "/dbfs/tmp/silver/employees"
# Path to the Gold Delta table (output)
GOLD_PATH = "/dbfs/tmp/gold/employees"

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger("gold_curate")

def main():
    spark = get_spark("GoldCurate")
    logger.info(f"Reading Silver data from {SILVER_PATH}")
    # Read validated employee data from Silver
    df = spark.read.format("delta").load(SILVER_PATH)
    input_count = df.count()
    logger.info(f"Number of records read from Silver: {input_count}")
    # Aggregate average salary by department
    df_gold = df.groupBy("department").agg(avg("salary").alias("avg_salary"))
    output_count = df_gold.count()
    logger.info(f"Number of records in Gold (departments): {output_count}")
    # Write the aggregated results to the Gold Delta table
    df_gold.write.format("delta").mode("overwrite").save(GOLD_PATH)
    logger.info(f"Gold table written to {GOLD_PATH}")

if __name__ == "__main__":
    main()
