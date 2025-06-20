"""
validate_employees.py

This module reads employee data from the Bronze Delta table, validates the records, and writes valid records to the Silver Delta table and invalid records to the Error Delta table.

Validation rules:
- 'name' must not be null
- 'salary' must be castable to double
- 'hire_date' must be a valid date in 'yyyy-MM-dd' format
- 'department' must not be null
- 'status' must be either 'Active' or 'Inactive'
- 'gender' must be one of 'Male', 'Female', or 'Other', or null
- 'date_of_birth' must be a valid date in 'yyyy-MM-dd' format or null
- 'employment_type' must be one of 'Full-Time', 'Part-Time', 'Contract', or null

Logging is used to track the number of records at each stage.
"""
from src.utils.spark_helpers import get_spark
from pyspark.sql.functions import col, to_date
import logging

# Paths to Delta tables for each stage
BRONZE_PATH = "/dbfs/tmp/bronze/employees"
SILVER_PATH = "/dbfs/tmp/silver/employees"
ERROR_PATH = "/dbfs/tmp/error/employees"

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger("silver_validate")

def main():
    spark = get_spark("SilverValidate")
    logger.info(f"Reading Bronze data from {BRONZE_PATH}")
    # Read all records from the Bronze Delta table
    df = spark.read.format("delta").load(BRONZE_PATH)
    input_count = df.count()
    logger.info(f"Number of records read from Bronze: {input_count}")

    # Apply validation rules to filter valid records
    valid_df = df \
        .filter(col("name").isNotNull()) \
        .filter(col("salary").cast("double").isNotNull()) \
        .withColumn("hire_date", to_date(col("hire_date"), "yyyy-MM-dd")) \
        .filter(col("hire_date").isNotNull()) \
        .filter(col("department").isNotNull()) \
        .filter((col("status") == "Active") | (col("status") == "Inactive")) \
        .filter((col("gender").isNull()) | (col("gender").isin(["Male", "Female", "Other"]))) \
        .withColumn("date_of_birth_parsed", to_date(col("date_of_birth"), "yyyy-MM-dd")) \
        .filter((col("date_of_birth").isNull()) | (col("date_of_birth_parsed").isNotNull())) \
        .filter((col("employment_type").isNull()) | (col("employment_type").isin(["Full-Time", "Part-Time", "Contract"])))
    output_count = valid_df.count()
    logger.info(f"Number of records after validation: {output_count}")

    # Write valid records to the Silver Delta table
    valid_df.write.format("delta").mode("overwrite").save(SILVER_PATH)
    logger.info(f"Silver table written to {SILVER_PATH}")

    # Identify and write invalid records to the Error Delta table using employee_id
    valid_ids = [row['employee_id'] for row in valid_df.select('employee_id').distinct().collect()]
    invalid_df = df.filter(~col('employee_id').isin(valid_ids))
    invalid_df.write.format("delta").mode("overwrite").save(ERROR_PATH)
    logger.info(f"Errored records written to {ERROR_PATH}")
    logger.info(f"Number of invalid records: {invalid_df.count()}")

if __name__ == "__main__":
    main()
