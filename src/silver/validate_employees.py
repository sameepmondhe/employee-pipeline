from src.utils.spark_helpers import get_spark
from pyspark.sql.functions import col, to_date
import logging

BRONZE_PATH = "/dbfs/tmp/bronze/employees"
SILVER_PATH = "/dbfs/tmp/silver/employees"
ERROR_PATH = "/dbfs/tmp/error/employees"

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger("silver_validate")

def main():
    spark = get_spark("SilverValidate")
    logger.info(f"Reading Bronze data from {BRONZE_PATH}")
    df = spark.read.format("delta").load(BRONZE_PATH)
    input_count = df.count()
    logger.info(f"Number of records read from Bronze: {input_count}")
    valid_df = df \
        .filter(col("name").isNotNull()) \
        .filter(col("salary").cast("double").isNotNull()) \
        .withColumn("hire_date", to_date(col("hire_date"), "yyyy-MM-dd")) \
        .filter(col("hire_date").isNotNull())
    output_count = valid_df.count()
    logger.info(f"Number of records after validation: {output_count}")
    # Save valid records
    valid_df.write.format("delta").mode("overwrite").save(SILVER_PATH)
    logger.info(f"Silver table written to {SILVER_PATH}")
    # Save invalid records
    invalid_df = df.subtract(valid_df)
    invalid_count = invalid_df.count()
    logger.info(f"Number of records errored out: {invalid_count}")
    invalid_df.write.format("delta").mode("overwrite").save(ERROR_PATH)
    logger.info(f"Errored records written to {ERROR_PATH}")

if __name__ == "__main__":
    main()
