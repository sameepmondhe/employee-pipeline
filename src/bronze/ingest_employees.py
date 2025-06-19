from src.utils.spark_helpers import get_spark
import os
import logging

DATA_PATH = os.path.abspath("data/employees.txt")
BRONZE_PATH = "/dbfs/tmp/bronze/employees"

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger("bronze_ingest")

def main():
    spark = get_spark("BronzeIngest")
    logger.info(f"Reading raw data from {DATA_PATH}")
    df = spark.read.option("header", True).option("inferSchema", True).csv(DATA_PATH)
    record_count = df.count()
    logger.info(f"Number of records ingested: {record_count}")
    df.write.format("delta").mode("overwrite").save(BRONZE_PATH)
    logger.info(f"Bronze table written to {BRONZE_PATH}")

if __name__ == "__main__":
    main()
