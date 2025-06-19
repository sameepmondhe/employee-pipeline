from src.utils.spark_helpers import get_spark
from pyspark.sql.functions import col, to_date
import logging

BRONZE_PATH = "/dbfs/tmp/bronze/employees"
SILVER_PATH = "/dbfs/tmp/silver/employees"

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger("silver_validate")

def main():
    spark = get_spark("SilverValidate")
    logger.info(f"Reading Bronze data from {BRONZE_PATH}")
    df = spark.read.format("delta").load(BRONZE_PATH)
    input_count = df.count()
    logger.info(f"Number of records read from Bronze: {input_count}")
    df_clean = df \
        .filter(col("name").isNotNull()) \
        .filter(col("salary").cast("double").isNotNull()) \
        .withColumn("hire_date", to_date(col("hire_date"), "yyyy-MM-dd")) \
        .filter(col("hire_date").isNotNull())
    output_count = df_clean.count()
    logger.info(f"Number of records after validation: {output_count}")
    df_clean.write.format("delta").mode("overwrite").save(SILVER_PATH)
    logger.info(f"Silver table written to {SILVER_PATH}")

if __name__ == "__main__":
    main()
