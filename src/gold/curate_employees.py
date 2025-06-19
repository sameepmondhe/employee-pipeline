from src.utils.spark_helpers import get_spark
from pyspark.sql.functions import avg
import logging

SILVER_PATH = "/dbfs/tmp/silver/employees"
GOLD_PATH = "/dbfs/tmp/gold/employees"

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger("gold_curate")

def main():
    spark = get_spark("GoldCurate")
    logger.info(f"Reading Silver data from {SILVER_PATH}")
    df = spark.read.format("delta").load(SILVER_PATH)
    input_count = df.count()
    logger.info(f"Number of records read from Silver: {input_count}")
    df_gold = df.groupBy("department").agg(avg("salary").alias("avg_salary"))
    output_count = df_gold.count()
    logger.info(f"Number of records in Gold (departments): {output_count}")
    df_gold.write.format("delta").mode("overwrite").save(GOLD_PATH)
    logger.info(f"Gold table written to {GOLD_PATH}")

if __name__ == "__main__":
    main()
