from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, isnan, when, count
from delta.tables import DeltaTable
import argparse
import os
import sys

def get_args():
    parser = argparse.ArgumentParser(description="Employee ETL Pipeline")
    parser.add_argument('--env', choices=['local', 'databricks'], default='databricks', help='Execution environment')
    return parser.parse_args()

def main():
    args = get_args()
    if args.env == 'local':
        # Local paths
        bronze_path = "./tmp/bronze/employees"
        silver_path = "./tmp/silver/employees"
        gold_path = "./tmp/gold/employees"
        raw_file_path = "./data/employees.txt"
        spark = SparkSession.builder \
            .appName("EmployeeETL") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0") \
            .getOrCreate()
    else:
        # Databricks/DBFS paths
        bronze_path = "/dbfs/tmp/bronze/employees"
        silver_path = "/dbfs/tmp/silver/employees"
        gold_path = "/dbfs/tmp/gold/employees"
        raw_file_path = "dbfs:/FileStore/tables/data/employees.txt"
        spark = SparkSession.builder.appName("EmployeeETL").getOrCreate()

    # Set Spark log level to WARN for less verbose output
    spark.sparkContext.setLogLevel("WARN")

    # 1. Bronze Layer: Ingest raw data
    df_bronze = spark.read.option("header", True).option("inferSchema", True).csv(raw_file_path)
    df_bronze.write.format("delta").mode("overwrite").save(bronze_path)

    # 2. Silver Layer: Validate and clean data
    bronze_df = spark.read.format("delta").load(bronze_path)
    silver_df = bronze_df \
        .filter(col("name").isNotNull()) \
        .filter(col("salary").cast("double").isNotNull()) \
        .withColumn("hire_date", to_date(col("hire_date"), "yyyy-MM-dd")) \
        .filter(col("hire_date").isNotNull())
    silver_df.write.format("delta").mode("overwrite").save(silver_path)

    # 3. Gold Layer: Example aggregation - average salary by department
    gold_df = silver_df.groupBy("department").agg({"salary": "avg"}).withColumnRenamed("avg(salary)", "avg_salary")
    gold_df.write.format("delta").mode("overwrite").save(gold_path)

    print("ETL pipeline complete. Bronze, Silver, and Gold Delta tables created.")

if __name__ == "__main__":
    main()
