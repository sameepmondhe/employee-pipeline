from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, isnan, when, count
from delta.tables import DeltaTable

# Paths for Delta tables
bronze_path = "/dbfs/tmp/bronze/employees"
silver_path = "/dbfs/tmp/silver/employees"
gold_path = "/dbfs/tmp/gold/employees"
raw_file_path = "/dbfs/Users/Sameep.Mondhe/learning/etl/employee-pipeline/employees.txt"

spark = SparkSession.builder.appName("EmployeeETL").getOrCreate()

# 1. Bronze Layer: Ingest raw data
df_bronze = spark.read.option("header", True).option("inferSchema", True).csv(raw_file_path)
df_bronze.write.format("delta").mode("overwrite").save(bronze_path)

# 2. Silver Layer: Validate and clean data
# Example validations: non-null name, valid salary, valid date
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

