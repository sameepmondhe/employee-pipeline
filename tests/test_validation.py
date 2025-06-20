import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from src.silver.validate_employees import main as validate_main
import os

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("TestSession") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0") \
        .getOrCreate()

def test_validation_removes_invalid_rows(spark, tmp_path, monkeypatch):
    # Prepare test data
    data = [
        Row(name="John Doe", address="123 Main St", salary=75000, job_title="Engineer", department="Engineering", hire_date="2018-06-01"),
        Row(name=None, address="456 Oak Ave", salary=82000, job_title="Data Scientist", department="Data", hire_date="2019-09-15"),
        Row(name="Alice Johnson", address="789 Pine Rd", salary=None, job_title="Manager", department="Product", hire_date="2017-03-22"),
        Row(name="Bob Lee", address="321 Maple Dr", salary=67000, job_title="QA Analyst", department="Quality", hire_date="invalid-date"),
    ]
    df = spark.createDataFrame(data)
    bronze_path = str(tmp_path / "bronze")
    silver_path = str(tmp_path / "silver")
    df.write.format("delta").mode("overwrite").save(bronze_path)

    # Patch paths in validate_employees.py
    monkeypatch.setattr("src.silver.validate_employees.BRONZE_PATH", bronze_path)
    monkeypatch.setattr("src.silver.validate_employees.SILVER_PATH", silver_path)

    # Run validation
    validate_main()

    # Read result
    result_df = spark.read.format("delta").load(silver_path)
    result = result_df.collect()
    # Only the first row is valid
    assert len(result) == 1
    assert result[0]["name"] == "John Doe"
