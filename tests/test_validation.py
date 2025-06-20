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
    # Prepare test data with all columns from the schema
    columns = [
        "employee_id", "name", "salary", "hire_date", "department", "email", "phone", "address", "city", "state", "zip", "country", "manager_id", "status", "termination_date", "gender", "date_of_birth", "job_level", "work_location", "employment_type"
    ]
    data = [
        # Valid row
        ("1001", "John Doe", "75000", "2018-06-01", "Engineering", "john.doe@example.com", "555-1234", "123 Main St", "Springfield", "CA", "90001", "USA", "1005", "Active", None, "Male", "1985-04-12", "Level 2", "San Francisco", "Full-Time"),
        # Invalid: name is None
        ("1002", None, "82000", "2019-09-15", "Data", "jane.smith@example.com", "555-5678", "456 Oak Ave", "Metropolis", "NY", "10001", "USA", "1001", "Active", None, "Female", "1990-07-23", "Level 3", "New York", "Full-Time"),
        # Invalid: salary is None
        ("1003", "Alice Johnson", None, "2017-03-22", "Product", "alice.johnson@example.com", "555-8765", "789 Pine Rd", "Centerville", "TX", "73301", "USA", "1001", "Inactive", "2022-12-31", "Female", "1982-11-05", "Level 4", "Austin", "Part-Time"),
        # Invalid: hire_date is invalid
        ("1004", "Bob Lee", "67000", "invalid-date", "Quality", "bob.lee@example.com", "555-4321", "321 Maple Dr", "Laketown", "FL", "33101", "USA", "1001", "Active", None, "Male", "1993-02-17", "Level 1", "Miami", "Full-Time"),
        # Invalid: gender is not allowed
        ("1005", "Invalid Gender", "50000", "2020-01-01", "QA", "invalid.gender@example.com", "555-0000", "1 Test St", "Testville", "TX", "73301", "USA", "1001", "Active", None, "Alien", "1990-01-01", "Level 1", "Houston", "Full-Time"),
        # Invalid: status is not allowed
        ("1006", "Invalid Status", "50000", "2020-01-01", "QA", "invalid.status@example.com", "555-0001", "2 Test St", "Testville", "TX", "73301", "USA", "1001", "Unknown", None, "Male", "1990-01-01", "Level 1", "Houston", "Full-Time"),
        # Invalid: employment_type is not allowed
        ("1007", "Invalid EmpType", "50000", "2020-01-01", "QA", "invalid.emptype@example.com", "555-0002", "3 Test St", "Testville", "TX", "73301", "USA", "1001", "Active", None, "Male", "1990-01-01", "Level 1", "Houston", "Intern"),
        # Invalid: date_of_birth is not a date
        ("1008", "Invalid DOB", "50000", "2020-01-01", "QA", "invalid.dob@example.com", "555-0003", "4 Test St", "Testville", "TX", "73301", "USA", "1001", "Active", None, "Male", "not-a-date", "Level 1", "Houston", "Full-Time"),
        # Invalid: department is None
        ("1009", "Missing Dept", "50000", "2020-01-01", None, "missing.dept@example.com", "555-0004", "5 Test St", "Testville", "TX", "73301", "USA", "1001", "Active", None, "Male", "1990-01-01", "Level 1", "Houston", "Full-Time"),
    ]
    df = spark.createDataFrame(data, columns)
    bronze_path = str(tmp_path / "bronze")
    silver_path = str(tmp_path / "silver")
    error_path = str(tmp_path / "error")
    df.write.format("delta").mode("overwrite").save(bronze_path)

    # Patch paths in validate_employees.py
    monkeypatch.setattr("src.silver.validate_employees.BRONZE_PATH", bronze_path)
    monkeypatch.setattr("src.silver.validate_employees.SILVER_PATH", silver_path)
    monkeypatch.setattr("src.silver.validate_employees.ERROR_PATH", error_path)

    # Run validation
    validate_main()

    # Read result
    result_df = spark.read.format("delta").load(silver_path)
    result = result_df.collect()
    # Only the first row is valid
    assert len(result) == 1
    assert result[0]["name"] == "John Doe"

    # Check that 8 records got errored out
    error_df = spark.read.format("delta").load(error_path)
    error_result = error_df.collect()
    assert len(error_result) == 8
