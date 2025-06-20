"""
spark_helpers.py

This utility module provides helper functions for initializing and configuring Spark sessions for the ETL pipeline.

Functions:
- get_spark: Returns a SparkSession with the specified application name.
"""

from pyspark.sql import SparkSession

def get_spark(app_name="EmployeeETL"):
    """
    Create and return a SparkSession with the given application name.

    Args:
        app_name (str): The name of the Spark application.

    Returns:
        SparkSession: Configured Spark session object.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()
