from typing import Any
from pyspark.sql import DataFrame, SparkSession

def execute_sql_query(spark_df: DataFrame, query: str) -> DataFrame:
    """
    Execute a SQL query on a Spark DataFrame.
    
    Args:
        spark_df (DataFrame): Input Spark DataFrame
        query (str): SQL query to execute
        
    Returns:
        DataFrame: Result of the SQL query
    """
    # Register the DataFrame as a temporary view
    spark_df.createOrReplaceTempView("temp_table")
    
    # Execute the query
    result_df = spark_df.sparkSession.sql(query)
    
    # Drop the temporary view
    spark_df.sparkSession.catalog.dropTempView("temp_table")
    
    return result_df

def save_as_csv(df: DataFrame, output_path: str, header: bool = True, mode: str = "overwrite"):
    """
    Save DataFrame as CSV file
    """
    df.write.mode(mode).option("header", header).csv(output_path)

def save_as_parquet(df: DataFrame, output_path: str, mode: str = "overwrite"):
    """
    Save DataFrame as Parquet file
    """
    df.coalesce(1).write.mode(mode).parquet(output_path)

def save_as_json(df: DataFrame, output_path: str, mode: str = "overwrite"):
    """
    Save DataFrame as JSON file
    """
    df.write.mode(mode).json(output_path)

def read_csv(spark: SparkSession, file_path: str, header: bool = True) -> DataFrame:
    """
    Read data from a CSV file
    """
    return spark.read.option("header", header).csv(file_path)

def read_parquet(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Read data from a Parquet file
    """
    return spark.read.parquet(file_path)

def read_json(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Read data from a JSON file
    """
    return spark.read.json(file_path)