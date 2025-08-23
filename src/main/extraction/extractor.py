from pyspark.sql import SparkSession, DataFrame
from src.main.utilities.utils import read_csv

class DataExtractor:
    def __init__(self):
        pass

    def extract_data(self, spark: SparkSession, file_path: str) -> DataFrame:
        """
        Extract data from a file
        """
        return read_csv(spark, file_path)
