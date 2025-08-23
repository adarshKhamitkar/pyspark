from pyspark.sql import DataFrame, SparkSession
from src.main.extraction.extractor import DataExtractor

class DataTransformer:
    def __init__(self, extractor:DataExtractor):
        self.extractor = extractor

    def transform_data(self, spark: SparkSession, file_path: str) -> DataFrame:

        extracted_DF:DataFrame = self.extractor.extract_data(spark, file_path)

        return extracted_DF