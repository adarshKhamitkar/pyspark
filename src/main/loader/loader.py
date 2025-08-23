from pyspark.sql import DataFrame, SparkSession
from src.main.transformation.transformer import DataTransformer
from src.main.utilities.utils import save_as_parquet

class DataLoader:
    def __init__(self, transformer: DataTransformer):
        self.transformer = transformer

    def load_data(self, spark: SparkSession, file_path: str, output_path: str):
        transformed_df:DataFrame = self.transformer.transform_data(spark, file_path)

        save_as_parquet(transformed_df, output_path)