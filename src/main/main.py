from pyspark.sql import SparkSession
from src.main.extraction.extractor import DataExtractor
from src.main.transformation.transformer import DataTransformer
from src.main.loader.loader import DataLoader
import time

def create_spark_session(app_name: str = "PySpark Data Pipeline") -> SparkSession:
    """
    Create and return a Spark session
    """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.ui.enabled", "true") \
        .config("spark.ui.retainedJobs", "10000") \
        .config("spark.ui.retainedStages", "10000") \
        .config("spark.history.ui.port", "18080") \
        .getOrCreate()

def main():
    # Initialize Spark Session
    spark = create_spark_session()

    try:
        
        DataLoader(
            DataTransformer(
                DataExtractor()
            )
        ).load_data(spark, 
                    "src/resources/input/", 
                    "src/resources/output/")
        
        time.sleep(300)
    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Stop Spark session
        print("Spark session stopped")
        spark.stop()

if __name__ == "__main__":
    main()