from pyspark.sql import SparkSession
from src.bronze.ingestion import load_bronze_data
from src.silver.staging import process_silver_data
from src.gold.dimensions import create_gold_dimensions
from src.gold.fact import create_gold_facts

def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("MedallionArchitecturePipeline") \
        .getOrCreate()

    # 1. Ingestion
    load_bronze_data(spark)

    # 2. Staging / Silver Layer
    process_silver_data(spark)

    # 3. Gold Layer (Dimensions & Facts)
    create_gold_dimensions(spark)
    create_gold_facts(spark)

    print("Pipeline Execution Completed.")

if __name__ == "__main__":
    main()
