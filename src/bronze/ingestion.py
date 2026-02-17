# ingestion module
from pyspark.sql import SparkSession

def load_bronze_data(spark: SparkSession):
    """
    Creates the Bronze schema and source table, simulating data ingestion.
    """
    print("Step 1: Ingesting Bronze Data...")
    
    # 1. Setup Schema
    spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.dwh_bronze")
    
    # 2. Create Source Table (Simulating an external data source)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS workspace.dwh_bronze.source_data ( 
            order_id INT,
            order_date DATE,
            customer_id INT,
            customer_name VARCHAR(100),
            customer_email VARCHAR (100),
            product_id INT,
            product_name VARCHAR(100),
            product_category VARCHAR(100),
            quantity INT,
            unit_price DECIMAL(10,2),
            payment_type VARCHAR (100),
            country VARCHAR(100),
            last_updated DATE
        )
    """)

    # 3. Simulate Data Load (Idempotent: won't duplicate if run twice)
    # In production, this would be spark.read.parquet(...)
    current_count = spark.table("workspace.dwh_bronze.source_data").count()
    if current_count == 0:
        spark.sql("""
            INSERT INTO workspace.dwh_bronze.source_data VALUES
            (1001, '2024-07-01', 1, 'Alice Johnson', 'alice@gmail.com', 501, 'iPhone 14', 'Electronics', 1, 999.99, 'Credit Card', 'USA', '2024-07-01'),
            (1002, '2024-07-01', 2, 'Bob Smith', 'bob@yahoo.com', 502, 'AirPods Pro', 'Electronics', 2, 499.99, 'PayPal', 'USA', '2024-07-01'),
            (1003, '2024-07-01', 3, 'Charlie Brown', 'charlie@outlook.com', 503, 'Nike Shoes', 'Footwear', 1, 129.99, 'Credit Card', 'Canada', '2024-07-01'),
            (1004, '2024-07-02', 4, 'David Lee', 'david@abc.com', 504, 'Samsung S23', 'Electronics', 1, 899.99, 'Credit Card', 'USA', '2024-07-02'),
            (1005, '2024-07-02', 1, 'Alice Johnson', 'alice@gmail.com', 503, 'Nike Shoes', 'Footwear', 2, 129.99, 'Credit Card', 'USA', '2024-07-02')
        """)
    
    # 4. Create a View for downstream consumption
    spark.sql("CREATE OR REPLACE TEMP VIEW bronze_view AS SELECT * FROM workspace.dwh_bronze.source_data")
    print("Bronze Data Loaded Successfully.")