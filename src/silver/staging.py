# staging module
from pyspark.sql import SparkSession

def process_silver_data(spark: SparkSession):
    """
    Reads from Bronze, applies transformations (Upper Case, Timestamps), 
    and writes to Silver.
    """
    print("Step 2: Processing Silver Data...")
    
    spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.dwh_silver")

    # 1. Apply Transformations (Data Cleaning)
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW silver_transformed AS
        SELECT *,
               upper(customer_name) AS Customer_Name_Upper,
               date(current_timestamp()) AS processed_date
        FROM bronze_view
    """)

    # 2. Write to Silver Table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS workspace.dwh_silver.silver_table
        USING DELTA
        AS SELECT * FROM silver_transformed
    """)
    
    # 3. Simple Merge (Upsert) Logic to keep Silver up to date
    spark.sql("""
        MERGE INTO workspace.dwh_silver.silver_table AS target
        USING silver_transformed AS source
        ON target.order_id = source.order_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("Silver Data Processed Successfully.")