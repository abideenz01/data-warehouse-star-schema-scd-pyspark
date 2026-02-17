from pyspark.sql import SparkSession

def create_gold_dimensions(spark: SparkSession):
    """
    Creates Star Schema Dimension Tables from Silver Data.
    """
    print("Step 3: Creating Gold Dimensions...")
    
    spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.dwh_gold")

    # 1. DimCustomer (Surrogate Key: DimCustomerKey)
    spark.sql("""
        CREATE OR REPLACE TABLE workspace.dwh_gold.DimCustomer AS
        SELECT 
            row_number() OVER(ORDER By customer_id) as DimCustomerKey,
            customer_id,
            customer_name,
            customer_email,
            Customer_Name_Upper
        FROM (
            SELECT DISTINCT customer_id, customer_name, customer_email, Customer_Name_Upper
            FROM workspace.dwh_silver.silver_table
        )
    """)

    # 2. DimProduct (Surrogate Key: DimProductKey)
    spark.sql("""
        CREATE OR REPLACE TABLE workspace.dwh_gold.DimProduct AS
        SELECT 
            row_number() OVER(ORDER By product_id) as DimProductKey,
            product_id,
            product_name,
            product_category
        FROM (
            SELECT DISTINCT product_id, product_name, product_category
            FROM workspace.dwh_silver.silver_table
        )
    """)
    
    print("Dimensions Created: DimCustomer, DimProduct.")
