from pyspark.sql import SparkSession

def create_gold_facts(spark: SparkSession):
    """
    Creates the Fact Table by joining Silver data with Gold Dimensions.
    """
    print("Step 4: Creating FactSales Table...")

    spark.sql("""
        CREATE OR REPLACE TABLE workspace.dwh_gold.FactSales AS
        SELECT 
            P.DimProductKey,
            C.DimCustomerKey,
            F.quantity,
            F.unit_price,
            (F.quantity * F.unit_price) as total_amount,
            F.processed_date
        FROM workspace.dwh_silver.silver_table F
        LEFT JOIN workspace.dwh_gold.DimCustomer C ON F.customer_id = C.customer_id
        LEFT JOIN workspace.dwh_gold.DimProduct P ON F.product_id = P.product_id
    """)
    
    print("FactSales Table Created Successfully.")
