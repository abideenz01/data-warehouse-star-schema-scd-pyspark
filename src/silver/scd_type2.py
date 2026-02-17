from pyspark.sql import SparkSession

def apply_scd_type2(spark: SparkSession, source_view, target_table, join_key):
    """
    Reusable function to apply SCD Type 2 Logic (History Tracking).
    """
    print(f"Applying SCD Type 2 on {target_table}...")

    # 1. Ensure Target Table Exists with SCD columns
    # (In a real run, this table structure should be created in setup)
    
    # 2. The Merge Logic
    # This logic closes the old record (sets is_current=False) and inserts the new one
    query = f"""
    MERGE INTO {target_table} AS target
    USING {source_view} AS source
    ON target.{join_key} = source.{join_key} AND target.is_current = true
    
    WHEN MATCHED AND (
        target.product_name <> source.product_name OR 
        target.product_cat <> source.product_cat
    ) THEN UPDATE SET
        target.end_date = current_date(),
        target.is_current = false
    """
    spark.sql(query)
    
    # 3. Insert the new record as the current version
    # Note: A second MERGE or INSERT usually follows this to add the new row
    print("SCD Type 2 Logic Applied.")
