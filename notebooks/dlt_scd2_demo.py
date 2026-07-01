"""
SCD Type 2 Customer Dimension Pipeline

This pipeline demonstrates SCD Type 2 implementation using:
- Auto Loader for incremental CSV ingestion (Bronze layer)
- dlt.apply_changes() for automatic history tracking (Silver layer)
"""

import dlt
from pyspark.sql.functions import col

# Source path where CSV files are stored
SOURCE_PATH = "/Volumes/workshop/scd2_demo/customer_volume/"


# ============================================================================
# BRONZE LAYER - Raw Data Ingestion
# ============================================================================

@dlt.table(
    name="bronze_customers",
    comment="Raw customer records loaded using Auto Loader"
)
def bronze_customers():
    """
    Bronze layer: Incrementally load CSV files using Auto Loader
    
    Auto Loader automatically:
    - Detects new files in the source path
    - Processes only new data (incremental)
    - Handles schema inference and evolution
    - Provides fault-tolerant processing
    """
    return (
        spark.readStream
        .format("cloudFiles")  # Auto Loader streaming source
        .option("cloudFiles.format", "csv")  # Source file format
        .option("header", "true")  # First row contains column names
        .option("inferSchema", "true")  # Automatically detect column types
        .load(SOURCE_PATH)
    )


# ============================================================================
# SILVER LAYER - SCD Type 2 Dimension
# ============================================================================

# Create the target streaming table for SCD Type 2
# This table will be populated by dlt.apply_changes() below
dlt.create_streaming_table(
    name="silver_customers",
    comment="Customer dimension table with SCD Type 2 history tracking"
)


# Apply CDC (Change Data Capture) logic with SCD Type 2
# This automatically handles inserts, updates, and history tracking
dlt.apply_changes(
    target="silver_customers",  # Destination table
    source="bronze_customers",  # Source table with changes
    keys=["customer_id"],  # Primary key to identify unique records
    sequence_by=col("last_updated"),  # Column to order changes chronologically
    stored_as_scd_type=2  # Enable SCD Type 2 history (creates __START_AT, __END_AT, __CURRENT)
)


"""
How SCD Type 2 Works:
---------------------
When a customer record changes:
1. The old record's __END_AT is set to the new record's timestamp
2. The old record's __CURRENT is set to False
3. A new record is inserted with __CURRENT=True and __END_AT=NULL
4. Both versions are preserved in the table

Query Examples:
--------------
-- Get current (active) records only
SELECT * FROM silver_customers WHERE __CURRENT = true

-- Get all historical versions for a specific customer
SELECT * FROM silver_customers WHERE customer_id = 1 ORDER BY __START_AT

-- Get records active at a specific point in time
SELECT * FROM silver_customers
WHERE __START_AT <= '2024-01-01' 
  AND (__END_AT > '2024-01-01' OR __END_AT IS NULL)
"""
