# Trace the full lineage of trading instruments (Derivatives ➝ Underlyings ➝ Root Indices)
# using PySpark self-joins for risk aggregation and compliance reporting.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize Spark session
spark = SparkSession.builder.appName("InstrumentHierarchyMapping").getOrCreate()

# Each tuple: (child_instrument_id, parent_instrument_id)
instrument_relationships = [
    ("OPT_1001", "STK_5001"),  # Option on Stock_A
    ("OPT_1002", "STK_5002"),  # Option on Stock_B
    ("OPT_1003", "STK_5003"),  # Option on Stock_C
    ("STK_5001", "IDX_NASDAQ"),  # Stock_A is part of NASDAQ Index
    ("STK_5002", "IDX_SP500"),  # Stock_B is part of S&P 500
    ("STK_5003", "IDX_DOWJONES")  # Stock_C is part of Dow Jones
]

# Step 1: Load data into DataFrame
instrument_df = spark.createDataFrame(instrument_relationships, ["instrument_id", "underlying_id"])
instrument_df.show()

# Step 2: Prepare alias for self-join to get grandparent/root index
underlying_df = instrument_df

# Rename columns for join clarity
root_index_df = (instrument_df.withColumnRenamed("instrument_id", "underlying_alias")
                 .withColumnRenamed("underlying_id", "root_index_id"))
root_index_df.show()

# Step 3: Perform inner join to build 3-level hierarchy
joined_df = underlying_df.join(root_index_df, underlying_df["underlying_id"] == root_index_df["underlying_alias"],
                               "inner")

joined_df.show()

# Step 4: Final cleanup and renaming for business clarity
hierarchy_df = joined_df.drop("underlying_alias") \
    .withColumnRenamed("instrument_id", "derivative_id") \
    .withColumnRenamed("underlying_id", "underlying_instrument_id") \
    .withColumnRenamed("root_index_id", "ultimate_underlying_id")

# Step 5: Display final hierarchy
hierarchy_df.show()
