# Scenario: Identify mismatched or missing trading orders between OMS and EMS systems

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Step 1: Spark Setup
conf = SparkConf().setMaster("local[*]").setAppName("OrderReconciliation")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()

# Step 2: Define OMS Data (Source)
oms_data = [
    (101, "AAPL"),
    (102, "GOOG"),
    (103, "TSLA"),
    (104, "MSFT")
]
oms_df = spark.createDataFrame(oms_data, ["order_id", "instrument"])

# Step 3: Define EMS Data (Target)
ems_data = [
    (101, "AAPL"),
    (102, "GOOG"),
    (104, "NFLX"),
    (105, "AMZN")
]
ems_df = spark.createDataFrame(ems_data, ["order_id", "instrument_ems"])

# Step 4: Perform Full Outer Join on order_id to find discrepancies
joined_df = oms_df.join(ems_df, ["order_id"], "full")

# Step 5: Identify Matching and Mismatched Orders
status_df = joined_df.withColumn(
    "status",
    expr("CASE WHEN instrument = instrument_ems THEN 'matched' ELSE 'mismatched' END")
)

# Step 6: Filter Only Mismatched Records
mismatched_df = status_df.filter("status = 'mismatched'")

# Step 7: Add Comments for Missing Orders in Either Source
final_df = mismatched_df.withColumn(
    "status",
    expr("""
        CASE 
            WHEN instrument IS NULL THEN 'New In Target'
            WHEN instrument_ems IS NULL THEN 'New In Source'
            ELSE status 
        END
    """)
).drop("instrument", "instrument_ems") \
    .withColumnRenamed("status", "comment")

# Step 8: Show Final Result
final_df.show()
