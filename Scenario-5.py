#Scenario : Generate all possible combinations of trading exchanges and order types to help configure trading systems

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Create a Spark session
spark = SparkSession.builder.appName("CrossJoinScenario").getOrCreate()

# Table 1: Trading Exchanges with professional variable names
exchanges = [
    (1, "NYSE"),
    (2, "NASDAQ"),
    (3, "LSE")
]
# Create Dataframe
exchanges_df = spark.createDataFrame(exchanges, ["exchange_id", "exchange_name"])

# Table 2: Order Types with a unique ID and order type name
order_types = [
    (1, "Market Order"),
    (2, "Limit Order"),
    (3, "Stop Order")
]
order_df = spark.createDataFrame(order_types, ["order_type_id", "order_type"])

# Perform a cross join to generate all combinations of exchanges and order types
result_df = exchanges_df.crossJoin(order_df)
result_df.show()
