#Scenario :Find traders with same trade amount but different trader IDs
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Set up Spark
conf = SparkConf().setMaster("local[*]").setAppName("TradingScenario")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()

# Trading data
trading_data = [
    ("T001", "Wei", "Tan", 100000, "2022-01-10 09:00:00", "Equities"),
    ("T002", "Nurul", "Aminah", 300000, "2022-01-11 09:00:00", "Bonds"),
    ("T003", "Jia", "Lim", 300000, "2022-01-10 09:00:00", "Equities"),
    ("T004", "Zhi", "Chen", 500000, "2022-01-12 09:00:00", "Derivatives"),
    ("T005", "Mei", "Chong", 500000, "2022-01-13 09:00:00", "Bonds")
]
# Define schema
schema = ["trader_id", "first_name", "last_name", "trade_amount", "trade_date", "asset_class"]

# Create DataFrame
trading_df = spark.createDataFrame(trading_data, schema=schema)
trading_df.show()

# SQL: Find traders with same trade amount but different trader IDs
trading_df.createOrReplaceTempView("trading_table")
spark.sql("""select a.trader_id,a.first_name,a.last_name,a.trade_amount,a.trade_date,a.asset_class
         from trading_table a,trading_table b where a.trade_amount = b.trade_amount AND a.trader_id != b.trader_id""").show()

# DSL equivalent
final_df = trading_df.alias("a") \
    .join(trading_df.alias("b"), (col("a.trade_amount") == col("b.trade_amount")) & (col("a.trader_id") != col("b.trader_id")), "inner") \
    .select(
    col("a.trader_id"),
    col("a.first_name"),
    col("a.last_name"),
    col("a.trade_amount"),
    col("a.trade_date"),
    col("a.asset_class")
)

final_df.show()