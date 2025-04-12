# Scenario: Merging customer data with trading instrument information,
# ensuring all customers are included and handling missing data
# by replacing null values with relevant IDs.

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Step 1: Create Spark session
spark = SparkSession.builder.appName("BankingTrading").getOrCreate()

# Step 2: Define Customer Data
data_customers = [
    (1, "Jin Wei Tan"),
    (2, "Wei Jie Lim"),
    (3, "Siti Nur Aisyah"),
    (5, "Ahmad Zaki Rahman")
]
df_customers = spark.createDataFrame(data_customers, ["id", "name"])
df_customers.createOrReplaceTempView("customers")

# Step 3: Define Instrument Data
data_instruments = [
    (1, "Stocks"),
    (3, "Bonds"),
    (7, "Mutual Funds")
]
df_instruments = spark.createDataFrame(data_instruments, ["id1", "instrument"])
df_instruments.createOrReplaceTempView("instruments")

# Step 4: Perform Full Outer Join to merge both datasets
# Ensuring all records are included and null `id`s are filled from `id1`
full_join_df = (
    df_customers.join(df_instruments, df_customers["id"] == df_instruments["id1"], "full")
    .withColumn("id", expr("CASE WHEN id IS NULL THEN id1 ELSE id END"))  # handle missing IDs
    .drop("id1")  # remove duplicate ID column
    .orderBy("id")
)

# Step 5: Show the result
full_join_df.show()
