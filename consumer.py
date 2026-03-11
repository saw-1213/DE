import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# ---------------------------------------------------------
# NEW: Open the Recipe Card (config.json)
# ---------------------------------------------------------
with open("config.json", "r") as f:
    config_data = json.load(f)

# Grab ONLY the two settings the Catcher needs, ignore the rest!
my_broker = config_data["kafka_broker"]
my_topic = config_data["topic_name"]

# ---------------------------------------------------------
# 1. Turn on the Spark Factory
# ---------------------------------------------------------
spark = SparkSession.builder.appName("PlumbingTest").getOrCreate()

# 2. The Blueprint (Must match the Producer exactly)
library_schema = StructType([
    StructField("student_id", IntegerType(), True),
    StructField("gate", StringType(), True),
    StructField("action", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])

# 3. The Catching (Use the variables from the config file!)
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", my_broker) \
    .option("subscribe", my_topic) \
    .load()

# 4. The Unboxing (Turn Kafka bytes into a readable table)
parsed_stream = raw_stream \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), library_schema).alias("data")) \
    .select("data.*")

# 5. The Serving (Print the RAW data directly to the screen)
query = parsed_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()