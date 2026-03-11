from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# 1. Turn on the Spark Factory
spark = SparkSession.builder.appName("LibraryLiveOccupancy").getOrCreate()

# ---------------------------------------------------------
# NEW: Take away Spark's microphone! (No more walls of text)
# ---------------------------------------------------------
spark.sparkContext.setLogLevel("WARN")

# 2. The PERFECT Blueprint (Matches your JSON exactly!)
library_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("student_id", StringType(), True), # Changed to String because of quotes!
    StructField("event_type", StringType(), True), # Changed from 'action'
    StructField("gate_type", StringType(), True),  # Changed from 'gate'
    StructField("location", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])

# 3. The Catching (Read from the Kafka belt)
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "library_events") \
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