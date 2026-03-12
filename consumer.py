import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

class LibraryStreamProcessor:
    def __init__(self):
        with open('config.json', 'r') as config_file:
            self.config = json.load(config_file)

        self.spark = SparkSession.builder \
            .appName("LibraryLiveOccupancy") \
            .getOrCreate()
            
        self.spark.sparkContext.setLogLevel("WARN")

        self.schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("student_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("gate_type", StringType(), True),
            StructField("location", StringType(), True),
            StructField("timestamp", TimestampType(), True)
        ])

    def read_stream(self):
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.config["KAFKA_BROKER"]) \
            .option("subscribe", self.config["TOPIC_NAME"]) \
            .option("startingOffsets", "latest") \
            .load()

    def write_raw(self, df):
        raw_df = df.selectExpr("CAST(value AS STRING)")
        return raw_df.writeStream \
            .format("text") \
            .option("path", self.config["HDFS_RAW_PATH"]) \
            .option("checkpointLocation", self.config["RAW_CHECKPOINT"]) \
            .start()

    def write_curated(self, df):
        parsed_df = df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), self.schema).alias("data")) \
            .select("data.*") \
            .filter(col("event_id").isNotNull())

        return parsed_df.writeStream \
            .format("parquet") \
            .option("path", self.config["HDFS_CURATED_PATH"]) \
            .option("checkpointLocation", self.config["CURATED_CHECKPOINT"]) \
            .start()

    def start_pipeline(self):
        raw_stream_df = self.read_stream()
        
        raw_query = self.write_raw(raw_stream_df)
        curated_query = self.write_curated(raw_stream_df)
        
        self.spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    processor = LibraryStreamProcessor()
    processor.start_pipeline()