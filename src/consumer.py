import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

class LibraryStreamProcessor:
    def __init__(self):
        with open('config.json', 'r') as config_file:
            self.config = json.load(config_file)

        self.setup_kafka_topics()

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

    def setup_kafka_topics(self):
        print("\n[Init] Checking Kafka Topics...")
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.config["kafka_broker"],
                client_id='consumer_setup'
            )
            
            topic_list = [
                NewTopic(name="main_gate_events", num_partitions=1, replication_factor=1),
                NewTopic(name="room_gate_events", num_partitions=1, replication_factor=1)
            ]
            
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            print("Topics 'main_gate_events' and 'room_gate_events' created successfully!")
            
        except TopicAlreadyExistsError:
            print("Topics already exist. Safe to proceed.")
        except Exception as e:
            print(f"Warning during topic creation: {e}")
        finally:
            if 'admin_client' in locals():
                admin_client.close()

    def read_stream(self):
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.config["kafka_broker"]) \
            .option("subscribe", self.config["topic_name"]) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", 30) \
            .load()

    def write_raw(self, df):
        raw_df = df.selectExpr("CAST(value AS STRING)")
        return raw_df.writeStream \
            .format("text") \
            .option("path", self.config["HDFS_RAW_PATH"]) \
            .option("checkpointLocation", self.config["RAW_CHECKPOINT"]) \
            .start()


    def write_curated(self, df):
        parsed_df = df.select(from_json(col("value").cast("string"), self.schema).alias("data")) \
            .select("data.*") \
            .filter(col("event_id").isNotNull())

        hdfs_query = parsed_df.writeStream \
            .format("parquet") \
            .option("path", self.config["HDFS_CURATED_PATH"]) \
            .option("checkpointLocation", self.config["CURATED_CHECKPOINT"]) \
            .start()

        return hdfs_query

    def start_pipeline(self):
        raw_stream_df = self.read_stream()

        raw_query = self.write_raw(raw_stream_df)
        hdfs_query = self.write_curated(raw_stream_df)

        print("\n==================================")
        print("Pipeline started - writing to HDFS")
        print("==================================\n")
        self.spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    processor = LibraryStreamProcessor()
    processor.start_pipeline()