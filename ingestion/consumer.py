# Author: Nga Zhi Ier

import json
from utils.config_manager import ConfigManager
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, date_format
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from pathlib import Path

class LibraryStreamProcessor:
    def __init__(self):
        config_mgr = ConfigManager('utils/config.json')
        self.config = config_mgr.get_config()

        self.setup_kafka_topics()

        self.spark = SparkSession.builder \
            .appName("LibraryLiveOccupancy") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("ERROR")

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

    def write_curated(self, transform_df):
        return transform_df.writeStream \
            .format("parquet") \
            .option("path", self.config["HDFS_CURATED_PATH"]) \
            .option("checkpointLocation", self.config["CURATED_CHECKPOINT"]) \
            .start()
    
    def write_batch(self, good_df):
        return good_df.writeStream \
            .format("console") \
            .option("truncate", "false") \
            .start()
    
    def write_corrupted(self, bad_df):
        return bad_df.selectExpr("raw_json AS corrupted_record").writeStream \
            .format("json") \
            .option("path", self.config["LOCAL_CORRUPTED_PATH"]) \
            .option("checkpointLocation", self.config["LOCAL_CORRUPTED_CHECKPOINT"]) \
            .start()
    
    def quality_check(self, df):
        checked_stream = df \
            .withColumn("raw_json", col("value").cast("string")) \
            .withColumn("parsed_data", from_json(col("raw_json"), self.schema))
        
        bad_df = checked_stream.filter(
            col("parsed_data").isNull() | col("parsed_data.event_id").isNull()
        )

        good_df = checked_stream.filter(
            col("parsed_data").isNotNull() & col("parsed_data.event_id").isNotNull()
        )

        return good_df, bad_df
    
    def transform_features(self, good_df):
        transformed_df = good_df.select("parsed_data.*") \
            .withColumn("date", to_date(col("timestamp"))) \
            .withColumn("time", date_format(col("timestamp"), "HH:mm:ss")) \
            .drop("timestamp")
        
        return transformed_df

    def start_pipeline(self):
        raw_stream_df = self.read_stream()
        raw_stream_df.printSchema()

        good_df, bad_df = self.quality_check(raw_stream_df)
        
        transform_df = self.transform_features(good_df)

        bad_query = self.write_corrupted(bad_df)
        curated_query = self.write_curated(transform_df)
        console_query = self.write_batch(transform_df)

        print("\n==================================")
        print("Pipeline started - writing to HDFS")
        print("==================================\n")

        while True:
            kill_switch = Path("/home/student/library/STOP_CONSUMER.txt")
            
            if kill_switch.exists():
                print("\nKill switch detected! Shutting down streams gracefully...")
                for stream in self.spark.streams.active:
                    stream.stop()
                    
                kill_switch.unlink() 
                break
            
            self.spark.streams.awaitAnyTermination(timeout=5)

if __name__ == "__main__":
    processor = LibraryStreamProcessor()
    processor.start_pipeline()