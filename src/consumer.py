import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from neo4j import GraphDatabase

class LibraryStreamProcessor:
    def __init__(self):
        with open('config.json', 'r') as config_file:
            self.config = json.load(config_file)

        # self.neo4j_driver = GraphDatabase.driver(
        #     self.config["neo4j_uri"],
        #     auth=(self.config["neo4j_username"], self.config["neo4j_password"])
        # )

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
            .option("kafka.bootstrap.servers", self.config["kafka_broker"]) \
            .option("subscribe", self.config["topic_name"]) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()

    def write_raw(self, df):
        raw_df = df.selectExpr("CAST(value AS STRING)")
        return raw_df.writeStream \
            .format("text") \
            .option("path", self.config["HDFS_RAW_PATH"]) \
            .option("checkpointLocation", self.config["RAW_CHECKPOINT"]) \
            .start()

    # def write_to_neo4j(self, event):
    #     """Write a single event to Neo4j"""
    #     with self.neo4j_driver.session() as session:
    #         session.run(
    #             """
    #             CREATE (e:Event {
    #                 event_id: $event_id,
    #                 event_type: $event_type,
    #                 gate_type: $gate_type,
    #                 location: $location,
    #                 timestamp: $timestamp
    #             })
    #             MERGE (s:Student {student_id: $student_id})
    #             MERGE (s)-[:PERFORMED]->(e)
    #             """,
    #             event_id=event['event_id'],
    #             student_id=event['student_id'],
    #             event_type=event['event_type'],
    #             gate_type=event['gate_type'],
    #             location=event['location'],
    #             timestamp=event['timestamp']
    #         )
    #         print(f"Written to Neo4j: {event['event_id']}")

    def write_curated(self, df):
        parsed_df = df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), self.schema).alias("data"), col("topic")) \
            .select("data.*") \
            .filter(col("event_id").isNotNull())

        # def write_to_neo4j_batch(batch_df, batch_id):
        #     events = batch_df.collect()
        #     for event in events:
        #         self.write_to_neo4j(event.asDict())

        hdfs_query = parsed_df.writeStream \
            .format("parquet") \
            .partitionBy("gate_type") \
            .option("path", self.config["HDFS_CURATED_PATH"]) \
            .option("checkpointLocation", self.config["CURATED_CHECKPOINT"]) \
            .start()

        # neo4j_query = parsed_df.writeStream \
        #     .foreachBatch(write_to_neo4j_batch) \
        #     .start()

        return hdfs_query #, neo4j_query

    def start_pipeline(self):
        raw_stream_df = self.read_stream()

        raw_query = self.write_raw(raw_stream_df)
        hdfs_query = self.write_curated(raw_stream_df)

        print("Pipeline started - writing to both HDFS and Neo4j")
        self.spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    processor = LibraryStreamProcessor()
    processor.start_pipeline()