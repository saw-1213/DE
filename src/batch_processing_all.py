# Author: Thee Hao Siang

import json
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, to_date, hour, count, \
    lead, unix_timestamp, round, current_timestamp, when, sum as spark_sum
from neo4j import GraphDatabase

class ConfigManager:
    def __init__(self, config_file):
        with open(config_file, 'r') as file:
            self.config = json.load(file)

    def get_config(self):
        return self.config
    
class Neo4jDataIngestor:
    def __init__(self, config):
        self.config = config
        self.driver = GraphDatabase.driver(
            self.config["neo4j_uri"],
            auth=(self.config["neo4j_username"], self.config["neo4j_password"])
        )

    def clear_database(self):
        print("--- Clearing existing Neo4j database ---")
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")

    def load_student_dimensions(self, spark):
        print("--- Loading Student Dimensions into Neo4j ---")
        df = spark.read.csv(self.config['local_student_path'], header=True)
        records = df.collect()
        
        with self.driver.session() as session:
            session.run("""
                UNWIND $records AS row
                MERGE (s:Student {student_id: row.student_id})
                SET s.major = row.major,
                    s.year_of_study = toInteger(row.year_of_study)
            """, records=[r.asDict() for r in records])

    def load_curated_events(self, df):
        print("--- Loading Curated Streaming Events into Neo4j ---")
        records = df.collect()
        
        formatted_records = []
        for r in records:
            d = r.asDict()
            d['timestamp'] = d['timestamp'].isoformat() if d['timestamp'] else None
            formatted_records.append(d)

        with self.driver.session() as session:
            session.run("""
                UNWIND $records AS event
                MERGE (l:Library {name: 'Main Library'})
                MERGE (s:Student {student_id: event.student_id})
                MERGE (e:Event {event_id: event.event_id})
                SET e.event_type = event.event_type,
                    e.gate_type = event.gate_type,
                    e.location = event.location,
                    e.timestamp = datetime(event.timestamp)

                MERGE (s)-[:PERFORMED]->(e)

                FOREACH (ignore IN CASE WHEN event.gate_type = 'MAIN_GATE' THEN [1] ELSE [] END |
                    MERGE (e)-[:AT_LIBRARY]->(l)
                )

                FOREACH (ignore IN CASE WHEN event.gate_type = 'ROOM_GATE' THEN [1] ELSE [] END |
                    MERGE (r:Room {location: event.location})
                    MERGE (e)-[:IN_ROOM]->(r)
                    MERGE (s)-[:ENTERED]->(r)
                )
            """, records=formatted_records)

    def load_batch_durations(self, df):
        print("--- Loading Batch Room Durations into Neo4j ---")
        records = df.collect()
        
        formatted_records = []
        for r in records:
            d = r.asDict()
            d['record_date'] = str(d['record_date'])
            d['entry_time'] = d['entry_time'].isoformat() if d['entry_time'] else None
            d['exit_time'] = d['exit_time'].isoformat() if d['exit_time'] else None
            formatted_records.append(d)

        with self.driver.session() as session:
            session.run("""
                UNWIND $records AS session_data
                MERGE (s:Student {student_id: session_data.student_id})
                MERGE (r:Room {location: session_data.room_id})
                MERGE (s)-[study:STUDIED_IN {date: session_data.record_date}]->(r)
                SET study.duration_minutes = session_data.occupied_minutes,
                    study.entry_time = datetime(session_data.entry_time),
                    study.exit_time = datetime(session_data.exit_time)
            """, records=formatted_records)
            
    def close(self):
        self.driver.close()

class LibraryBatchProcessor:
    def __init__(self, config):
        self.config = config
        self.spark = SparkSession.builder \
            .appName("LibraryBatchAnalytics") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")

    def load_curated_data(self):
        df = self.spark.read.parquet(self.config["HDFS_CURATED_PATH"])
        df_with_date = df.withColumn("record_date", to_date(col("timestamp")))

        return df_with_date

    def perform_quality_checks(self, df):
        deduplicated_df = df.dropDuplicates(["event_id"])
        
        valid_df = deduplicated_df.filter(
            col("student_id").isNotNull() & 
            (col("timestamp") <= current_timestamp())
        )
        return valid_df

    def generate_hourly_traffic_report(self, df):
        hourly_df = df.filter(col("gate_type") == "MAIN_GATE") \
            .withColumn("record_hour", hour(col("timestamp"))) \
            .groupBy("record_date", "record_hour") \
            .agg(
                spark_sum(when(col("event_type" == "ENTRY", 1).otherwise(0))).alias("total_hourly_entries"), \
                spark_sum(when(col("event_type" == "EXIT", 1).otherwise(0))).alias("total_hourly_exits") \
            ) \
            .orderBy("record_date", "record_hour")
            
        return hourly_df

    def generate_daily_room_report(self, df):
        room_df = df.filter((col("gate_type") == "ROOM_GATE") & (col("event_type") == "ENTRY")) \
            .groupBy("record_date", "location") \
            .agg(count("event_id").alias("total_room_entries")) \
            .orderBy("record_date", col("total_room_entries").desc())
            
        return room_df
    
    def generate_hourly_room_usage_report(self, df):
        pax_df = df.filter(col("gate_type") == "ROOM_GATE") \
            .withColumn("record_hour", hour(col("timestamp"))) \
            .groupBy("record_date", "location", "record_hour") \
            .agg(
                spark_sum(when(col("event_type" == "ENTRY", 1).otherwise(0))).alias("total_entries"), \
                spark_sum(when(col("event_type" == "EXIT", 1).otherwise(0))).alias("total_exits") \
            ) \
            .select(
                col("record_date"),
                col("location").alias("room_id"),
                col("record_hour"),
                col("total_entries"),
                col("total_exits")
            ).orderBy("record_date", "room_id", "record_hour")
            
        return pax_df
    
    def generate_room_duration_report(self, df):
        window_spec = Window.partitionBy("student_id", "location").orderBy("timestamp")
        
        room_events = df.filter(col("gate_type") == "ROOM_GATE")
        
        paired_df = room_events.withColumn("exit_timestamp", lead("timestamp").over(window_spec)) \
                               .withColumn("next_event", lead("event_type").over(window_spec))
        
        visits_df = paired_df.filter((col("event_type") == "ENTRY") & (col("next_event") == "EXIT"))
        
        duration_df = visits_df.withColumn(
            "occupied_minutes",
            round((unix_timestamp(col("exit_timestamp")) - unix_timestamp(col("timestamp"))) / 60, 2)
        )
        
        final_report = duration_df.select(
            col("record_date"),
            col("location").alias("room_id"),
            col("student_id"),
            col("timestamp").alias("entry_time"),
            col("exit_timestamp").alias("exit_time"),
            col("occupied_minutes")
        ).orderBy("record_date", "room_id", "entry_time")
        
        return final_report

    def save_and_display_report(self, df, output_path, report_name):
        print(f"--- Displaying {report_name} ---")
        df.show(20, truncate=False)
        
        df.write.mode("overwrite").parquet(output_path)

    def execute_batch_pipeline(self):
        raw_curated_df = self.load_curated_data()
        clean_df = self.perform_quality_checks(raw_curated_df)
        
        hourly_report = self.generate_hourly_traffic_report(clean_df)
        daily_room_report = self.generate_daily_room_report(clean_df)
        hourly_room_usage_report = self.generate_hourly_room_usage_report(clean_df)
        room_duration_report = self.generate_room_duration_report(clean_df)

        self.save_and_display_report(
            hourly_report, 
            self.config["HDFS_HOURLY_GATE_PATH"], 
            "Hourly Main Gate Traffic"
        )
        
        self.save_and_display_report(
            daily_room_report, 
            self.config["HDFS_DAILY_ROOM_PATH"], 
            "Daily Room Usage"
        )

        self.save_and_display_report(
            hourly_room_usage_report,
            self.config["HDFS_HOURLY_ROOM_PATH"],
            "Hourly Room Usage"
        )
        
        self.save_and_display_report(
            room_duration_report, 
            self.config["HDFS_ROOM_DURATION_PATH"],
            "Room Occupancy Durations"
        )

        neo4j_loader = Neo4jDataIngestor(self.config)
        neo4j_loader.clear_database()
        neo4j_loader.load_student_dimensions(self.spark)
        neo4j_loader.load_curated_events(clean_df)
        neo4j_loader.load_batch_durations(room_duration_report)
        neo4j_loader.close()
        
        print("--- Batch Pipeline Execution Complete ---")
        self.spark.stop()

def run_batch_job():
    config_mgr = ConfigManager('config.json')
    app_settings = config_mgr.get_config()
    
    processor = LibraryBatchProcessor(app_settings)
    processor.execute_batch_pipeline()

if __name__ == "__main__":
    run_batch_job()