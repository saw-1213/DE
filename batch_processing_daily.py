# Author: Thee Hao Siang

import json
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, to_date, hour, count, max as spark_max, lead, unix_timestamp, round

class ConfigManager:
    def __init__(self, config_file):
        with open(config_file, 'r') as file:
            self.config = json.load(file)

    def get_config(self):
        return self.config

class LibraryBatchProcessor:
    def __init__(self, config):
        self.config = config
        self.spark = SparkSession.builder \
            .appName("LibraryBatchAnalytics") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")

    def load_latest_curated_data(self):
        df = self.spark.read.parquet(self.config["HDFS_CURATED_PATH"])
        
        df_with_date = df.withColumn("record_date", to_date(col("timestamp")))
        
        max_date_row = df_with_date.select(spark_max("record_date")).collect()[0]
        latest_date = max_date_row[0]
        
        latest_df = df_with_date.filter(col("record_date") == latest_date)
        
        return latest_df

    def perform_quality_checks(self, df):
        deduplicated_df = df.dropDuplicates(["event_id"])
        
        valid_df = deduplicated_df.filter(col("student_id").isNotNull())
        
        return valid_df

    def generate_hourly_traffic_report(self, df):
        hourly_df = df.filter((col("gate_type") == "MAIN_GATE") & (col("event_type") == "ENTRY")) \
            .withColumn("report_hour", hour(col("timestamp"))) \
            .groupBy("record_date", "report_hour") \
            .agg(count("event_id").alias("total_hourly_entries")) \
            .orderBy("record_date", "report_hour")
            
        return hourly_df

    def generate_daily_room_report(self, df):
        room_df = df.filter((col("gate_type") == "ROOM_GATE") & (col("event_type") == "ENTRY")) \
            .groupBy("record_date", "location") \
            .agg(count("event_id").alias("total_room_entries")) \
            .orderBy("record_date", col("total_room_entries").desc())
            
        return room_df
    
    def generate_room_duration_report(self, df):
        window_spec = Window.partitionBy("student_id", "location").orderBy("timestamp")
        
        room_events = df.filter(col("gate_type") == "ROOM_GATE")
        
        paired_df = room_events.withColumn("exit_timestamp", lead("timestamp").over(window_spec)) \
                               .withColumn("next_event", lead("event_type").over(window_spec))
        
        visits_df = paired_df.filter((col("event_type") == "ENTRY") & (col("next_event") == "EXIT"))
        
        duration_df = visits_df.withColumn(
            "occupied_duration_minutes",
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
        
        df.write.mode("append") \
            .parquet(output_path)

    def execute_batch_pipeline(self):
        latest_curated_df = self.load_latest_curated_data()
        clean_df = self.perform_quality_checks(latest_curated_df)
        
        hourly_report = self.generate_hourly_traffic_report(clean_df)
        self.save_and_display_report(
            hourly_report, 
            self.config["HDFS_HOURLY_REPORT_PATH"], 
            "Hourly Main Gate Traffic"
        )
        
        daily_room_report = self.generate_daily_room_report(clean_df)
        self.save_and_display_report(
            daily_room_report, 
            self.config["HDFS_DAILY_ROOM_PATH"], 
            "Daily Room Usage"
        )

        room_duration_report = self.generate_room_duration_report(clean_df)
        self.save_and_display_report(
            room_duration_report, 
            self.config["HDFS_ROOM_DURATION_PATH"],
            "Room Occupancy Durations"
        )
        
        self.spark.stop()

def run_batch_job():
    config_mgr = ConfigManager('config.json')
    app_settings = config_mgr.get_config()
    
    processor = LibraryBatchProcessor(app_settings)
    processor.execute_batch_pipeline()

if __name__ == "__main__":
    run_batch_job()