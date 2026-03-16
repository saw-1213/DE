# Author: [Insert Your Name Here]

import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, hour, count, current_timestamp

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

    def load_curated_data(self):
        return self.spark.read.parquet(self.config["HDFS_CURATED_PATH"])

    def perform_quality_checks(self, df):
        deduplicated_df = df.dropDuplicates(["event_id"])
        
        valid_df = deduplicated_df.filter(
            col("student_id").isNotNull() & 
            (col("timestamp") <= current_timestamp())
        )
        return valid_df

    def generate_hourly_traffic_report(self, df):
        hourly_df = df.filter((col("gate_type") == "MAIN_GATE") & (col("event_type") == "ENTRY")) \
            .withColumn("report_date", to_date(col("timestamp"))) \
            .withColumn("report_hour", hour(col("timestamp"))) \
            .groupBy("report_date", "report_hour") \
            .agg(count("event_id").alias("total_hourly_entries")) \
            .orderBy("report_date", "report_hour")
            
        return hourly_df

    def generate_daily_room_report(self, df):
        room_df = df.filter((col("gate_type") == "ROOM_GATE") & (col("event_type") == "ENTRY")) \
            .withColumn("report_date", to_date(col("timestamp"))) \
            .groupBy("report_date", "location") \
            .agg(count("event_id").alias("total_room_entries")) \
            .orderBy("report_date", col("total_room_entries").desc())
            
        return room_df

    def save_and_display_report(self, df, output_path, report_name):
        print(f"--- Displaying {report_name} ---")
        df.show(20, truncate=False)
        
        df.write.mode("overwrite") \
            .parquet(output_path)

    def execute_batch_pipeline(self):
        raw_curated_df = self.load_curated_data()
        clean_df = self.perform_quality_checks(raw_curated_df)
        
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
        
        self.spark.stop()

def run_batch_job():
    config_mgr = ConfigManager('config.json')
    app_settings = config_mgr.get_config()
    
    processor = LibraryBatchProcessor(app_settings)
    processor.execute_batch_pipeline()

if __name__ == "__main__":
    run_batch_job()