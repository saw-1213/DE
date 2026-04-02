# Author: Thee Hao Siang

import json
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, to_date, hour, count, \
    lead, unix_timestamp, round, when, sum as spark_sum, \
    current_date, concat_ws, to_timestamp, date_format
from utils.config_manager import ConfigManager

class LibraryBatchProcessor:
    def __init__(self):
        config_mgr = ConfigManager('utils/config.json')
        self.config = config_mgr.get_config()

        self.spark = SparkSession.builder \
            .appName("LibraryBatchAnalytics") \
            .config("spark.ui.showConsoleProgress", "false") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")

    def load_curated_data(self):
        df = self.spark.read.parquet(self.config["HDFS_CURATED_PATH"])
        df_with_date = df.withColumnRenamed("date", "record_date")
        df_optimized = df_with_date.withColumn("timestamp", to_timestamp(concat_ws(" ", col("record_date"), col("time"))))

        return df_optimized

    def perform_quality_checks(self, df):
        deduplicated_df = df.dropDuplicates(["event_id"])
        
        valid_df = deduplicated_df.filter(
            col("student_id").isNotNull() & 
            (col("record_date") <= current_date()) 
        )
        return valid_df

    def generate_hourly_traffic_report(self, df):
        hourly_df = df.filter(col("gate_type") == "MAIN_GATE") \
            .withColumn("record_hour", hour(col("timestamp"))) \
            .groupBy("record_date", "record_hour") \
            .agg(
                spark_sum(when(col("event_type") == "ENTRY", 1).otherwise(0)).alias("total_hourly_entries"), \
                spark_sum(when(col("event_type") == "EXIT", 1).otherwise(0)).alias("total_hourly_exits") \
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
                spark_sum(when(col("event_type") == "ENTRY", 1).otherwise(0)).alias("total_entries"), \
                spark_sum(when(col("event_type") == "EXIT", 1).otherwise(0)).alias("total_exits") \
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
            col("time").alias("entry_time"),
            date_format(col("exit_timestamp"), "HH:mm:ss").alias("exit_time"),
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
        
        self.spark.stop()

if __name__ == "__main__":
    processor = LibraryBatchProcessor()
    processor.execute_batch_pipeline()