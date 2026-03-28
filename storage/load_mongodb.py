# Author: Yap Wan Shuen

from pyspark.sql import SparkSession
from pymongo import MongoClient
import json


class MongoLoader:
    def __init__(self, config_path):
        with open(config_path) as f:
            self.config = json.load(f)

        self.spark = SparkSession.builder \
            .appName("LoadToMongoDB_Final") \
            .getOrCreate()

        self.client = MongoClient(self.config["mongodb_uri"])
        self.db = self.client["library_db"]


    def load_students(self):
        try:
            path = self.config["hdfs_student_path"]
            print(f"Reading Students from {path}...")

            df = self.spark.read.csv(path, header=True)
            data = [json.loads(row) for row in df.toJSON().collect()]

            collection = self.db["students"]
            collection.delete_many({})
                        
            if data:
                collection.insert_many(data)
                print(f"{len(data)} Students migrated to MongoDB!")
            else:
                print("No student data found.")

        except Exception as e:
            print(f"Student Load Failed: {e}")

    def load_events(self):
        try:
            path = self.config["HDFS_CURATED_PATH"]
            print(f"Reading Events from {path}...")

            df = self.spark.read.parquet(path)
            data = [json.loads(row) for row in df.toJSON().collect()]

            collection = self.db["library_events"]
            collection.delete_many({})

            if data:
                collection.insert_many(data)
                print(f"{len(data)} Events migrated to MongoDB!")
            else:
                print("No event data found.")

        except Exception as e:
            print(f"Events Load Failed: {e}")

    def run(self):
        self.load_students()
        self.load_events()
        print("\nFull Migration Complete!")
        self.spark.stop()


if __name__ == "__main__":
    loader = MongoLoader("config.json")
    loader.run()