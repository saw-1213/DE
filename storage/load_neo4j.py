# Author: Tee Min Jie

import json
from pyspark.sql import SparkSession
from neo4j import GraphDatabase

class Neo4jBatchLoader:
    def __init__(self, config_file):
        with open(config_file, 'r') as file:
            self.config = json.load(file)

        self.driver = GraphDatabase.driver(
            self.config["neo4j_uri"],
            auth=(self.config["neo4j_username"], self.config["neo4j_password"])
        )

        self.spark = SparkSession.builder \
            .appName("Neo4jBatchIngestion") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")

    def load_student_dimensions(self):
        try:
            path = self.config["hdfs_student_path"]
            print(f"Reading Students from {path}...")

            df = self.spark.read.csv(path, header=True)
            records = df.collect()

            with self.driver.session() as session:
                for row in records:
                    d = row.asDict()
                    session.run("""
                        MERGE (s:Student {student_id: $student_id})
                        SET s.major = $major,
                            s.year_of_study = $year_of_study,
                            s.study_level = $study_level
                    """,
                    student_id=d['student_id'],
                    major=d['major'],
                    year_of_study=int(d['year_of_study']),
                    study_level=d['study_level']
                    )
            print(f"Loaded {len(records)} students")
        except Exception as e:
            print(f"Student Load Failed: {e}")

    def load_curated_events(self):
        try:
            path = self.config["HDFS_CURATED_PATH"]
            print(f"Reading Events from {path}...")

            df = self.spark.read.parquet(path)
            records = df.collect()

            print(f"Found {len(records)} records in HDFS")

            batch_data = [row.asDict() for row in records]

            batch_size = 5000
            total_loaded = 0

            with self.driver.session() as session:
                for i in range(0, len(batch_data), batch_size):
                    batch = batch_data[i:i+batch_size]

                    session.run("""
                        UNWIND $batch AS event
                        MERGE (e:Event {event_id: event.event_id})
                        SET e.event_type = event.event_type,
                            e.gate_type = event.gate_type,
                            e.location = event.location,
                            e.date = date(event.date),
                            e.time = time(event.time),
                            e.student_id = event.student_id
                        MERGE (s:Student {student_id: event.student_id})
                        MERGE (s)-[:PERFORMED]->(e)

                        FOREACH (ignore IN CASE WHEN event.gate_type = 'MAIN_GATE' THEN [1] ELSE [] END |
                            MERGE (l:Library {name: 'Main Library'})
                            MERGE (e)-[:AT_LIBRARY]->(l)
                            MERGE (s)-[:VISITED]->(l)
                        )

                        FOREACH (ignore IN CASE WHEN event.gate_type = 'ROOM_GATE' THEN [1] ELSE [] END |
                            MERGE (r:Room {location: event.location})
                            MERGE (e)-[:IN_ROOM]->(r)
                            MERGE (s)-[:ENTERED]->(r)
                        )
                    """, batch=batch)

                    total_loaded += len(batch)
                    print(f"Loaded {total_loaded} events...")

            print(f"Loaded {total_loaded} events")
        except Exception as e:
            print(f"Events Load Failed: {e}")

    def execute_ingestion(self):
        self.load_student_dimensions()
        self.load_curated_events()
        print("--- Neo4j Ingestion Complete ---")
        self.driver.close()
        self.spark.stop()

if __name__ == "__main__":
    loader = Neo4jBatchLoader('utils/config.json')
    loader.execute_ingestion()