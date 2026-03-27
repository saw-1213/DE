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

    def clear_database(self):
        print("--- Clearing existing Neo4j database ---")
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")

    def load_student_dimensions(self):
        print("--- Loading Student Dimensions into Neo4j ---")
        df = self.spark.read.csv(self.config['hdfs_student_path'], header=True)
        records = df.collect()
        
        with self.driver.session() as session:
            session.run("""
                UNWIND $records AS row
                MERGE (s:Student {student_id: row.student_id})
                SET s.major = row.major,
                    s.year_of_study = toInteger(row.year_of_study)
            """, records=[r.asDict() for r in records])

    def load_curated_events(self):
        print("--- Loading Curated Streaming Events from HDFS ---")
        df = self.spark.read.parquet(self.config['HDFS_CURATED_PATH'])
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

    def load_batch_durations(self):
        print("--- Loading Batch Room Durations from HDFS ---")
        df = self.spark.read.parquet(self.config['HDFS_ROOM_DURATION_PATH'])
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

    def execute_ingestion(self):
        try:
            self.clear_database()
            self.load_student_dimensions()
            self.load_curated_events()
            self.load_batch_durations()
            print("--- Neo4j Ingestion Complete ---")
        finally:
            self.driver.close()
            self.spark.stop()

if __name__ == "__main__":
    loader = Neo4jBatchLoader('utils/config.json')
    loader.execute_ingestion()