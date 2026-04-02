import multiprocessing
import time
import sys
from pathlib import Path

from utils.upload_students import execute_upload
from ingestion.consumer import LibraryStreamProcessor
from ingestion.producer import LibraryEventProducer
from processing.batch_processing import LibraryBatchProcessor
from storage.load_neo4j import Neo4jBatchLoader
from storage.load_mongodb import MongoLoader

def start_background_consumer():
    processor = LibraryStreamProcessor()
    processor.start_pipeline()

def start_background_producer(is_fast):
    producer = LibraryEventProducer(fast_mode=is_fast)
    producer.send_events()

def run_pipeline():
    try:
        is_fast_mode = "--fast" in sys.argv
        is_full_pipeline = "--db" in sys.argv

        speed_label = "FAST MODE" if is_fast_mode else "SIMULATION MODE"
        print("==================================================")
        print(f"         STARTING PIPELINE ({speed_label})       ")
        print("==================================================")
        print("Ensure HDFS, Zookeeper, and Kafka are already running!\n")

        print("Uploading static student file...")
        execute_upload()

        print("\n[Phase 1] Starting Live Streaming Consumer (Background)...")
        consumer_process = multiprocessing.Process(target=start_background_consumer)
        consumer_process.start()

        print("Waiting 20 seconds for Spark to initialize...")
        time.sleep(20)
        print("Consumer is listening.")

        print("\n[Phase 2] Kafka Producer")
        producer_process = multiprocessing.Process(
            target=start_background_producer, 
            args=(is_fast_mode)
        )
        producer_process.start()

        if is_fast_mode:
            wait_time = 60
        else:
            wait_time = 30
        print(f"\nWaiting {wait_time} seconds for Consumer to write to HDFS...")
        time.sleep(wait_time)

        print("\nTriggering Consumer Shutdown")
        with open("STOP_CONSUMER.txt", "w") as f:
            f.write("stop")
        time.sleep(6)
        print("Consumer safely shut down.")

        consumer_process.terminate()
        consumer_process.join()

        print("\n[Phase 3] Batch Aggregation")
        batch_process = LibraryBatchProcessor()
        batch_process.execute_batch_pipeline()

        if is_full_pipeline:
            print("\n[Phase 4] Loading Data into Neo4j")
            neo4j = Neo4jBatchLoader("utils/config.json")
            neo4j.execute_ingestion()

            print("\n[Phase 5] Loading Data into Mongodb")
            mongodb = MongoLoader("utils/config.json")
            mongodb.run()


    except KeyboardInterrupt:
        print("\nPipeline forcefully interrupted by user...")

    finally:
        with open("STOP_CONSUMER.txt", "w") as f:
            f.write("stop")     
        time.sleep(6)

        if "consumer_process" in locals() and consumer_process.is_alive():
            print("\nTerminating Kafka Consumer...")
            consumer_process.terminate()
            consumer_process.join()
            
        if "producer_process" in locals() and producer_process.is_alive():
            print("\nTerminating Kafka Producer...")
            producer_process.terminate()
            producer_process.join()
 
        print("\nPipeline shut down")

        kill_file = Path("STOP_CONSUMER.txt")
        if kill_file.exists():
            kill_file.unlink()

if __name__ == "__main__":
    run_pipeline()