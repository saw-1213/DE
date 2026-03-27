import subprocess
import time
import sys

def run_command_blocking(command, step_name):
    print(f"\n[{step_name}] Starting...")
    print(f"Executing: {command}")
    
    process = subprocess.run(command, shell=True)
    
    if process.returncode != 0:
        print(f"\nERROR: [{step_name}] failed. Stopping pipeline.")
        sys.exit(1)
    
    print(f"[{step_name}] Completed Successfully.")

def run_pipeline():
    is_fast_mode = "--fast" in sys.argv
    speed_label = "FAST MODE" if is_fast_mode else "SIMULATION MODE"

    print("==================================================")
    print(f"         STARTING PIPELINE ({speed_label})       ")
    print("==================================================")
    print("Ensure HDFS, Zookeeper, and Kafka are already running!\n")
    time.sleep(3)

    student_cmd = "python utils/upload_students.py"
    consumer_cmd = "PYTHONPATH=. spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1 ingestion/consumer.py 2> logs.txt"
    producer_cmd = "PYTHONPATH=. python ingestion/producer.py --fast" if is_fast_mode else "PYTHONPATH=. python ingestion/producer.py"
    batch_cmd = "PYTHONPATH=. spark-submit processing/batch_processing.py"
    neo4j_cmd = "PYTHONPATH=. spark-submit storage/load_neo4j.py"

    consumer_process = None

    try:
        run_command_blocking(student_cmd, "Uploading static student file...")

        print("\n[Phase 1] Starting Live Stream Consumer (Background)...")
        consumer_process = subprocess.Popen(consumer_cmd, shell=True)
        print("Waiting 20 seconds for Spark to initialize...")
        time.sleep(20)
        print("Consumer is listening.")

        run_command_blocking(producer_cmd, "Phase 2: Kafka Producer")

        wait_time = 60 if is_fast_mode else 30
        print(f"\nWaiting {wait_time} seconds for Consumer to finish writing to HDFS...")
        time.sleep(wait_time)

        run_command_blocking(batch_cmd, "Phase 3: Batch Aggregation")

        run_command_blocking(neo4j_cmd, "Phase 4: Neo4j Ingestion")

        print("\n==================================================")
        print("             FULL PIPELINE RUN COMPLETE          ")
        print("==================================================")

    except KeyboardInterrupt:
        print("\nPipeline forcefully interrupted by user.")
        
    finally:
        if consumer_process:
            print("\nCleaning up: Terminating the background Consumer process...")
            consumer_process.terminate()
            consumer_process.wait()
            print("Consumer safely shut down.")

if __name__ == "__main__":
    run_pipeline()