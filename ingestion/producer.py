# Author: Saw Yan Xu

import json
import time
from kafka import KafkaProducer
import sys
import subprocess
from utils.config_manager import ConfigManager

class LibraryEventProducer:
    def __init__(self, fast_mode):
        self.fast_mode = fast_mode
        self.success_count = 0
        
        try:
            config_manager = ConfigManager('utils/config.json')
            config_data = config_manager.get_config()
            
            self.main_topic = config_data['topic_main_gate']
            self.room_topic = config_data['topic_room_gate']
            self.data_file = config_data['input_file']
            self.sleep_time = config_data['sleep_interval']
            self.kafka_server = config_data['kafka_broker']
            self.hdfs_raw_path = config_data['HDFS_RAW_PATH']
            
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_server,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3
            )
            print("Producer initialized with no duplication")
        except Exception as e:
            print("Error setting up producer: " + str(e))
            sys.exit(1)

    def on_send_success(self, record_metadata):
        self.success_count += 1

    def on_send_error(self, excp):
        print("Failed to deliver message to Kafka: " + str(excp))
        
    def write_raw_to_hdfs(self):
        print("\nStarting dumping raw data to HDFS...")
        
        try:
            subprocess.run(["hdfs", "dfs", "-mkdir", "-p", self.hdfs_raw_path], check=True)
            subprocess.run(["hdfs", "dfs", "-put", "-f", self.data_file, self.hdfs_raw_path], check=True)
            
            print(f"SUCCESS: Raw data safely stored in {self.hdfs_raw_path}\n")
        except subprocess.CalledProcessError as e:
            print(f"ERROR: Failed to upload raw data. Is Hadoop running? {e}\n")
    
    def send_events(self):
        try:
            self.write_raw_to_hdfs()
            
            with open(self.data_file, 'r') as file:
                events = json.load(file)

            if self.fast_mode == False:
                for event in events:
                    if event['gate_type'] == 'MAIN_GATE':
                        self.producer.send(self.main_topic, event).add_callback(self.on_send_success).add_errback(self.on_send_error)
                        print(f"Sent to {self.main_topic}: {event['event_id']}")
                    elif event['gate_type'] == 'ROOM_GATE':
                        self.producer.send(self.room_topic, event).add_callback(self.on_send_success).add_errback(self.on_send_error)
                        print(f"Sent to {self.room_topic}: {event['event_id']}")
                    time.sleep(self.sleep_time)
            else:
                for event in events:
                    if event['gate_type'] == 'MAIN_GATE':
                        self.producer.send(self.main_topic, event).add_callback(self.on_send_success).add_errback(self.on_send_error)
                    elif event['gate_type'] == 'ROOM_GATE':
                        self.producer.send(self.room_topic, event).add_callback(self.on_send_success).add_errback(self.on_send_error)

        except Exception as e:
            print("Error sending events: " + str(e))
        finally:
            self.producer.flush()
            print("=== ALL RECORDS SENT SUCCESSFULLY! ===")
            print(f"=== Total Confirmed Delivery Events: {self.success_count} ===")
            

def run_producer():
    fast_flag = False
    
    if "--fast" in sys.argv:
        fast_flag = True
        
    if fast_flag == True:
        print("=== FAST MODE ===")
    else:
        print("=== SIMULATION MODE ===")
        
    my_producer = LibraryEventProducer(fast_flag)
    my_producer.send_events()

if __name__ == "__main__":
    run_producer()