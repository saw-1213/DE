import json
import time
from kafka import KafkaProducer
import sys
from utils.config_manager import ConfigManager

class LibraryEventProducer:
    def __init__(self, fast_mode):
        self.fast_mode = fast_mode
        
        try:
            config_manager = ConfigManager('utils/config.json')
            config_data = config_manager.get_config()
            
            self.main_topic = config_data['topic_main_gate']
            self.room_topic = config_data['topic_room_gate']
            self.data_file = config_data['input_file']
            self.sleep_time = config_data['sleep_interval']
            self.kafka_server = config_data['kafka_broker']
            
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_server,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
        except Exception as e:
            print("Error setting up producer: " + str(e))
            sys.exit(1)

    def send_events(self):
        try:
            with open(self.data_file, 'r') as file:
                events = json.load(file)

            for event in events:
                if event['gate_type'] == 'MAIN_GATE':
                    self.producer.send(self.main_topic, event)
                    print(f"Sent to {self.main_topic}: {event['event_id']}")
                elif event['gate_type'] == 'ROOM_GATE':
                    self.producer.send(self.room_topic, event)
                    print(f"Sent to {self.room_topic}: {event['event_id']}")


                if self.fast_mode == False:
                    time.sleep(self.sleep_time)
            
        except Exception as e:
            print("Error sending events: " + str(e))
        finally:
            self.producer.flush()
            print("=== ALL RECORDS SENT SUCCESSFULLY! ===")
            

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