import json
import time
from kafka import KafkaProducer
import sys

class LibraryEventProducer:
    def __init__(self, config_file):
        with open(config_file, 'r') as file:
            config = json.load(file)

        self.topic_main = config['topic_main_gate']
        self.topic_room = config['topic_room_gate']
        self.input_file = config['input_file']
        self.delay = config['sleep_interval']
      
        self.producer = KafkaProducer(
            bootstrap_servers=config['kafka_broker'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def send_events(self):
        with open(self.input_file, 'r') as file:
            events = json.load(file)

        for event in events:
            if event['gate_type'] == 'MAIN_GATE':
                self.producer.send(self.topic_main, event)
                print(f"Sent to {self.topic_main}: {event['event_id']}")
            elif event['gate_type'] == 'ROOM_GATE':
                self.producer.send(self.topic_room, event)
                print(f"Sent to {self.topic_room}: {event['event_id']}")
            print(event)
            time.sleep(self.delay)

class LibraryEventProducerFast:
    def __init__(self, config_file):
        with open(config_file, 'r') as file:
            config = json.load(file)

        self.topic_main = config['topic_main_gate']
        self.topic_room = config['topic_room_gate']
        self.input_file = config['input_file']
      
        self.producer = KafkaProducer(
            bootstrap_servers=config['kafka_broker'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def send_events(self):
        with open(self.input_file, 'r') as file:
            events = json.load(file)

        for event in events:
            if event['gate_type'] == 'MAIN_GATE':
                self.producer.send(self.topic_main, event)
                print(f"Sent to {self.topic_main}: {event['event_id']}")
            elif event['gate_type'] == 'ROOM_GATE':
                self.producer.send(self.topic_room, event)
                print(f"Sent to {self.topic_room}: {event['event_id']}")
            print(event)

        self.producer.flush()
        print("=== ALL RECORDS SENT SUCCESSFULLY! ===")

def run_producer():
    if "--fast" in sys.argv:
        print("=== FAST MODE ===")
        producer = LibraryEventProducerFast('config.json')
    else:
        print("=== SIMULATION MODE ===")
        producer = LibraryEventProducer('config.json')

    producer.send_events()
    
if __name__ == "__main__":
    run_producer()