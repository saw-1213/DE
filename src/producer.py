import json
import time
from kafka import KafkaProducer

class LibraryEventProducer:
    def __init__(self, config_file):
        with open(config_file, 'r') as file:
            config = json.load(file)

        self.topic = config['topic_name']
        self.input_file = config['input_file']
        self.delay = config['sleep_interval']
      
        self.producer = KafkaProducer(
            bootstrap_servers=config['kafka_broker'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def send_events(self, file_path, delay):
        with open(file_path, 'r') as file:
            events = json.load(file)

        for event in events:
            self.producer.send(self.topic, event)
            print(event)
            time.sleep(delay)

def run_producer():
    producer = LibraryEventProducer('config.json')
    producer.send_events()
    
if __name__ == "__main__":
    run_producer()