import json
import time
from kafka import KafkaProducer

class ConfigManager:
    def __init__(self, config_file):
        with open(config_file, 'r') as file:
            self.config = json.load(file)

    def get_config(self):
        return self.config

class LibraryEventProducer:
    def __init__(self, broker, topic):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=broker,
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
    config_mgr = ConfigManager('config.json')
    settings = config_mgr.get_config()
    
    event_producer = LibraryEventProducer(
        settings['kafka_broker'], 
        settings['topic_name']
    )
    
    event_producer.send_events(
        settings['input_file'], 
        settings['sleep_interval']
    )

if __name__ == "__main__":
    run_producer()