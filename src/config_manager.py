import json

class ConfigManager:
    def __init__(self, config_file='config.json'):
        with open(config_file, 'r') as file:
            self.config = json.load(file)

    def get_config(self):
        return self.config
    
    def get_neo4j_config(self):
        return {
            'uri': self.config['neo4j_uri'],
            'username': self.config['neo4j_username'],
            'password': self.config['neo4j_password']
        }