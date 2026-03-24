import subprocess
import sys
import os
import json

class ConfigManager:
    def __init__(self, config_file):
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

class HDFSManager:
    def __init__(self, target_dir):
        self.target_dir = target_dir

    def create_directory(self):
        print(f"Creating HDFS directory: {self.target_dir}")
        result = subprocess.run(
            ["hdfs", "dfs", "-mkdir", "-p", self.target_dir],
            capture_output=True, 
            text=True
        )
        if result.returncode != 0:
            print(f"Directory creation failed: {result.stderr}")
            sys.exit(1)

    def upload_file(self, local_file_path):
        if not os.path.exists(local_file_path):
            print(f"Upload failed: '{local_file_path}' does not exist locally.")
            sys.exit(1)
            
        print(f"Uploading {local_file_path} to {self.target_dir}")
        result = subprocess.run(
            ["hdfs", "dfs", "-put", "-f", local_file_path, self.target_dir],
            capture_output=True, 
            text=True
        )
        if result.returncode != 0:
            print(f"File upload failed: {result.stderr}")
            sys.exit(1)
            
        print("Student data successfully loaded into HDFS.")

def execute_upload():
    config_mgr = ConfigManager('config.json')
    app_settings = config_mgr.get_config()
    hdfs_student_path = app_settings["hdfs_student_path"]
    local_student_path = app_settings["local_student_path"]
    
    hdfs_client = HDFSManager(hdfs_student_path)
    hdfs_client.create_directory()
    hdfs_client.upload_file(local_student_path)

if __name__ == "__main__":
    execute_upload()