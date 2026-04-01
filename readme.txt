Team Reference: G5-2
Submission Date: 2/4/2026

-----------------------------------------------------------
1. Project Title:
-----------------------------------------------------------
Library Occupancy and Queue Analytics

-----------------------------------------------------------
2. Project Folder Structure: 
-----------------------------------------------------------
Your Team Reference/
│
├── requirements.txt
├── readme.txt
├── main.py
│
├── ingestion/
│   ├── producer.py              
│   └── consumer.py        
│
├── processing/             
│   ├── batch_processing.py 
│   ├── run_neo4j_queries.py
│   └── run_mongodb_queries.py 
│
├── storage/
│   ├── load_neo4j.py             
│   └── load_mongodb.py  
│
└── utils/
    ├── data_generator.py
    ├── upload_students.py 
    ├── config_manager.py               
    └── config.json 


-----------------------------------------------------------
3. Setup Instructions:
-----------------------------------------------------------
3.1 Install dependencies:
    $ pip install -r requirements.txt

3.2 Start the necessary services:
    // Start the services as hduser
    $ start-dfs.sh
    $ start-yarn.sh
    $ zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties &
    $ kafka-server-start.sh $KAFKA_HOME/config/server.properties &

3.3 Prepare the data:
    // Run the following as student:
    $ source de-venv/bin/activate
    $ mkdir /home/student/library
    $ cp -r <windows_path> /home/student/library/
    $ cd library
    $ python utils/data_generator.py
    $ python utils/upload_students.py

3.4 Run the demo:
    // Run the following as student in the library directory:
    $ cd home/student/library
    $ python main.py --fast

3.5 To run the modules independently:
    // Method 1: Run as Pythonic Module
    $ python -m ingestion.consumer 2> logs.txt
    $ python -m ingestion.producer
    $ python -m processing.batch_processing 2> batch_logs.txt
    $ python -m storage.load_neo4j
    $ python -m storage.load_mongodb
    $ python -m processing.run_neo4j_queries
    $ python -m processing.run_mongodb_queries

    // Method 2: Run as OS Environment Variable 
    $ PYTHONPATH=. python ingestion/consumer.py 2> logs.txt
    $ PYTHONPATH=. python ingestion/producer.py
    $ PYTHONPATH=. python processing/batch_processing.py 2> batch_logs.txt
    $ PYTHONPATH=. python storage/load_neo4j.py
    $ PYTHONPATH=. python storage/load_mongodb.py
    $ PYTHONPATH=. python processing/run_neo4j_queries.py
    $ PYTHONPATH=. python processing/run_mongodb_queries.py

3.6 Debugging:
    // Clear the storage in HDFS before demo if previous testing cache remains
    $ hdfs dfs -rm -r /user/student/library

    // Remove STOP_CONSUMER.txt that stops the consumer after every demo
    $ rm /home/student/library/STOP_CONSUMER.txt

    // Clear the kafka topics if it fails to terminate itself
    $ kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic main_gate_events
    $ kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic room_gate_events