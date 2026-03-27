Team Reference: G5-2
Submission Date: Date

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
│   └── run_neo4jqueries.py 
│
├── storage/
│   ├── load_neo4j.py             
│   └── etc.py  
│
└── utils/
    ├── data_generator.py                
    └── upload_students.py 


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
    $ kafka-server-start/sh $KAFKA_HOME/config/server.properties &

3.2 Prepare the data:
    // Run the following as student:
    $ cp -r <windows_path> /home/student/library/
    $ cd library
    $ python utils/data_generator.py
    $ python utils/upload_students.py

3.4 Run the demo:
    // Run the following as student:
    $ python main.py

// Delete later
source /home/student/de-venv/bin/activate
python data_generator.py
python upload_students.py
python producer.py
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1 consumer.py 2> logs.txt
spark-submit batch_processing.py
spark-submit load_neo4j.py