source /home/student/de-venv/bin/activate
python data_generator.py
python upload_students.py
python producer.py
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1 consumer.py 2> logs.txt
spark-submit batch_processing.py
spark-submit load_neo4j.py