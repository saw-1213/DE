python data_generator.py
python producer.py
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1 consumer.py
spark-submit batch_processing.py
spark-submit load_neo4j_batch.py