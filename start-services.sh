#!/bin/bash

echo "[1/4] Starting HDFS..."
cd ~/hadoop3
sbin/start-dfs.sh
sleep 15

echo "[2/4] Starting yarn..."
sbin/start-yarn.sh
sleep 15

cd ~
echo "[3/4] Starting Zookeeper..."
cd ~/kafka
bin/zookeeper-server-start.sh
sleep 30

echo "[4/4] Starting Kafka Broker..."
bin/kafka-server-start.sh
sleep 30
cd ~

echo "========================================"
echo " All background services are running!"
echo "========================================"