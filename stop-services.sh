#!/bin/bash

echo "[1/4] Stopping Kafka Broker..."
cd ~/kafka
bin/kafka-server-stop.sh
sleep 30

echo "[2/4] Stopping Zookeeper..."
bin/zookeeper-server-stop.sh
sleep 30

cd ~
echo "[3/4] Stopping yarn..."
cd ~/hadoop3
sbin/stop-yarn.sh
sleep 15

echo "[4/4] Stopping HDFS..."
sbin/stop-dfs.sh
sleep 15
cd ~

echo "========================================"
echo " All services are successfully stopped!"
echo "========================================"