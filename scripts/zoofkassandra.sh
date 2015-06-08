#!/bin/sh
./kafka/kafka*/bin/zookeeper-server-start.sh ./kafka/kafka*/config/zookeeper.properties &
./kafka/kafka*/bin/kafka-server-start.sh ./kafka/kafka*/config/server.properties &
./cassandra/dsc*/bin/cassandra &
