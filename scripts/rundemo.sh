#!/bin/sh
./linestreamviz.py &
./cassandra/dsc*/bin/cqlsh -e "truncate sparkml.accuracy" &
./producerSim.py &
./spark/spark-1.3-bin/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka_2.10:1.3.0 --jars jars/spark-cassandra-connector-1.3.0-M2.jar --class SpeedLayer jars/streaming_ml_2.10-1.0.jar