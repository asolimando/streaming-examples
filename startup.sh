#!/bin/bash

kafkadir="kafka_2.11-1.1.0"

~/apps/$kafkadir/bin/zookeeper-server-start.sh ~/apps/$kafkadir//config/zookeeper.properties > zookeeper.log&
sleep 10
~/apps/$kafkadir/bin/kafka-server-start.sh ~/apps/$kafkadir/config/server.properties > kafka.log&
sleep 10

declare -a topics=( "origin" "flink-destination" "spark-destination" "kafka-destination" )

for i in "${topics[@]}"
do
  ~/apps/$kafkadir/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic $i
  ~/apps/$kafkadir/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic $i
done

#./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic flink-destination --from-beginning --property print.key=true --property key.separator=:
#./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic origin --property parse.key=true --property key.separator=:
