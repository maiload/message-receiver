#!/bin/bash

# Kafka 토픽 생성 스크립트
# docker exec message-receiver-kafka /opt/kafka/init/create-topics.sh

BOOTSTRAP_SERVER="localhost:9092"
KAFKA_BIN="/opt/kafka/bin"

# CDR 이벤트 토픽
$KAFKA_BIN/kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER --create --if-not-exists \
  --topic cdr.events \
  --partitions 6 \
  --replication-factor 1 \
  --config retention.ms=604800000

# Bulk 메시지 토픽
$KAFKA_BIN/kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER --create --if-not-exists \
  --topic message.bulk \
  --partitions 6 \
  --replication-factor 1 \
  --config retention.ms=604800000

# DLQ 토픽
$KAFKA_BIN/kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER --create --if-not-exists \
  --topic cdr.events.dlq \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=2592000000

$KAFKA_BIN/kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER --create --if-not-exists \
  --topic message.bulk.dlq \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=2592000000

echo "Kafka topics created successfully"
$KAFKA_BIN/kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER --list
