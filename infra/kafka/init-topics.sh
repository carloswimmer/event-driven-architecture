#!/bin/bash
set -euo pipefail

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVER:-kafka:9092}"
KAFKA_TOPICS="/opt/kafka/bin/kafka-topics.sh"

# Single-broker setup uses replication-factor=1.
# In production with 3+ brokers, set REPLICATION_FACTOR=3 and min.insync.replicas=2.
REPLICATION_FACTOR="${KAFKA_REPLICATION_FACTOR:-1}"
MIN_INSYNC_REPLICAS="${KAFKA_MIN_INSYNC_REPLICAS:-1}"
RETENTION_MS="${KAFKA_RETENTION_MS:-604800000}" # 7 days

create_topic() {
  local topic="$1"
  local partitions="$2"

  echo "Creating topic '${topic}' (partitions=${partitions}, rf=${REPLICATION_FACTOR})..."

  "$KAFKA_TOPICS" \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor "$REPLICATION_FACTOR" \
    --config "retention.ms=${RETENTION_MS}" \
    --config "min.insync.replicas=${MIN_INSYNC_REPLICAS}" \
    --config "compression.type=producer" \
    --config "cleanup.policy=delete"
}

# orders.payment.succeeded: fan-out to API, Availability, Analytics, Invoice.
# 6 partitions allow parallel consumption within a consumer group; use orderNumber as the message key.
create_topic "orders.payment.succeeded" 6

# billing.invoice.created: fan-out to Analytics, Notification.
# 3 partitions; use orderNumber or customerId as the message key.
create_topic "billing.invoice.created" 3

echo "Kafka topics ready:"
"$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP_SERVER" --list
