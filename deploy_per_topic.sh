#!/bin/bash

# Deploy script để chạy từng topic như một Spark job riêng biệt
# Usage: ./deploy_per_topic.sh <app_name> <topic1,topic2,topic3>

set -e

APP_NAME=$1
TOPICS=$2

if [ -z "$APP_NAME" ] || [ -z "$TOPICS" ]; then
    echo "Usage: $0 <app_name> <topic1,topic2,topic3>"
    echo "Example: $0 my_app orders,payments,users"
    exit 1
fi

# Spark submit configuration
SPARK_MASTER="yarn"  # hoặc "local[*]" cho testing
EXECUTOR_MEMORY="4g"
EXECUTOR_CORES="2"
DRIVER_MEMORY="2g"

# Kafka config
KAFKA_SERVERS="10.165.11.236:9093,10.165.11.237:9093,10.165.11.238:9093"
CHECKPOINT_DIR="s3a://iceberg/checkpoints/"

# Split topics by comma
IFS=',' read -ra TOPIC_ARRAY <<< "$TOPICS"

echo "=========================================="
echo "Deploying Spark jobs per topic"
echo "App: $APP_NAME"
echo "Topics: ${TOPIC_ARRAY[@]}"
echo "=========================================="
echo ""

for TOPIC in "${TOPIC_ARRAY[@]}"; do
    echo "→ Submitting job for topic: $TOPIC"
    
    spark-submit \
        --master $SPARK_MASTER \
        --deploy-mode cluster \
        --name "${APP_NAME}_${TOPIC}" \
        --executor-memory $EXECUTOR_MEMORY \
        --executor-cores $EXECUTOR_CORES \
        --driver-memory $DRIVER_MEMORY \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \
        main.py \
        --app-name "$APP_NAME" \
        --topic-name "$TOPIC" \
        --kafka-bootstrap-servers "$KAFKA_SERVERS" \
        --checkpoint-dir "$CHECKPOINT_DIR"
    
    echo "✓ Submitted: ${APP_NAME}_${TOPIC}"
    echo ""
    
    # Sleep để tránh overwhelm cluster
    sleep 5
done

echo "=========================================="
echo "✓ All jobs submitted successfully!"
echo "=========================================="
echo ""
echo "Check job status with:"
echo "  yarn application -list"
echo ""
echo "Check logs with:"
echo "  yarn logs -applicationId <app_id>"
