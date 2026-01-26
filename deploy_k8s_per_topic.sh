#!/bin/bash

# Deploy script để chạy từng topic như một Spark job riêng biệt trên Kubernetes
# Usage: ./deploy_k8s_per_topic.sh <app_name> <topic1,topic2,topic3>

set -e

APP_NAME=$1
TOPICS=$2

if [ -z "$APP_NAME" ] || [ -z "$TOPICS" ]; then
    echo "Usage: $0 <app_name> <topic1,topic2,topic3>"
    echo "Example: $0 my_app orders,payments,users"
    exit 1
fi

# ==================== Configuration ====================
# Kubernetes config
K8S_NAMESPACE="default"
K8S_SERVICE_ACCOUNT="spark"
DOCKER_IMAGE="<YOUR_DOCKER_REGISTRY>/starrock-streaming:latest"  # Thay bằng image của bạn

# Spark config
SPARK_MASTER="k8s://https://kubernetes.default.svc:443"  # Hoặc URL của K8s API server
EXECUTOR_MEMORY="4g"
EXECUTOR_CORES="2"
DRIVER_MEMORY="2g"
EXECUTOR_INSTANCES="3"

# Kafka config
KAFKA_SERVERS="10.165.11.236:9093,10.165.11.237:9093,10.165.11.238:9093"
CHECKPOINT_DIR="s3a://iceberg/checkpoints/"
EVENT_LOG_DIR="s3a://iceberg/spark-logs/"  # Cho History Server

# Split topics by comma
IFS=',' read -ra TOPIC_ARRAY <<< "$TOPICS"

echo "=========================================="
echo "🚀 Deploying Spark jobs to Kubernetes"
echo "App: $APP_NAME"
echo "Topics: ${TOPIC_ARRAY[@]}"
echo "K8s Namespace: $K8S_NAMESPACE"
echo "=========================================="
echo ""

for TOPIC in "${TOPIC_ARRAY[@]}"; do
    JOB_NAME="${APP_NAME}-${TOPIC}"
    echo "→ Submitting job for topic: $TOPIC (Job: $JOB_NAME)"
    
    spark-submit \
        --master $SPARK_MASTER \
        --deploy-mode cluster \
        --name "$JOB_NAME" \
        --conf spark.kubernetes.namespace=$K8S_NAMESPACE \
        --conf spark.kubernetes.authenticate.driver.serviceAccountName=$K8S_SERVICE_ACCOUNT \
        --conf spark.kubernetes.container.image=$DOCKER_IMAGE \
        --conf spark.kubernetes.driver.label.app=$APP_NAME \
        --conf spark.kubernetes.driver.label.topic=$TOPIC \
        --conf spark.kubernetes.executor.label.app=$APP_NAME \
        --conf spark.kubernetes.executor.label.topic=$TOPIC \
        --conf spark.kubernetes.driver.pod.name="${JOB_NAME}-driver" \
        --conf spark.executor.instances=$EXECUTOR_INSTANCES \
        --conf spark.executor.memory=$EXECUTOR_MEMORY \
        --conf spark.executor.cores=$EXECUTOR_CORES \
        --conf spark.driver.memory=$DRIVER_MEMORY \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \
        --conf spark.ui.port=4040 \
        --conf spark.eventLog.enabled=true \
        --conf spark.eventLog.dir=$EVENT_LOG_DIR \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        main.py \
        --app-name "$APP_NAME" \
        --topic-name "$TOPIC" \
        --kafka-bootstrap-servers "$KAFKA_SERVERS" \
        --checkpoint-dir "$CHECKPOINT_DIR"
    
    echo "✓ Submitted: $JOB_NAME"
    echo "  📊 View UI with: kubectl port-forward ${JOB_NAME}-driver 4040:4040"
    echo ""
    
    # Sleep để tránh overwhelm cluster
    sleep 5
done

echo "=========================================="
echo "✓ All jobs submitted successfully!"
echo "=========================================="
echo ""
echo "📋 Useful commands:"
echo ""
echo "List all Spark driver pods:"
echo "  kubectl get pods -l app=$APP_NAME"
echo ""
echo "View logs for a specific topic:"
echo "  kubectl logs -f ${APP_NAME}-<topic>-driver"
echo ""
echo "Port-forward to view Spark UI:"
echo "  kubectl port-forward ${APP_NAME}-<topic>-driver 4040:4040"
echo "  Then open: http://localhost:4040"
echo ""
echo "Access via NodePort (if service deployed):"
echo "  http://<node-ip>:30040"
echo ""
echo "View History Server (for completed jobs):"
echo "  http://<node-ip>:30080"
echo ""
