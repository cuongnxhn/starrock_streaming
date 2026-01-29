#!/bin/bash

# Deploy script đơn giản cho Spark Streaming jobs trên Kubernetes
# Usage: ./deploy_k8s_per_topic_kubectl.sh <app_name> <topic1,topic2,topic3>

set -e

APP_NAME=$1
TOPICS=$2

if [ -z "$APP_NAME" ] || [ -z "$TOPICS" ]; then
    echo "Usage: $0 <app_name> <topic1,topic2,topic3>"
    echo "Example: $0 my_app orders,payments"
    exit 1
fi

# ==================== Configuration ====================
K8S_NAMESPACE="default"
K8S_SERVICE_ACCOUNT="spark"
DOCKER_IMAGE="10.165.13.117:5000/starrock-streaming:latest"

# Spark config - đơn giản hóa
EXECUTOR_MEMORY="4G"
EXECUTOR_CORES="2"
DRIVER_MEMORY="2G"
EXECUTOR_INSTANCES="2"

# Kafka config
KAFKA_SERVERS="10.165.11.236:9093,10.165.11.237:9093,10.165.11.238:9093"
CHECKPOINT_DIR="s3a://iceberg/checkpoints/"

# Split topics
IFS=',' read -ra TOPIC_ARRAY <<< "$TOPICS"

echo "==========================================="
echo "🚀 Deploying Spark Streaming Jobs"
echo "App: $APP_NAME"
echo "Topics: ${TOPIC_ARRAY[@]}"
echo "==========================================="
echo ""



for TOPIC in "${TOPIC_ARRAY[@]}"; do
    DEPLOYMENT_NAME="${APP_NAME}-${TOPIC}"
    DEPLOYMENT_NAME="${DEPLOYMENT_NAME,,}"
    DEPLOYMENT_NAME="${DEPLOYMENT_NAME//_/-}"
    
    echo "→ Deploying: $DEPLOYMENT_NAME"
    
    # Tạo Deployment cho streaming job
    cat > "/tmp/${DEPLOYMENT_NAME}.yaml" <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${DEPLOYMENT_NAME}
  namespace: ${K8S_NAMESPACE}
  labels:
    app: ${APP_NAME}
    topic: ${TOPIC}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: ${APP_NAME}
      topic: ${TOPIC}
  template:
    metadata:
      labels:
        app: ${APP_NAME}
        topic: ${TOPIC}
        spark-role: driver
    spec:
      serviceAccountName: ${K8S_SERVICE_ACCOUNT}
      containers:
      - name: spark-driver
        image: ${DOCKER_IMAGE}
        imagePullPolicy: Always
        command:
        - /opt/spark/bin/spark-submit
        - --master
        - k8s://https://kubernetes.default.svc.cluster.local:443
        - --deploy-mode
        - client
        - --name
        - ${DEPLOYMENT_NAME}
        - --conf
        - spark.kubernetes.namespace=${K8S_NAMESPACE}
        - --conf
        - spark.kubernetes.authenticate.driver.serviceAccountName=${K8S_SERVICE_ACCOUNT}
        - --conf
        - spark.kubernetes.container.image=${DOCKER_IMAGE}
        - --conf
        - spark.executor.instances=${EXECUTOR_INSTANCES}
        - --conf
        - spark.executor.memory=${EXECUTOR_MEMORY}
        - --conf
        - spark.executor.cores=${EXECUTOR_CORES}
        - --conf
        - spark.driver.memory=${DRIVER_MEMORY}
        - --conf
        - spark.driver.host=\$(POD_IP)
        - --conf
        - spark.driver.port=7078
        - --conf
        - spark.driver.blockManager.port=7079
        - --conf
        - spark.kubernetes.executor.podNamePrefix=${DEPLOYMENT_NAME}
        - --conf
        - spark.kubernetes.executor.deleteOnTermination=true
        - --conf
        - spark.kubernetes.executor.secrets.app-env-secret=/mnt/secrets
        - --conf
        - spark.kubernetes.driver.podTemplateFile=/app/k8s/spark-driver-pod-template.yaml
        - --conf
        - spark.kubernetes.executor.podTemplateFile=/app/k8s/spark-executor-pod-template.yaml
        - --conf
        - spark.kubernetes.memoryOverheadFactor=0.1
        - /app/main.py
        - --app-name
        - ${APP_NAME}
        - --topic-name
        - ${TOPIC}
        - --kafka-bootstrap-servers
        - ${KAFKA_SERVERS}
        - --checkpoint-dir
        - ${CHECKPOINT_DIR}
        resources:
          requests:
            memory: 2Gi
            cpu: "1000m"
          limits:
            memory: 2Gi
            cpu: "2000m"
        ports:
        - containerPort: 7078
          name: driver-rpc
        - containerPort: 7079
          name: blockmanager
        - containerPort: 4040
          name: spark-ui
        env:
        - name: POD_IP
          valueFrom:
            fieldRef:
              fieldPath: status.podIP
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        # Point to Kafka client.properties
        - name: KAFKA_CLIENT_PROPERTIES
          value: /etc/kafka/secrets/client.properties
        # Load all environment variables from .env Secret
        envFrom:
        - secretRef:
            name: app-env-secret
        # Volume mounts for Kafka authentication
        volumeMounts:
        - name: kafka-client-config
          mountPath: /etc/kafka/secrets
          readOnly: true
        - name: kafka-ssl-certs
          mountPath: /etc/kafka/ssl
          readOnly: true
      # Volumes for Kafka authentication
      volumes:
      - name: kafka-client-config
        secret:
          secretName: kafka-client-config
          defaultMode: 0444  # Read-only for all users (fixes PermissionError)
      - name: kafka-ssl-certs
        secret:
          secretName: kafka-ssl-certs
          defaultMode: 0444  # Read-only for all users (fixes PermissionError)
EOF
    
    # Apply deployment
    kubectl apply -f "/tmp/${DEPLOYMENT_NAME}.yaml"
    
    echo "✓ Deployed: ${DEPLOYMENT_NAME}"
    echo ""
    
    # Cleanup
    rm -f "/tmp/${DEPLOYMENT_NAME}.yaml"
    
    sleep 1
done

echo "=========================================="
echo "✅ All deployments created!"
echo "=========================================="
echo ""
echo "📋 Useful commands:"
echo ""
echo "View all pods:"
echo "  kubectl get pods -l app=${APP_NAME} -n ${K8S_NAMESPACE}"
echo ""
echo "View logs:"
echo "  kubectl logs -f deployment/${DEPLOYMENT_NAME} -n ${K8S_NAMESPACE}"
echo ""
echo "View Spark UI (port-forward):"
echo "  kubectl port-forward deployment/${DEPLOYMENT_NAME} 4040:4040 -n ${K8S_NAMESPACE}"
echo ""
echo "Delete all jobs:"
echo "  kubectl delete deployment -l app=${APP_NAME} -n ${K8S_NAMESPACE}"
echo ""
