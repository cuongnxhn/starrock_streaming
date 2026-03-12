#!/bin/bash

# Deploy script cho Bronze Streaming Job (Kafka → rawdata, append-only)
# Deploy theo app_name — 1 Deployment duy nhất per app, topics được load từ DB
#
# Usage: ./deploy_bronze_k8s.sh <app_name>
# Example: ./deploy_bronze_k8s.sh onebss_bcss
#
# Để debug riêng 1 topic, truyền thêm tham số thứ 2 (optional):
# Example: ./deploy_bronze_k8s.sh onebss_bcss CT_NO

set -e

APP_NAME=$1
SINGLE_TOPIC=$2   # optional — dùng khi cần debug/chạy riêng 1 topic

if [ -z "$APP_NAME" ]; then
    echo "Usage: $0 <app_name> [topic_name]"
    echo ""
    echo "  app_name    : Tên app trong bảng spark_app_config / topic_configurations"
    echo "  topic_name  : (optional) Chỉ chạy 1 topic cụ thể để debug"
    echo ""
    echo "Example (all topics):  $0 onebss_bcss"
    echo "Example (one topic):   $0 onebss_bcss CT_NO"
    exit 1
fi

# ==================== Configuration ====================
K8S_NAMESPACE="default"
K8S_SERVICE_ACCOUNT="spark"
DOCKER_IMAGE="10.165.13.117:5000/starrock-streaming:latest"

EXECUTOR_MEMORY="4G"
EXECUTOR_CORES="2"
DRIVER_MEMORY="2G"
EXECUTOR_INSTANCES="2"

KAFKA_SERVERS="10.165.11.236:9093,10.165.11.237:9093,10.165.11.238:9093"
CHECKPOINT_DIR="s3a://iceberg/checkpoints/"

# Tên Deployment: bronze-<app_name> (gạch dưới → gạch ngang, chữ thường)
DEPLOYMENT_NAME="bronze-${APP_NAME}"
DEPLOYMENT_NAME="${DEPLOYMENT_NAME,,}"
DEPLOYMENT_NAME="${DEPLOYMENT_NAME//_/-}"

echo "==========================================="
echo "🟤 Deploying Bronze Streaming Job"
echo "App:        $APP_NAME"
echo "Deployment: $DEPLOYMENT_NAME"
if [ -n "$SINGLE_TOPIC" ]; then
    echo "Topic:      $SINGLE_TOPIC (single-topic mode)"
else
    echo "Topics:     ALL (loaded from topic_configurations)"
fi
echo "Mode:       Kafka → rawdata (append-only)"
echo "==========================================="
echo ""

# Build spark-submit args: thêm --topic-name nếu chạy single-topic mode
TOPIC_ARG=""
if [ -n "$SINGLE_TOPIC" ]; then
    TOPIC_ARG="
        - --topic-name
        - ${SINGLE_TOPIC}"
fi

cat > "/tmp/${DEPLOYMENT_NAME}.yaml" <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${DEPLOYMENT_NAME}
  namespace: ${K8S_NAMESPACE}
  labels:
    app: ${APP_NAME}
    layer: bronze
spec:
  replicas: 1
  selector:
    matchLabels:
      app: ${APP_NAME}
      layer: bronze
  template:
    metadata:
      labels:
        app: ${APP_NAME}
        layer: bronze
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
        - /app/bronze_main.py
        - --app-name
        - ${APP_NAME}${TOPIC_ARG}
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
        - name: KAFKA_CLIENT_PROPERTIES
          value: /etc/kafka/secrets/client.properties
        envFrom:
        - secretRef:
            name: app-env-secret
        volumeMounts:
        - name: kafka-client-config
          mountPath: /etc/kafka/secrets
          readOnly: true
        - name: kafka-ssl-certs
          mountPath: /etc/kafka/ssl
          readOnly: true
      volumes:
      - name: kafka-client-config
        secret:
          secretName: kafka-client-config
          defaultMode: 0444
      - name: kafka-ssl-certs
        secret:
          secretName: kafka-ssl-certs
          defaultMode: 0444
EOF

kubectl apply -f "/tmp/${DEPLOYMENT_NAME}.yaml"
echo "✓ Deployed: ${DEPLOYMENT_NAME}"
rm -f "/tmp/${DEPLOYMENT_NAME}.yaml"

echo ""
echo "=========================================="
echo "✅ Bronze deployment created!"
echo "=========================================="
echo ""
echo "📋 Useful commands:"
echo ""
echo "View pod:    kubectl get pods -l app=${APP_NAME},layer=bronze -n ${K8S_NAMESPACE}"
echo "View logs:   kubectl logs -f deployment/${DEPLOYMENT_NAME} -n ${K8S_NAMESPACE}"
echo "Spark UI:    kubectl port-forward deployment/${DEPLOYMENT_NAME} 4040:4040 -n ${K8S_NAMESPACE}"
echo "Restart:     kubectl rollout restart deployment/${DEPLOYMENT_NAME} -n ${K8S_NAMESPACE}"
echo "Delete:      kubectl delete deployment ${DEPLOYMENT_NAME} -n ${K8S_NAMESPACE}"
echo ""
