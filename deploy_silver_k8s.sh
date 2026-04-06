#!/bin/bash

# Deploy script cho Silver Streaming Job (rawdata → warehouse, MERGE upsert)
# Deploy theo app_name — 1 Deployment duy nhất per app, topics được load từ DB
# Job chạy liên tục (long-running) với ProcessingTime trigger mỗi 10 phút
#
# Usage: ./deploy_silver_k8s.sh <app_name>
# Example: ./deploy_silver_k8s.sh onebss_bcss
#
# Để debug riêng 1 topic, truyền thêm tham số thứ 2 (optional):
# Example: ./deploy_silver_k8s.sh onebss_bcss CT_NO

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

EXECUTOR_MEMORY="16G"
EXECUTOR_CORES="4"
DRIVER_MEMORY="6G"
EXECUTOR_INSTANCES="2"


# Tên Deployment: silver-<app_name> (gạch dưới → gạch ngang, chữ thường)
DEPLOY_NAME="silver-${APP_NAME}"
DEPLOY_NAME="${DEPLOY_NAME,,}"
DEPLOY_NAME="${DEPLOY_NAME//_/-}"

echo "==========================================="
echo "🥈 Deploying Silver Streaming Job (Deployment)"
echo "App:        $APP_NAME"
echo "Deployment: $DEPLOY_NAME"
if [ -n "$SINGLE_TOPIC" ]; then
    echo "Topic:      $SINGLE_TOPIC (single-topic mode)"
else
    echo "Topics:     ALL (loaded from topic_configurations)"
fi
echo "Trigger:    ProcessingTime(10 min) — long-running"
echo "Mode:       rawdata (Bronze) → warehouse (Silver)"
echo "==========================================="
echo ""

# Build spark-submit args: thêm --topic-name nếu chạy single-topic mode
TOPIC_ARG=""
if [ -n "$SINGLE_TOPIC" ]; then
    TOPIC_ARG="
            - --topic-name
            - ${SINGLE_TOPIC}"
fi

cat > "/tmp/${DEPLOY_NAME}.yaml" <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${DEPLOY_NAME}
  namespace: ${K8S_NAMESPACE}
  labels:
    app: ${APP_NAME}
    layer: silver
spec:
  # Chỉ 1 replica — tránh 2 instance cùng ghi Silver table
  replicas: 1
  selector:
    matchLabels:
      app: ${APP_NAME}
      layer: silver
  # Recreate strategy: dừng pod cũ hoàn toàn trước khi tạo pod mới
  # (tránh 2 Spark driver cùng lúc tranh chấp executor + checkpoint)
  strategy:
    type: Recreate
  template:
    metadata:
      labels:
        app: ${APP_NAME}
        layer: silver
        spark-role: driver
    spec:
      serviceAccountName: ${K8S_SERVICE_ACCOUNT}
      # Pod crash → K8s tự restart (Deployment default = Always)
      restartPolicy: Always
      # Cho Spark 5 phút để graceful shutdown (dừng stream, commit checkpoint)
      terminationGracePeriodSeconds: 300
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
        - ${DEPLOY_NAME}
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
        - spark.kubernetes.driver.pod.name=\$(POD_NAME)
        - --conf
        - spark.kubernetes.executor.podNamePrefix=${DEPLOY_NAME}
        - --conf
        - spark.kubernetes.executor.deleteOnTermination=true
        - --conf
        - spark.kubernetes.executor.secrets.app-env-secret=/mnt/secrets
        - --conf
        - spark.kubernetes.driver.podTemplateFile=/app/k8s/spark-driver-pod-template.yaml
        - --conf
        - spark.kubernetes.executor.podTemplateFile=/app/k8s/spark-executor-pod-template.yaml
        - --conf
        - spark.executor.memoryOverhead=4g
        - --conf
        - spark.memory.fraction=0.7
        - --conf
        - spark.memory.storageFraction=0.2
        # Tắt Adaptive Broadcast Join trong AQE (vẫn có thể trigger broadcast)
        - --conf
        - spark.sql.adaptive.enabled=true
        - --conf
        - spark.sql.adaptive.localShuffleReader.enabled=true
        # Giảm broadcastTimeout → fail-fast thay vì treo 10 phút
        - --conf
        - spark.sql.broadcastTimeout=120
        # Network timeout đủ lớn cho MERGE bảng lớn
        - --conf
        - spark.network.timeout=600s
        - --conf
        - spark.executor.heartbeatInterval=60s
        # TẮT speculation — gây task storm trên bảng lớn, làm executor bị OOM
        - --conf
        - spark.speculation=false
        - --conf
        - spark.task.maxFailures=4
        # Iceberg MERGE: đọc file theo sequence thay vì parallel quá mức
        - --conf
        - spark.sql.iceberg.merge-on-read.enabled=false
        - --conf
        - spark.sql.shuffle.partitions=200
        # Spill configs: cho phép Sort-Merge Join spill xuống disk thay vì OOM
        # Khi sort buffer đầy, Spark sẽ ghi tạm xuống disk rồi merge lại
        - --conf
        - spark.shuffle.spill.compress=true
        - --conf
        - spark.shuffle.compress=true
        # Tăng số kết nối shuffle để giảm thời gian fetch
        - --conf
        - spark.shuffle.service.enabled=false
        - --conf
        - spark.reducer.maxSizeInFlight=96m
        - --conf
        - spark.shuffle.file.buffer=1m
        - /app/silver_main.py
        - --app-name
        - ${APP_NAME}${TOPIC_ARG}
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

kubectl apply -f "/tmp/${DEPLOY_NAME}.yaml"
echo "✓ Deployed Deployment: ${DEPLOY_NAME}"
rm -f "/tmp/${DEPLOY_NAME}.yaml"

echo ""
echo "=========================================="
echo "✅ Silver Deployment created!"
echo "=========================================="
echo ""
echo "📋 Useful commands:"
echo ""
echo "View Deployment:  kubectl get deployment ${DEPLOY_NAME} -n ${K8S_NAMESPACE}"
echo "View pods:        kubectl get pods -l app=${APP_NAME},layer=silver -n ${K8S_NAMESPACE}"
echo "View logs:        kubectl logs -l app=${APP_NAME},layer=silver -n ${K8S_NAMESPACE} --tail=200 -f"
echo "Restart:          kubectl rollout restart deployment ${DEPLOY_NAME} -n ${K8S_NAMESPACE}"
echo ""
echo "Scale down (pause):  kubectl scale deployment ${DEPLOY_NAME} --replicas=0 -n ${K8S_NAMESPACE}"
echo "Scale up  (resume):  kubectl scale deployment ${DEPLOY_NAME} --replicas=1 -n ${K8S_NAMESPACE}"
echo ""
echo "Delete:           kubectl delete deployment ${DEPLOY_NAME} -n ${K8S_NAMESPACE}"
echo ""
