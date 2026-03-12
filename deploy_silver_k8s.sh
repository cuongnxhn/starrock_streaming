#!/bin/bash

# Deploy script cho Silver Batch Job (rawdata → warehouse, MERGE upsert)
# Deploy theo app_name — 1 CronJob duy nhất per app, topics được load từ DB
# Dùng K8s CronJob — chạy mỗi 5 phút rồi exit
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

EXECUTOR_MEMORY="4G"
EXECUTOR_CORES="2"
DRIVER_MEMORY="2G"
EXECUTOR_INSTANCES="2"

# Silver job chạy mỗi 5 phút, lookback 15 phút để đảm bảo không bỏ sót
CRON_SCHEDULE="*/5 * * * *"
LOOKBACK_MINUTES="15"

# Tên CronJob: silver-<app_name> (gạch dưới → gạch ngang, chữ thường)
CRONJOB_NAME="silver-${APP_NAME}"
CRONJOB_NAME="${CRONJOB_NAME,,}"
CRONJOB_NAME="${CRONJOB_NAME//_/-}"

echo "==========================================="
echo "🥈 Deploying Silver Batch Job (CronJob)"
echo "App:      $APP_NAME"
echo "CronJob:  $CRONJOB_NAME"
if [ -n "$SINGLE_TOPIC" ]; then
    echo "Topic:    $SINGLE_TOPIC (single-topic mode)"
else
    echo "Topics:   ALL (loaded from topic_configurations)"
fi
echo "Schedule: $CRON_SCHEDULE (every 5 min)"
echo "Lookback: ${LOOKBACK_MINUTES} min"
echo "Mode:     rawdata (Bronze) → warehouse (Silver)"
echo "==========================================="
echo ""

# Build spark-submit args: thêm --topic-name nếu chạy single-topic mode
TOPIC_ARG=""
if [ -n "$SINGLE_TOPIC" ]; then
    TOPIC_ARG="
            - --topic-name
            - ${SINGLE_TOPIC}"
fi

cat > "/tmp/${CRONJOB_NAME}.yaml" <<EOF
apiVersion: batch/v1
kind: CronJob
metadata:
  name: ${CRONJOB_NAME}
  namespace: ${K8S_NAMESPACE}
  labels:
    app: ${APP_NAME}
    layer: silver
spec:
  schedule: "${CRON_SCHEDULE}"
  # Nếu job trước chưa xong thì bỏ qua lần chạy hiện tại
  concurrencyPolicy: Forbid
  # Giữ 3 job thành công / 1 job thất bại để debug
  successfulJobsHistoryLimit: 3
  failedJobsHistoryLimit: 1
  jobTemplate:
    spec:
      # Timeout sau 4 phút (< 5 phút cron interval) để tránh chồng chất
      activeDeadlineSeconds: 240
      template:
        metadata:
          labels:
            app: ${APP_NAME}
            layer: silver
            spark-role: driver
        spec:
          serviceAccountName: ${K8S_SERVICE_ACCOUNT}
          # Job chạy xong thì không restart
          restartPolicy: Never
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
            - ${CRONJOB_NAME}
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
            - spark.kubernetes.executor.podNamePrefix=${CRONJOB_NAME}
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
            - /app/silver_main.py
            - --app-name
            - ${APP_NAME}${TOPIC_ARG}
            - --lookback-minutes
            - "${LOOKBACK_MINUTES}"
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
            envFrom:
            - secretRef:
                name: app-env-secret
            volumeMounts:
            - name: kafka-ssl-certs
              mountPath: /etc/kafka/ssl
              readOnly: true
          volumes:
          - name: kafka-ssl-certs
            secret:
              secretName: kafka-ssl-certs
              defaultMode: 0444
EOF

kubectl apply -f "/tmp/${CRONJOB_NAME}.yaml"
echo "✓ Deployed CronJob: ${CRONJOB_NAME}"
rm -f "/tmp/${CRONJOB_NAME}.yaml"

echo ""
echo "=========================================="
echo "✅ Silver CronJob created!"
echo "=========================================="
echo ""
echo "📋 Useful commands:"
echo ""
echo "View CronJob:  kubectl get cronjob ${CRONJOB_NAME} -n ${K8S_NAMESPACE}"
echo "View Jobs:     kubectl get jobs -l app=${APP_NAME},layer=silver -n ${K8S_NAMESPACE}"
echo "View pods:     kubectl get pods -l app=${APP_NAME},layer=silver -n ${K8S_NAMESPACE}"
echo "View logs:     kubectl logs -l app=${APP_NAME},layer=silver -n ${K8S_NAMESPACE} --tail=200"
echo ""
echo "Trigger manually:"
echo "  kubectl create job silver-manual-\$(date +%s) --from=cronjob/${CRONJOB_NAME} -n ${K8S_NAMESPACE}"
echo ""
echo "Delete:        kubectl delete cronjob ${CRONJOB_NAME} -n ${K8S_NAMESPACE}"
echo ""
