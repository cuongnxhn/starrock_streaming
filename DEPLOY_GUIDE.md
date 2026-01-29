# Quick Start Guide - Deploy to Kubernetes

## Chọn Script Deploy

Bạn có 2 options để deploy:

### Option 1: `deploy_k8s_per_topic.sh` (Recommended - Cluster Mode)
- ✅ **Spark cluster mode** - Driver chạy trong K8s cluster
- ✅ Dễ scale và manage
- ✅ Tốt cho production
- ❌ Cần có `spark-submit` binary
- ❌ Cần network access đến K8s API server

```bash
./deploy_k8s_per_topic.sh cdc_streaming topic1,topic2,topic3
```

### Option 2: `deploy_k8s_per_topic_kubectl.sh` (Client Mode - Deployment)
- ✅ Tạo Kubernetes Deployment
- ✅ Auto-restart nếu fail
- ✅ Chỉ cần `kubectl`
- ✅ **Spark client mode** - Driver chạy trong pod
- ❌ Driver pod cần nhiều resources hơn

```bash
./deploy_k8s_per_topic_kubectl.sh cdc_streaming topic1,topic2,topic3
```

## Setup trong 5 Bước

### Bước 1: Chuẩn Bị Kafka Authentication Config

Tạo file `client.properties`:

```properties
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="your-user" password="your-pass";
ssl.truststore.location=/etc/kafka/ssl/kafka.client.truststore.jks
ssl.truststore.password=your-password
```

**Lưu ý**: Nếu Kafka cluster của bạn không cần authentication, bỏ qua bước này.

### Bước 2: Setup Kafka Secrets trong K8s

```bash
# Make script executable
chmod +x setup_kafka_secrets.sh

# Run setup (default namespace)
./setup_kafka_secrets.sh

# Hoặc specify namespace
./setup_kafka_secrets.sh my-namespace
```

Script sẽ tự động:
- Tạo secret `kafka-client-config` từ `client.properties`
- Tạo secret `kafka-ssl-certs` từ các file `.jks` (nếu có)
- Verify và show commands để check

### Bước 3: Build và Push Docker Image

```bash
# Build image
docker build -t your-registry/starrocks-streaming:latest .

# Push to registry
docker push your-registry/starrocks-streaming:latest
```

**Chỉnh sửa** image name trong deploy script (line 21):
```bash
DOCKER_IMAGE="your-registry/starrocks-streaming:latest"
```

### Bước 4: Cấu Hình Deploy Script

Edit file `deploy_k8s_per_topic.sh` hoặc `deploy_k8s_per_topic_kubectl.sh`:

```bash
# Kubernetes config
K8S_NAMESPACE="default"                    # Namespace của bạn
K8S_SERVICE_ACCOUNT="spark"                # Service account với quyền tạo pods
DOCKER_IMAGE="your-registry/image:tag"     # Image đã build

# Spark config
SPARK_MASTER="k8s://https://your-k8s-api:6443"  # K8s API server URL
EXECUTOR_MEMORY="4g"
EXECUTOR_CORES="2"
DRIVER_MEMORY="2g"
EXECUTOR_INSTANCES="3"

# Kafka config
KAFKA_SERVERS="broker1:9093,broker2:9093,broker3:9093"
CHECKPOINT_DIR="s3a://bucket/checkpoints/"
```

### Bước 5: Deploy!

```bash
# Make scripts executable
chmod +x deploy_k8s_per_topic.sh
chmod +x deploy_k8s_per_topic_kubectl.sh

# Deploy with cluster mode (option 1)
./deploy_k8s_per_topic.sh cdc_streaming topic1,topic2

# OR Deploy with kubectl (option 2)
./deploy_k8s_per_topic_kubectl.sh cdc_streaming topic1,topic2
```

## Verify Deployment

### Check Pods

```bash
# List all pods
kubectl get pods -l app=cdc_streaming

# Watch pods
kubectl get pods -w -l app=cdc_streaming
```

### Check Logs

```bash
# Get pod name
POD_NAME=$(kubectl get pods -l app=cdc_streaming -o jsonpath='{.items[0].metadata.name}')

# Tail logs
kubectl logs -f $POD_NAME

# Check for Kafka auth
kubectl logs $POD_NAME | grep "Loading Kafka config"
```

### Verify Kafka Secrets Mounted

```bash
# List mounted files
kubectl exec $POD_NAME -- ls -la /etc/kafka/secrets/

# View client.properties
kubectl exec $POD_NAME -- cat /etc/kafka/secrets/client.properties

# Check environment variable
kubectl exec $POD_NAME -- env | grep KAFKA_CLIENT_PROPERTIES
```

### Access Spark UI

```bash
# Port-forward to Spark UI
kubectl port-forward $POD_NAME 4040:4040

# Open browser
open http://localhost:4040
```

## Common Issues & Solutions

### Issue 1: Secrets not found

**Error**: `Error from server (NotFound): secrets "kafka-client-config" not found`

**Solution**:
```bash
# Run setup script
./setup_kafka_secrets.sh default
```

### Issue 2: Image pull error

**Error**: `Failed to pull image "your-image:tag": rpc error`

**Solution**:
- Verify image exists: `docker images | grep starrocks-streaming`
- Push to registry: `docker push your-registry/starrocks-streaming:latest`
- Update DOCKER_IMAGE in deploy script
- Check image pull secrets if using private registry

### Issue 3: Permission denied for Kafka auth files

**Error**: `Permission denied: /etc/kafka/secrets/client.properties`

**Solution**: Files should be readable. Check in pod:
```bash
kubectl exec $POD_NAME -- ls -la /etc/kafka/secrets/
# Should show: -r-------- (mode 0400)
```

### Issue 4: Pod can't create executors

**Error**: `User spark cannot create resource "pods" in API group`

**Solution**: Apply RBAC:
```bash
kubectl apply -f k8s/spark-rbac.yaml
```

### Issue 5: Out of Memory

**Error**: `Container killed: OOMKilled`

**Solution**: Increase memory in deploy script:
```bash
EXECUTOR_MEMORY="8g"
DRIVER_MEMORY="4g"
```

## Monitoring

### Watch Job Status

```bash
# Get all streaming jobs
kubectl get pods -l app=cdc_streaming --show-labels

# Describe pod for events
kubectl describe pod $POD_NAME

# Top (resource usage)
kubectl top pod $POD_NAME
```

### Check Kafka Consumption

```bash
# Access pod shell
kubectl exec -it $POD_NAME -- bash

# Inside pod - check consumer groups
kafka-consumer-groups.sh --bootstrap-server $KAFKA_SERVERS \
  --describe --group spark-kafka-streaming
```

## Cleanup

### Delete Specific Job

```bash
# For cluster mode (spark-submit)
kubectl delete pod cdc_streaming-topic1-driver

# For deployment mode
kubectl delete deployment cdc-streaming-topic1
```

### Delete All Jobs for App

```bash
# Delete all pods/deployments
kubectl delete all -l app=cdc_streaming

# Delete secrets (if needed)
kubectl delete secret kafka-client-config kafka-ssl-certs
```

## Next Steps

1. **Setup Monitoring**: Deploy Prometheus & Grafana for metrics
2. **Setup Alerting**: Configure alerts for job failures
3. **Optimize Performance**: Tune Spark configs based on workload
4. **Setup CI/CD**: Automate build & deploy
5. **Multi-Environment**: Setup dev/staging/prod namespaces

## Need Help?

- Check `k8s/README.md` for Spark UI access methods
- Check `k8s/KAFKA_AUTH_GUIDE.md` for Kafka authentication details
- Check `docs/KAFKA_AUTHENTICATION.md` for local development

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster                    │
│                                                          │
│  ┌────────────────┐         ┌──────────────────┐       │
│  │  Kafka Secrets │────────▶│  Spark Driver    │       │
│  │  - client.prop │         │  Pod             │       │
│  │  - SSL certs   │         └────────┬─────────┘       │
│  └────────────────┘                  │                  │
│                                      │ Manages          │
│                          ┌───────────┴──────────┐       │
│                          │                       │       │
│                  ┌───────▼──────┐       ┌───────▼──────┐│
│                  │  Executor 1  │       │  Executor 2  ││
│                  │  Pod         │       │  Pod         ││
│                  └──────────────┘       └──────────────┘│
│                          │                       │       │
└──────────────────────────┼───────────────────────┼──────┘
                           │                       │
                           ▼                       ▼
                    ┌──────────────────────────────────┐
                    │        Kafka Cluster             │
                    │  (with SASL/SSL Authentication)  │
                    └──────────────────────────────────┘
```
