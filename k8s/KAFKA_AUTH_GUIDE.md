# Kubernetes Deployment Guide - Kafka Authentication

## Tổng Quan

Khi deploy Spark application lên Kubernetes, **KHÔNG CẦN** upload `client.properties` thủ công lên từng worker node. Thay vào đó, sử dụng **Kubernetes Secrets** để distribute config một cách tự động và bảo mật.

## Cách Hoạt Động

1. **Tạo Kubernetes Secret** chứa `client.properties` và SSL certificates
2. **Mount Secret vào Pod** thông qua volume mounts
3. **App tự động đọc** từ mounted path
4. Kubernetes tự động distribute secret đến tất cả worker nodes khi pod được schedule

## Setup Steps

### Bước 1: Chuẩn Bị Files

Tạo file `client.properties` với authentication config:

```properties
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="your-user" password="your-pass";
ssl.truststore.location=/etc/kafka/ssl/kafka.client.truststore.jks
ssl.truststore.password=your-truststore-password
```

**Lưu ý**: Path trong `ssl.truststore.location` phải match với mount path trong pod template.

### Bước 2: Tạo Kubernetes Secrets

#### Option 1: Từ File (Khuyến Khích)

```bash
# Tạo secret cho client.properties
kubectl create secret generic kafka-client-config \
  --from-file=client.properties=./client.properties \
  --namespace=your-namespace

# Tạo secret cho SSL certificates (nếu cần)
kubectl create secret generic kafka-ssl-certs \
  --from-file=kafka.client.truststore.jks=./kafka.client.truststore.jks \
  --from-file=kafka.client.keystore.jks=./kafka.client.keystore.jks \
  --namespace=your-namespace
```

#### Option 2: Từ YAML

```bash
# Encode file sang base64
cat client.properties | base64 -w 0

# Edit kafka-secret.yaml với base64 content
# Apply secret
kubectl apply -f k8s/kafka-secret.yaml
```

### Bước 3: Deploy Application

#### Cách 1: Spark-Submit với Pod Templates

```bash
spark-submit \
  --master k8s://https://kubernetes-api-server:6443 \
  --deploy-mode cluster \
  --name starrocks-streaming \
  --conf spark.kubernetes.namespace=your-namespace \
  --conf spark.kubernetes.driver.podTemplateFile=k8s/spark-driver-pod-template.yaml \
  --conf spark.kubernetes.executor.podTemplateFile=k8s/spark-executor-pod-template.yaml \
  --conf spark.kubernetes.container.image=your-registry/spark:3.5.0 \
  --conf spark.executor.instances=3 \
  --conf spark.executor.memory=4g \
  --conf spark.executor.cores=2 \
  --py-files dist/app.zip \
  main.py cdc_streaming
```

#### Cách 2: Kubernetes Deployment

```bash
# Apply deployment
kubectl apply -f k8s/deployment-example.yaml

# Check status
kubectl get pods -l app=starrocks-streaming
kubectl logs -f deployment/starrocks-streaming
```

### Bước 4: Verify

Kiểm tra secret đã được mount vào pod:

```bash
# Get pod name
POD_NAME=$(kubectl get pods -l app=starrocks-streaming -o jsonpath='{.items[0].metadata.name}')

# Exec vào pod và check
kubectl exec -it $POD_NAME -- ls -la /etc/kafka/secrets/
kubectl exec -it $POD_NAME -- cat /etc/kafka/secrets/client.properties

# Check environment variable
kubectl exec -it $POD_NAME -- env | grep KAFKA_CLIENT_PROPERTIES
```

## File Structure

```
k8s/
├── kafka-secret.yaml                 # Secret definition cho Kafka config
├── spark-driver-pod-template.yaml    # Driver pod template với secrets
├── spark-executor-pod-template.yaml  # Executor pod template với secrets
├── deployment-example.yaml           # Deployment example
└── KAFKA_AUTH_GUIDE.md               # Hướng dẫn này
```

## Security Best Practices

### 1. RBAC - Giới Hạn Access

Tạo ServiceAccount và RBAC để giới hạn quyền access secrets:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: spark-streaming-sa
  namespace: your-namespace
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: spark-streaming-role
  namespace: your-namespace
rules:
- apiGroups: [""]
  resources: ["secrets"]
  resourceNames: ["kafka-client-config", "kafka-ssl-certs"]
  verbs: ["get"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: spark-streaming-binding
  namespace: your-namespace
subjects:
- kind: ServiceAccount
  name: spark-streaming-sa
roleRef:
  kind: Role
  name: spark-streaming-role
  apiGroup: rbac.authorization.k8s.io
```

Thêm vào pod template:

```yaml
spec:
  serviceAccountName: spark-streaming-sa
```

### 2. Secret Encryption at Rest

Enable encryption at rest cho secrets trong etcd:

```yaml
# /etc/kubernetes/encryption-config.yaml
apiVersion: apiserver.config.k8s.io/v1
kind: EncryptionConfiguration
resources:
  - resources:
    - secrets
    providers:
    - aescbc:
        keys:
        - name: key1
          secret: <base64-encoded-32-byte-key>
    - identity: {}
```

### 3. Secrets Management Tools

Cho production, khuyến khích dùng external secrets management:

- **HashiCorp Vault** với External Secrets Operator
- **AWS Secrets Manager** với External Secrets
- **Azure Key Vault** với Secrets Store CSI Driver
- **Google Secret Manager**

Ví dụ với External Secrets Operator:

```yaml
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: kafka-client-config
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: vault-backend
    kind: SecretStore
  target:
    name: kafka-client-config
  data:
  - secretKey: client.properties
    remoteRef:
      key: kafka/client-config
      property: client.properties
```

## Troubleshooting

### Secret không được mount

```bash
# Check secret exists
kubectl get secret kafka-client-config -n your-namespace

# Describe secret
kubectl describe secret kafka-client-config -n your-namespace

# Check pod events
kubectl describe pod $POD_NAME
```

### Permission Denied

```bash
# Check file permissions trong pod
kubectl exec $POD_NAME -- ls -la /etc/kafka/secrets/

# Nếu cần, set defaultMode trong secret volume:
volumes:
- name: kafka-client-config
  secret:
    secretName: kafka-client-config
    defaultMode: 0400  # Owner read-only
```

### File không tồn tại

Kiểm tra environment variable đang point đúng path:

```bash
kubectl exec $POD_NAME -- env | grep KAFKA_CLIENT_PROPERTIES
# Should output: KAFKA_CLIENT_PROPERTIES=/etc/kafka/secrets/client.properties
```

### Authentication vẫn fail

1. Check log của application:
```bash
kubectl logs $POD_NAME | grep -A 10 "Loading Kafka config"
```

2. Verify nội dung secret:
```bash
kubectl get secret kafka-client-config -o jsonpath='{.data.client\.properties}' | base64 -d
```

3. Test connectivity từ pod:
```bash
kubectl exec $POD_NAME -- ping -c 3 10.165.11.236
kubectl exec $POD_NAME -- nc -zv 10.165.11.236 9093
```

## Update Secrets

Khi cần update authentication config:

```bash
# Delete old secret
kubectl delete secret kafka-client-config -n your-namespace

# Create new secret
kubectl create secret generic kafka-client-config \
  --from-file=client.properties=./client.properties \
  --namespace=your-namespace

# Restart pods để pick up new secret
kubectl rollout restart deployment/starrocks-streaming
```

**Lưu ý**: Secrets được mount vào pod sẽ **không tự động update** khi secret thay đổi. Cần restart pod.

## Multi-Environment Setup

Sử dụng namespaces khác nhau cho dev/staging/prod:

```bash
# Dev
kubectl create secret generic kafka-client-config \
  --from-file=client.properties=./client.properties.dev \
  --namespace=dev

# Staging
kubectl create secret generic kafka-client-config \
  --from-file=client.properties=./client.properties.staging \
  --namespace=staging

# Production
kubectl create secret generic kafka-client-config \
  --from-file=client.properties=./client.properties.prod \
  --namespace=production
```

## Monitoring

Theo dõi secret access:

```bash
# Audit logs (nếu enable)
kubectl logs -n kube-system kube-apiserver-xxx | grep "secrets/kafka-client-config"

# Check pod status
kubectl get pods -w -l app=starrocks-streaming
```

## Tổng Kết

✅ **KHÔNG CẦN** upload file lên từng worker  
✅ Sử dụng Kubernetes Secrets  
✅ Mount secrets vào pods qua pod templates  
✅ Kubernetes tự động distribute đến tất cả nodes  
✅ Secure và dễ quản lý  

Secrets sẽ được **tự động mount** vào tất cả driver và executor pods, cho dù chúng chạy trên worker node nào.
