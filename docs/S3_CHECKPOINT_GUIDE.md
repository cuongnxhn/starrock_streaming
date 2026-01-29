# S3 Checkpoint Configuration Guide

## Vấn đề

Spark Streaming job không thể ghi checkpoint vào S3, dẫn đến:
- Checkpoint được lưu ở local filesystem trong pod (mất khi pod restart)
- Không tìm thấy `checkpoints/` folder trong S3 bucket
- Job luôn đọc từ latest offset thay vì earliest

## Nguyên nhân

Executor pods thiếu S3 credentials nên không thể access S3. Spark tự động fallback về local filesystem (`/tmp`).

## Giải pháp

### 1. Tạo S3 Credentials Secret

```bash
# Chạy script để tạo secret
chmod +x setup_s3_secrets.sh
./setup_s3_secrets.sh
```

Hoặc tạo thủ công:

```bash
kubectl create secret generic s3-credentials \
    --from-literal=access-key="minioadmin" \
    --from-literal=secret-key="minioadmin" \
    -n default
```

**Lưu ý**: Thay `minioadmin/minioadmin` bằng credentials thực tế của bạn.

### 2. Verify Secret đã được tạo

```bash
kubectl get secret s3-credentials -n default
kubectl describe secret s3-credentials -n default
```

### 3. Update Pod Templates (ĐÃ HOÀN THÀNH)

✓ Pod templates đã được update với S3 environment variables:
- `k8s/spark-driver-pod-template.yaml`
- `k8s/spark-executor-pod-template.yaml`

### 4. Redeploy Job

```bash
# Xóa job cũ
kubectl delete pods -l app=<your-app-name> -n default

# Deploy lại job mới
./deploy_k8s_per_topic.sh <app-name> <topic-name>
```

### 5. Verify Checkpoint được tạo trong S3

Sau khi job chạy, kiểm tra:

```bash
# Nếu dùng MinIO client
mc ls minio/iceberg/checkpoints/

# Nếu dùng AWS CLI
aws s3 ls s3a://iceberg/checkpoints/ --endpoint-url http://10.165.13.116:9000
```

Bạn sẽ thấy:
```
checkpoints/
├── <app-name>/
│   ├── <topic-name>/
│   │   ├── commits/
│   │   ├── metadata
│   │   └── offsets/
```

## Xóa Checkpoint để đọc lại từ đầu

Nếu muốn job đọc lại từ earliest offset:

```bash
# Delete checkpoint directory trong S3
mc rm --recursive minio/iceberg/checkpoints/<app-name>/<topic-name>

# Hoặc dùng AWS CLI
aws s3 rm s3a://iceberg/checkpoints/<app-name>/<topic-name> --recursive \
    --endpoint-url http://10.165.13.116:9000

# Restart job
kubectl delete pod -l topic=<topic-name> -n default
```

## Environment Variables trong Pod Templates

Pod templates hiện có các biến:

```yaml
env:
  # Kafka
  - name: KAFKA_CLIENT_PROPERTIES
    value: /etc/kafka/secrets/client.properties
  
  # S3 (MỚI THÊM)
  - name: S3_ENDPOINT
    value: "http://10.165.13.116:9000"
  - name: S3_ACCESS_KEY
    valueFrom:
      secretKeyRef:
        name: s3-credentials
        key: access-key
  - name: S3_SECRET_KEY
    valueFrom:
      secretKeyRef:
        name: s3-credentials
        key: secret-key
  
  # Iceberg
  - name: STARROCK_FE
    value: "http://10.165.13.116:8181"
  - name: ICEBERG_WAREHOUSE
    value: "s3a://iceberg/warehouse"
```

## Troubleshooting

### Lỗi: "No FileSystem for scheme: s3a"

Đảm bảo JAR files đã có trong Docker image:
- `hadoop-aws-3.3.4.jar`
- `aws-java-sdk-bundle-1.12.x.jar`

### Lỗi: "Access Denied" khi ghi vào S3

Kiểm tra:
1. Secret có chứa đúng credentials không
2. MinIO/S3 bucket policy có cho phép ghi không
3. Endpoint URL có đúng không

```bash
# Test S3 access từ pod
kubectl exec -it <pod-name> -- aws s3 ls s3a://iceberg/ \
    --endpoint-url http://10.165.13.116:9000
```

### Checkpoint vẫn ghi vào local filesystem

Kiểm tra log của executor:

```bash
kubectl logs <executor-pod-name> | grep -i "checkpoint\|s3"
```

Nếu thấy lỗi S3, verify:
1. Secret đã được tạo chưa
2. Pod template đã mount secret chưa
3. Environment variables có xuất hiện trong pod không

```bash
kubectl exec -it <pod-name> -- env | grep S3
```

## Best Practices

1. **Sử dụng Kubernetes Secrets** cho credentials, không hardcode trong YAML
2. **Enable versioning** trong S3 bucket để có thể rollback checkpoint
3. **Monitor checkpoint size** để tránh tốn storage
4. **Cleanup old checkpoints** định kỳ
5. **Backup checkpoint** trước khi xóa để reset offset

## Next Steps

Sau khi setup xong:
1. ✅ Tạo S3 credentials secret
2. ✅ Verify secret đã được tạo
3. ✅ Redeploy job
4. ✅ Kiểm tra checkpoint trong S3
5. ✅ Monitor job logs để đảm bảo đọc được message

## References

- [Spark Structured Streaming Checkpointing](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#recovering-from-failures-with-checkpointing)
- [Hadoop AWS S3A](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html)
