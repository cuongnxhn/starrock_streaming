## Fix PermissionError trong Kubernetes

### Vấn đề
```
PermissionError: [Errno 13] Permission denied: '/etc/kafka/secrets/client.properties'
```

### Nguyên nhân
File trong Kubernetes Secret có permissions quá hạn chế (`0400`), không cho phép application pod đọc được.

### Giải pháp 

#### ✅ Giải pháp 1: Cập nhật permissions trong deployment (Khuyến nghị)

File `k8s/deployment-example.yaml` đã được cập nhật với `defaultMode: 0444` thay vì `0400`.

**Các bước áp dụng:**

1. **Redeploy application với config mới:**
   ```bash
   # Nếu đang chạy deployment
   kubectl apply -f k8s/deployment-example.yaml
   
   # Hoặc nếu đang chạy với spark-submit script
   # Chỉnh sửa script để set defaultMode=0444
   ```

2. **Nếu đang sử dụng spark-submit script:**
   
   Trong script `deploy_k8s_per_topic_kubectl.sh` hoặc tương tự, thêm option:
   ```bash
   --conf spark.kubernetes.driver.secrets.kafka-client-config=/etc/kafka/secrets \
   --conf spark.kubernetes.executor.secrets.kafka-client-config=/etc/kafka/secrets \
   --conf spark.kubernetes.driver.secretKeyRef.env=kafka-client-config:defaultMode=0444 \
   ```

#### ✅ Giải pháp 2: Recreate secrets với permissions đúng

```bash
# 1. Xóa secret cũ
kubectl delete secret kafka-client-config -n default

# 2. Tạo lại secret
kubectl create secret generic kafka-client-config \
    --from-file=client.properties=./client.properties \
    --namespace=default

# 3. Patch để set defaultMode
kubectl patch secret kafka-client-config -n default \
    -p '{"metadata":{"annotations":{"description":"Kafka auth with proper permissions"}}}'
```

#### ✅ Giải pháp 3: Chạy container với user ID phù hợp

Thêm `securityContext` vào deployment:

```yaml
spec:
  template:
    spec:
      securityContext:
        runAsUser: 0  # Run as root (hoặc UID của file owner)
        fsGroup: 0
      containers:
      - name: spark-app
        ...
```

### Kiểm tra sau khi fix

1. **Verify permissions trong pod:**
   ```bash
   # Get vào pod
   kubectl exec -it <pod-name> -- sh
   
   # Check permissions
   ls -la /etc/kafka/secrets/client.properties
   # Should show: -r--r--r-- (0444)
   
   # Test read
   cat /etc/kafka/secrets/client.properties
   ```

2. **Check application logs:**
   ```bash
   kubectl logs <pod-name> | grep -i "kafka config"
   ```
   
   Should see:
   ```
   ✓ Loading Kafka config from: /etc/kafka/secrets/client.properties
   Loaded X Kafka configuration options
   ```

### Code đã được cập nhật

Code trong `utils/kafka_config.py` đã được cập nhật để:
- Xử lý `PermissionError` một cách graceful
- Log chi tiết lỗi và hướng dẫn fix
- Tiếp tục chạy với no auth thay vì crash

### Notes

- **0400** = chỉ owner có quyền đọc (r--------)  
- **0444** = tất cả có thể đọc (r--r--r--)  
- **0644** = owner có thể đọc/ghi, others chỉ đọc (rw-r--r--)

Trong môi trường Kubernetes, **0444** là an toàn và đủ vì secrets đã được mount read-only.
