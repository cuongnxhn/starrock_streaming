# Spark on Kubernetes - Hướng dẫn Deploy và Debug

## 🔧 Chuẩn bị

### 1. Build Docker Image
```bash
# Tạo Dockerfile nếu chưa có
docker build -t <your-registry>/starrock-streaming:latest .
docker push <your-registry>/starrock-streaming:latest
```

### 2. Setup Kubernetes RBAC
```bash
kubectl apply -f k8s/spark-rbac.yaml
```

### 3. Deploy Spark History Server (optional nhưng recommended)
```bash
# Sửa S3 credentials trong spark-history-server.yaml trước
kubectl apply -f k8s/spark-history-server.yaml
```

### 4. Deploy Spark UI Service (optional - để expose UI ra ngoài)
```bash
kubectl apply -f k8s/spark-ui-service.yaml
```

---

## 🚀 Deploy Jobs

### Sửa config trong `deploy_k8s_per_topic.sh`:
- `DOCKER_IMAGE`: Image Docker của bạn
- `SPARK_MASTER`: URL của Kubernetes API (nếu chạy từ bên ngoài cluster)

### Chạy deployment:
```bash
chmod +x deploy_k8s_per_topic.sh
./deploy_k8s_per_topic.sh my_app orders,payments,users
```

---

## 🔍 Xem Spark UI - 3 Cách

### **Cách 1: Port Forward (Đơn giản nhất - cho dev)**
```bash
# Tìm driver pod
kubectl get pods -l spark-role=driver

# Port forward
kubectl port-forward my_app-orders-driver 4040:4040

# Mở browser
open http://localhost:4040
```

**✅ Ưu điểm:**
- Đơn giản, nhanh
- Không cần config network
- An toàn (chỉ local access)

**❌ Nhược điểm:**
- Chỉ xem được 1 job tại 1 thời điểm
- Phải giữ terminal mở
- Chỉ xem được job đang chạy

---

### **Cách 2: NodePort Service (Cho team)**
```bash
# Service đã được tạo ở bước 4
# Lấy Node IP
kubectl get nodes -o wide

# Access UI
# http://<node-ip>:30040
```

**✅ Ưu điểm:**
- Nhiều người cùng xem
- Không cần port-forward
- Persistent

**❌ Nhược điểm:**
- Cần mở port trên firewall
- Chỉ xem được job có label `spark-role=driver`
- Nếu nhiều job, service sẽ load-balance (có thể nhảy qua nhảy lại)

---

### **Cách 3: Spark History Server (Cho production)**
```bash
# Access History Server
# http://<node-ip>:30080

# Xem tất cả jobs đã chạy, đang chạy, hoàn thành
```

**✅ Ưu điểm:**
- Xem được TẤT CẢ jobs (đang chạy + đã hoàn thành)
- Persistent storage
- Tốt nhất cho debug sau khi job fail
- Có thể replay events

**❌ Nhược điểm:**
- Phải config S3/HDFS để lưu event logs
- Delay nhỏ (10s update interval)

---

## 🐛 Debug Commands

### Xem logs real-time:
```bash
kubectl logs -f my_app-orders-driver
```

### Xem tất cả pods của app:
```bash
kubectl get pods -l app=my_app
```

### Xem executor logs:
```bash
kubectl get pods -l spark-role=executor,app=my_app
kubectl logs -f <executor-pod-name>
```

### Describe pod (xem events, errors):
```bash
kubectl describe pod my_app-orders-driver
```

### Xem resource usage:
```bash
kubectl top pod my_app-orders-driver
```

### Exec vào pod:
```bash
kubectl exec -it my_app-orders-driver -- /bin/bash
```

---

## 📊 Spark UI Features

Khi vào Spark UI, bạn có thể xem:

1. **Jobs Tab**: Tất cả jobs, stages, tasks
2. **Stages Tab**: Chi tiết từng stage, task timeline
3. **Storage Tab**: Cached data, memory usage
4. **Environment Tab**: Spark config, JVM properties
5. **Executors Tab**: Executor metrics, GC time, shuffle
6. **SQL Tab**: Query plans, execution details (quan trọng!)
7. **Streaming Tab**: Streaming metrics (nếu dùng Structured Streaming)

---

## 🎯 Tips cho Debug

### 1. Job chạy chậm:
- Xem **Stages → Event Timeline** để tìm bottleneck
- Check **Executors Tab** xem có executor nào die không
- Xem **SQL Tab → Physical Plan** xem có shuffle không cần thiết

### 2. Job fail:
- Xem **Executors Tab → Logs** để xem stderr/stdout
- Hoặc dùng `kubectl logs`
- Check History Server nếu driver pod đã bị xóa

### 3. Out of Memory:
- Xem **Storage Tab** xem cached data
- Xem **Executors Tab → Memory Usage**
- Adjust `spark.executor.memory`, `spark.driver.memory`

### 4. Slow shuffle:
- Xem **Stages → Shuffle Read/Write**
- Consider tăng `spark.sql.shuffle.partitions`
- Enable AQE (đã enable trong script)

---

## 🔒 Security Notes

Nếu expose UI ra public internet:

1. **Sử dụng Ingress với authentication:**
```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: spark-ui-ingress
  annotations:
    nginx.ingress.kubernetes.io/auth-type: basic
    nginx.ingress.kubernetes.io/auth-secret: basic-auth
spec:
  rules:
  - host: spark-ui.example.com
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: spark-ui-service
            port:
              number: 4040
```

2. **Hoặc dùng VPN/Bastion host**

3. **Hạn chế IP ranges với NetworkPolicy**

---

## 📖 Tham khảo

- [Spark on Kubernetes Documentation](https://spark.apache.org/docs/latest/running-on-kubernetes.html)
- [Spark Configuration](https://spark.apache.org/docs/latest/configuration.html)
- [Kubernetes RBAC](https://kubernetes.io/docs/reference/access-authn-authz/rbac/)
