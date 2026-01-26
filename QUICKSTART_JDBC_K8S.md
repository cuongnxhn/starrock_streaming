# Quick Start Guide - Oracle JDBC trên Kubernetes

## 📋 Tóm tắt vấn đề

Khi bạn submit Spark job lên K8s với `spark-submit --master k8s://...`, driver và executors chạy trong các pods riêng biệt. Mỗi executor cần có Oracle JDBC driver (`ojdbc8.jar`) để kết nối database.

**Câu hỏi**: Worker nodes biết `oracle.jdbc.driver.OracleDriver` ở đâu?
**Trả lời**: Thông qua Docker image! Tất cả pods (driver + executors) đều dùng cùng 1 image có sẵn JAR file.

## 🚀 Hướng dẫn triển khai (3 bước)

### Bước 1: Download và đặt Oracle JDBC JAR

```powershell
# Tạo thư mục jars
mkdir jars

# Download ojdbc8.jar từ Oracle và đặt vào:
# jars/ojdbc8.jar
```

📥 Link download: https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html

### Bước 2: Build Docker Image

```powershell
# Sửa registry trong build_and_push.ps1
# REGISTRY = "your-registry"  → REGISTRY = "gcr.io/your-project"

# Build và push
.\build_and_push.ps1 v1.0
```

### Bước 3: Deploy lên K8s

```bash
# Sửa DOCKER_IMAGE trong deploy_k8s_per_topic.sh
# DOCKER_IMAGE="gcr.io/your-project/starrock-streaming:v1.0"

# Deploy
./deploy_k8s_per_topic.sh my_app orders,payments
```

## 📦 Cách hoạt động

```
┌─────────────────────────────────────────────┐
│  Docker Image                               │
│  (được pull về mỗi K8s node)               │
├─────────────────────────────────────────────┤
│  /opt/spark/jars/                          │
│    ├── ojdbc8.jar          ← JDBC driver   │
│    ├── spark-core.jar                      │
│    └── ...                                 │
│                                            │
│  /app/                                     │
│    ├── main.py                             │
│    ├── config/app_config.py                │
│    └── ...                                 │
└─────────────────────────────────────────────┘
           │
           ├─────────────┬──────────────┐
           ▼             ▼              ▼
     ┌─────────┐   ┌─────────┐   ┌─────────┐
     │Driver   │   │Executor │   │Executor │
     │Pod      │   │Pod 1    │   │Pod 2    │
     │         │   │         │   │         │
     │ojdbc8✓  │   │ojdbc8✓  │   │ojdbc8✓  │
     └─────────┘   └─────────┘   └─────────┘
```

**Khi code gọi:**
```python
temp_spark.read.jdbc(
    url=jdbc_url,
    table=query,
    properties={"driver": "oracle.jdbc.driver.OracleDriver"}
)
```

**Spark tự động:**
1. Tìm `oracle.jdbc.driver.OracleDriver` trong classpath
2. Load class từ `/opt/spark/jars/ojdbc8.jar`
3. Tạo JDBC connection
4. ✅ SUCCESS!

## ✅ Verify

### Check JAR trong pod
```bash
kubectl get pods -l app=my_app
kubectl exec -it <pod-name> -- ls -la /opt/spark/jars/ojdbc8.jar
```

### Check logs
```bash
kubectl logs -f <pod-name>

# Nếu thành công, sẽ thấy:
# ✓ Loaded 3 topics for my_app

# Nếu thiếu driver, sẽ thấy:
# java.sql.SQLException: No suitable driver found
```

## 🔧 Troubleshooting

### Lỗi: "No suitable driver found"
- ❌ Image chưa có ojdbc8.jar
- ✅ Build lại image với JAR file

### Lỗi: "ClassNotFoundException: oracle.jdbc.driver.OracleDriver"
- ❌ JAR file không đúng vị trí
- ✅ Phải ở `/opt/spark/jars/` không phải nơi khác

### Lỗi: Image pull failed
- ❌ Registry URL sai hoặc chưa push
- ✅ Check `docker images` và `docker push`

## 📚 Chi tiết

Xem file đầy đủ: `docs/JDBC_DRIVER_K8S.md`
