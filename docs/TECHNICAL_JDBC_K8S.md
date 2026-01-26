# Oracle JDBC Driver trong Kubernetes - Chi tiết kỹ thuật

## 🎯 Vấn đề cốt lõi

Khi chạy Spark trên Kubernetes, mỗi executor chạy trong một pod riêng biệt. Code Python của bạn (`app_config.py:L32`) cần load Oracle JDBC driver:

```python
jdbc_properties = {
    "driver": "oracle.jdbc.driver.OracleDriver"  # ← Class này ở đâu?
}
```

**Câu hỏi**: Làm sao K8s worker nodes biết driver này ở đâu?

## 📊 Kiến trúc luồng hoạt động

### 1. Local Development (Không có vấn đề)
```
┌─────────────────────────────┐
│ Local Machine               │
│                             │
│ Spark Session               │
│   ↓                         │
│ Load từ local jars/         │
│   ojdbc8.jar ✓              │
│   ↓                         │
│ Connect Oracle ✓            │
└─────────────────────────────┘
```

### 2. Kubernetes Cluster (VẤN ĐỀ!)
```
┌──────────────────────────────────────────────────────────┐
│ Kubernetes Cluster                                        │
│                                                           │
│  ┌──────────────┐         ┌──────────────┐              │
│  │ Driver Pod   │         │ Executor Pod │              │
│  │              │         │              │              │
│  │ app_config.py│────────▶│ Read JDBC    │              │
│  │ calls        │         │              │              │
│  │ read.jdbc()  │         │ ❌ ojdbc8.jar │              │
│  │              │         │    NOT FOUND! │              │
│  └──────────────┘         └──────────────┘              │
│                                                           │
│  Problem: Executor không có driver!                      │
└──────────────────────────────────────────────────────────┘
```

## ✅ Giải pháp: Đóng gói vào Docker Image

### Build Phase
```
─────────────────────────────────────────────────
 BUILD TIME (Developer's machine or CI/CD)
─────────────────────────────────────────────────

1. Download ojdbc8.jar
   │
   └─▶ jars/ojdbc8.jar
       
2. Docker build
   │
   └─▶ Dockerfile:
       COPY jars/ojdbc8.jar /opt/spark/jars/
       
3. Docker image created
   │
   └─▶ Registry: gcr.io/project/starrock-streaming:v1.0
       
       Image contents:
       ├── /opt/spark/jars/
       │   ├── ojdbc8.jar    ← JDBC driver
       │   ├── spark-*.jar
       │   └── iceberg-*.jar
       └── /app/
           ├── main.py
           ├── config/app_config.py
           └── ...
```

### Runtime Phase
```
─────────────────────────────────────────────────
 RUNTIME (Kubernetes Cluster)
─────────────────────────────────────────────────

spark-submit --master k8s://...
│
├─▶ 1. Pull image từ registry
│      gcr.io/project/starrock-streaming:v1.0
│
├─▶ 2. Tạo Driver Pod
│      │
│      └─▶ Container:
│          ├── /opt/spark/jars/ojdbc8.jar ✓
│          └── /app/config/app_config.py
│
└─▶ 3. Tạo Executor Pods (N pods)
       │
       ├─▶ Executor Pod 1
       │   └─▶ /opt/spark/jars/ojdbc8.jar ✓
       │
       ├─▶ Executor Pod 2
       │   └─▶ /opt/spark/jars/ojdbc8.jar ✓
       │
       └─▶ Executor Pod 3
           └─▶ /opt/spark/jars/ojdbc8.jar ✓

Tất cả pods đều có ojdbc8.jar!
```

## 🔍 Chi tiết kỹ thuật

### Spark Classpath Resolution

Khi Spark khởi động trong K8s:

1. **Driver JVM** khởi động:
   ```
   java -cp /opt/spark/jars/*:/app/* \
        org.apache.spark.deploy.SparkSubmit
   ```
   
2. **Executor JVM** khởi động (trên worker pods):
   ```
   java -cp /opt/spark/jars/*:/app/* \
        org.apache.spark.executor.CoarseGrainedExecutorBackend
   ```

3. Khi code gọi:
   ```python
   temp_spark.read.jdbc(
       url="jdbc:oracle:thin:@//host:1521/service",
       properties={"driver": "oracle.jdbc.driver.OracleDriver"}
   )
   ```

4. **Spark/JDBC flow**:
   ```
   Python call
     ↓
   PySpark RDD/DataFrame API
     ↓
   Spark SQL (Scala/Java)
     ↓
   JDBC DataSource
     ↓
   DriverManager.getConnection()
     ↓
   Class.forName("oracle.jdbc.driver.OracleDriver")
     ↓
   JVM searches classpath:
     - /opt/spark/jars/ojdbc8.jar ← FOUND! ✓
     ↓
   Load OracleDriver class
     ↓
   Create Connection ✓
   ```

### Why /opt/spark/jars/?

Trong Apache Spark Docker images (base: `apache/spark:3.5.0`):

```dockerfile
# Spark startup script automatically includes:
SPARK_CLASSPATH="/opt/spark/jars/*"
```

Mọi JAR file trong thư mục này tự động được thêm vào classpath của:
- Driver JVM
- Executor JVMs
- PySpark processes

## 🆚 So sánh các giải pháp

### Giải pháp 1: Đóng gói vào image (✅ RECOMMENDED)

**Dockerfile**:
```dockerfile
COPY jars/ojdbc8.jar /opt/spark/jars/
```

**Ưu điểm**:
- ✅ Simple: Build once, run anywhere
- ✅ Fast: JAR đã có sẵn, không cần download
- ✅ Reliable: Không phụ thuộc external services
- ✅ Secure: JAR nằm trong private image
- ✅ Version control: Image tag = full app version

**Nhược điểm**:
- ❌ Image size tăng ~5MB (acceptable)
- ❌ Cần rebuild image khi update JAR

### Giải pháp 2: Upload JAR lên S3/HDFS

**spark-submit**:
```bash
spark-submit \
  --master k8s://... \
  --jars s3a://bucket/jars/ojdbc8.jar \
  main.py
```

**Luồng hoạt động**:
```
Submit
  ↓
Driver starts
  ↓
Download ojdbc8.jar from S3 to driver
  ↓
Executors start
  ↓
Download ojdbc8.jar from S3 to each executor
  ↓
Run job
```

**Ưu điểm**:
- ✅ Không cần rebuild image khi update JAR
- ✅ Share JAR across multiple apps

**Nhược điểm**:
- ❌ Slower startup (download mỗi lần)
- ❌ Cần S3/HDFS infrastructure
- ❌ Network dependency (nếu S3 down → fail)
- ❌ Bandwidth cost
- ❌ Phức tạp config (credentials, permissions)

### Giải pháp 3: Maven packages

**spark-submit**:
```bash
spark-submit \
  --master k8s://... \
  --packages com.oracle.database.jdbc:ojdbc8:21.1.0.0 \
  main.py
```

**Luồng hoạt động**:
```
Submit
  ↓
Resolve dependencies from Maven Central
  ↓
Download ojdbc8.jar
  ↓
Copy to driver & executors
  ↓
Run job
```

**Ưu điểm**:
- ✅ Automatic dependency management
- ✅ Version specification

**Nhược điểm**:
- ❌ Oracle JDBC không có trên Maven Central (!!)
- ❌ Cần config private Maven repo
- ❌ Cần internet trong pods
- ❌ Slower startup
- ❌ Unreliable (Maven server issues)

## 🎓 Best Practices

### Production Checklist

- [ ] Oracle JDBC JAR trong `/opt/spark/jars/`
- [ ] Image được tag với version cụ thể (không dùng `latest`)
- [ ] `.gitignore` excludes `jars/ojdbc*.jar`
- [ ] CI/CD pipeline download JAR automatically
- [ ] Health checks verify JDBC connectivity
- [ ] Monitor connection pool metrics

### Security

```dockerfile
# Switch to root để install
USER root
COPY jars/ojdbc8.jar /opt/spark/jars/

# Switch back to non-root user
USER 185  # spark user
```

### Multi-database Support

Nếu cần nhiều database drivers:

```dockerfile
# Copy multiple drivers
COPY jars/ojdbc8.jar /opt/spark/jars/
COPY jars/postgresql-42.6.0.jar /opt/spark/jars/
COPY jars/mysql-connector-java-8.0.33.jar /opt/spark/jars/
```

Tất cả sẽ có sẵn trong classpath!

## 📚 Tài liệu tham khảo

- [Spark on Kubernetes](https://spark.apache.org/docs/latest/running-on-kubernetes.html)
- [Oracle JDBC Downloads](https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html)
- [Spark Docker Images](https://hub.docker.com/r/apache/spark)
