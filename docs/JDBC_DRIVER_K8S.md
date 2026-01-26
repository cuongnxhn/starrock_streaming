# Oracle JDBC Driver Setup cho Kubernetes

## Vấn đề
Khi submit Spark job lên K8s, worker nodes (executors) cần có Oracle JDBC driver (`ojdbc8.jar`) để kết nối Oracle database. Driver class `oracle.jdbc.driver.OracleDriver` phải được load được trên tất cả các executors.

## Giải pháp: Đóng gói JDBC driver vào Docker Image

### Bước 1: Download Oracle JDBC Driver

1. Tải `ojdbc8.jar` từ Oracle:
   - Link: https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html
   - Chọn phiên bản phù hợp với Oracle Database của bạn (thường là ojdbc8.jar cho Oracle 12c+)

2. Tạo thư mục `jars` trong root project:
   ```bash
   mkdir jars
   ```

3. Copy file `ojdbc8.jar` vào thư mục `jars/`:
   ```
   starrock_streaming/
   ├── jars/
   │   └── ojdbc8.jar  <-- File này
   ├── Dockerfile
   ├── main.py
   └── ...
   ```

### Bước 2: Build Docker Image

```bash
# Build image với Oracle JDBC driver đã được đóng gói sẵn
docker build -t your-registry/starrock-streaming:latest .

# Push to registry
docker push your-registry/starrock-streaming:latest
```

### Bước 3: Update deploy script

File `deploy_k8s_per_topic.sh` đã sẵn sàng. Chỉ cần update biến `DOCKER_IMAGE`:

```bash
DOCKER_IMAGE="your-registry/starrock-streaming:latest"
```

### Bước 4: Deploy lên K8s

```bash
./deploy_k8s_per_topic.sh my_app orders,payments
```

## Cách hoạt động

1. **Docker Image**: 
   - File `ojdbc8.jar` được copy vào `/opt/spark/jars/` trong image
   - Spark tự động load tất cả JAR files trong thư mục này

2. **K8s Executors**:
   - Mỗi executor pod được tạo từ cùng Docker image
   - Tất cả executors đều có `ojdbc8.jar` trong classpath
   - Oracle driver class `oracle.jdbc.driver.OracleDriver` có thể được load thành công

3. **JDBC Connection**:
   - Khi code gọi `temp_spark.read.jdbc(...)` trong `app_config.py`
   - Spark tự động tìm và load driver từ `/opt/spark/jars/ojdbc8.jar`
   - Connection hoạt động trên cả driver và executors

## Kiểm tra

### Verify JAR trong Pod
```bash
# Get pod name
kubectl get pods -l app=my_app

# Check jars trong executor pod
kubectl exec -it <pod-name> -- ls -la /opt/spark/jars/
# Phải thấy ojdbc8.jar trong danh sách
```

### Check logs
```bash
kubectl logs -f <pod-name>
```

Nếu thiếu driver, bạn sẽ thấy lỗi:
```
java.sql.SQLException: No suitable driver found for jdbc:oracle:thin
```

## Giải pháp thay thế (KHÔNG khuyến nghị cho K8s)

### Cách 2: Sử dụng --jars trong spark-submit
```bash
# Cần upload jar lên S3/HDFS trước
spark-submit \
  --master k8s://... \
  --jars s3a://bucket/jars/ojdbc8.jar \
  main.py
```

**Nhược điểm**: 
- Phải upload jar lên distributed storage
- Slower startup time (download mỗi lần chạy)
- Phức tạp hơn

### Cách 3: Sử dụng --packages (Maven coordinates)
```bash
spark-submit \
  --master k8s://... \
  --packages com.oracle.database.jdbc:ojdbc8:21.1.0.0 \
  main.py
```

**Nhược điểm**:
- Cần internet access từ K8s pods
- Oracle JDBC không có sẵn trên Maven Central (phải config private repo)
- Unreliable trong production

## Khuyến nghị

✅ **Sử dụng Cách 1**: Đóng gói JAR vào Docker image
- Simple và reliable
- Fast startup
- No external dependencies
- Best practice cho K8s deployment
