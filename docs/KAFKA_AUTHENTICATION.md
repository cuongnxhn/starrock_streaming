# Hướng Dẫn Cấu Hình Kafka Authentication

## Tổng Quan

Dự án đã được cập nhật để hỗ trợ Kafka authentication thông qua file `client.properties`. Tất cả các kết nối Kafka (trong schema inference, streaming, và debug) đã được cập nhật để sử dụng authentication config.

## Các File Đã Cập Nhật

1. **`utils/kafka_config.py`** - Helper class mới để load và apply Kafka authentication
2. **`core/schema_processors/default.py`** - Thêm auth vào schema inference từ Kafka
3. **`core/etl_job.py`** - Thêm auth vào streaming ETL job
4. **`debug_kafka.py`** - Thêm auth vào debug script
5. **`streaming.py`** - Thêm auth vào streaming script

## Cách Sử Dụng

### Bước 1: Tạo File `client.properties`

1. Copy file mẫu:
   ```bash
   cp client.properties.example client.properties
   ```

2. Chỉnh sửa `client.properties` với thông tin authentication thực tế của Kafka cluster:

   ```properties
   # Ví dụ SASL/PLAIN với SSL
   security.protocol=SASL_SSL
   sasl.mechanism=PLAIN
   sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="your-username" password="your-password";
   ssl.truststore.location=/path/to/truststore.jks
   ssl.truststore.password=your-truststore-password
   ```

3. Đảm bảo file `client.properties` nằm ở một trong các vị trí sau:
   - Cùng thư mục với project: `./client.properties`
   - Trong folder config: `./config/client.properties`
   - Đường dẫn trong biến môi trường: `KAFKA_CLIENT_PROPERTIES`
   - Hệ thống: `/etc/kafka/client.properties`

### Bước 2: Cấu Hình Cho Các Loại Authentication

#### SASL/PLAIN (Username/Password)

```properties
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin-secret";

# Nếu có SSL
ssl.truststore.location=/path/to/kafka.client.truststore.jks
ssl.truststore.password=truststore-password
```

#### SASL/SCRAM-SHA-256

```properties
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="admin-secret";
ssl.truststore.location=/path/to/kafka.client.truststore.jks
ssl.truststore.password=truststore-password
```

#### SSL/TLS (Mutual TLS)

```properties
security.protocol=SSL
ssl.truststore.location=/path/to/kafka.client.truststore.jks
ssl.truststore.password=truststore-password
ssl.keystore.location=/path/to/kafka.client.keystore.jks
ssl.keystore.password=keystore-password
ssl.key.password=key-password
```

#### Kerberos (GSSAPI)

```properties
security.protocol=SASL_SSL
sasl.mechanism=GSSAPI
sasl.kerberos.service.name=kafka
sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true keyTab="/path/to/client.keytab" principal="kafka-client@REALM";
ssl.truststore.location=/path/to/kafka.client.truststore.jks
ssl.truststore.password=truststore-password
```

### Bước 3: Chạy Application

Sau khi cấu hình `client.properties`, chạy application như bình thường:

```bash
# Debug Kafka connection
python debug_kafka.py

# Chạy streaming job
python main.py cdc_streaming
```

## Cách Hoạt Động

1. **Load Config**: Khi khởi động, `KafkaConfig.load_auth_config()` sẽ tự động tìm và đọc file `client.properties`

2. **Parse Properties**: File được parse và convert sang format Spark Kafka options (prefix `kafka.`)

3. **Apply Options**: Các options được apply vào tất cả Kafka readers/writers thông qua `KafkaConfig.apply_auth_options()`

4. **Log Output**: System sẽ log các config đã load (hide password/secret)

## Troubleshooting

### File `client.properties` không tìm thấy

**Triệu chứng**: Warning message `⚠ WARNING: Kafka client.properties not found. Using no authentication.`

**Giải pháp**:
- Kiểm tra file tồn tại ở một trong các vị trí được tìm kiếm
- Hoặc set environment variable: `export KAFKA_CLIENT_PROPERTIES=/path/to/client.properties`

### Authentication Failed

**Triệu chứng**: Lỗi kết nối Kafka với message về authentication

**Giải pháp**:
1. Kiểm tra username/password đúng
2. Kiểm tra `security.protocol` và `sasl.mechanism` match với Kafka broker
3. Kiểm tra SSL certificates nếu dùng SSL
4. Xem log của Kafka broker để biết chi tiết lỗi

### SSL Certificate Errors

**Triệu chứng**: SSL handshake failed, certificate errors

**Giải pháp**:
1. Kiểm tra truststore/keystore path đúng
2. Kiểm tra password của truststore/keystore
3. Kiểm tra certificate còn hạn sử dụng
4. Verify hostname trong certificate match với broker hostname

## Testing

Để test authentication config:

```bash
# Test với debug script
python debug_kafka.py

# Hoặc set specific topic
export DEBUG_TOPIC=your-topic-name
python debug_kafka.py
```

Script sẽ:
1. Load authentication config từ `client.properties`
2. Kết nối tới Kafka với authentication
3. List topics và đọc sample data
4. Report kết quả

## Ví Dụ Output

```
✓ Loading Kafka config from: ./client.properties
  Loaded 4 Kafka configuration options
    - kafka.security.protocol: SASL_SSL
    - kafka.sasl.mechanism: PLAIN
    - kafka.sasl.jaas.config: ***
    - kafka.ssl.truststore.location: /path/to/truststore.jks
```

## Lưu Ý Bảo Mật

1. **Không commit** file `client.properties` vào Git (đã thêm vào `.gitignore`)
2. **Protect file permissions**: `chmod 600 client.properties`
3. **Sử dụng secrets management** trong production (Kubernetes Secrets, HashiCorp Vault, etc.)
4. **Rotate credentials** định kỳ

## Environment Variables Alternative

Nếu không muốn dùng file, có thể set từng option qua environment variables và modify code để load từ env:

```python
# Trong utils/kafka_config.py, có thể extend để support:
kafka_options = {
    "kafka.security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
    "kafka.sasl.mechanism": os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
    # etc...
}
```

## Tham Khảo

- [Kafka Security Documentation](https://kafka.apache.org/documentation/#security)
- [Spark Kafka Integration Guide](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
- [SASL Configuration](https://kafka.apache.org/documentation/#security_sasl)
- [SSL Configuration](https://kafka.apache.org/documentation/#security_ssl)
