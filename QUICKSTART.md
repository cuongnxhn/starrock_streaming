# Quick Start Guide

## 🚀 Chạy Job theo Topic

### 1. Chạy tất cả topics (Cách cũ)
```bash
spark-submit main.py --app-name my_app
```

### 2. Chạy 1 topic cụ thể (RECOMMENDED ⭐)
```bash
# Topic: orders
spark-submit main.py \
    --app-name my_app \
    --topic-name orders

# Topic: payments
spark-submit main.py \
    --app-name my_app \
    --topic-name payments
```

### 3. Deploy nhiều topics cùng lúc
```powershell
# Windows
.\deploy_per_topic.ps1 -AppName "my_app" -Topics "orders,payments,users"

# Linux/Mac
./deploy_per_topic.sh my_app orders,payments,users
```

---

## 🎨 Tạo Custom Schema Processor

### Khi nào cần?
- Topic có format envelope khác CDC standard
- Cần hardcode schema (không trust metadata)
- Cần validation/transformation đặc biệt

### Example: Topic có envelope đơn giản

**File:** `core/schema_processors/custom/simple_events.py`
```python
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from ..base import BaseSchemaProcessor

class SimpleEventsProcessor(BaseSchemaProcessor):
    """Processor cho events topic có envelope đơn giản"""
    
    def get_data_schema(self, topic_config):
        # Hardcode schema
        return StructType([
            StructField("event_id", StringType(), False),
            StructField("user_id", StringType(), True),
            StructField("action", StringType(), True),
            StructField("timestamp", TimestampType(), True),
        ])
    
    def get_envelope_schema(self, topic_config, data_schema):
        # Envelope đơn giản, không phải CDC
        return StructType() \
            .add("event_type", StringType()) \
            .add("payload", data_schema)
```

**Register:** `core/schema_processors/custom/__init__.py`
```python
from .simple_events import SimpleEventsProcessor
from ..factory import SchemaProcessorFactory

# Register cho topic 'user_events'
SchemaProcessorFactory.register_processor('user_events', SimpleEventsProcessor)
```

**Chạy:**
```bash
spark-submit main.py --app-name my_app --topic-name user_events
# → Tự động dùng SimpleEventsProcessor
```

---

## 📋 Testing Local

### 1. Test với 1 topic
```bash
# Local mode
spark-submit \
    --master local[*] \
    main.py \
    --app-name test_app \
    --topic-name orders \
    --kafka-bootstrap-servers localhost:9092 \
    --checkpoint-dir file:///tmp/checkpoints
```

### 2. Kiểm tra available topics
```bash
# Chạy với topic không tồn tại
spark-submit main.py --app-name my_app --topic-name nonexistent

# Output:
# Error: Topic 'nonexistent' not found
# Available topics: ['orders', 'payments', 'users']
```

### 3. Debug schema processor
```python
# Trong custom processor, thêm logging
def get_data_schema(self, topic_config):
    print(f"  🔍 DEBUG: Processing topic {topic_config['topic_name']}")
    print(f"  🔍 DEBUG: Schema definition: {topic_config.get('schema_definition')}")
    # ... your logic
```

---

## 🛠 Production Deployment

### YARN Cluster Mode
```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --name "my_app_orders" \
    --executor-memory 4g \
    --executor-cores 2 \
    --driver-memory 2g \
    --conf spark.sql.adaptive.enabled=true \
    main.py \
    --app-name my_app \
    --topic-name orders \
    --kafka-bootstrap-servers prod-kafka:9092 \
    --checkpoint-dir s3a://iceberg/checkpoints/
```

### Với automation (recommended)
```powershell
# Deploy tất cả topics của my_app
.\deploy_per_topic.ps1 -AppName "my_app" -Topics "orders,payments,users,inventory,logs"

# Script sẽ:
# 1. Submit từng topic như 1 job riêng
# 2. Tự động đặt tên job: my_app_orders, my_app_payments, ...
# 3. Sleep 5s giữa các submission
```

---

## 📊 Monitoring

### Check running jobs
```bash
# List all jobs
yarn application -list

# Filter by app name
yarn application -list | grep my_app

# Output:
# my_app_orders       RUNNING
# my_app_payments     RUNNING
# my_app_users        RUNNING
```

### Check logs
```bash
# Xem logs của job cụ thể
yarn logs -applicationId application_1234567890_0001

# Hoặc trong Spark UI
http://your-spark-master:8080
```

### Check Kafka lag (monitoring alert nên track)
```bash
# Check consumer group lag
kafka-consumer-groups.sh \
    --bootstrap-server kafka:9092 \
    --describe \
    --group my_app_orders
```

---

## 🔥 Common Issues

### Issue 1: Topic not found
```
Error: Topic 'orders' not found in config for app 'my_app'
Available topics: ['payments', 'users']
```
**Fix:** Kiểm tra topic có trong database config không.

### Issue 2: Schema inference failed
```
✗ ERROR: No messages found in topic 'new_topic' for schema inference
```
**Fix:** 
- Ensure topic có data
- Hoặc tạo custom processor với hardcoded schema

### Issue 3: Multiple processors registered
```python
# WRONG - gây conflict
SchemaProcessorFactory.register_processor('orders', ProcessorA)
SchemaProcessorFactory.register_processor('orders', ProcessorB)  # Override!

# RIGHT - dùng pattern
SchemaProcessorFactory.register_processor('orders_v1', ProcessorA)
SchemaProcessorFactory.register_processor('orders_v2', ProcessorB)
```

---

## 📚 More Info

- **Full documentation:** `core/schema_processors/README.md`
- **Refactoring summary:** `REFACTORING_SUMMARY.md`
- **Example custom processor:** `core/schema_processors/custom/example_special_topic.py`

---

## ✅ Checklist per Deployment

- [ ] Config trong database đã có topic chưa?
- [ ] Topic có data trong Kafka chưa? (nếu dùng schema inference)
- [ ] Cần custom processor không?
- [ ] Kafka lag có bị quá cao không? (adjust `maxOffsetsPerTrigger`)
- [ ] Checkpoint location có quyền write không?
- [ ] Resources (memory, cores) có đủ không?
- [ ] Monitoring & alerting đã setup chưa?

---

Happy Streaming! 🚀
