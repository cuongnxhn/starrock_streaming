# StarRock Streaming - Kafka to Iceberg ETL

Hệ thống streaming ETL để sync dữ liệu từ Kafka topics vào Iceberg tables với support CDC (Change Data Capture).

## 🌟 Tính năng chính

- ✅ **Per-Topic Deployment**: Mỗi topic có thể chạy như một Spark job riêng biệt
- ✅ **Modular Schema Processing**: Tách logic schema processing, dễ customize cho từng topic
- ✅ **CDC Support**: Hỗ trợ Insert/Update operations với CDC envelope
- ✅ **Schema Evolution**: Tự động detect và add columns mới
- ✅ **Auto Schema Inference**: Tự động infer schema từ Kafka nếu không có metadata
- ✅ **Configurable**: Metadata-driven configuration từ database
- ✅ **Production-Ready**: Monitoring, error handling, checkpointing

## 🏗 Architecture

```
Kafka Topics  →  Spark Streaming Jobs  →  Iceberg/StarRocks Tables
                         ↓
                 Schema Processors
                    (Modular)
```

**Highlights:**
- **Isolation**: Mỗi topic = 1 Spark job riêng → lỗi không lan rộng
- **Flexibility**: Custom schema processor cho topics đặc biệt
- **Scalability**: Dễ dàng scale từng topic độc lập

Chi tiết: [`ARCHITECTURE.md`](ARCHITECTURE.md)

## 🚀 Quick Start

### 1. Chạy tất cả topics (Legacy mode)
```bash
spark-submit main.py --app-name my_app
```

### 2. Chạy 1 topic cụ thể (RECOMMENDED ⭐)
```bash
spark-submit main.py \
    --app-name my_app \
    --topic-name orders
```

### 3. Deploy nhiều topics tự động
```powershell
# Windows
.\deploy_per_topic.ps1 -AppName "my_app" -Topics "orders,payments,users"

# Linux/Mac
./deploy_per_topic.sh my_app orders,payments,users
```

Hướng dẫn chi tiết: [`QUICKSTART.md`](QUICKSTART.md)

## 📁 Project Structure

```
starrock_streaming/
├── main.py                      # Entry point
├── core/
│   ├── etl_job.py              # Main ETL logic
│   └── schema_processors/      # ⭐ Schema processing system
│       ├── base.py             # Abstract base class
│       ├── default.py          # Default implementation
│       ├── factory.py          # Factory pattern
│       ├── custom/             # Custom processors
│       └── README.md           # Detailed docs
├── config/
│   └── app_config.py           # Config loader
├── utils/
│   ├── spark.py                # Spark session factory
│   └── common.py               # Helper functions
├── deploy_per_topic.ps1        # Deployment script (Windows)
├── deploy_per_topic.sh         # Deployment script (Linux/Mac)
└── docs/
    ├── QUICKSTART.md           # Quick start guide
    ├── ARCHITECTURE.md         # Architecture details
    ├── REFACTORING_SUMMARY.md  # Recent changes
    └── CHECKLIST.md            # Implementation checklist
```

## 🎨 Custom Schema Processors

Tạo custom processor cho topics đặc biệt:

```python
# File: core/schema_processors/custom/my_topic.py
from ..base import BaseSchemaProcessor
from pyspark.sql.types import StructType, StructField, StringType

class MyTopicProcessor(BaseSchemaProcessor):
    def get_data_schema(self, topic_config):
        # Custom schema logic
        return StructType([
            StructField("id", StringType(), False),
            StructField("data", StringType(), True),
        ])
    
    def get_envelope_schema(self, topic_config, data_schema):
        # Custom envelope
        return StructType() \
            .add("event_type", StringType()) \
            .add("payload", data_schema)
```

**Register:**
```python
# File: core/schema_processors/custom/__init__.py
from .my_topic import MyTopicProcessor
from ..factory import SchemaProcessorFactory

SchemaProcessorFactory.register_processor('my_topic', MyTopicProcessor)
```

Chi tiết: [`core/schema_processors/README.md`](core/schema_processors/README.md)

## ⚙️ Configuration

### Database Metadata

Hệ thống đọc config từ database tables:

**spark_app_config**: Application-level config
```sql
{
  app_name: 'my_app',
  executor_cores: 4,
  executor_memory: '8g',
  scheduler_mode: 'FAIR',
  max_concurrent_queries: 5
}
```

**spark_topics**: Topic-level config
```sql
{
  topic_name: 'orders',
  table_name: 'orders',
  schema_definition: '...',  -- JSON schema (optional)
  partition_columns: 'order_date',
  expected_msg_per_day: 1000000
}
```

### Environment Variables

```bash
STARROCK_FE=http://your-starrocks:8181
ICEBERG_WAREHOUSE=s3a://iceberg/warehouse
```

## 🔧 Development

### Requirements
```
pyspark==3.5.0
```

### Local Testing
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

### Running Tests
```bash
# Full tests (requires PySpark)
python tests/test_refactoring.py

# Lightweight tests (no PySpark needed)
python tests/test_refactoring_lightweight.py
```

## 📊 Monitoring

### Spark UI
- Application tracking: `http://spark-master:8080`
- Job details, stages, tasks

### YARN UI
```bash
yarn application -list
yarn logs -applicationId <app_id>
```

### Kafka Consumer Lag
```bash
kafka-consumer-groups.sh \
    --bootstrap-server kafka:9092 \
    --describe \
    --group my_app_orders
```

### Iceberg Tables
```sql
-- Check table status
DESCRIBE FORMATTED iceberg.lakehouse.orders;

-- Check snapshots
SELECT * FROM iceberg.lakehouse.orders.snapshots;
```

## 🔥 Common Issues & Solutions

### Issue: Topic not found
```
Error: Topic 'orders' not found in config for app 'my_app'
```
**Solution**: Check topic exists in `spark_topics` table with matching app_name

### Issue: Schema inference failed
```
ERROR: No messages found in topic 'new_topic' for schema inference
```
**Solutions**:
1. Ensure topic has data
2. Create custom processor with hardcoded schema
3. Add `schema_definition` to database config

### Issue: Checkpoint conflict
```
ERROR: Checkpoint location already in use
```
**Solution**: Each job needs unique checkpoint path (auto-handled by --topic-name)

## 📚 Documentation

- **[QUICKSTART.md](QUICKSTART.md)**: Get started quickly
- **[ARCHITECTURE.md](ARCHITECTURE.md)**: System design & diagrams
- **[core/schema_processors/README.md](core/schema_processors/README.md)**: Schema processor guide
- **[REFACTORING_SUMMARY.md](REFACTORING_SUMMARY.md)**: Recent architectural changes
- **[CHECKLIST.md](CHECKLIST.md)**: Implementation verification

## 🤝 Contributing

1. Đọc architecture docs
2. Tạo branch mới cho feature/fix
3. Test thoroughly (local + dev environment)
4. Update documentation
5. Submit PR với description rõ ràng

## 📝 Changelog

### v2.0 (2026-01-26) - Schema Processors Refactoring
- ✅ Tách schema processing thành modular system
- ✅ Support per-topic deployment
- ✅ Custom processors cho topics đặc biệt
- ✅ Pattern matching cho processor registration
- ✅ Comprehensive documentation
- ✅ Deployment automation scripts

### v1.0 - Initial Release
- CDC streaming from Kafka to Iceberg
- Schema evolution support
- Metadata-driven configuration

## 📧 Contact

- **Team**: Data Engineering
- **Maintainer**: [Your Name]
- **Issues**: [Issue Tracker URL]

## 📄 License

[Your License]

---

## 🎯 Example Deployment

```bash
# 1. Verify config
python -c "from config.app_config import AppConfig; \
           config = AppConfig('my_app'); \
           config.load(); \
           print(f'Topics: {[t[\"topic_name\"] for t in config.topic_configs]}')"

# 2. Deploy to YARN cluster
.\deploy_per_topic.ps1 `
    -AppName "my_app" `
    -Topics "orders,payments,users,inventory"

# 3. Monitor
yarn application -list | grep my_app

# 4. Check logs
yarn logs -applicationId application_xxx_yyy

# 5. Verify data
spark-sql -e "SELECT COUNT(*) FROM iceberg.lakehouse.orders"
```

---

**Happy Streaming! 🚀**
