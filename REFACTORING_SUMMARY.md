# Refactoring Summary: Per-Topic Jobs với Schema Processors

## 📝 Thay đổi chính

### 1. **Tạo Schema Processors System** ✅

**Cấu trúc mới:**
```
core/schema_processors/
├── base.py                      # Abstract base class
├── default.py                   # Default implementation (logic cũ)
├── factory.py                   # Factory pattern để chọn processor
├── custom/
│   ├── __init__.py             # Register custom processors
│   └── example_special_topic.py # Example custom processor
└── README.md                    # Full documentation
```

**Lợi ích:**
- ✅ Tách biệt logic schema ra khỏi ETL job chính
- ✅ Dễ dàng tạo custom processor cho topic đặc biệt
- ✅ Support pattern matching (`orders_*`, `payments_*`)
- ✅ Backward compatible (default behavior giống code cũ)

### 2. **Refactor etl_job.py** ✅

**Removed methods:**
- `parse_and_validate_schema()`
- `infer_schema_from_kafka()`
- `merge_schemas()`
- `get_or_infer_schema()`

**Added method:**
- `get_schema_processor(topic_name)` - Lấy processor phù hợp

**Updated usages:**
```python
# Cũ:
data_schema = self.get_or_infer_schema(topic_config)

# Mới:
schema_processor = self.get_schema_processor(topic_name)
data_schema = schema_processor.get_data_schema(topic_config)
envelope_schema = schema_processor.get_envelope_schema(topic_config, data_schema)
```

### 3. **Refactor main.py** ✅

**Added parameter:**
```bash
--topic-name <topic_name>  # Optional, nếu không có sẽ xử lý all topics
```

**Logic mới:**
- Filter `topic_configs` theo `--topic-name` nếu được chỉ định
- Validate topic tồn tại trong config
- Show available topics nếu topic không tìm thấy

### 4. **Deployment Scripts** ✅

**Files:**
- `deploy_per_topic.sh` (Linux/Mac)
- `deploy_per_topic.ps1` (Windows)

**Usage:**
```bash
# Deploy 3 topics riêng biệt
./deploy_per_topic.sh my_app orders,payments,users

# Hoặc Windows:
.\deploy_per_topic.ps1 -AppName "my_app" -Topics "orders,payments,users"
```

---

## 🚀 Cách sử dụng

### Option 1: Chạy tất cả topics (như cũ)
```bash
spark-submit main.py --app-name my_app
```

### Option 2: Chạy 1 topic cụ thể (RECOMMENDED)
```bash
# Chỉ chạy orders topic
spark-submit main.py --app-name my_app --topic-name orders

# Chỉ chạy payments topic
spark-submit main.py --app-name my_app --topic-name payments
```

### Option 3: Deploy nhiều topics với script
```powershell
# Deploy hàng loạt
.\deploy_per_topic.ps1 -AppName "my_app" -Topics "orders,payments,users,inventory"
```

---

## 🎯 Cách tạo Custom Schema Processor

### Ví dụ: Topic có envelope format đặc biệt

**Step 1:** Tạo processor file
```python
# core/schema_processors/custom/special_orders.py
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from ..base import BaseSchemaProcessor

class SpecialOrdersProcessor(BaseSchemaProcessor):
    def get_data_schema(self, topic_config):
        # Hardcode schema
        return StructType([
            StructField("order_id", IntegerType(), False),
            StructField("amount", StringType(), True),
        ])
    
    def get_envelope_schema(self, topic_config, data_schema):
        # Custom envelope
        return StructType() \
            .add("event_type", StringType()) \
            .add("payload", data_schema)
```

**Step 2:** Register trong `custom/__init__.py`
```python
from .special_orders import SpecialOrdersProcessor
from ..factory import SchemaProcessorFactory

SchemaProcessorFactory.register_processor('special_orders', SpecialOrdersProcessor)
```

**Step 3:** Chạy như bình thường
```bash
spark-submit main.py --app-name my_app --topic-name special_orders
# → Tự động dùng SpecialOrdersProcessor
```

---

## 📊 So sánh Before/After

### Before (Multi-topic trong 1 job)
```
1 Spark Job
├── Topic 1: orders
├── Topic 2: payments
├── Topic 3: users
└── Topic 4: inventory

❌ Lỗi ở 1 topic → toàn bộ job fail
❌ Log lẫn lộn
❌ Debug khó
❌ Không thể tune riêng từng topic
```

### After (1 topic per job)
```
Job 1: orders        → Isolated
Job 2: payments      → Isolated  
Job 3: users         → Isolated
Job 4: inventory     → Isolated

✅ Lỗi ở 1 topic → chỉ job đó fail
✅ Log riêng biệt, rõ ràng
✅ Debug dễ dàng
✅ Tune resources riêng cho từng topic
✅ Custom schema processor cho từng topic
```

---

## ✅ Testing Checklist

- [ ] Test default processor (không custom): `spark-submit main.py --app-name test --topic-name normal_topic`
- [ ] Test custom processor: Create custom processor và test
- [ ] Test pattern matching: Register `orders_*` và test `orders_v1`
- [ ] Test invalid topic: `--topic-name nonexistent` → Should show error
- [ ] Test deploy script: `.\deploy_per_topic.ps1 -AppName "test" -Topics "t1,t2,t3"`
- [ ] Test backward compatibility: Không dùng `--topic-name` → run all topics

---

## 🔍 Files Changed

1. **New files:**
   - `core/schema_processors/__init__.py`
   - `core/schema_processors/base.py`
   - `core/schema_processors/default.py`
   - `core/schema_processors/factory.py`
   - `core/schema_processors/custom/__init__.py`
   - `core/schema_processors/custom/example_special_topic.py`
   - `core/schema_processors/README.md`
   - `deploy_per_topic.sh`
   - `deploy_per_topic.ps1`
   - `REFACTORING_SUMMARY.md` (this file)

2. **Modified files:**
   - `core/etl_job.py` - Dùng schema processors
   - `main.py` - Added `--topic-name` parameter

---

## 📚 Documentation

Full documentation: `core/schema_processors/README.md`

Bao gồm:
- Architecture overview
- How to create custom processors
- Pattern matching examples
- Use cases
- Best practices
- FAQ
