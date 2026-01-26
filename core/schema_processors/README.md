# Schema Processors

## 📚 Tổng quan

Schema Processors cho phép bạn customize cách xử lý schema cho từng topic riêng biệt. Hệ thống sử dụng **Strategy Pattern** để tách logic schema ra khỏi ETL job chính.

## 🏗 Architecture

```
schema_processors/
├── __init__.py              # Package exports
├── base.py                  # BaseSchemaProcessor (abstract class)
├── default.py               # DefaultSchemaProcessor (implementation mặc định)
├── factory.py               # SchemaProcessorFactory (tạo processor phù hợp)
└── custom/
    ├── __init__.py          # Import custom processors
    └── example_special_topic.py  # Example custom processor
```

## 🔧 Cách sử dụng

### 1. Default Behavior (Không cần làm gì)

Mặc định, tất cả topics sẽ dùng `DefaultSchemaProcessor`:
- Parse schema từ `schema_definition` trong config
- Nếu không có, infer schema từ Kafka data
- Tạo standard CDC envelope schema

```bash
# Chạy bình thường, dùng default processor
spark-submit main.py --app-name my_app --topic-name orders
```

### 2. Tạo Custom Processor cho Topic Đặc Biệt

**Bước 1:** Tạo file processor mới trong `custom/`

```python
# custom/orders_special.py
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from ..base import BaseSchemaProcessor

class OrdersSpecialProcessor(BaseSchemaProcessor):
    """Custom processor cho orders topic có format đặc biệt"""
    
    def get_data_schema(self, topic_config):
        """Override để define schema cứng"""
        # Hardcode schema cho topic này
        schema = StructType([
            StructField("order_id", IntegerType(), False),
            StructField("customer_id", IntegerType(), True),
            StructField("amount", StringType(), True),
        ])
        return schema
    
    def get_envelope_schema(self, topic_config, data_schema):
        """Dùng envelope đơn giản hơn standard CDC"""
        envelope_schema = StructType() \
            .add("event_type", StringType()) \
            .add("data", data_schema)
        return envelope_schema
```

**Bước 2:** Register processor trong `custom/__init__.py`

```python
# custom/__init__.py
from .orders_special import OrdersSpecialProcessor
from ..factory import SchemaProcessorFactory

# Register cho topic cụ thể
SchemaProcessorFactory.register_processor('orders_topic', OrdersSpecialProcessor)

# Hoặc register cho pattern (wildcard)
SchemaProcessorFactory.register_processor('orders_*', OrdersSpecialProcessor)
```

**Bước 3:** Import trong main package (optional)

```python
# schema_processors/__init__.py
from .custom import *  # Auto-register all custom processors
```

### 3. Pattern Matching

Factory hỗ trợ pattern matching với wildcard:

```python
# Sẽ match tất cả topics bắt đầu bằng "orders_"
SchemaProcessorFactory.register_processor('orders_*', OrdersProcessor)

# Topics: orders_v1, orders_v2, orders_staging → đều dùng OrdersProcessor
```

## 📝 Abstract Methods cần Override

Khi tạo custom processor, bạn **PHẢI** override 2 methods:

### 1. `get_data_schema(topic_config)`
Trả về schema của data (không bao gồm envelope).

**Input:**
```python
topic_config = {
    'topic_name': 'my_topic',
    'table_name': 'my_table',
    'schema_definition': '...',  # JSON string or None
    'partition_columns': 'id',
    ...
}
```

**Output:**
```python
StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
])
```

### 2. `get_envelope_schema(topic_config, data_schema)`
Trả về envelope schema (bao gồm metadata + data).

**Standard CDC envelope:**
```python
StructType()
    .add("table", StringType())
    .add("op_type", StringType())
    .add("before", data_schema)
    .add("after", data_schema)
```

**Custom envelope (ví dụ cho event stream):**
```python
StructType()
    .add("event_type", StringType())
    .add("timestamp", StringType())
    .add("payload", data_schema)
```

## 🎯 Use Cases

### 1. Topic không có schema_definition trong DB
```python
def get_data_schema(self, topic_config):
    # Hardcode schema thay vì infer
    return StructType([...])
```

### 2. Topic có envelope format đặc biệt
```python
def get_envelope_schema(self, topic_config, data_schema):
    # Custom envelope khác CDC standard
    return StructType().add("custom_field", StringType())
```

### 3. Topic cần validation đặc biệt
```python
def validate_schema(self, schema, topic_name):
    if not super().validate_schema(schema, topic_name):
        return False
    # Custom validation
    if 'required_field' not in [f.name for f in schema.fields]:
        print(f"ERROR: Missing required_field")
        return False
    return True
```

### 4. Topic cần merge schema theo cách khác
```python
def merge_schemas(self, old_schema, new_schema):
    # Custom merge logic
    # Ví dụ: không cho phép add column, chỉ update type
    ...
```

## 🚀 Deployment Strategy

### Cách cũ (Multiple topics trong 1 job):
```bash
spark-submit main.py --app-name my_app
# → Xử lý tất cả topics cùng lúc
```

### Cách mới (1 topic per job - RECOMMENDED):
```bash
# Job 1: Chỉ xử lý orders
spark-submit main.py --app-name my_app --topic-name orders

# Job 2: Chỉ xử lý payments
spark-submit main.py --app-name my_app --topic-name payments

# Job 3: Chỉ xử lý users
spark-submit main.py --app-name my_app --topic-name users
```

**Lợi ích:**
- ✅ Isolation: Lỗi ở 1 topic không ảnh hưởng topics khác
- ✅ Debug dễ: Log và metrics riêng biệt
- ✅ Flexibility: Mỗi topic có thể dùng processor khác nhau
- ✅ Tuning: Config resources riêng cho từng topic

## 📊 Example: Complete Custom Processor

```python
# custom/financial_transactions.py
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from ..base import BaseSchemaProcessor

class FinancialTransactionProcessor(BaseSchemaProcessor):
    """
    Custom processor cho financial transactions topic.
    - Schema cứng (không trust metadata)
    - Validation nghiêm ngặt
    - Envelope format khác standard
    """
    
    def get_data_schema(self, topic_config):
        print(f"  → Using hardcoded schema for financial transactions")
        return StructType([
            StructField("transaction_id", StringType(), False),
            StructField("account_id", StringType(), False),
            StructField("amount", DecimalType(18, 2), False),
            StructField("currency", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("status", StringType(), True),
        ])
    
    def get_envelope_schema(self, topic_config, data_schema):
        # Financial transactions có audit metadata
        return StructType() \
            .add("event_id", StringType()) \
            .add("event_time", TimestampType()) \
            .add("source_system", StringType()) \
            .add("transaction", data_schema)
    
    def validate_schema(self, schema, topic_name):
        if not super().validate_schema(schema, topic_name):
            return False
        
        # Ensure critical fields exist
        field_names = [f.name for f in schema.fields]
        required_fields = ['transaction_id', 'amount', 'currency']
        
        for field in required_fields:
            if field not in field_names:
                print(f"  ✗ ERROR: Missing required field '{field}'")
                return False
        
        print(f"  ✓ Financial transaction validation passed")
        return True
```

**Register:**
```python
# custom/__init__.py
from .financial_transactions import FinancialTransactionProcessor
from ..factory import SchemaProcessorFactory

SchemaProcessorFactory.register_processor(
    'financial_transactions',
    FinancialTransactionProcessor
)
```

## 🔍 Debugging

Để biết topic đang dùng processor nào:

```python
# Log sẽ hiển thị:
# → Using custom processor for topic: orders
# hoặc
# (không có log gì) → Dùng DefaultSchemaProcessor
```

## 💡 Best Practices

1. **Keep it simple:** Chỉ tạo custom processor khi thực sự cần
2. **Use patterns:** Dùng wildcard để group topics tương tự
3. **Document:** Comment rõ lý do tại sao topic cần custom processor
4. **Test thoroughly:** Test schema validation trước khi deploy production
5. **Version control:** Commit custom processors vào Git

## ❓ FAQ

**Q: Khi nào nên tạo custom processor?**  
A: Khi:
- Topic có format envelope khác CDC standard
- Cần hardcode schema thay vì dùng metadata
- Cần validation logic đặc biệt
- Topic có yêu cầu transformation phức tạp

**Q: Có thể dùng 1 processor cho nhiều topics?**  
A: Có, dùng pattern matching:
```python
SchemaProcessorFactory.register_processor('events_*', EventsProcessor)
```

**Q: Nếu không register, topic sẽ dùng processor nào?**  
A: `DefaultSchemaProcessor` (behavior giống code cũ)

**Q: Custom processor có override được merge_schemas?**  
A: Có, override bất kỳ method nào trong `BaseSchemaProcessor`
