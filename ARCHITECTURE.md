# Architecture Diagram

## 🏗 Before vs After

### ❌ Before: Multi-Topic trong 1 Job
```
                    ┌─────────────────────────────────┐
    Kafka Topics    │   Spark Job: my_app            │    Iceberg
                    │                                 │
  ┌──orders─────────┼─► Topic 1: orders              ├────► lakehouse.orders
  │                 │                                 │
  ├──payments───────┼─► Topic 2: payments            ├────► lakehouse.payments
  │                 │                                 │
  ├──users──────────┼─► Topic 3: users               ├────► lakehouse.users
  │                 │                                 │
  └──inventory──────┼─► Topic 4: inventory           ├────► lakehouse.inventory
                    │                                 │
                    └─────────────────────────────────┘
                              Single Point of Failure!
                              
Problems:
  ❌ 1 topic fail → Toàn bộ job fail
  ❌ Log lẫn lộn, debug khó
  ❌ Không tune riêng được từng topic
  ❌ Blast radius lớn
```

### ✅ After: 1 Topic per Job

```
                    ┌─────────────────────────┐
    Kafka Topics    │ Spark Job: my_app_orders│      Iceberg
                    │   Topic: orders          │
  ┌──orders─────────┼─► [Default Processor]   ├────► lakehouse.orders
  │                 └─────────────────────────┘
  │
  │                 ┌─────────────────────────┐
  │                 │Spark Job:my_app_payments│
  ├──payments───────┼─► [Custom Processor A]  ├────► lakehouse.payments
  │                 └─────────────────────────┘
  │
  │                 ┌─────────────────────────┐
  │                 │  Spark Job:my_app_users │
  ├──users──────────┼─► [Default Processor]   ├────► lakehouse.users
  │                 └─────────────────────────┘
  │
  │                 ┌─────────────────────────┐
  │                 │Spark Job:my_app_inventory│
  └──inventory──────┼─► [Custom Processor B]  ├────► lakehouse.inventory
                    └─────────────────────────┘
                           Isolated Jobs!
                           
Benefits:
  ✅ Isolation: Lỗi ở 1 job không ảnh hưởng jobs khác
  ✅ Debug dễ: Log riêng, Spark UI riêng
  ✅ Flexibility: Custom processor cho từng topic
  ✅ Tuning: Resources riêng cho từng topic
```

---

## 🎯 Schema Processor Flow

```
                      ┌─────────────────────────────────┐
                      │  SchemaProcessorFactory         │
                      │                                 │
                      │  get_processor(topic_name)      │
                      └──────────┬──────────────────────┘
                                 │
                    ┌────────────┼────────────┐
                    │            │            │
                    ▼            ▼            ▼
         ┌──────────────┐  ┌─────────┐  ┌─────────┐
         │   Default    │  │Custom A │  │Custom B │
         │  Processor   │  │Processor│  │Processor│
         └──────┬───────┘  └────┬────┘  └────┬────┘
                │               │            │
                ├───────────────┴────────────┘
                │
                ▼
    ┌───────────────────────────────────┐
    │   Topic Config                    │
    │   {                               │
    │     'topic_name': 'orders',       │
    │     'schema_definition': '...'    │
    │   }                               │
    └───────────────┬───────────────────┘
                    │
                    ▼
         ┌──────────────────────┐
         │  get_data_schema()   │
         │  get_envelope_schema()│
         └──────────┬───────────┘
                    │
                    ▼
              Data + Envelope Schema
                    │
                    ▼
            ┌───────────────┐
            │ Spark Streaming│
            └───────┬────────┘
                    │
                    ▼
          Write to Iceberg Table
```

---

## 🔄 Custom Processor Registration Flow

```
Step 1: Create Custom Processor
──────────────────────────────────────────────────────
File: core/schema_processors/custom/my_processor.py

    class MyCustomProcessor(BaseSchemaProcessor):
        def get_data_schema(self, topic_config):
            # Custom logic
            return schema
        
        def get_envelope_schema(self, topic_config, data_schema):
            # Custom envelope
            return envelope


Step 2: Register in Factory
──────────────────────────────────────────────────────
File: core/schema_processors/custom/__init__.py

    from .my_processor import MyCustomProcessor
    from ..factory import SchemaProcessorFactory
    
    SchemaProcessorFactory.register_processor(
        'my_special_topic',      # Topic name
        MyCustomProcessor        # Processor class
    )


Step 3: Run Job
──────────────────────────────────────────────────────
Command:
    spark-submit main.py \
        --app-name my_app \
        --topic-name my_special_topic

Runtime:
    1. main.py receives --topic-name
    2. ETL job calls get_schema_processor('my_special_topic')
    3. Factory checks registry → Found MyCustomProcessor
    4. Returns instance of MyCustomProcessor
    5. Job uses custom schema handling
    
Output:
    → Using custom processor for topic: my_special_topic
    ✓ Started stream: my_special_topic -> my_table
```

---

## 📦 Deployment Flow

```
┌────────────────────────────────────────────────────────────┐
│  deploy_per_topic.ps1 -AppName "app" -Topics "t1,t2,t3"   │
└───────────────────────┬────────────────────────────────────┘
                        │
        ┌───────────────┼───────────────┐
        │               │               │
        ▼               ▼               ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│ spark-submit │ │ spark-submit │ │ spark-submit │
│              │ │              │ │              │
│ --app my_app │ │ --app my_app │ │ --app my_app │
│ --topic t1   │ │ --topic t2   │ │ --topic t3   │
└──────┬───────┘ └──────┬───────┘ └──────┬───────┘
       │                │                │
       ▼                ▼                ▼
  ┌─────────┐      ┌─────────┐     ┌─────────┐
  │YARN Job │      │YARN Job │     │YARN Job │
  │my_app_t1│      │my_app_t2│     │my_app_t3│
  └────┬────┘      └────┬────┘     └────┬────┘
       │                │                │
       └────────────────┼────────────────┘
                        │
                        ▼
                ┌───────────────┐
                │  Monitoring   │
                │  - Spark UI   │
                │  - YARN UI    │
                │  - Kafka Lag  │
                └───────────────┘
```

---

## 🧩 Class Hierarchy

```
BaseSchemaProcessor (Abstract)
│
├── DefaultSchemaProcessor
│   │
│   ├── get_data_schema()
│   │   ├── parse_and_validate_schema()
│   │   └── infer_schema_from_kafka()
│   │
│   └── get_envelope_schema()
│       └── Standard CDC envelope
│
└── Custom Processors
    │
    ├── SpecialTopicSchemaProcessor
    │   ├── get_data_schema() → Hardcoded schema
    │   └── get_envelope_schema() → Custom envelope
    │
    ├── FinancialTransactionProcessor
    │   ├── get_data_schema() → Strict schema
    │   ├── get_envelope_schema() → Audit envelope
    │   └── validate_schema() → Extra validation
    │
    └── ... (Add more as needed)


SchemaProcessorFactory
│
├── _processors = {}          # Registry
│
├── register_processor(topic, processor_class)
│   └── Store in registry
│
└── get_processor(topic_name, spark, kafka_servers)
    ├── Check exact match in registry
    ├── Check pattern match (wildcard)
    └── Return DefaultProcessor (fallback)
```

---

## 📝 Summary

**Old Architecture:**
- ❌ Multiple topics per job
- ❌ Schema logic embedded in ETL job
- ❌ Hard to customize per topic
- ❌ Single point of failure

**New Architecture:**
- ✅ One topic per job (isolation)
- ✅ Schema processors (modular)
- ✅ Easy customization per topic
- ✅ Fault tolerance
- ✅ Better monitoring & debugging
