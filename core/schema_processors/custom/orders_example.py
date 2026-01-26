"""
Example: Custom Schema Processor cho Orders Topic
Topic này có những đặc điểm:
1. Envelope không phải CDC standard (dùng event-driven format)
2. Cần hardcode schema (không trust metadata)
3. Cần validation nghiêm ngặt cho các trường critical
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType, BooleanType
)
from ..base import BaseSchemaProcessor


class OrdersTopicProcessor(BaseSchemaProcessor):
    """
    Custom processor cho orders topic.
    
    Use case:
    - Orders topic có envelope format khác CDC (event-driven)
    - Schema cần được hardcode để đảm bảo consistency
    - Validate các trường critical (order_id, amount, customer_id)
    """
    
    def get_data_schema(self, topic_config):
        """
        Hardcode schema cho orders thay vì parse/infer.
        Đảm bảo schema luôn consistent.
        """
        topic_name = topic_config['topic_name']
        print(f"  → Using hardcoded schema for orders topic: {topic_name}")
        
        # Define schema cứng cho orders
        schema = StructType([
            # Primary fields
            StructField("order_id", StringType(), nullable=False),
            StructField("customer_id", StringType(), nullable=False),
            StructField("order_date", TimestampType(), nullable=False),
            
            # Order details
            StructField("total_amount", DoubleType(), nullable=False),
            StructField("currency", StringType(), nullable=False),
            StructField("status", StringType(), nullable=True),
            
            # Optional fields
            StructField("discount_amount", DoubleType(), nullable=True),
            StructField("tax_amount", DoubleType(), nullable=True),
            StructField("shipping_address", StringType(), nullable=True),
            StructField("payment_method", StringType(), nullable=True),
            
            # Metadata
            StructField("created_at", TimestampType(), nullable=True),
            StructField("updated_at", TimestampType(), nullable=True),
            StructField("is_deleted", BooleanType(), nullable=True),
        ])
        
        print(f"  ✓ Orders schema: {len(schema.fields)} fields defined")
        return schema
    
    def get_envelope_schema(self, topic_config, data_schema):
        """
        Orders topic dùng event-driven envelope thay vì CDC standard.
        
        Envelope format:
        {
            "event_type": "OrderCreated" | "OrderUpdated" | "OrderCancelled",
            "event_id": "uuid",
            "event_timestamp": "2024-01-26T10:00:00Z",
            "source": "order-service",
            "order": { ... actual order data ... }
        }
        """
        print(f"  → Using event-driven envelope for orders (not standard CDC)")
        
        envelope_schema = StructType() \
            .add("event_type", StringType(), nullable=False) \
            .add("event_id", StringType(), nullable=False) \
            .add("event_timestamp", TimestampType(), nullable=False) \
            .add("source", StringType(), nullable=True) \
            .add("order", data_schema, nullable=False)
        
        return envelope_schema
    
    def validate_schema(self, schema, topic_name):
        """
        Custom validation cho orders schema.
        Ensure các trường critical tồn tại và có đúng type.
        """
        # Call parent validation first
        if not super().validate_schema(schema, topic_name):
            return False
        
        # Critical fields for orders
        required_fields = {
            'order_id': StringType,
            'customer_id': StringType,
            'total_amount': DoubleType,
            'currency': StringType,
        }
        
        field_dict = {f.name: f for f in schema.fields}
        
        # Check each required field
        for field_name, expected_type in required_fields.items():
            if field_name not in field_dict:
                print(f"  ✗ ERROR: Missing critical field '{field_name}' in orders schema")
                return False
            
            field = field_dict[field_name]
            if not isinstance(field.dataType, expected_type):
                print(f"  ✗ ERROR: Field '{field_name}' has wrong type. "
                      f"Expected {expected_type.__name__}, got {type(field.dataType).__name__}")
                return False
        
        print(f"  ✓ Orders schema validation passed: all critical fields present")
        return True
    
    def merge_schemas(self, old_schema, new_schema):
        """
        Override merge logic cho orders.
        
        Rule: Không cho phép thay đổi type của critical fields.
        Chỉ cho phép thêm optional fields mới.
        """
        if not old_schema:
            return new_schema
        if not new_schema:
            return old_schema
        
        print(f"  → Custom merge logic for orders (strict mode)")
        
        old_fields = {f.name: f for f in old_schema.fields}
        new_fields = {f.name: f for f in new_schema.fields}
        
        # Critical fields không được thay đổi type
        critical_fields = ['order_id', 'customer_id', 'total_amount', 'currency']
        
        for field_name in critical_fields:
            if field_name in old_fields and field_name in new_fields:
                old_type = old_fields[field_name].dataType
                new_type = new_fields[field_name].dataType
                
                if old_type != new_type:
                    raise ValueError(
                        f"Cannot change type of critical field '{field_name}' "
                        f"from {old_type.simpleString()} to {new_type.simpleString()}"
                    )
        
        # Merge: Start with old schema, add new fields
        merged_fields = list(old_schema.fields)
        added_count = 0
        
        for field_name, field in new_fields.items():
            if field_name not in old_fields:
                merged_fields.append(field)
                added_count += 1
                print(f"    + Adding new optional field: '{field_name}' ({field.dataType.simpleString()})")
        
        if added_count > 0:
            print(f"  ✓ Schema merged: added {added_count} new field(s)")
        else:
            print(f"  ✓ Schema unchanged")
        
        return StructType(merged_fields)


# ============================================================================
# REGISTRATION
# ============================================================================

# Uncomment để auto-register khi import module này
# from ..factory import SchemaProcessorFactory
# SchemaProcessorFactory.register_processor('orders', OrdersTopicProcessor)

# Hoặc có thể register trong custom/__init__.py
