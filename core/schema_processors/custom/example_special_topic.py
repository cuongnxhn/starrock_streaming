"""
Example custom schema processor cho topic đặc biệt.
Copy file này và customize cho topic cụ thể của bạn.
"""

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from ..base import BaseSchemaProcessor


class SpecialTopicSchemaProcessor(BaseSchemaProcessor):
    """
    Example: Custom processor cho topic có format đặc biệt.
    
    Use case:
    - Topic không dùng CDC envelope standard
    - Topic có nested schema phức tạp
    - Topic cần transformation đặc biệt trước khi parse
    """
    
    def get_data_schema(self, topic_config):
        """
        Override để define schema cứng cho topic đặc biệt này.
        Không cần parse từ config hay infer từ Kafka.
        """
        topic_name = topic_config['topic_name']
        print(f"  → Using hardcoded schema for special topic: {topic_name}")
        
        # Define schema cứng
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("metadata", StringType(), True),  # JSON string
        ])
        
        return schema
    
    def get_envelope_schema(self, topic_config, data_schema):
        """
        Override nếu envelope schema của topic này khác standard CDC.
        
        Example: Topic này có envelope schema đơn giản hơn.
        """
        envelope_schema = StructType() \
            .add("event_type", StringType()) \
            .add("timestamp", StringType()) \
            .add("payload", data_schema)
        
        return envelope_schema
    
    def validate_schema(self, schema, topic_name):
        """
        Override để add validation đặc biệt.
        """
        # Call parent validation
        if not super().validate_schema(schema, topic_name):
            return False
        
        # Custom validation: Ensure 'id' field exists
        field_names = [f.name for f in schema.fields]
        if 'id' not in field_names:
            print(f"  ✗ ERROR: Schema for '{topic_name}' must have 'id' field")
            return False
        
        print(f"  ✓ Custom validation passed for '{topic_name}'")
        return True


# Uncomment to auto-register when imported:
# from ..factory import SchemaProcessorFactory
# SchemaProcessorFactory.register_processor('special_topic_name', SpecialTopicSchemaProcessor)
