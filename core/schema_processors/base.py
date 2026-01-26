from abc import ABC, abstractmethod
from pyspark.sql.types import StructType


class BaseSchemaProcessor(ABC):
    """
    Base class cho tất cả schema processors.
    Mỗi topic có thể có processor riêng để handle schema theo cách đặc biệt.
    """
    
    def __init__(self, spark, kafka_bootstrap_servers):
        self.spark = spark
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
    
    @abstractmethod
    def get_data_schema(self, topic_config):
        """
        Lấy schema cho data của topic.
        
        Args:
            topic_config: Dict chứa config của topic (topic_name, schema_definition, etc.)
            
        Returns:
            StructType: Schema cho data
        """
        pass
    
    @abstractmethod
    def get_envelope_schema(self, topic_config, data_schema):
        """
        Tạo envelope schema cho CDC.
        
        Args:
            topic_config: Dict chứa config của topic
            data_schema: StructType của data
            
        Returns:
            StructType: Envelope schema cho CDC
        """
        pass
    
    def validate_schema(self, schema, topic_name):
        """
        Validate schema trước khi dùng.
        Override method này nếu cần validation logic đặc biệt.
        
        Args:
            schema: StructType cần validate
            topic_name: Tên topic
            
        Returns:
            bool: True nếu valid
        """
        if schema is None:
            return False
        if len(schema.fields) == 0:
            print(f"  ⚠ WARNING: Empty schema for topic '{topic_name}'")
            return False
        return True
    
    def merge_schemas(self, old_schema, new_schema):
        """
        Merge hai schemas để handle schema evolution (add new columns).
        Override nếu cần logic merge đặc biệt.
        """
        if not old_schema:
            return new_schema
        if not new_schema:
            return old_schema
            
        old_fields = {f.name: f for f in old_schema.fields}
        new_fields = {f.name: f for f in new_schema.fields}
        
        # Bắt đầu với old schema
        merged_fields = list(old_schema.fields)
        added_count = 0
        
        # Thêm các cột mới từ new_schema
        for field_name, field in new_fields.items():
            if field_name not in old_fields:
                merged_fields.append(field)
                added_count += 1
                print(f"    + Adding new column: '{field_name}' ({field.dataType.simpleString()})")
        
        if added_count > 0:
            print(f"  ✓ Schema merged: added {added_count} new column(s)")
        
        return StructType(merged_fields)
