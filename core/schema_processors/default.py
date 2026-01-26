import json
from pyspark.sql.types import StructType, StringType
from .base import BaseSchemaProcessor


class DefaultSchemaProcessor(BaseSchemaProcessor):
    """
    Default schema processor - implement logic hiện tại của bạn.
    Dùng cho hầu hết các topic standard.
    """
    
    def parse_and_validate_schema(self, schema_definition, topic_name):
        """Parse và validate schema definition với error handling"""
        if not schema_definition:
            print(f"  ⚠ WARNING: schema_definition is null for topic '{topic_name}'")
            return None
            
        try:
            schema = StructType.fromJson(json.loads(schema_definition))
            if len(schema.fields) == 0:
                print(f"  ⚠ WARNING: schema_definition is empty for topic '{topic_name}'")
                return None
            return schema
        except json.JSONDecodeError as e:
            print(f"  ✗ ERROR: Invalid JSON in schema_definition for topic '{topic_name}': {e}")
            return None
        except Exception as e:
            print(f"  ✗ ERROR: Failed to parse schema for topic '{topic_name}': {e}")
            return None
    
    def infer_schema_from_kafka(self, topic_name):
        """Infer schema từ Kafka data (fallback khi schema_definition null)"""
        print(f"  → Attempting to infer schema from Kafka topic '{topic_name}'...")
        try:
            # Đọc vài messages để infer schema
            sample_df = self.spark.read \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
                .option("subscribe", topic_name) \
                .option("startingOffsets", "earliest") \
                .option("endingOffsets", "latest") \
                .load() \
                .selectExpr("CAST(value AS STRING) as json_value") \
                .limit(100)
            
            if sample_df.isEmpty():
                print(f"  ✗ ERROR: No messages found in topic '{topic_name}' for schema inference")
                return None
            
            # Infer schema từ JSON
            first_row = sample_df.first()
            if first_row:
                inferred = self.spark.read.json(
                    self.spark.sparkContext.parallelize([first_row.json_value])
                ).schema
                
                # Extract "after" schema từ envelope nếu có
                if "after" in [f.name for f in inferred.fields]:
                    after_field = [f for f in inferred.fields if f.name == "after"][0]
                    data_schema = after_field.dataType
                    print(f"  ✓ Inferred schema from Kafka data: {len(data_schema.fields)} fields")
                    return data_schema
                else:
                    print(f"  ✓ Inferred schema from Kafka data: {len(inferred.fields)} fields")
                    return inferred
                    
        except Exception as e:
            print(f"  ✗ ERROR: Failed to infer schema from Kafka: {e}")
            return None
    
    def get_data_schema(self, topic_config):
        """
        Lấy schema cho data của topic.
        Try parse từ config trước, nếu không có thì infer từ Kafka.
        """
        topic_name = topic_config['topic_name']
        schema_definition = topic_config.get('schema_definition')
        
        # Try parse từ config trước
        data_schema = self.parse_and_validate_schema(schema_definition, topic_name)
        
        # Nếu không có hoặc invalid, infer từ Kafka
        if data_schema is None:
            data_schema = self.infer_schema_from_kafka(topic_name)
            
        # Nếu vẫn không có schema, raise error
        if data_schema is None:
            raise ValueError(
                f"Cannot determine schema for topic '{topic_name}'. "
                f"Please ensure schema_definition is valid or topic has data."
            )
        
        return data_schema
    
    def get_envelope_schema(self, topic_config, data_schema):
        """
        Tạo standard CDC envelope schema.
        """
        envelope_schema = StructType() \
            .add("table", StringType()) \
            .add("op_type", StringType()) \
            .add("op_ts", StringType()) \
            .add("current_ts", StringType()) \
            .add("pos", StringType()) \
            .add("before", data_schema) \
            .add("after", data_schema)
        
        return envelope_schema
