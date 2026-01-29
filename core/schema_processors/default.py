import json
from pyspark.sql.types import StructType, StringType
from .base import BaseSchemaProcessor
from utils.kafka_config import KafkaConfig


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
        print(f"     Kafka bootstrap servers: {self.kafka_bootstrap_servers}")
        
        # Load Kafka authentication config
        kafka_auth = KafkaConfig.load_auth_config()
        
        try:
            # Đọc vài messages để infer schema
            reader = self.spark.read \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
                .option("subscribe", topic_name) \
                .option("startingOffsets", "earliest") \
                .option("endingOffsets", "latest")
            
            # Apply authentication options
            reader = KafkaConfig.apply_auth_options(reader, kafka_auth)
            
            sample_df = reader.load() \
                .selectExpr("CAST(value AS STRING) as json_value") \
                .limit(100)
            
            # Count để debug
            sample_count = sample_df.count()
            print(f"     Đọc được {sample_count} messages từ Kafka")
            
            if sample_df.isEmpty():
                print(f"  ✗ ERROR: No messages found in topic '{topic_name}' for schema inference")
                print(f"     Debug steps:")
                print(f"       1. Verify topic '{topic_name}' exists in Kafka")
                print(f"       2. Verify topic has data: kafka-console-consumer --bootstrap-server {self.kafka_bootstrap_servers} --topic {topic_name} --from-beginning --max-messages 1")
                print(f"       3. Check network connectivity to Kafka brokers")
                print(f"       4. Run debug_kafka.py script for detailed diagnostics")
                return None
            
            # Infer schema từ JSON
            first_row = sample_df.first()
            if first_row:
                inferred = self.spark.read.json(
                    self.spark.sparkContext.parallelize([first_row.json_value])
                ).schema
                

                op_type = None
                if "op_type" in [f.name for f in inferred.fields]:
                    # Parse JSON để lấy op_type value
                    json_data = json.loads(first_row.json_value)
                    op_type = json_data.get("op_type")
                    print(f"  → Detected op_type: {op_type}")
                
                # Extract schema dựa trên op_type
                if op_type == "U":  # Update - lấy schema từ "before"
                    if "before" in [f.name for f in inferred.fields]:
                        before_field = [f for f in inferred.fields if f.name == "before"][0]
                        data_schema = before_field.dataType
                        print(f"  ✓ Inferred schema from 'before' (UPDATE): {len(data_schema.fields)} fields")
                        return data_schema
                    else:
                        print(f"  ⚠ WARNING: op_type is 'U' but 'before' field not found")
                        return None
                elif "after" in [f.name for f in inferred.fields]:  # Insert hoặc các op khác - lấy từ "after"
                    after_field = [f for f in inferred.fields if f.name == "after"][0]
                    data_schema = after_field.dataType
                    print(f"  ✓ Inferred schema from 'after' (INSERT/other): {len(data_schema.fields)} fields")
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
        schema_definition = topic_config['schema_definition']
        
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
