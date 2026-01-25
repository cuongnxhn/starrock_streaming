import unittest
from unittest.mock import MagicMock, call
import sys
import os
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Ensure we can import from parent directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.etl_job import KafkaToIcebergETL

class TestKafkaToIcebergETL_CDC(unittest.TestCase):
    def setUp(self):
        self.mock_spark = MagicMock()
        self.app_name = "test_app"
        self.app_config = {"max_concurrent_queries": 2}
        self.kafka_bootstrap = "localhost:9092"
        self.checkpoint = "/tmp/checkpoint"
        
        # Mock schema definition
        self.schema_json = '{"fields": [{"name": "id", "type": "integer", "nullable": false}, {"name": "val", "type": "string", "nullable": true}]}'
        
        self.topic_configs = [{
            'topic_name': 'test_topic',
            'table_name': 'test_table',
            'schema_definition': self.schema_json,
            'partition_columns': 'id', # Key for upsert
            'expected_msg_per_day': 1000
        }]
        
        self.etl = KafkaToIcebergETL(
            self.mock_spark, self.app_name, self.app_config, 
            self.topic_configs, self.kafka_bootstrap, self.checkpoint
        )

    def test_merge_sql_generation(self):
        """Test if Merge SQL is constructed correctly generally. 
        Note: Testing exact foreachBatch logic is hard without a real Spark Context.
        This test mainly verifies we can instantiate and run basic setup without errors."""
        
        # We can't easily mock the internal function defined inside process_topic_stream 
        # without refactoring, but we can verify process_topic_stream calls readStream
        
        self.etl.process_topic_stream(self.topic_configs[0], "pool_0")
        
        # Check if readStream was called
        self.mock_spark.readStream.format.assert_called_with("kafka")
        
        # You would typically extract the process_batch function to a method to unit test it properly
        # For now, we rely on the integration/manual verification plan.

if __name__ == '__main__':
    unittest.main()
