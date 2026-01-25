import unittest
from unittest.mock import MagicMock
import sys
import os

# Ensure we can import from parent directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.etl_job import KafkaToIcebergETL

class TestKafkaToIcebergETL(unittest.TestCase):
    def setUp(self):
        self.mock_spark = MagicMock()
        self.app_name = "test_app"
        self.app_config = {"max_concurrent_queries": 2}
        self.topic_configs = []
        self.kafka_bootstrap = "localhost:9092"
        self.checkpoint = "/tmp/checkpoint"
        
        self.etl = KafkaToIcebergETL(
            self.mock_spark, self.app_name, self.app_config, 
            self.topic_configs, self.kafka_bootstrap, self.checkpoint
        )

    def test_init(self):
        self.assertEqual(self.etl.app_name, "test_app")
        self.assertEqual(self.etl.kafka_bootstrap_servers, "localhost:9092")
        self.assertEqual(self.etl.app_config['max_concurrent_queries'], 2)

if __name__ == '__main__':
    unittest.main()
