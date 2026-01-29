"""
Kafka configuration helper - Load authentication config từ client.properties
"""
import os
from typing import Dict


class KafkaConfig:
    """Helper class để load Kafka authentication config"""
    
    @staticmethod
    def load_auth_config(properties_file: str = None) -> Dict[str, str]:
        """
        Load Kafka authentication config từ client.properties file
        
        Args:
            properties_file: Đường dẫn đến file client.properties
                           Nếu None, sẽ tìm trong các vị trí mặc định
        
        Returns:
            Dict chứa các kafka options để dùng với Spark
        """
        # Tìm file properties
        if properties_file is None:
            # Thử các vị trí mặc định
            possible_paths = [
                os.path.join(os.getcwd(), "client.properties"),
                os.path.join(os.getcwd(), "config", "client.properties"),
                os.getenv("KAFKA_CLIENT_PROPERTIES"),
                "/etc/kafka/client.properties",
            ]
            
            for path in possible_paths:
                if path and os.path.exists(path):
                    properties_file = path
                    break
        
        # Nếu không tìm thấy file, return empty config (no auth)
        if not properties_file or not os.path.exists(properties_file):
            print(f"⚠ WARNING: Kafka client.properties not found. Using no authentication.")
            return {}
        
        print(f"✓ Loading Kafka config from: {properties_file}")
        
        # Parse properties file
        kafka_options = {}
        
        try:
            with open(properties_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    
                    # Skip comments and empty lines
                    if not line or line.startswith('#'):
                        continue
                    
                    # Parse key=value
                    if '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip()
                        
                        # Convert từ Kafka client properties format sang Spark format
                        # Spark cần prefix "kafka." cho các config
                        spark_key = f"kafka.{key}"
                        kafka_options[spark_key] = value
        
        except PermissionError as e:
            print(f"❌ ERROR: Permission denied reading {properties_file}")
            print(f"   Details: {e}")
            print(f"   Please check file permissions (should be 0444 or 0644)")
            print(f"   If running in Kubernetes, check Secret defaultMode setting")
            print(f"   Continuing with no authentication...")
            return {}
        
        # Log các config đã load (hide sensitive info)
        print(f"  Loaded {len(kafka_options)} Kafka configuration options")
        for key in kafka_options.keys():
            if 'password' in key.lower() or 'secret' in key.lower():
                print(f"    - {key}: ***")
            else:
                print(f"    - {key}: {kafka_options[key]}")
        
        return kafka_options
    
    @staticmethod
    def apply_auth_options(reader_or_writer, auth_config: Dict[str, str]):
        """
        Apply authentication options vào Spark Kafka DataStreamReader/Writer
        
        Args:
            reader_or_writer: Spark DataStreamReader or DataStreamWriter with format("kafka")
            auth_config: Dict các kafka options từ load_auth_config()
        
        Returns:
            reader_or_writer với auth options đã được apply
        """
        for key, value in auth_config.items():
            reader_or_writer = reader_or_writer.option(key, value)
        
        return reader_or_writer
