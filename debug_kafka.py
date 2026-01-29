"""
Debug script để kiểm tra Kafka connectivity và data availability.
Sử dụng script này để troubleshoot các vấn đề về Kafka consumption.

Usage:
    python debug_kafka.py
    # hoặc
    spark-submit debug_kafka.py
"""

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from utils.kafka_config import KafkaConfig

# Load environment variables
load_dotenv()

def check_kafka_with_native_client():
    """Thử kết nối Kafka bằng native Python client (nếu có)"""
    print("\n" + "="*60)
    print("1. KIỂM TRA KAFKA VỚI NATIVE CLIENT")
    print("="*60)
    
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "10.165.11.236:9093,10.165.11.237:9093,10.165.11.238:9093")
    print(f"Bootstrap servers: {kafka_bootstrap}")
    
    try:
        from confluent_kafka import Consumer, KafkaException
        print("✓ confluent-kafka library available")
        
        # Tạo consumer để list topics
        conf = {
            'bootstrap.servers': kafka_bootstrap,
            'group.id': 'debug_group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        }
        
        consumer = Consumer(conf)
        metadata = consumer.list_topics(timeout=10)
        
        print(f"\n✓ Kết nối thành công đến Kafka!")
        print(f"Tìm thấy {len(metadata.topics)} topics:")
        
        for topic_name, topic_metadata in metadata.topics.items():
            if not topic_name.startswith('__'):  # Skip internal topics
                partitions = topic_metadata.partitions
                print(f"\n  Topic: {topic_name}")
                print(f"    Partitions: {len(partitions)}")
                
                # Lấy watermarks cho mỗi partition
                for partition_id in partitions:
                    try:
                        low, high = consumer.get_watermark_offsets(
                            (topic_name, partition_id), timeout=5
                        )
                        msg_count = high - low
                        print(f"      Partition {partition_id}: {msg_count} messages (offsets: {low} -> {high})")
                    except Exception as e:
                        print(f"      Partition {partition_id}: Error getting offsets - {e}")
        
        consumer.close()
        return True
        
    except ImportError:
        print("⚠ confluent-kafka not installed")
        try:
            from kafka import KafkaConsumer, KafkaAdminClient
            print("✓ kafka-python library available")
            
            # Thử kết nối
            admin = KafkaAdminClient(
                bootstrap_servers=kafka_bootstrap,
                request_timeout_ms=10000
            )
            
            topics = admin.list_topics()
            print(f"\n✓ Kết nối thành công đến Kafka!")
            print(f"Tìm thấy {len(topics)} topics:")
            
            for topic in topics:
                if not topic.startswith('__'):
                    print(f"  - {topic}")
                    
                    # Tạo consumer để check offsets
                    consumer = KafkaConsumer(
                        topic,
                        bootstrap_servers=kafka_bootstrap,
                        auto_offset_reset='earliest',
                        enable_auto_commit=False,
                        consumer_timeout_ms=5000
                    )
                    
                    # Get partitions
                    partitions = consumer.partitions_for_topic(topic)
                    if partitions:
                        print(f"    Partitions: {len(partitions)}")
                        for partition_id in partitions:
                            beginning = consumer.beginning_offsets([(topic, partition_id)])
                            end = consumer.end_offsets([(topic, partition_id)])
                            
                            begin_offset = beginning[(topic, partition_id)]
                            end_offset = end[(topic, partition_id)]
                            msg_count = end_offset - begin_offset
                            
                            print(f"      Partition {partition_id}: {msg_count} messages (offsets: {begin_offset} -> {end_offset})")
                    
                    consumer.close()
            
            admin.close()
            return True
            
        except ImportError:
            print("⚠ kafka-python not installed")
            print("→ Chỉ dùng PySpark để kiểm tra")
            return False
        except Exception as e:
            print(f"✗ Lỗi khi kết nối: {e}")
            return False
    except Exception as e:
        print(f"✗ Lỗi khi kết nối: {e}")
        return False


def check_kafka_with_spark(spark, kafka_bootstrap, test_topic=None):
    """Kiểm tra Kafka bằng PySpark (giống logic trong app)"""
    print("\n" + "="*60)
    print("2. KIỂM TRA KAFKA VỚI PYSPARK")
    print("="*60)
    
    print(f"Bootstrap servers: {kafka_bootstrap}")
    
    # Nếu không chỉ định topic, thử đọc tất cả topics
    if test_topic:
        topics_to_test = [test_topic]
    else:
        # Lấy danh sách topics từ database config
        try:
            from config.app_config import AppConfig
            config = AppConfig("cdc_streaming")
            config.load()
            topics_to_test = [t.topic_name for t in config.topic_configs]
            print(f"✓ Loaded {len(topics_to_test)} topics from config")
        except Exception as e:
            print(f"⚠ Không thể load config: {e}")
            print("→ Vui lòng chỉ định topic để test")
            return
    
    for topic_name in topics_to_test:
        print(f"\n--- Testing topic: {topic_name} ---")
        
        try:
            # Thử đọc earliest -> latest (giống logic trong infer_schema_from_kafka)
            print(f"  Đọc từ earliest đến latest...")
            
            # Load Kafka authentication config
            kafka_auth = KafkaConfig.load_auth_config()
            
            reader = spark.read \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_bootstrap) \
                .option("subscribe", topic_name) \
                .option("startingOffsets", "earliest") \
                .option("endingOffsets", "latest")
            
            # Apply authentication options
            reader = KafkaConfig.apply_auth_options(reader, kafka_auth)
            
            df = reader.load()
            
            # Kiểm tra count
            count = df.count()
            print(f"  ✓ Đọc được {count} messages từ topic '{topic_name}'")
            
            if count > 0:
                # Show schema
                print(f"\n  Schema của Kafka message:")
                df.printSchema()
                
                # Show sample data
                print(f"\n  Sample data (raw):")
                df.select("key", "value", "topic", "partition", "offset").show(5, truncate=False)
                
                # Thử parse value as JSON
                print(f"\n  Sample data (parsed value as string):")
                df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as json_value") \
                    .show(5, truncate=False)
            else:
                print(f"  ⚠ WARNING: Topic '{topic_name}' có 0 messages!")
                print(f"    Kiểm tra:")
                print(f"      1. Topic có tồn tại không?")
                print(f"      2. Topic có data không? (dùng kafka-console-consumer để verify)")
                print(f"      3. Kafka bootstrap servers đúng không? ({kafka_bootstrap})")
                
        except Exception as e:
            print(f"  ✗ ERROR khi đọc topic '{topic_name}': {e}")
            print(f"    Nguyên nhân có thể:")
            print(f"      - Topic không tồn tại")
            print(f"      - Kafka bootstrap servers sai")
            print(f"      - Network/firewall block")
            print(f"      - Kafka broker down")


def main():
    print("="*60)
    print("KAFKA DEBUGGING TOOL")
    print("="*60)
    
    # 1. Check với native client (nếu có)
    check_kafka_with_native_client()
    
    # 2. Check với PySpark
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    
    spark = SparkSession.builder \
        .appName("KafkaDebugger") \
        .getOrCreate()
    
    try:
        # Có thể override topic để test
        test_topic = os.getenv("DEBUG_TOPIC", None)
        check_kafka_with_spark(spark, kafka_bootstrap, test_topic)
    finally:
        spark.stop()
    
    print("\n" + "="*60)
    print("DEBUG HOÀN TẤT")
    print("="*60)


if __name__ == "__main__":
    main()
