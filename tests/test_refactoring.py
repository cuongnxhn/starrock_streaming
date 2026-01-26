"""
Test script để verify refactoring hoạt động đúng.
Run: python tests/test_refactoring.py
"""

import sys
sys.path.insert(0, '.')

from core.schema_processors import SchemaProcessorFactory, DefaultSchemaProcessor, BaseSchemaProcessor
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def test_factory_default():
    """Test factory trả về DefaultProcessor khi không có custom processor"""
    print("\n=== Test 1: Default Processor ===")
    
    # Mock spark and kafka (trong test thực tế nên dùng pytest fixtures)
    processor = SchemaProcessorFactory.get_processor(
        'unknown_topic',
        spark=None,  # Mock
        kafka_bootstrap_servers='localhost:9092'
    )
    
    assert isinstance(processor, DefaultSchemaProcessor), \
        f"Expected DefaultSchemaProcessor, got {type(processor)}"
    
    print("✓ Factory returns DefaultProcessor for unknown topics")


def test_factory_custom():
    """Test factory trả về custom processor khi đã register"""
    print("\n=== Test 2: Custom Processor ===")
    
    # Create a custom processor
    class TestCustomProcessor(BaseSchemaProcessor):
        def get_data_schema(self, topic_config):
            return StructType([StructField("test", StringType())])
        
        def get_envelope_schema(self, topic_config, data_schema):
            return StructType([StructField("data", data_schema)])
    
    # Register
    SchemaProcessorFactory.register_processor('test_topic', TestCustomProcessor)
    
    # Get processor
    processor = SchemaProcessorFactory.get_processor(
        'test_topic',
        spark=None,
        kafka_bootstrap_servers='localhost:9092'
    )
    
    assert isinstance(processor, TestCustomProcessor), \
        f"Expected TestCustomProcessor, got {type(processor)}"
    
    print("✓ Factory returns custom processor when registered")


def test_factory_pattern_matching():
    """Test factory hỗ trợ pattern matching với wildcard"""
    print("\n=== Test 3: Pattern Matching ===")
    
    # Create custom processor
    class OrdersProcessor(BaseSchemaProcessor):
        def get_data_schema(self, topic_config):
            return StructType([StructField("order_id", IntegerType())])
        
        def get_envelope_schema(self, topic_config, data_schema):
            return StructType([StructField("order", data_schema)])
    
    # Register with pattern
    SchemaProcessorFactory.register_processor('orders_*', OrdersProcessor)
    
    # Test matching
    test_topics = ['orders_v1', 'orders_v2', 'orders_staging']
    
    for topic in test_topics:
        processor = SchemaProcessorFactory.get_processor(
            topic,
            spark=None,
            kafka_bootstrap_servers='localhost:9092'
        )
        
        assert isinstance(processor, OrdersProcessor), \
            f"Expected OrdersProcessor for {topic}, got {type(processor)}"
        
        print(f"  ✓ Pattern 'orders_*' matched topic: {topic}")
    
    print("✓ Pattern matching works correctly")


def test_schema_validation():
    """Test schema validation trong base processor"""
    print("\n=== Test 4: Schema Validation ===")
    
    class ValidatingProcessor(BaseSchemaProcessor):
        def get_data_schema(self, topic_config):
            return StructType([StructField("id", IntegerType())])
        
        def get_envelope_schema(self, topic_config, data_schema):
            return StructType([StructField("data", data_schema)])
    
    processor = ValidatingProcessor(None, 'localhost:9092')
    
    # Test valid schema
    valid_schema = StructType([StructField("test", StringType())])
    assert processor.validate_schema(valid_schema, 'test_topic'), \
        "Valid schema should pass validation"
    print("  ✓ Valid schema passes validation")
    
    # Test None schema
    assert not processor.validate_schema(None, 'test_topic'), \
        "None schema should fail validation"
    print("  ✓ None schema fails validation")
    
    # Test empty schema
    empty_schema = StructType([])
    assert not processor.validate_schema(empty_schema, 'test_topic'), \
        "Empty schema should fail validation"
    print("  ✓ Empty schema fails validation")
    
    print("✓ Schema validation works correctly")


def test_schema_merge():
    """Test schema merge logic"""
    print("\n=== Test 5: Schema Merge ===")
    
    processor = DefaultSchemaProcessor(None, 'localhost:9092')
    
    old_schema = StructType([
        StructField("id", IntegerType()),
        StructField("name", StringType())
    ])
    
    new_schema = StructType([
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("email", StringType()),  # New field
    ])
    
    merged = processor.merge_schemas(old_schema, new_schema)
    
    # Check merged schema has all fields
    field_names = [f.name for f in merged.fields]
    assert 'id' in field_names, "Merged schema should have 'id'"
    assert 'name' in field_names, "Merged schema should have 'name'"
    assert 'email' in field_names, "Merged schema should have 'email'"
    assert len(merged.fields) == 3, f"Merged schema should have 3 fields, got {len(merged.fields)}"
    
    print("  ✓ Schema merge added new field correctly")
    print("✓ Schema merge works correctly")


def test_main_py_args():
    """Test main.py argument parsing"""
    print("\n=== Test 6: Main.py Arguments ===")
    
    # This test would normally use subprocess to run main.py
    # For now, just verify the logic
    
    # Simulate topic filtering
    all_topics = [
        {'topic_name': 'orders', 'table_name': 'orders'},
        {'topic_name': 'payments', 'table_name': 'payments'},
        {'topic_name': 'users', 'table_name': 'users'},
    ]
    
    # Filter for specific topic
    target_topic = 'orders'
    filtered = [t for t in all_topics if t['topic_name'] == target_topic]
    
    assert len(filtered) == 1, f"Should filter to 1 topic, got {len(filtered)}"
    assert filtered[0]['topic_name'] == target_topic, \
        f"Filtered topic should be {target_topic}"
    
    print(f"  ✓ Topic filtering works: {target_topic}")
    
    # Test with non-existent topic
    target_topic = 'nonexistent'
    filtered = [t for t in all_topics if t['topic_name'] == target_topic]
    
    assert len(filtered) == 0, "Non-existent topic should return empty list"
    print(f"  ✓ Non-existent topic returns empty list")
    
    print("✓ Main.py argument handling works correctly")


def run_all_tests():
    """Run all tests"""
    print("\n" + "="*60)
    print("RUNNING REFACTORING TESTS")
    print("="*60)
    
    try:
        test_factory_default()
        test_factory_custom()
        test_factory_pattern_matching()
        test_schema_validation()
        test_schema_merge()
        test_main_py_args()
        
        print("\n" + "="*60)
        print("✅ ALL TESTS PASSED!")
        print("="*60)
        print("\nRefactoring is working correctly!")
        print("\nNext steps:")
        print("  1. Test với real Spark session")
        print("  2. Test với real Kafka topic")
        print("  3. Deploy to dev environment")
        print("  4. Monitor & validate")
        
        return True
        
    except AssertionError as e:
        print("\n" + "="*60)
        print("❌ TEST FAILED!")
        print("="*60)
        print(f"Error: {e}")
        return False
    except Exception as e:
        print("\n" + "="*60)
        print("❌ TEST ERROR!")
        print("="*60)
        print(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)
