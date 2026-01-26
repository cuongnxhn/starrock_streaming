from .default import DefaultSchemaProcessor


class SchemaProcessorFactory:
    """
    Factory để tạo schema processor phù hợp cho mỗi topic.
    Có thể extend để add custom processors cho các topic đặc biệt.
    """
    
    # Registry để map topic_name -> processor class
    _processors = {}
    
    @classmethod
    def register_processor(cls, topic_name, processor_class):
        """
        Đăng ký custom processor cho một topic.
        
        Example:
            SchemaProcessorFactory.register_processor(
                'special_topic', 
                SpecialTopicSchemaProcessor
            )
        
        Args:
            topic_name: Tên topic (hoặc pattern)
            processor_class: Class của processor (must inherit BaseSchemaProcessor)
        """
        cls._processors[topic_name] = processor_class
        print(f"  ✓ Registered custom processor for topic: {topic_name}")
    
    @classmethod
    def get_processor(cls, topic_name, spark, kafka_bootstrap_servers):
        """
        Lấy processor phù hợp cho topic.
        
        Args:
            topic_name: Tên topic
            spark: SparkSession
            kafka_bootstrap_servers: Kafka servers
            
        Returns:
            BaseSchemaProcessor: Instance của processor
        """
        # Check nếu có custom processor cho topic này
        if topic_name in cls._processors:
            processor_class = cls._processors[topic_name]
            print(f"  → Using custom processor for topic: {topic_name}")
            return processor_class(spark, kafka_bootstrap_servers)
        
        # Check pattern matching (ví dụ: 'orders_*' match 'orders_v1', 'orders_v2')
        for pattern, processor_class in cls._processors.items():
            if '*' in pattern:
                import re
                regex_pattern = pattern.replace('*', '.*')
                if re.match(regex_pattern, topic_name):
                    print(f"  → Using custom processor for topic: {topic_name} (matched pattern: {pattern})")
                    return processor_class(spark, kafka_bootstrap_servers)
        
        # Default processor
        return DefaultSchemaProcessor(spark, kafka_bootstrap_servers)
