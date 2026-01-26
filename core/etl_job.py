from pyspark.sql.functions import from_json, col, schema_of_json
from pyspark.sql.types import StructType, StructField, StringType
import json
from utils.common import calculate_max_offsets, calculate_trigger_interval
from core.schema_processors import SchemaProcessorFactory

class KafkaToIcebergETL:
    def __init__(self, spark, app_name, app_config, topic_configs, kafka_bootstrap_servers, checkpoint_base_dir):
        self.spark = spark
        self.app_name = app_name
        self.app_config = app_config
        self.topic_configs = topic_configs
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.checkpoint_base_dir = checkpoint_base_dir

    def get_schema_processor(self, topic_name):
        """Get schema processor cho topic"""
        return SchemaProcessorFactory.get_processor(
            topic_name, 
            self.spark, 
            self.kafka_bootstrap_servers
        )

    def check_and_evolve_table_schema(self, database, table_name, new_schema):
        """Kiểm tra và evolve schema của bảng Iceberg nếu cần"""
        full_table_name = f"iceberg.{database}.{table_name}"
        
        try:
            # Lấy schema hiện tại của bảng
            existing_df = self.spark.table(full_table_name)
            existing_schema = existing_df.schema
            
            # So sánh schemas
            existing_fields = {f.name for f in existing_schema.fields}
            new_fields = {f.name for f in new_schema.fields}
            
            # Tìm cột mới cần thêm
            columns_to_add = new_fields - existing_fields
            
            if columns_to_add:
                print(f"  → Evolving table schema for {table_name}...")
                for col_name in columns_to_add:
                    field = [f for f in new_schema.fields if f.name == col_name][0]
                    nullable = "NULL" if field.nullable else "NOT NULL"
                    alter_sql = f"""
                    ALTER TABLE {full_table_name} 
                    ADD COLUMN {col_name} {field.dataType.simpleString()} {nullable}
                    """
                    self.spark.sql(alter_sql)
                    print(f"    + Added column: {col_name}")
                print(f"  ✓ Schema evolved: added {len(columns_to_add)} column(s)")
                
            # Warning về cột bị xóa (không tự động xóa)
            columns_removed = existing_fields - new_fields
            if columns_removed:
                print(f"  ⚠ WARNING: Columns exist in table but missing in new schema: {columns_removed}")
                print(f"    (These columns will NOT be automatically removed)")
                
        except Exception as e:
            # Bảng chưa tồn tại hoặc lỗi khác
            pass

    def create_iceberg_table_if_not_exists(self, database, table_name, schema, partition_cols):
        """Tạo bảng Iceberg nếu chưa tồn tại"""
        full_table_name = f"iceberg.{database}.{table_name}"

        try:
            # Kiểm tra table existence
            self.spark.sql(f"DESCRIBE TABLE {full_table_name}")
            print(f"  ✓ Table exists: {table_name}")
            return
        except Exception:
            pass

        # Tạo DDL
        fields = []
        for field in schema.fields:
            nullable = "NULL" if field.nullable else "NOT NULL"
            fields.append(f"{field.name} {field.dataType.simpleString()} {nullable}")

        fields_sql = ",\n    ".join(fields)

        # Partition clause
        partition_clause = ""
        if partition_cols:
            # Partition by days của timestamp column
            partition_clause = f"PARTITIONED BY (days({partition_cols}))"

        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
            {fields_sql}
        ) USING iceberg
        {partition_clause}
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
        """

        self.spark.sql(create_sql)
        print(f"  ✓ Created table: {table_name}")

    def process_topic_stream(self, topic_config, pool_name):
        """Xử lý streaming cho một topic với hỗ trợ CDC"""
        topic_name = topic_config['topic_name']
        table_name = topic_config['table_name']
        
        # Get schema processor cho topic này
        schema_processor = self.get_schema_processor(topic_name)
        
        # Get schema using processor
        data_schema = schema_processor.get_data_schema(topic_config)
        partition_cols = topic_config['partition_columns'] # Used as MERGE keys
        
        # Get envelope schema using processor
        envelope_schema = schema_processor.get_envelope_schema(topic_config, data_schema)

        # Set scheduler pool cho query này
        self.spark.sparkContext.setLocalProperty("spark.scheduler.pool", pool_name)

        # Đọc từ Kafka
        kafka_stream = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", topic_name) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", calculate_max_offsets(
                topic_config['expected_msg_per_day'])) \
            .load()

        # Parse JSON với envelope schema
        parsed_stream = kafka_stream.select(
            from_json(col("value").cast("string"), envelope_schema).alias("data")
        ).select("data.*")

        # Function để xử lý từng batch
        def process_batch(batch_df, batch_id):
            if batch_df.isEmpty():
                return
                
            # Cache batch để dùng lại
            batch_df.cache()
            
            try:
                full_table_name = f"starrocks_catalog.lakehouse.{table_name}"
                
                # 1. Handle Inserts (op_type = 'I')
                inserts = batch_df.filter(col("op_type") == "I").select("after.*")
                if not inserts.isEmpty():
                    inserts.writeTo(full_table_name).append()
                    print(f"    - Appended {inserts.count()} rows to {table_name}")

                # 2. Handle Upserts (op_type = 'U')
                updates = batch_df.filter(col("op_type") == "U")
                if not updates.isEmpty():
                    # Construct ON clause based on partition_cols (User requirement: upsert by partition_cols)
                    if not partition_cols:
                        print(f"WARNING: No keys defined for upsert on {table_name}, skipping updates.")
                    else:
                        keys = [k.strip() for k in partition_cols.split(",")]
                        on_conditions = [f"t.{k} = s.after.{k}" for k in keys]
                        on_clause = " AND ".join(on_conditions)
                        
                        # Create temp view for the update batch
                        updates.createOrReplaceTempView(f"updates_{table_name}")
                        
                        # Construct SET clause for all columns to support partial updates
                        # COALESCE(s.after.col, t.col) keeps existing value if new value is null (simplistic partial update)
                        # Note: This logic assumes 'before' might miss some fields or we want to merge.
                        # However, for pure CDC 'after' usually contains the full new state. 
                        # If 'after' is partial, COALESCE is needed.
                        set_clauses = []
                        for field in data_schema.fields:
                            col_name = field.name
                            set_clauses.append(f"t.{col_name} = COALESCE(s.after.{col_name}, t.{col_name})")
                        
                        set_clause = ", ".join(set_clauses)

                        merge_sql = f"""
                        MERGE INTO {full_table_name} t
                        USING updates_{table_name} s
                        ON {on_clause}
                        WHEN MATCHED THEN
                            UPDATE SET {set_clause}
                        """
                        self.spark.sql(merge_sql)
                        print(f"    - Merged {updates.count()} rows into {table_name}")
                        
            except Exception as e:
                print(f"Error processing batch {batch_id} for {table_name}: {e}")
            finally:
                batch_df.unpersist()

        # Write stream using foreachBatch
        query = parsed_stream.writeStream \
            .foreachBatch(process_batch) \
            .trigger(processingTime=calculate_trigger_interval(
                topic_config['expected_msg_per_day'])) \
            .option("checkpointLocation",
                    f"{self.checkpoint_base_dir}/{self.app_name}/{topic_name}") \
            .queryName(f"{self.app_name}_{topic_name}") \
            .start()

        print(f"  ✓ Started stream: {topic_name} -> {table_name} (pool: {pool_name})")
        return query

    def run(self):
        """Chạy ETL job"""
        print(f"========================================")
        print(f"Starting ETL Job Logic: {self.app_name}")
        print(f"========================================")

        if len(self.topic_configs) == 0:
            print(f"No active topics found for {self.app_name}")
            return

        # Tạo tables nếu chưa có và evolve schemas
        print("\nCreating/Evolving Iceberg tables...")
        for config in self.topic_configs:
            # Get schema processor
            schema_processor = self.get_schema_processor(config['topic_name'])
            # Get schema using processor
            schema = schema_processor.get_data_schema(config)
            
            # Tạo table nếu chưa có
            self.create_iceberg_table_if_not_exists(
                "lakehouse",
                config['table_name'],
                schema,
                config['partition_columns']
            )
            
            # Evolve schema nếu cần
            self.check_and_evolve_table_schema(
                "lakehouse",
                config['table_name'],
                schema
            )

        # Start streaming queries với pool allocation
        print("\nStarting streaming queries...")
        active_queries = []
        max_concurrent = self.app_config.get('max_concurrent_queries', 5) # Default 5 if not found

        for idx, config in enumerate(self.topic_configs):
            # Phân bổ pool theo round-robin
            pool_name = f"pool_{idx % max_concurrent}"
            query = self.process_topic_stream(config, pool_name)
            active_queries.append(query)

        print(f"\n✓ All {len(active_queries)} streams started successfully")
        print(f"Waiting for termination...\n")

        # Wait for any query to terminate
        self.spark.streams.awaitAnyTermination()
