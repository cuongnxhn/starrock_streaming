from pyspark.sql.functions import from_json, col, schema_of_json, concat, lit, when, coalesce, to_json, to_date, date_format, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType, DateType
import json
from utils.common import calculate_max_offsets, calculate_trigger_interval
from core.schema_processors import SchemaProcessorFactory
from utils.kafka_config import KafkaConfig
import os

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

    def create_log_table_if_not_exists(self, table_name):
        """Tạo bảng log cho Kafka messages nếu chưa tồn tại"""
        log_catalog = os.getenv("ICEBERG_LOG_CATALOG", "iceberg")
        log_namespace = os.getenv("ICEBERG_LOG_NAMESPACE", "rawdata")
        log_table_name = f"{table_name}"
        full_log_table_name = f"{log_catalog}.{log_namespace}.{log_table_name}"
        
        # Define log table schema
        log_schema = StructType([
            StructField("message_id", StringType(), False),
            StructField("kafka_partition", IntegerType(), True),
            StructField("kafka_offset", LongType(), True),
            StructField("kafka_timestamp", TimestampType(), True),
            StructField("source_type", StringType(), True),
            StructField("operation", StringType(), True),
            StructField("op_ts", TimestampType(), True),
            StructField("current_ts", TimestampType(), True),
            StructField("before_data", StringType(), True),
            StructField("after_data", StringType(), True),
            StructField("raw_json", StringType(), True),
            StructField("data_date_key", StringType(), True)
        ])
        
        try:
            # Kiểm tra table existence
            self.spark.sql(f"DESCRIBE TABLE {full_log_table_name}")
            print(f"  ✓ Log table exists: {log_table_name}")
            return
        except Exception:
            pass
        
        # Tạo DDL cho log table
        fields = []
        for field in log_schema.fields:
            nullable = "" if field.nullable else "NOT NULL"
            fields.append(f"{field.name} {field.dataType.simpleString()} {nullable}")
        
        fields_sql = ",\n    ".join(fields)
        
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {full_log_table_name} (
            {fields_sql}
        ) USING iceberg
        PARTITIONED BY (data_date_key)
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
        """
        
        self.spark.sql(create_sql)
        print(f"  ✓ Created log table: {log_table_name}")
    
    def cast_to_table_schema(self, df, table_name, database="warehouse"):
        """
        Cast DataFrame columns to match target table schema.
        Handles type mismatches like STRING to DECIMAL casting.
        
        Args:
            df: Input DataFrame
            table_name: Target table name
            database: Target database/namespace
            
        Returns:
            DataFrame with casted columns
        """
        catalog = os.getenv("ICEBERG_CATALOG", "iceberg")
        full_table_name = f"{catalog}.{database}.{table_name}"
        
        try:
            # Get target table schema
            target_df = self.spark.table(full_table_name)
            target_schema = target_df.schema
            
            # Build column list with casting
            casted_cols = []
            for field in target_schema.fields:
                col_name = field.name
                target_type = field.dataType
                
                if col_name in df.columns:
                    # Cast to target type
                    # Handle special cases for DECIMAL to prevent casting errors
                    if target_type.simpleString().startswith("decimal"):
                        # For DECIMAL, handle empty strings and nulls
                        casted_col = when(
                            (col(col_name).isNull()) | (col(col_name) == ""),
                            lit(None).cast(target_type)
                        ).otherwise(
                            col(col_name).cast(target_type)
                        ).alias(col_name)
                    else:
                        casted_col = col(col_name).cast(target_type)
                    
                    casted_cols.append(casted_col)
                else:
                    # Column missing in source, use NULL
                    casted_cols.append(lit(None).cast(target_type).alias(col_name))
            
            return df.select(*casted_cols)
            
        except Exception as e:
            print(f"  ⚠ WARNING: Failed to cast DataFrame to table schema: {e}")
            print(f"  → Returning original DataFrame")
            return df
    
    def create_iceberg_table_if_not_exists(self, database, table_name, schema, partition_cols):
        """Tạo bảng Iceberg nếu chưa tồn tại"""
        catalog = os.getenv("ICEBERG_CATALOG", "iceberg")
        full_table_name = f"{catalog}.{database}.{table_name}"

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
            nullable = "" if field.nullable else "NOT NULL"
            fields.append(f"{field.name} {field.dataType.simpleString()} {nullable}")

        fields_sql = ",\n    ".join(fields)

        # Partition clause
        partition_clause = ""
        if partition_cols:
            # Partition by days của timestamp column
            partition_clause = f"PARTITIONED BY ({partition_cols})" 

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
        
        # Tạo log table nếu chưa có
        self.create_log_table_if_not_exists(table_name)

        # Set scheduler pool cho query này
        self.spark.sparkContext.setLocalProperty("spark.scheduler.pool", pool_name)

        # Load Kafka authentication config
        kafka_auth = KafkaConfig.load_auth_config()

        # Đọc từ Kafka
        stream_reader = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", topic_name) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", calculate_max_offsets(
                topic_config['expected_msg_per_day']))
        
        # Apply authentication options
        stream_reader = KafkaConfig.apply_auth_options(stream_reader, kafka_auth)
        
        kafka_stream = stream_reader.load()

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
                # Get catalog and namespace from env
                catalog = os.getenv("ICEBERG_CATALOG", "iceberg")
                namespace = os.getenv("ICEBERG_NAMESPACE", "warehouse")
                log_catalog = os.getenv("ICEBERG_LOG_CATALOG", "iceberg")
                log_namespace = os.getenv("ICEBERG_LOG_NAMESPACE", "rawdata")
                
                full_table_name = f"{catalog}.{namespace}.{table_name}"
                log_table_name = f"{log_catalog}.{log_namespace}.{table_name}"
                
                # ====== LOG KAFKA MESSAGES ======
                try:
                    # Detect source type first by checking fields
                    first_row = batch_df.first()
                    is_goldengate = first_row and hasattr(first_row, 'op_type') and first_row.op_type is not None
                    
                    if is_goldengate:
                        # GoldenGate format
                        log_df = batch_df.select(
                            # Create unique message ID
                            concat(lit(topic_name), lit("_"), lit(batch_id), lit("_"), 
                                   col("op_ts")).alias("message_id"),
                            
                            # Kafka metadata (placeholder for future enhancement)
                            lit(None).cast("int").alias("kafka_partition"),
                            lit(None).cast("bigint").alias("kafka_offset"),
                            lit(None).cast("timestamp").alias("kafka_timestamp"),
                            
                            # Source type
                            lit("goldengate").alias("source_type"),
                            
                            # Standardize operation (GoldenGate uses op_type: I/U/D)
                            when(col("op_type") == "I", lit("insert"))
                            .when(col("op_type") == "U", lit("update"))
                            .when(col("op_type") == "D", lit("delete"))
                            .otherwise(lit("unknown")).alias("operation"),
                            
                            # Operation timestamps (GoldenGate format)
                            col("op_ts").cast("timestamp").alias("op_ts"),
                            col("current_ts").cast("timestamp").alias("current_ts"),
                            
                            # Before/After as JSON strings (direct access)
                            to_json(col("before")).alias("before_data"),
                            to_json(col("after")).alias("after_data"),
                            
                            # Raw JSON
                            to_json(struct("*")).alias("raw_json"),
                            
                            # Data date key for partitioning (YYYYMMDD format)
                            date_format(to_date(coalesce(col("op_ts"), col("current_ts"))), "yyyyMMdd").alias("data_date_key")
                        )
                    else:
                        # Debezium format
                        log_df = batch_df.select(
                            # Create unique message ID
                            concat(lit(topic_name), lit("_"), lit(batch_id), lit("_"), 
                                   col("payload.ts_ms").cast("string")).alias("message_id"),
                            
                            # Kafka metadata (placeholder for future enhancement)
                            lit(None).cast("int").alias("kafka_partition"),
                            lit(None).cast("bigint").alias("kafka_offset"),
                            lit(None).cast("timestamp").alias("kafka_timestamp"),
                            
                            # Source type
                            lit("debezium").alias("source_type"),
                            
                            # Standardize operation (Debezium uses payload.op: c/u/d/r)
                            when(col("payload.op") == "c", lit("insert"))  # create
                            .when(col("payload.op") == "r", lit("insert"))  # read (snapshot)
                            .when(col("payload.op") == "u", lit("update"))
                            .when(col("payload.op") == "d", lit("delete"))
                            .otherwise(lit("unknown")).alias("operation"),
                            
                            # Operation timestamps (Debezium format)
                            # op_ts from source.ts_ms, current_ts from payload.ts_ms
                            (col("payload.source.ts_ms") / 1000).cast("timestamp").alias("op_ts"),
                            (col("payload.ts_ms") / 1000).cast("timestamp").alias("current_ts"),
                            
                            # Before/After as JSON strings (access via payload)
                            to_json(col("payload.before")).alias("before_data"),
                            to_json(col("payload.after")).alias("after_data"),
                            
                            # Raw JSON
                            to_json(struct("*")).alias("raw_json"),
                            
                            # Data date key for partitioning (YYYYMMDD format)
                            date_format(to_date(coalesce(col("op_ts"), col("current_ts"))), "yyyyMMdd").alias("data_date_key")
                        )
                    
                    # Append to log table
                    if not log_df.isEmpty():
                        log_df.writeTo(log_table_name).append()
                        source_type = "GoldenGate" if is_goldengate else "Debezium"
                        print(f"    - Logged {log_df.count()} {source_type} messages to {table_name}")
                    
                except Exception as log_error:
                    print(f"    ⚠ Warning: Failed to log messages for batch {batch_id}: {log_error}")
                    # Continue with main processing even if logging fails
                
                # 1. Handle Inserts (op_type = 'I')
                inserts = batch_df.filter(col("op_type") == "I").select("after.*")
                if not inserts.isEmpty():
                    # Cast to table schema to prevent type mismatches
                    inserts_casted = self.cast_to_table_schema(inserts, table_name, namespace)
                    inserts_casted.writeTo(full_table_name).append()
                    print(f"    - Appended {inserts_casted.count()} rows to {table_name}")

                # 2. Handle Upserts (op_type = 'U')
                updates = batch_df.filter(col("op_type") == "U")
                if not updates.isEmpty():
                    # Construct ON clause based on partition_cols (User requirement: upsert by partition_cols)
                    if not partition_cols:
                        print(f"WARNING: No keys defined for upsert on {table_name}, skipping updates.")
                    else:
                        keys = [k.strip() for k in partition_cols.split(",")]
                        
                        # Extract and cast 'after' data to match table schema
                        updates_data = updates.select("after.*")
                        updates_casted = self.cast_to_table_schema(updates_data, table_name, namespace)
                        
                        # Create GLOBAL temp view for the update batch
                        # Using global temp view ensures visibility across catalogs (including Iceberg)
                        updates_casted.createOrReplaceGlobalTempView(f"updates_{table_name}")
                        
                        # Construct ON clause with keys
                        on_conditions = [f"t.{k} = s.{k}" for k in keys]
                        on_clause = " AND ".join(on_conditions)
                        
                        # Construct SET clause for all columns to support partial updates
                        # COALESCE(s.col, t.col) keeps existing value if new value is null (simplistic partial update)
                        # Note: This logic assumes 'before' might miss some fields or we want to merge.
                        # However, for pure CDC 'after' usually contains the full new state. 
                        # If 'after' is partial, COALESCE is needed.
                        set_clauses = []
                        insert_cols = []
                        insert_values = []
                        
                        for field in updates_casted.schema.fields:
                            col_name = field.name
                            set_clauses.append(f"t.{col_name} = COALESCE(s.{col_name}, t.{col_name})")
                            insert_cols.append(col_name)
                            insert_values.append(f"s.{col_name}")
                        
                        set_clause = ", ".join(set_clauses)
                        insert_cols_clause = ", ".join(insert_cols)
                        insert_values_clause = ", ".join(insert_values)

                        # IMPORTANT: Add WHEN NOT MATCHED to handle UPDATE messages for non-existent records
                        # This is critical when:
                        # 1. No initial snapshot was taken
                        # 2. INSERT messages were missed
                        # 3. Records existed before streaming started
                        merge_sql = f"""
                        MERGE INTO {full_table_name} t
                        USING global_temp.updates_{table_name} s
                        ON {on_clause}
                        WHEN MATCHED THEN
                            UPDATE SET {set_clause}
                        WHEN NOT MATCHED THEN
                            INSERT ({insert_cols_clause})
                            VALUES ({insert_values_clause})
                        """
                        self.spark.sql(merge_sql)
                        print(f"    - Merged {updates_casted.count()} rows into {table_name}")
                
                # 3. Handle Deletes (op_type = 'D')
                deletes = batch_df.filter(col("op_type") == "D")
                if not deletes.isEmpty():
                    if not partition_cols:
                        print(f"WARNING: No keys defined for delete on {table_name}, skipping deletes.")
                    else:
                        keys = [k.strip() for k in partition_cols.split(",")]
                        
                        # Extract 'before' data (contains the record to be deleted)
                        # For DELETE, 'before' contains the last state before deletion
                        deletes_data = deletes.select("before.*")
                        deletes_casted = self.cast_to_table_schema(deletes_data, table_name, namespace)
                        
                        # Create GLOBAL temp view for the delete batch
                        deletes_casted.createOrReplaceGlobalTempView(f"deletes_{table_name}")
                        
                        # Construct ON clause with keys
                        on_conditions = [f"t.{k} = s.{k}" for k in keys]
                        on_clause = " AND ".join(on_conditions)
                        
                        # MERGE with DELETE: physically remove records from table
                        delete_sql = f"""
                        MERGE INTO {full_table_name} t
                        USING global_temp.deletes_{table_name} s
                        ON {on_clause}
                        WHEN MATCHED THEN DELETE
                        """
                        self.spark.sql(delete_sql)
                        print(f"    - Deleted {deletes_casted.count()} rows from {table_name}")
                        
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
            namespace = os.getenv("ICEBERG_NAMESPACE", "warehouse")
            # Tạo table nếu chưa có
            self.create_iceberg_table_if_not_exists(
                namespace,
                config['table_name'],
                schema,
                config['partition_columns']
            )
            
            # Evolve schema nếu cần
            self.check_and_evolve_table_schema(
                namespace,
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
