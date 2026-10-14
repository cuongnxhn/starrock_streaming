"""
Bronze Layer Job: Kafka → rawdata (append-only)

Nhiệm vụ:
- Đọc CDC messages từ Kafka
- Parse JSON envelope (GoldenGate / Debezium)
- Append raw messages vào Bronze layer (Iceberg namespace: rawdata)
- Không thực hiện bất kỳ transform hay MERGE nào

Kiến trúc: Bronze/Silver (Medallion)
  Kafka → [bronze_job.py] → rawdata (Bronze) → [silver_job.py] → warehouse (Silver)
"""

from pyspark.sql.functions import (
    from_json, col, concat, lit, when, coalesce,
    to_json, to_date, date_format, struct
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, TimestampType
)
from utils.common import calculate_max_offsets, calculate_trigger_interval
from core.schema_processors import SchemaProcessorFactory
from utils.kafka_config import KafkaConfig
import os


class KafkaToBronzeETL:
    """
    Streaming job: đọc Kafka và append raw CDC messages vào Bronze layer (rawdata).
    Chỉ lưu trữ, không transform. Idempotency được đảm bảo ở Silver layer.
    """

    def __init__(self, spark, app_name, app_config, topic_configs,
                 kafka_bootstrap_servers, checkpoint_base_dir):
        self.spark = spark
        self.app_name = app_name
        self.app_config = app_config
        self.topic_configs = topic_configs
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.checkpoint_base_dir = checkpoint_base_dir

    def get_schema_processor(self, topic_name):
        """Lấy schema processor cho topic"""
        return SchemaProcessorFactory.get_processor(
            topic_name,
            self.spark,
            self.kafka_bootstrap_servers
        )

    def create_bronze_table_if_not_exists(self, table_name):
        """Tạo Bronze table trong rawdata namespace nếu chưa tồn tại"""
        log_catalog = os.getenv("ICEBERG_CATALOG_BRONZE", "iceberg")
        log_namespace = os.getenv("ICEBERG_NAMESPACE_BRONZE", "rawdata")
        full_table_name = f"{log_catalog}.{log_namespace}.{table_name}"

        bronze_schema = StructType([
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
            self.spark.sql(f"DESCRIBE TABLE {full_table_name}")
            print(f"  ✓ Bronze table exists: {table_name}")
            # Đảm bảo table đã có đủ retention properties
            # (cho các table được tạo trước khi có config này)
            self._ensure_retention_properties(full_table_name)
            return
        except Exception:
            pass

        fields = []
        for field in bronze_schema.fields:
            nullable = "" if field.nullable else "NOT NULL"
            fields.append(f"{field.name} {field.dataType.simpleString()} {nullable}")

        fields_sql = ",\n    ".join(fields)

        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
            {fields_sql}
        ) USING iceberg
        PARTITIONED BY (data_date_key)
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy',

            -- Snapshot retention: đồng bộ với silver_job expire settings
            'history.expire.max-snapshot-age-ms' = '604800000',
            'history.expire.min-snapshots-to-keep' = '200',

            'write.metadata.delete-after-commit.enabled' = 'true',
            'write.metadata.previous-versions-max' = '10'
        )
        """
        self.spark.sql(create_sql)
        print(f"  ✓ Created Bronze table: {table_name}")

    def _ensure_retention_properties(self, full_table_name):
        """Đảm bảo table đã có retention + metadata cleanup properties (cho các table tạo trước khi có config)."""
        try:
            self.spark.sql(f"""
                ALTER TABLE {full_table_name} SET TBLPROPERTIES (
                    'history.expire.max-snapshot-age-ms' = '604800000',
                    'history.expire.min-snapshots-to-keep' = '200',
                    'write.metadata.delete-after-commit.enabled' = 'true',
                    'write.metadata.previous-versions-max' = '10'
                )
            """)
        except Exception as e:
            print(f"  ⚠ Could not update retention properties for {full_table_name}: {e}")

    def process_topic_stream(self, topic_config, pool_name):
        """
        Xử lý streaming cho một topic:
          1. Đọc từ Kafka
          2. Parse JSON envelope
          3. Append raw message vào Bronze (rawdata) — không MERGE, không transform
        """
        topic_name = topic_config['topic_name']
        table_name = topic_config['table_name']

        # Schema processor
        schema_processor = self.get_schema_processor(topic_name)
        data_schema = schema_processor.get_data_schema(topic_config)
        
        if data_schema is None:
            print(f"  ⚠ WARNING: Skipping stream for topic '{topic_name}' due to missing schema.")
            return None

        envelope_schema = schema_processor.get_envelope_schema(topic_config, data_schema)

        # Đảm bảo Bronze table tồn tại
        self.create_bronze_table_if_not_exists(table_name)

        # Set scheduler pool
        self.spark.sparkContext.setLocalProperty("spark.scheduler.pool", pool_name)

        # Kafka authentication
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

        stream_reader = KafkaConfig.apply_auth_options(stream_reader, kafka_auth)
        kafka_stream = stream_reader.load()

        # Parse JSON với envelope schema, giữ lại Kafka metadata để làm unique key
        parsed_stream = kafka_stream.select(
            col("partition").alias("_kafka_partition"),
            col("offset").alias("_kafka_offset"),
            col("timestamp").alias("_kafka_timestamp"),
            from_json(col("value").cast("string"), envelope_schema).alias("data")
        ).select("_kafka_partition", "_kafka_offset", "_kafka_timestamp", "data.*")

        def process_batch(batch_df, batch_id):
            """Chỉ append vào Bronze — không MERGE, không transform"""
            if batch_df.isEmpty():
                return

            batch_df.cache()
            try:
                log_catalog = os.getenv("ICEBERG_CATALOG_BRONZE", "iceberg")
                log_namespace = os.getenv("ICEBERG_NAMESPACE_BRONZE", "rawdata")
                bronze_table = f"{log_catalog}.{log_namespace}.{table_name}"

                # Detect source type
                first_row = batch_df.first()
                is_goldengate = (
                    first_row and
                    hasattr(first_row, 'op_type') and
                    first_row.op_type is not None
                )

                if is_goldengate:
                    bronze_df = batch_df.select(
                        concat(
                            col("_kafka_partition").cast("string"), lit("_"),
                            col("_kafka_offset").cast("string")
                        ).alias("message_id"),
                        col("_kafka_partition").cast("int").alias("kafka_partition"),
                        col("_kafka_offset").cast("bigint").alias("kafka_offset"),
                        col("_kafka_timestamp").cast("timestamp").alias("kafka_timestamp"),
                        lit("goldengate").alias("source_type"),
                        when(col("op_type") == "I", lit("insert"))
                        .when(col("op_type") == "U", lit("update"))
                        .when(col("op_type") == "D", lit("delete"))
                        .otherwise(lit("unknown")).alias("operation"),
                        col("op_ts").cast("timestamp").alias("op_ts"),
                        col("current_ts").cast("timestamp").alias("current_ts"),
                        to_json(col("before")).alias("before_data"),
                        to_json(col("after")).alias("after_data"),
                        to_json(struct("*")).alias("raw_json"),
                        date_format(
                            to_date(coalesce(col("op_ts"), col("current_ts"))),
                            "yyyyMMdd"
                        ).alias("data_date_key")
                    )
                else:
                    # Debezium format
                    bronze_df = batch_df.select(
                        concat(
                            col("_kafka_partition").cast("string"), lit("_"),
                            col("_kafka_offset").cast("string")
                        ).alias("message_id"),
                        col("_kafka_partition").cast("int").alias("kafka_partition"),
                        col("_kafka_offset").cast("bigint").alias("kafka_offset"),
                        col("_kafka_timestamp").cast("timestamp").alias("kafka_timestamp"),
                        lit("debezium").alias("source_type"),
                        when(col("payload.op") == "c", lit("insert"))
                        .when(col("payload.op") == "r", lit("insert"))
                        .when(col("payload.op") == "u", lit("update"))
                        .when(col("payload.op") == "d", lit("delete"))
                        .otherwise(lit("unknown")).alias("operation"),
                        (col("payload.source.ts_ms") / 1000).cast("timestamp").alias("op_ts"),
                        (col("payload.ts_ms") / 1000).cast("timestamp").alias("current_ts"),
                        to_json(col("payload.before")).alias("before_data"),
                        to_json(col("payload.after")).alias("after_data"),
                        to_json(struct("*")).alias("raw_json"),
                        date_format(
                            to_date(coalesce(
                                (col("payload.source.ts_ms") / 1000).cast("timestamp"),
                                (col("payload.ts_ms") / 1000).cast("timestamp")
                            )),
                            "yyyyMMdd"
                        ).alias("data_date_key")
                    )

                if not bronze_df.isEmpty():
                    # Dùng MERGE ON message_id để đảm bảo idempotency:
                    # Nếu job crash sau khi write nhưng trước khi commit checkpoint,
                    # batch sẽ được replay nhưng record đã có sẽ không bị ghi lại (WHEN NOT MATCHED)
                    #
                    # Dùng Global Temp View thay vì Session Temp View:
                    # Khi active catalog là Iceberg, Spark resolve tên bảng trong Iceberg catalog
                    # → session temp view bị "khuất" sau catalog namespace.
                    # Global temp view luôn nằm trong database 'global_temp' built-in,
                    # có thể truy cập được bất kể catalog hiện tại là gì.
                    tmp_view = f"bronze_batch_{table_name}_{batch_id}"
                    bronze_df.createOrReplaceGlobalTempView(tmp_view)
                    try:
                        self.spark.sql(f"""
                            MERGE INTO {bronze_table} AS target
                            USING global_temp.{tmp_view} AS source
                            ON target.message_id = source.message_id
                            WHEN NOT MATCHED THEN INSERT *
                        """)
                    finally:
                        self.spark.catalog.dropGlobalTempView(tmp_view)
                    source_label = "GoldenGate" if is_goldengate else "Debezium"
                    print(f"    ✓ [Bronze] Merged {bronze_df.count()} "
                          f"{source_label} messages → {table_name} (batch {batch_id})")

            except Exception as e:
                print(f"    ✗ [Bronze] Error processing batch {batch_id} for {table_name}: {e}")
            finally:
                batch_df.unpersist()

        # Start streaming query
        query = parsed_stream.writeStream \
            .foreachBatch(process_batch) \
            .trigger(processingTime=calculate_trigger_interval(
                topic_config['expected_msg_per_day'])) \
            .option("checkpointLocation",
                    f"{self.checkpoint_base_dir}/bronze/{topic_name}") \
            .queryName(f"bronze_{self.app_name}_{topic_name}") \
            .start()

        print(f"  ✓ [Bronze] Started stream: {topic_name} → rawdata.{table_name} (pool: {pool_name})")
        return query

    def run(self):
        """Chạy Bronze streaming job — chờ không kết thúc"""
        print(f"========================================")
        print(f"[BRONZE JOB] Starting: {self.app_name}")
        print(f"Mode: Streaming append-only → rawdata")
        print(f"========================================")

        if not self.topic_configs:
            print(f"No active topics found for {self.app_name}")
            return

        # Tạo Bronze tables
        print("\nEnsuring Bronze (rawdata) tables exist...")
        for config in self.topic_configs:
            self.create_bronze_table_if_not_exists(config['table_name'])

        # Start streaming queries
        print("\nStarting streaming queries...")
        active_queries = []
        max_concurrent = self.app_config.get('max_concurrent_queries', 5)

        for idx, config in enumerate(self.topic_configs):
            pool_name = f"pool_{idx % max_concurrent}"
            query = self.process_topic_stream(config, pool_name)
            if query is not None:
                active_queries.append(query)

        print(f"\n✓ [Bronze] All {len(active_queries)} streams started successfully")
        print(f"Waiting for termination...\n")
        self.spark.streams.awaitAnyTermination()
