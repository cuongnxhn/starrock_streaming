from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType
import json
import sys
import argparse


class KafkaToIcebergETL:
    def __init__(self, app_name, kafka_bootstrap_servers, checkpoint_base_dir):
        self.app_name = app_name
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.checkpoint_base_dir = checkpoint_base_dir
        self.spark = None
        self.app_config = None
        self.topic_configs = []

    def load_spark_config(self):
        """Load cấu hình Spark từ metadata table"""
        # Tạo Spark session tạm để load config
        temp_spark = SparkSession.builder \
            .appName(f"{self.app_name}_config_loader") \
            .config("spark.sql.catalog.starrocks_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.starrocks_catalog.type", "rest") \
            .config("spark.sql.catalog.starrocks_catalog.uri", "http://starrocks-fe:8181") \
            .getOrCreate()

        # Load app config
        config_df = temp_spark.sql(f"""
            SELECT * FROM metadata.spark_app_config 
            WHERE app_name = '{self.app_name}'
        """)

        if config_df.count() == 0:
            temp_spark.stop()
            raise ValueError(f"App config not found for: {self.app_name}")

        self.app_config = config_df.first().asDict()
        temp_spark.stop()

    def initialize_spark_session(self):
        """Khởi tạo Spark session với config từ metadata"""
        conf_builder = SparkSession.builder \
            .appName(self.app_name) \
            .config("spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.starrocks_catalog",
                    "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.starrocks_catalog.type", "rest") \
            .config("spark.sql.catalog.starrocks_catalog.uri",
                    "http://starrocks-fe:8181") \
            .config("spark.sql.catalog.starrocks_catalog.warehouse",
                    "s3://your-bucket/warehouse") \
            .config("spark.sql.catalog.starrocks_catalog.io-impl",
                    "org.apache.iceberg.aws.s3.S3FileIO") \
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.0")

        # Apply dynamic config từ metadata
        conf_builder = conf_builder \
            .config("spark.executor.cores", self.app_config['executor_cores']) \
            .config("spark.executor.memory", self.app_config['executor_memory']) \
            .config("spark.executor.instances", self.app_config['executor_instances']) \
            .config("spark.driver.memory", self.app_config['driver_memory']) \
            .config("spark.driver.cores", self.app_config['driver_cores']) \
            .config("spark.scheduler.mode", self.app_config['scheduler_mode'])

        # Cấu hình cho multiple streaming queries
        if self.app_config['scheduler_mode'] == 'FAIR':
            conf_builder = conf_builder \
                .config("spark.scheduler.allocation.file",
                        "/opt/spark/conf/fairscheduler.xml")

        self.spark = conf_builder.getOrCreate()

        print(f"✓ Spark session initialized for {self.app_name}")
        print(f"  - Executor cores: {self.app_config['executor_cores']}")
        print(f"  - Executor memory: {self.app_config['executor_memory']}")
        print(f"  - Executor instances: {self.app_config['executor_instances']}")

    def load_topic_configs(self):
        """Load danh sách topics cho app này"""
        topics_df = self.spark.sql(f"""
            SELECT * FROM metadata.topic_configurations
            WHERE app_name = '{self.app_name}' AND is_active = TRUE
            ORDER BY expected_msg_per_day DESC
        """)

        self.topic_configs = topics_df.collect()
        print(f"✓ Loaded {len(self.topic_configs)} topics for {self.app_name}")

    def create_iceberg_table_if_not_exists(self, database, table_name,
                                           schema, partition_cols):
        """Tạo bảng Iceberg nếu chưa tồn tại"""
        full_table_name = f"starrocks_catalog.{database}.{table_name}"

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
        """Xử lý streaming cho một topic"""
        topic_name = topic_config['topic_name']
        table_name = topic_config['table_name']
        schema = StructType.fromJson(json.loads(topic_config['schema_definition']))
        partition_cols = topic_config['partition_columns']

        # Set scheduler pool cho query này
        self.spark.sparkContext.setLocalProperty("spark.scheduler.pool", pool_name)

        # Đọc từ Kafka
        kafka_stream = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", topic_name) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", self._calculate_max_offsets(
            topic_config['expected_msg_per_day'])) \
            .load()

        # Parse JSON
        parsed_stream = kafka_stream.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")

        # Write stream
        query = parsed_stream.writeStream \
            .format("iceberg") \
            .outputMode("append") \
            .trigger(processingTime=self._calculate_trigger_interval(
            topic_config['expected_msg_per_day'])) \
            .option("checkpointLocation",
                    f"{self.checkpoint_base_dir}/{self.app_name}/{topic_name}") \
            .option("fanout-enabled", "true") \
            .queryName(f"{self.app_name}_{topic_name}") \
            .toTable(f"starrocks_catalog.lakehouse.{table_name}")

        print(f"  ✓ Started stream: {topic_name} -> {table_name} (pool: {pool_name})")
        return query

    def _calculate_max_offsets(self, expected_msg_per_day):
        """Tính max offsets per trigger dựa trên volume"""
        if expected_msg_per_day < 100000:
            return 10000
        elif expected_msg_per_day < 1000000:
            return 50000
        else:
            return 100000

    def _calculate_trigger_interval(self, expected_msg_per_day):
        """Tính trigger interval dựa trên volume"""
        if expected_msg_per_day < 100000:
            return "5 minutes"
        elif expected_msg_per_day < 1000000:
            return "2 minutes"
        else:
            return "1 minute"

    def run(self):
        """Chạy ETL job"""
        print(f"========================================")
        print(f"Starting ETL Job: {self.app_name}")
        print(f"========================================")

        # Load config và khởi tạo Spark
        self.load_spark_config()
        self.initialize_spark_session()
        self.load_topic_configs()

        if len(self.topic_configs) == 0:
            print(f"No active topics found for {self.app_name}")
            return

        # Tạo tables nếu chưa có
        print("\nCreating Iceberg tables...")
        for config in self.topic_configs:
            schema = StructType.fromJson(json.loads(config['schema_definition']))
            self.create_iceberg_table_if_not_exists(
                "lakehouse",
                config['table_name'],
                schema,
                config['partition_columns']
            )

        # Start streaming queries với pool allocation
        print("\nStarting streaming queries...")
        active_queries = []
        max_concurrent = self.app_config['max_concurrent_queries']

        for idx, config in enumerate(self.topic_configs):
            # Phân bổ pool theo round-robin
            pool_name = f"pool_{idx % max_concurrent}"
            query = self.process_topic_stream(config, pool_name)
            active_queries.append(query)

        print(f"\n✓ All {len(active_queries)} streams started successfully")
        print(f"Waiting for termination...\n")

        # Wait for any query to terminate
        self.spark.streams.awaitAnyTermination()


def main():
    parser = argparse.ArgumentParser(description='Kafka to Iceberg ETL Job')
    parser.add_argument('--app-name', required=True,
                        help='Application name (from spark_app_config table)')
    parser.add_argument('--kafka-bootstrap-servers',
                        default='localhost:9092',
                        help='Kafka bootstrap servers')
    parser.add_argument('--checkpoint-dir',
                        default='/opt/spark/checkpoints',
                        help='Base directory for checkpoints')

    args = parser.parse_args()

    etl = KafkaToIcebergETL(
        app_name=args.app_name,
        kafka_bootstrap_servers=args.kafka_bootstrap_servers,
        checkpoint_base_dir=args.checkpoint_dir
    )

    etl.run()


if __name__ == "__main__":
    main()
