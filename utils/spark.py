from pyspark.sql import SparkSession
import os

def create_spark_session(app_name, app_config):
    """Khởi tạo Spark session với config từ metadata - based on working Java config"""
    
    # Get S3 credentials from environment
    s3_endpoint = os.getenv("S3_ENDPOINT", "http://10.165.13.116:9000")
    s3_access_key = os.getenv("S3_ACCESS_KEY", "minioadmin")
    s3_secret_key = os.getenv("S3_SECRET_KEY", "minioadmin")
    iceberg_rest_uri = os.getenv("STARROCK_FE", "http://10.165.13.116:8181")
    iceberg_warehouse = os.getenv("ICEBERG_WAREHOUSE", "s3a://iceberg/warehouse")
    
    conf_builder = SparkSession.builder \
        .appName(app_name) \
        \
        .config("spark.jars.packages", 
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        \
        .config("spark.hadoop.io.native.lib.available", "false") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.sql.catalogImplementation", "in-memory") \
        .config("spark.hadoop.yarn.timeline-service.enabled", "false") \
        .config("spark.eventLog.enabled", "false") \
        .config("spark.ui.enabled", "true") \
        .config("spark.ui.showConsoleProgress", "false") \
        \
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", s3_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.buffer.dir", "/tmp") \
        .config("spark.hadoop.fs.s3a.block.size", "67108864") \
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "array") \
        \
        .config("spark.sql.extensions", 
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "rest") \
        .config("spark.sql.catalog.iceberg.uri", iceberg_rest_uri) \
        .config("spark.sql.catalog.iceberg.warehouse", iceberg_warehouse) \
        .config("spark.sql.catalog.iceberg.cache-enabled", "true")

    # Apply dynamic config from database
    conf_builder = conf_builder \
        .config("spark.executor.cores", app_config.get('executor_cores')) \
        .config("spark.executor.memory", app_config.get('executor_memory')) \
        .config("spark.executor.instances", app_config.get('executor_instances')) \
        .config("spark.driver.memory", app_config.get('driver_memory')) \
        .config("spark.driver.cores", app_config.get('driver_cores')) \
        .config("spark.scheduler.mode", app_config.get('scheduler_mode')) \
        \
        .config("spark.executor.memoryOverhead", "1g") \
        .config("spark.driver.memoryOverhead", "1g") \
        \
        .config("spark.memory.fraction", "0.6") \
        .config("spark.memory.storageFraction", "0.3") \
        \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.default.parallelism", "200") \
        \
        .config("spark.sql.files.maxPartitionBytes", "134217728") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        \
        .config("spark.kubernetes.executor.deleteOnTermination", "true") \
        .config("spark.kubernetes.submission.waitAppCompletion", "false") \
        .config("spark.kubernetes.executor.podNamePrefix", app_name.lower().replace("_", "-"))

    spark = conf_builder.getOrCreate()
    
    print(f"✓ Spark session initialized for {app_name}")
    print(f"  - Executor cores: {app_config.get('executor_cores')}")
    print(f"  - Executor memory: {app_config.get('executor_memory')}")
    
    return spark
