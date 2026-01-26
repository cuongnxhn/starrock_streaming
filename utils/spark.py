from pyspark.sql import SparkSession
import os

def create_spark_session(app_name, app_config):
    """Khởi tạo Spark session với config từ metadata"""
    conf_builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.starrocks_catalog",
                "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.starrocks_catalog.type", "rest") \
        .config("spark.sql.catalog.starrocks_catalog.uri",
                os.getenv("STARROCK_FE", "http://10.165.13.116:8181")) \
        .config("spark.sql.catalog.starrocks_catalog.warehouse",
                os.getenv("ICEBERG_WAREHOUSE", "s3a://iceberg/warehouse")) \
        .config("spark.sql.catalog.starrocks_catalog.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.0")

    # Apply dynamic config
    # app_config is expected to be a dict-like object (or the AppConfig class instance if it had __getitem__)
    # Here assuming app_config is the dict loaded from database
    
    conf_builder = conf_builder \
        .config("spark.executor.cores", app_config.get('executor_cores')) \
        .config("spark.executor.memory", app_config.get('executor_memory')) \
        .config("spark.executor.instances", app_config.get('executor_instances')) \
        .config("spark.driver.memory", app_config.get('driver_memory')) \
        .config("spark.driver.cores", app_config.get('driver_cores')) \
        .config("spark.scheduler.mode", app_config.get('scheduler_mode'))


#     if app_config.get('scheduler_mode') == 'FAIR':
#         conf_builder = conf_builder \
#             .config("spark.scheduler.allocation.file",
#                     "/opt/spark/conf/fairscheduler.xml")

    spark = conf_builder.getOrCreate()
    
    print(f"✓ Spark session initialized for {app_name}")
    print(f"  - Executor cores: {app_config.get('executor_cores')}")
    print(f"  - Executor memory: {app_config.get('executor_memory')}")
    
    return spark
