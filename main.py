import argparse
from config.app_config import AppConfig
from utils.spark import create_spark_session
from core.etl_job import KafkaToIcebergETL

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

    print(f"Initializing job for {args.app_name}...")

    # 1. Load Configuration
    config = AppConfig(args.app_name)
    try:
        config.load()
    except Exception as e:
        print(f"Error loading config: {e}")
        # In a real environment we might fail, but for now we proceed or exit? 
        # config.load raises ValueError if app not found, we should probably exit
        return

    # 2. Initialize Spark Session
    # config.app_config is a dict
    spark = create_spark_session(args.app_name, config.app_config)

    # 3. Initialize and Run ETL Job
    etl = KafkaToIcebergETL(
        spark=spark,
        app_name=args.app_name,
        app_config=config.app_config,
        topic_configs=config.topic_configs,
        kafka_bootstrap_servers=args.kafka_bootstrap_servers,
        checkpoint_base_dir=args.checkpoint_dir
    )

    try:
        etl.run()
    except KeyboardInterrupt:
        print("Stopping ETL job...")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
