"""
Entry point cho Bronze Streaming Job.
Chạy Kafka → rawdata (append-only) liên tục.

Usage:
    spark-submit bronze_main.py --app-name <name> [--topic-name <topic>] \\
        [--kafka-bootstrap-servers <servers>] [--checkpoint-dir <dir>]
"""

import argparse
from config.app_config import AppConfig
from utils.spark import create_spark_session
from core.bronze_job import KafkaToBronzeETL


def main():
    parser = argparse.ArgumentParser(description='Bronze Streaming Job: Kafka → rawdata (append-only)')
    parser.add_argument('--app-name', required=True,
                        help='Application name (from spark_app_config table)')
    parser.add_argument('--topic-name', required=False,
                        help='Specific topic name to process (optional, processes all if not specified)')
    parser.add_argument('--kafka-bootstrap-servers',
                        default='10.165.11.236:9093,10.165.11.237:9093,10.165.11.238:9093',
                        help='Kafka bootstrap servers')
    parser.add_argument('--checkpoint-dir',
                        default='s3a://iceberg/checkpoints/',
                        help='Base directory for checkpoints')

    args = parser.parse_args()

    print(f"[Bronze] Initializing streaming job for {args.app_name}...")

    # Load config
    config = AppConfig(args.app_name)
    try:
        config.load()
    except Exception as e:
        print(f"Error loading config: {e}")
        return

    # Filter topic nếu chỉ định
    topic_configs = config.topic_configs
    if args.topic_name:
        print(f"Filtering for specific topic: {args.topic_name}")
        topic_configs = [tc for tc in topic_configs if tc['topic_name'] == args.topic_name]
        if not topic_configs:
            print(f"Error: Topic '{args.topic_name}' not found for app '{args.app_name}'")
            print(f"Available topics: {[tc['topic_name'] for tc in config.topic_configs]}")
            return
        print(f"✓ Found topic configuration for '{args.topic_name}'")

    # Spark session
    spark = create_spark_session(args.app_name + "_bronze", config.app_config)

    # Run Bronze ETL
    etl = KafkaToBronzeETL(
        spark=spark,
        app_name=args.app_name,
        app_config=config.app_config,
        topic_configs=topic_configs,
        kafka_bootstrap_servers=args.kafka_bootstrap_servers,
        checkpoint_base_dir=args.checkpoint_dir
    )

    try:
        etl.run()
    except KeyboardInterrupt:
        print("[Bronze] Stopping streaming job...")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
