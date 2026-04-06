"""
Entry point cho Silver Streaming Job.
Chạy liên tục (long-running) trên K8s Deployment:
  rawdata (Bronze) → warehouse (Silver)
Trigger: ProcessingTime(10 min) — mỗi 10 phút xử lý 1 micro-batch.

Usage:
    spark-submit silver_main.py --app-name <name> [--topic-name <topic>]
"""

import argparse
from config.app_config import AppConfig
from utils.spark import create_spark_session
from core.silver_job import BronzeToSilverETL


def main():
    parser = argparse.ArgumentParser(
        description='Silver Batch Job: rawdata (Bronze) → warehouse (Silver)'
    )
    parser.add_argument('--app-name', required=True,
                        help='Application name (from spark_app_config table)')
    parser.add_argument('--topic-name', required=False,
                        help='Specific topic name to process (optional, processes all if not specified)')
    parser.add_argument('--lookback-minutes', type=int, default=15,
                        help='Lookback window in minutes to read from Bronze (default: 15)')

    args = parser.parse_args()

    print(f"[Silver] Initializing streaming job for {args.app_name}...")
    print(f"[Silver] Mode: ProcessingTime(10 min) — long-running")

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

    # Spark session — không cần Spark Streaming config
    spark = create_spark_session(args.app_name + "_silver", config.app_config)

    # Run Silver ETL
    etl = BronzeToSilverETL(
        spark=spark,
        app_name=args.app_name,
        app_config=config.app_config,
        topic_configs=topic_configs,
        lookback_minutes=args.lookback_minutes
    )

    try:
        etl.run()
    except Exception as e:
        print(f"[Silver] Job failed: {e}")
        raise
    finally:
        spark.stop()
        print("[Silver] Spark session stopped.")


if __name__ == "__main__":
    main()
