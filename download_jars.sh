#!/bin/bash

# Script to download required Spark JARs for Kafka and Iceberg
# Run this script to prepare JARs for Docker image (Solution 2)

set -e

echo "=========================================="
echo "📦 Downloading Spark JARs"
echo "=========================================="
echo ""

# Create jars directory
echo "→ Creating jars directory..."
mkdir -p jars

# Kafka Connector
echo "→ Downloading Kafka SQL connector..."
wget -q --show-progress -O jars/spark-sql-kafka-0-10_2.12-3.5.0.jar \
  https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar

# Kafka Clients
echo "→ Downloading Kafka clients..."
wget -q --show-progress -O jars/kafka-clients-3.4.1.jar \
  https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar

# Spark Token Provider for Kafka
echo "→ Downloading Spark Token Provider for Kafka..."
wget -q --show-progress -O jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
  https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar

# Commons Pool2
echo "→ Downloading Commons Pool2..."
wget -q --show-progress -O jars/commons-pool2-2.11.1.jar \
  https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# Iceberg Spark Runtime
echo "→ Downloading Iceberg Spark Runtime..."
wget -q --show-progress -O jars/iceberg-spark-runtime-3.5_2.12-1.10.1.jar \
  https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.1/iceberg-spark-runtime-3.5_2.12-1.10.1.jar

# Hadoop AWS (for S3 support)
echo "→ Downloading Hadoop AWS..."
wget -q --show-progress -O jars/hadoop-aws-3.3.4.jar \
  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# AWS Java SDK Bundle (for S3 support)
echo "→ Downloading AWS Java SDK Bundle..."
wget -q --show-progress -O jars/aws-java-sdk-bundle-1.12.262.jar \
  https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

echo ""
echo "=========================================="
echo "✓ All JARs downloaded successfully!"
echo "=========================================="
echo ""
echo "Downloaded files:"
ls -lh jars/*.jar | tail -n +2 | awk '{printf "  - %-50s %s\n", $9, $5}'
echo ""
echo "Next steps:"
echo "  1. Update Dockerfile to copy these JARs to /opt/spark/jars/"
echo "  2. Rebuild Docker image: docker build -t 10.165.13.117:5000/starrock-streaming:latest ."
echo "  3. Push to registry: docker push 10.165.13.117:5000/starrock-streaming:latest"
echo "  4. Remove --packages flag from deploy_k8s_per_topic.sh"
echo ""
echo "See KAFKA_JARS_FIX.md for detailed instructions."
echo ""
