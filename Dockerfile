FROM spark:3.5.0-python3

# Switch to root to install packages
USER root

# Create directory for JDBC drivers if not exists
RUN mkdir -p /opt/spark/jars

# Copy JDBC drivers
COPY jars/mysql-*.jar /opt/spark/jars/
COPY jars/postgresql-42.7.8.jar /opt/spark/jars/

# Copy Kafka connector and dependencies
COPY jars/spark-sql-kafka-0-10_2.12-3.5.0.jar /opt/spark/jars/
COPY jars/kafka-clients-3.4.1.jar /opt/spark/jars/
COPY jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar /opt/spark/jars/
COPY jars/commons-pool2-2.11.1.jar /opt/spark/jars/

# Copy Iceberg runtime
COPY jars/iceberg-spark-runtime-3.5_2.12-1.10.1.jar /opt/spark/jars/

# Copy AWS S3 support JARs (required for s3a:// filesystem)
COPY jars/hadoop-aws-3.3.4.jar /opt/spark/jars/
COPY jars/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/

# Set working directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Set PYTHONPATH
ENV PYTHONPATH=/app:$PYTHONPATH
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Switch back to spark user for security
USER 185

# Spark will use this as entrypoint
# No need to set CMD, spark-submit will handle it
