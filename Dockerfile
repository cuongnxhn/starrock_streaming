FROM apache/spark:3.5.0-python3

# Switch to root to install packages
USER root

# Create directory for JDBC drivers if not exists
RUN mkdir -p /opt/spark/jars


COPY jars/ojdbc17.jar /opt/spark/jars/
COPY jars/mysql-*.jar /opt/spark/jars/

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
