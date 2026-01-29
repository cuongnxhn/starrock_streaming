#!/bin/bash

# Script để setup Kafka authentication secrets trong Kubernetes
# Usage: ./setup_kafka_secrets.sh [namespace]

set -e

NAMESPACE=${1:-default}

echo "==========================================="
echo "🔐 Setting up Kafka Authentication Secrets"
echo "Namespace: $NAMESPACE"
echo "==========================================="
echo ""

# Check if client.properties exists
if [ ! -f "client.properties" ]; then
    echo "❌ ERROR: client.properties not found!"
    echo ""
    echo "Please create client.properties with your Kafka authentication config."
    echo "Example:"
    echo ""
    echo "  security.protocol=SASL_SSL"
    echo "  sasl.mechanism=PLAIN"
    echo "  sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"pass\";"
    echo "  ssl.truststore.location=/etc/kafka/ssl/kafka.client.truststore.jks"
    echo "  ssl.truststore.password=password"
    echo ""
    echo "See client.properties.example for full example."
    exit 1
fi

# Check if namespace exists
if ! kubectl get namespace "$NAMESPACE" &>/dev/null; then
    echo "⚠  Namespace '$NAMESPACE' does not exist. Creating..."
    kubectl create namespace "$NAMESPACE"
    echo "✓ Created namespace: $NAMESPACE"
fi

echo "→ Creating Kafka client config secret..."

# Delete existing secret if exists
if kubectl get secret kafka-client-config -n "$NAMESPACE" &>/dev/null; then
    echo "  ⚠ Secret 'kafka-client-config' already exists. Deleting..."
    kubectl delete secret kafka-client-config -n "$NAMESPACE"
fi

# Create secret from client.properties
kubectl create secret generic kafka-client-config \
    --from-file=client.properties=./client.properties \
    --namespace="$NAMESPACE"

echo "✓ Created secret: kafka-client-config"

# Check for SSL certificates
if [ -f "kafka.client.truststore.jks" ] || [ -f "kafka.client.keystore.jks" ]; then
    echo ""
    echo "→ Creating Kafka SSL certificates secret..."
    
    # Delete existing secret if exists
    if kubectl get secret kafka-ssl-certs -n "$NAMESPACE" &>/dev/null; then
        echo "  ⚠ Secret 'kafka-ssl-certs' already exists. Deleting..."
        kubectl delete secret kafka-ssl-certs -n "$NAMESPACE"
    fi
    
    # Build the create command
    CREATE_CMD="kubectl create secret generic kafka-ssl-certs"
    
    if [ -f "kafka.client.truststore.jks" ]; then
        CREATE_CMD="$CREATE_CMD --from-file=kafka.client.truststore.jks=./kafka.client.truststore.jks"
        echo "  → Adding truststore"
    fi
    
    if [ -f "kafka.client.keystore.jks" ]; then
        CREATE_CMD="$CREATE_CMD --from-file=kafka.client.keystore.jks=./kafka.client.keystore.jks"
        echo "  → Adding keystore"
    fi
    
    CREATE_CMD="$CREATE_CMD --namespace=$NAMESPACE"
    
    # Execute
    eval $CREATE_CMD
    
    echo "✓ Created secret: kafka-ssl-certs"
else
    echo ""
    echo "⚠  SSL certificate files not found (kafka.client.truststore.jks, kafka.client.keystore.jks)"
    echo "   Creating empty kafka-ssl-certs secret for compatibility..."
    
    # Delete existing secret if exists
    if kubectl get secret kafka-ssl-certs -n "$NAMESPACE" &>/dev/null; then
        kubectl delete secret kafka-ssl-certs -n "$NAMESPACE"
    fi
    
    # Create empty secret
    kubectl create secret generic kafka-ssl-certs \
        --from-literal=.keep="" \
        --namespace="$NAMESPACE"
    
    echo "✓ Created empty secret: kafka-ssl-certs"
    echo "   (You can add SSL certs later if needed)"
fi

echo ""
echo "==========================================="
echo "✅ Kafka secrets setup complete!"
echo "==========================================="
echo ""
echo "Verify secrets:"
echo "  kubectl get secrets -n $NAMESPACE | grep kafka"
echo ""
echo "View secret details:"
echo "  kubectl describe secret kafka-client-config -n $NAMESPACE"
echo "  kubectl describe secret kafka-ssl-certs -n $NAMESPACE"
echo ""
echo "Decode and view client.properties:"
echo "  kubectl get secret kafka-client-config -n $NAMESPACE -o jsonpath='{.data.client\.properties}' | base64 -d"
echo ""
echo "Now you can deploy your Spark jobs with:"
echo "  ./deploy_k8s_per_topic.sh <app_name> <topics>"
echo "  or"
echo "  ./deploy_k8s_per_topic_kubectl.sh <app_name> <topics>"
echo ""
