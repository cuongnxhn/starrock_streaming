#!/bin/bash

# Script để tạo Kubernetes Secret từ file .env
# Usage: ./setup_k8s_secret.sh

set -e

NAMESPACE="default"
SECRET_NAME="app-env-secret"
ENV_FILE=".env"

echo "=========================================="
echo "🔐 Creating Kubernetes Secret from .env"
echo "=========================================="
echo ""

# Kiểm tra .env file có tồn tại không
if [ ! -f "$ENV_FILE" ]; then
    echo "❌ Error: .env file not found!"
    echo "Please create .env file first"
    exit 1
fi

# Xóa secret cũ nếu tồn tại (để update)
echo "→ Removing old secret if exists..."
kubectl delete secret $SECRET_NAME -n $NAMESPACE 2>/dev/null || true

# Tạo secret mới từ .env file
echo "→ Creating new secret from .env file..."
kubectl create secret generic $SECRET_NAME \
    --from-env-file=$ENV_FILE \
    -n $NAMESPACE

echo ""
echo "✅ Secret created successfully!"
echo ""
echo "📋 Verify secret:"
echo "  kubectl get secret $SECRET_NAME -n $NAMESPACE"
echo ""
echo "View secret keys (not values):"
echo "  kubectl describe secret $SECRET_NAME -n $NAMESPACE"
echo ""
echo "🚀 Now run your deployment script:"
echo "  ./deploy_k8s_per_topic_kubectl.sh <app_name> <topics>"
echo ""
