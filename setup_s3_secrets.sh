#!/bin/bash

# Script to create Kubernetes secret for S3 credentials
# Usage: ./setup_s3_secrets.sh

set -e

echo "=========================================="
echo "🔐 Setting up S3 Credentials Secret"
echo "=========================================="

# Configuration
NAMESPACE="default"
SECRET_NAME="s3-credentials"

# Configuration - Load from .env if exists
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/.env"

if [ -f "$ENV_FILE" ]; then
    echo "Loading configuration from .env file..."
    # Export all variables from .env
    export $(grep -v '^#' "$ENV_FILE" | xargs)
else
    echo "⚠ .env file not found, using default values"
fi

# Set defaults if not defined in .env
S3_ENDPOINT="${S3_ENDPOINT:-http://10.165.13.116:9000}"
S3_ACCESS_KEY="${S3_ACCESS_KEY:-minioadmin}"
S3_SECRET_KEY="${S3_SECRET_KEY:-minioadmin}"

echo ""
echo "Creating secret: $SECRET_NAME in namespace: $NAMESPACE"
echo ""

# Delete existing secret if exists (ignore errors)
kubectl delete secret $SECRET_NAME -n $NAMESPACE 2>/dev/null || true

# Create secret from literals
kubectl create secret generic $SECRET_NAME \
    --from-literal=access-key="$S3_ACCESS_KEY" \
    --from-literal=secret-key="$S3_SECRET_KEY" \
    -n $NAMESPACE

echo ""
echo "✓ Secret created successfully!"
echo ""
echo "Verify with:"
echo "  kubectl get secret $SECRET_NAME -n $NAMESPACE"
echo "  kubectl describe secret $SECRET_NAME -n $NAMESPACE"
echo ""
