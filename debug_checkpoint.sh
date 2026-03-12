#!/bin/bash

# Script để debug checkpoint location issues
# Usage: ./debug_checkpoint.sh <app_name> [topic_name]

APP_NAME=$1
TOPIC_NAME=$2

if [ -z "$APP_NAME" ]; then
    echo "Usage: $0 <app_name> [topic_name]"
    echo "Example: $0 my_app CDC_LOG_ONEBSS_CSS_HD_BANCHEO"
    exit 1
fi

echo "🔍 Debugging Checkpoint for $APP_NAME"
echo "========================================"

# 1. Check S3 secret
echo -e "\n1️⃣ Checking S3 credentials secret..."
if kubectl get secret app-env-secret -n default &>/dev/null; then
    echo "   ✅ Secret exists"
    kubectl describe secret app-env-secret -n default | grep "Data"
else
    echo "   ❌ Secret NOT found! Run: ./setup_k8s_secret.sh"
fi

# 2. Check pod templates
echo -e "\n2️⃣ Checking pod templates..."
if grep -q "S3_ACCESS_KEY" k8s/spark-driver-pod-template.yaml; then
    echo "   ✅ Driver template has S3 credentials"
else
    echo "   ❌ Driver template MISSING S3 credentials"
fi

if grep -q "S3_ACCESS_KEY" k8s/spark-executor-pod-template.yaml; then
    echo "   ✅ Executor template has S3 credentials"
else
    echo "   ❌ Executor template MISSING S3 credentials"
fi

# 3. Check deployment script uses pod templates
echo -e "\n3️⃣ Checking deployment configuration..."
if grep -q "podTemplateFile" deploy_k8s_per_topic_kubectl.sh; then
    echo "   ✅ Deployment script uses pod templates"
    grep "podTemplateFile" deploy_k8s_per_topic_kubectl.sh | head -2
else
    echo "   ❌ Deployment script NOT using pod templates"
fi

# 4. Check running pods
echo -e "\n4️⃣ Checking running pods..."
PODS=$(kubectl get pods -l app=$APP_NAME -o name 2>/dev/null)
if [ -z "$PODS" ]; then
    echo "   ⚠️  No pods found for app: $APP_NAME"
else
    for POD in $PODS; do
        POD_NAME=$(basename $POD)
        echo "   📦 Pod: $POD_NAME"
        
        # Check pod status
        POD_STATUS=$(kubectl get pod $POD_NAME -o jsonpath='{.status.phase}' 2>/dev/null)
        echo "      Status: $POD_STATUS"
        
        if [ "$POD_STATUS" = "Running" ]; then
            # Check env vars
            HAS_S3_KEY=$(kubectl exec $POD_NAME -- printenv S3_ACCESS_KEY 2>/dev/null)
            if [ -n "$HAS_S3_KEY" ]; then
                echo "      ✅ Has S3_ACCESS_KEY: ${HAS_S3_KEY:0:5}***"
            else
                echo "      ❌ MISSING S3_ACCESS_KEY env variable"
            fi
            
            HAS_S3_SECRET=$(kubectl exec $POD_NAME -- printenv S3_SECRET_KEY 2>/dev/null)
            if [ -n "$HAS_S3_SECRET" ]; then
                echo "      ✅ Has S3_SECRET_KEY"
            else
                echo "      ❌ MISSING S3_SECRET_KEY env variable"
            fi
            
            HAS_S3_ENDPOINT=$(kubectl exec $POD_NAME -- printenv S3_ENDPOINT 2>/dev/null)
            if [ -n "$HAS_S3_ENDPOINT" ]; then
                echo "      ✅ S3_ENDPOINT: $HAS_S3_ENDPOINT"
            else
                echo "      ❌ MISSING S3_ENDPOINT env variable"
            fi
            
            # Check local checkpoint existence
            echo "      🔍 Checking for local checkpoints in pod..."
            LOCAL_CHECKPOINTS=$(kubectl exec $POD_NAME -- find /tmp -name "*checkpoint*" -type d 2>/dev/null | head -5)
            if [ -n "$LOCAL_CHECKPOINTS" ]; then
                echo "      ⚠️  Found LOCAL checkpoints (should be in S3 instead):"
                echo "$LOCAL_CHECKPOINTS" | sed 's/^/         /'
            else
                echo "      ✅ No local checkpoints found"
            fi
            
            # Check checkpoint location in logs
            echo "      🔍 Checking checkpoint location in logs..."
            CHECKPOINT_LOC=$(kubectl logs $POD_NAME 2>/dev/null | grep -i "checkpoint" | grep -i "location\|path" | head -3)
            if [ -n "$CHECKPOINT_LOC" ]; then
                echo "$CHECKPOINT_LOC" | sed 's/^/         /'
            else
                echo "      ℹ️  No checkpoint location found in logs yet"
            fi
        fi
    done
fi

# 5. Check S3 checkpoints
echo -e "\n5️⃣ Checking S3 checkpoints..."
if command -v mc &>/dev/null; then
    # Configure mc alias from .env
    if [ -f .env ]; then
        source .env
        mc alias set minio $S3_ENDPOINT $S3_ACCESS_KEY $S3_SECRET_KEY 2>/dev/null || true
    fi
    
    echo "   🔍 Listing S3 checkpoints..."
    if [ -n "$TOPIC_NAME" ]; then
        if mc ls minio/iceberg/checkpoints/$APP_NAME/$TOPIC_NAME/ 2>/dev/null; then
            echo "   ✅ Found checkpoint in S3 for topic: $TOPIC_NAME"
            mc tree minio/iceberg/checkpoints/$APP_NAME/$TOPIC_NAME/ 2>/dev/null || true
        else
            echo "   ⚠️  No checkpoint found in S3 for topic: $TOPIC_NAME"
        fi
    else
        if mc ls minio/iceberg/checkpoints/$APP_NAME/ 2>/dev/null; then
            echo "   ✅ Found checkpoints in S3:"
            mc ls minio/iceberg/checkpoints/$APP_NAME/ 2>/dev/null
        else
            echo "   ⚠️  No checkpoints found in S3 for app: $APP_NAME"
        fi
    fi
else
    echo "   ⚠️  MinIO client (mc) not installed, skipping S3 check"
    echo "   Install: https://min.io/docs/minio/linux/reference/minio-mc.html"
fi

# 6. Summary and recommendations
echo -e "\n========================================"
echo "📊 Summary & Recommendations"
echo "========================================"

# Check if all prerequisites are met
SECRET_OK=$(kubectl get secret app-env-secret -n default &>/dev/null && echo "true" || echo "false")
TEMPLATE_OK=$(grep -q "S3_ACCESS_KEY" k8s/spark-driver-pod-template.yaml && echo "true" || echo "false")

if [ "$SECRET_OK" = "false" ]; then
    echo "❌ Action Required: Create app-env-secret secret"
    echo "   Run: ./setup_k8s_secret.sh"
fi

if [ "$TEMPLATE_OK" = "false" ]; then
    echo "❌ Action Required: Pod templates missing S3 credentials"
    echo "   This should have been fixed. Check k8s/spark-*-pod-template.yaml"
fi

if [ "$SECRET_OK" = "true" ] && [ "$TEMPLATE_OK" = "true" ]; then
    if [ -n "$PODS" ]; then
        echo "✅ Prerequisites met. Check pod env vars above."
        echo ""
        echo "If pods don't have S3 env vars, redeploy:"
        echo "   kubectl delete deployment -l app=$APP_NAME"
        echo "   ./deploy_k8s_per_topic_kubectl.sh $APP_NAME <topic_name>"
    else
        echo "✅ Prerequisites met. Ready to deploy."
        echo ""
        echo "Deploy your app:"
        echo "   ./deploy_k8s_per_topic_kubectl.sh $APP_NAME <topic_name>"
    fi
fi

echo ""
echo "📖 For detailed troubleshooting, see: CHECKPOINT_TROUBLESHOOTING.md"
