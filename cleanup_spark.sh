#!/bin/bash

# Quick cleanup script for Spark resources
# Usage: ./cleanup_spark.sh [app_name] [namespace]

APP_NAME=${1:-""}
NAMESPACE=${2:-"default"}

echo "🧹 Cleaning up Spark resources..."
echo "Namespace: $NAMESPACE"
if [ -n "$APP_NAME" ]; then
    echo "App: $APP_NAME"
fi
echo ""

# Delete ConfigMaps
echo "→ Deleting Spark ConfigMaps..."
if [ -n "$APP_NAME" ]; then
    kubectl get configmaps -n $NAMESPACE -o name | grep "spark-exec" | grep "$APP_NAME" | xargs -r kubectl delete -n $NAMESPACE 2>/dev/null || true
else
    kubectl delete configmaps -l spark-role=executor -n $NAMESPACE --ignore-not-found=true 2>/dev/null || true
    kubectl get configmaps -n $NAMESPACE -o name | grep "spark-exec" | xargs -r kubectl delete -n $NAMESPACE 2>/dev/null || true
fi

# Delete failed pods
echo "→ Deleting failed/completed pods..."
if [ -n "$APP_NAME" ]; then
    kubectl delete pods --field-selector=status.phase=Failed -n $NAMESPACE -l app=$APP_NAME --ignore-not-found=true 2>/dev/null || true
    kubectl delete pods --field-selector=status.phase=Succeeded -n $NAMESPACE -l app=$APP_NAME --ignore-not-found=true 2>/dev/null || true
else
    kubectl delete pods --field-selector=status.phase=Failed -n $NAMESPACE --ignore-not-found=true 2>/dev/null || true
    kubectl delete pods --field-selector=status.phase=Succeeded -n $NAMESPACE --ignore-not-found=true 2>/dev/null || true
fi

# Restart existing deployments (để apply config mới)
if [ -n "$APP_NAME" ]; then
    echo "→ Restarting deployments for app: $APP_NAME..."
    kubectl rollout restart deployment -l app=$APP_NAME -n $NAMESPACE 2>/dev/null || true
fi

echo ""
echo "✅ Cleanup completed!"
echo ""
echo "Remaining Spark resources:"
if [ -n "$APP_NAME" ]; then
    kubectl get pods,configmaps -n $NAMESPACE -l app=$APP_NAME 2>/dev/null | grep -E "(spark|$APP_NAME)" || echo "  None"
else
    kubectl get pods,configmaps -n $NAMESPACE 2>/dev/null | grep spark || echo "  None"
fi
