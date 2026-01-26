#!/bin/bash

# Script để build và push Docker image với Oracle JDBC driver
# Usage: ./build_and_push.sh [tag]

set -e

# Configuration
REGISTRY="your-registry"  # Thay bằng registry của bạn (e.g., gcr.io/project-id, docker.io/username)
IMAGE_NAME="starrock-streaming"
TAG="${1:-latest}"
FULL_IMAGE="$REGISTRY/$IMAGE_NAME:$TAG"

echo "=========================================="
echo "🐳 Building Docker Image"
echo "=========================================="
echo "Image: $FULL_IMAGE"
echo ""

# Check if ojdbc8.jar exists
if [ ! -f "jars/ojdbc8.jar" ]; then
    echo "❌ ERROR: jars/ojdbc8.jar not found!"
    echo ""
    echo "Please download ojdbc8.jar from:"
    echo "https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html"
    echo ""
    echo "Then place it in jars/ojdbc8.jar"
    exit 1
fi

echo "✓ Found ojdbc8.jar"
echo ""

# Build image
echo "→ Building image..."
docker build -t $FULL_IMAGE .

if [ $? -eq 0 ]; then
    echo "✓ Build successful!"
    echo ""
    
    # Show image size
    echo "📦 Image details:"
    docker images $FULL_IMAGE
    echo ""
    
    # Ask to push
    read -p "Push to registry? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "→ Pushing to registry..."
        docker push $FULL_IMAGE
        echo "✓ Pushed successfully!"
        echo ""
        echo "🎉 Done! Update your deploy script with:"
        echo "   DOCKER_IMAGE=\"$FULL_IMAGE\""
    else
        echo "Skipped push."
    fi
else
    echo "❌ Build failed!"
    exit 1
fi

echo ""
echo "=========================================="
echo "Next steps:"
echo "1. Update deploy_k8s_per_topic.sh:"
echo "   DOCKER_IMAGE=\"$FULL_IMAGE\""
echo ""
echo "2. Deploy your app:"
echo "   ./deploy_k8s_per_topic.sh my_app topic1,topic2"
echo "=========================================="
