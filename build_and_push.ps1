# Script để build và push Docker image với Oracle JDBC driver
# Usage: 
#   .\build_and_push.ps1 [tag]              # Build với registry prefix (cho push)
#   .\build_and_push.ps1 -LocalOnly [tag]   # Build local only (không cần registry)

param(
    [string]$Tag = "latest",
    [switch]$LocalOnly = $false
)

$ErrorActionPreference = "Stop"

# Configuration
$REGISTRY = "your-registry"  # Thay bằng registry của bạn (e.g., gcr.io/project-id, docker.io/username)
$IMAGE_NAME = "starrock-streaming"

# Nếu build local only, không thêm registry prefix
if ($LocalOnly) {
    $FULL_IMAGE = "${IMAGE_NAME}:$Tag"
} else {
    $FULL_IMAGE = "$REGISTRY/${IMAGE_NAME}:$Tag"
}

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "🐳 Building Docker Image" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "Image: $FULL_IMAGE" -ForegroundColor Yellow
Write-Host ""

# Check if ojdbc8.jar exists
if (-not (Test-Path "jars\ojdbc8.jar")) {
    Write-Host "❌ ERROR: jars\ojdbc8.jar not found!" -ForegroundColor Red
    Write-Host ""
    Write-Host "Please download ojdbc8.jar from:"
    Write-Host "https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html"
    Write-Host ""
    Write-Host "Then place it in jars\ojdbc8.jar"
    exit 1
}

Write-Host "✓ Found ojdbc8.jar" -ForegroundColor Green
Write-Host ""

# Build image
Write-Host "→ Building image..." -ForegroundColor Yellow
docker build -t $FULL_IMAGE .

if ($LASTEXITCODE -eq 0) {
    Write-Host "✓ Build successful!" -ForegroundColor Green
    Write-Host ""
    
    # Show image size
    Write-Host "📦 Image details:" -ForegroundColor Cyan
    docker images $FULL_IMAGE
    Write-Host ""
    
    # Nếu build local only, skip push
    if ($LocalOnly) {
        Write-Host "✓ Local image ready to use!" -ForegroundColor Green
        Write-Host "   Image name: $FULL_IMAGE" -ForegroundColor Yellow
    } else {
        # Ask to push
        $push = Read-Host "Push to registry? (y/N)"
        if ($push -eq "y" -or $push -eq "Y") {
            Write-Host "→ Pushing to registry..." -ForegroundColor Yellow
            docker push $FULL_IMAGE
            Write-Host "✓ Pushed successfully!" -ForegroundColor Green
            Write-Host ""
            Write-Host "🎉 Done! Update your deploy script with:" -ForegroundColor Green
            Write-Host "   DOCKER_IMAGE=`"$FULL_IMAGE`""
        } else {
            Write-Host "Skipped push." -ForegroundColor Yellow
        }
    }
} else {
    Write-Host "❌ Build failed!" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "Next steps:"
Write-Host "1. Update deploy_k8s_per_topic.sh:"
Write-Host "   DOCKER_IMAGE=`"$FULL_IMAGE`""
Write-Host ""
Write-Host "2. Deploy your app:"
Write-Host "   ./deploy_k8s_per_topic.sh my_app topic1,topic2"
Write-Host "==========================================" -ForegroundColor Cyan
