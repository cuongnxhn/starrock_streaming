# Deploy script để chạy từng topic như một Spark job riêng biệt (Windows PowerShell)
# Usage: .\deploy_per_topic.ps1 -AppName "my_app" -Topics "orders,payments,users"

param(
    [Parameter(Mandatory=$true)]
    [string]$AppName,
    
    [Parameter(Mandatory=$true)]
    [string]$Topics
)

# Spark submit configuration
$SparkMaster = "yarn"  # hoặc "local[*]" cho testing
$ExecutorMemory = "4g"
$ExecutorCores = "2"
$DriverMemory = "2g"

# Kafka config
$KafkaServers = "10.165.11.236:9093,10.165.11.237:9093,10.165.11.238:9093"
$CheckpointDir = "s3a://iceberg/checkpoints/"

# Split topics
$TopicArray = $Topics -split ','

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "Deploying Spark jobs per topic" -ForegroundColor Cyan
Write-Host "App: $AppName" -ForegroundColor Cyan
Write-Host "Topics: $($TopicArray -join ', ')" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""

foreach ($Topic in $TopicArray) {
    Write-Host "→ Submitting job for topic: $Topic" -ForegroundColor Yellow
    
    $JobName = "${AppName}_${Topic}"
    
    & spark-submit `
        --master $SparkMaster `
        --deploy-mode cluster `
        --name $JobName `
        --executor-memory $ExecutorMemory `
        --executor-cores $ExecutorCores `
        --driver-memory $DriverMemory `
        --conf spark.sql.adaptive.enabled=true `
        --conf spark.sql.adaptive.coalescePartitions.enabled=true `
        main.py `
        --app-name $AppName `
        --topic-name $Topic `
        --kafka-bootstrap-servers $KafkaServers `
        --checkpoint-dir $CheckpointDir
    
    Write-Host "✓ Submitted: $JobName" -ForegroundColor Green
    Write-Host ""
    
    # Sleep để tránh overwhelm cluster
    Start-Sleep -Seconds 5
}

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "✓ All jobs submitted successfully!" -ForegroundColor Green
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Check job status with:"
Write-Host "  yarn application -list" -ForegroundColor Gray
Write-Host ""
Write-Host "Check logs with:"
Write-Host "  yarn logs -applicationId <app_id>" -ForegroundColor Gray
