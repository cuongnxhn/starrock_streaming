# 🚀 Quick Build & Deploy Checklist

## ✅ Completed (Already Done)

- [x] Downloaded all required JARs to `jars/` directory
- [x] Updated `Dockerfile` to COPY JARs into `/opt/spark/jars/`
- [x] Removed `--packages` flag from `deploy_k8s_per_topic.sh`
- [x] Removed `--packages` flag from `deploy_k8s_per_topic_kubectl.sh`
- [x] Removed `spark.jars.packages` config from `utils/spark.py`

---

## 📋 TODO: Build and Deploy

### 1. Build Docker Image
```bash
docker build -t 10.165.13.117:5000/starrock-streaming:latest .
```

Expected time: 5-10 minutes

---

### 2. Verify Image Contains JARs
```bash
docker run --rm 10.165.13.117:5000/starrock-streaming:latest ls -lh /opt/spark/jars/ | grep -E "(kafka|iceberg)"
```

You should see:
- ✅ spark-sql-kafka-0-10_2.12-3.5.0.jar
- ✅ kafka-clients-3.4.1.jar
- ✅ spark-token-provider-kafka-0-10_2.12-3.5.0.jar
- ✅ commons-pool2-2.11.1.jar
- ✅ iceberg-spark-runtime-3.5_2.12-1.10.1.jar

---

### 3. Push to Registry
```bash
docker push 10.165.13.117:5000/starrock-streaming:latest
```

Expected time: 10-20 minutes (first time)

---

### 4. Deploy Test Job
```bash
# Deploy a test job with one topic
./deploy_k8s_per_topic.sh test_app your_topic_name
```

---

### 5. Verify Deployment
```bash
# Check driver pod status
kubectl get pods -l app=test_app

# View logs
kubectl logs -f <driver-pod-name>
```

Look for:
- ✅ No "Resolving dependencies..." messages (faster startup!)
- ✅ No "Downloaded from central..." messages
- ✅ "Đọc được X messages từ Kafka" (Kafka works!)
- ✅ Startup time ~20-30 seconds (not 60-90s)

---

### 6. Deploy Production Jobs
```bash
# Once test succeeds, deploy all topics
./deploy_k8s_per_topic.sh production_app topic1,topic2,topic3
```

---

## 🎯 Success Criteria

Your deployment is successful when you see:

1. ✅ **Fast startup**: ~20-30 seconds (vs 60-90s before)
2. ✅ **No downloads**: No Maven/JAR download messages in logs
3. ✅ **Kafka works**: Successfully reading messages from Kafka
4. ✅ **No errors**: No "Failed to find data source: kafka" errors

---

## 📚 Reference Documents

- `PRODUCTION_DEPLOYMENT_GUIDE.md` - Full deployment guide
- `KAFKA_DEPENDENCY_ANALYSIS.md` - Why JARs are required
- `KAFKA_JARS_FIX.md` - Technical details

---

## 🆘 Quick Troubleshooting

### If build fails:
```bash
# Check if all JARs exist
ls -lh jars/*.jar
```

### If "COPY failed: file not found":
```bash
# Re-download missing JAR
# See PRODUCTION_DEPLOYMENT_GUIDE.md for download commands
```

### If still getting Kafka error after deploy:
```bash
# Force pull latest image
kubectl delete pod <driver-pod-name>

# Or add imagePullPolicy: Always to deployment
```

---

## 🎉 You're Ready!

All code changes are complete. Just need to:
1. Build Docker image
2. Push to registry
3. Deploy!

Good luck! 🚀
