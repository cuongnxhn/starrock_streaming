# 📚 Oracle JDBC trên Kubernetes - Hướng dẫn đầy đủ

## 🎯 Vấn đề

Khi deploy Spark job lên Kubernetes, làm sao để worker pods biết Oracle JDBC driver (`oracle.jdbc.driver.OracleDriver`) ở đâu?

## ✅ Giải pháp

**Đóng gói JDBC driver vào Docker Image** - Đơn giản, tự động, và là cách DUY NHẤT khả thi trong K8s!

---

## 📖 Tài liệu (Đọc theo thứ tự)

### 🚀 Bắt đầu nhanh
1. **[QUICKSTART_JDBC_K8S.md](QUICKSTART_JDBC_K8S.md)** ⭐ BẮT ĐẦU TỪ ĐÂY
   - Quick start guide với sơ đồ trực quan
   - 3 bước deploy đơn giản
   - Troubleshooting phổ biến

### ❓ Câu hỏi thường gặp
2. **[FAQ_JDBC_K8S.md](FAQ_JDBC_K8S.md)** ⭐ QUAN TRỌNG
   - "Cách này có phức tạp không?"
   - "Tại sao không upload lên worker nodes?"
   - "Muốn dùng MySQL driver thì sao?"
   - So sánh trực quan giữa các cách

### 📝 Chi tiết kỹ thuật
3. **[SUMMARY_JDBC_K8S.md](SUMMARY_JDBC_K8S.md)**
   - Tổng kết toàn bộ giải pháp
   - Files đã tạo/sửa
   - Architecture overview
   - Verify setup

4. **[WHY_NOT_MANUAL_UPLOAD.md](WHY_NOT_MANUAL_UPLOAD.md)**
   - Tại sao upload thủ công KHÔNG khả thi
   - Container isolation explained
   - Dynamic workers problem
   - So sánh chi tiết

5. **[EXAMPLE_MYSQL_DRIVER.md](EXAMPLE_MYSQL_DRIVER.md)**
   - Ví dụ thêm MySQL driver (cực kỳ đơn giản!)
   - Multi-database support
   - Database factory pattern
   - Download links for popular drivers

6. **[docs/JDBC_DRIVER_K8S.md](docs/JDBC_DRIVER_K8S.md)**
   - Hướng dẫn chi tiết step-by-step
   - So sánh 3 giải pháp (Image, S3, Maven)
   - Best practices
   - Production checklist

7. **[docs/TECHNICAL_JDBC_K8S.md](docs/TECHNICAL_JDBC_K8S.md)**
   - Deep-dive kỹ thuật
   - Spark classpath resolution
   - JVM startup sequence
   - Build vs Runtime phases

### 🛠️ Setup
8. **[jars/README.md](jars/README.md)**
   - Hướng dẫn download Oracle JDBC driver
   - Link download từ Oracle
   - Verify installation

---

## ⚡ Quick Start (TL;DR)

### 3 bước đơn giản:

```powershell
# 1. Download ojdbc8.jar → đặt vào jars/

# 2. Build Docker image
.\build_and_push.ps1 v1.0

# 3. Deploy
./deploy_k8s_per_topic.sh my_app orders,payments
```

**✅ XONG!** Tất cả worker pods giờ có Oracle driver!

---

## 💡 Key Points

### ✅ Nên làm
- **Đóng gói JAR vào Docker image** - Cách tốt nhất
- **Sử dụng `/opt/spark/jars/`** - Spark tự động load
- **Git ignore `jars/*.jar`** - Licensed files
- **Tag image với version** - Không dùng `latest`

### ❌ Không nên
- **Upload JAR lên worker nodes** - Không khả thi
- **Hardcode credentials** - Dùng environment variables
- **Share 1 JAR cho nhiều apps** - Mỗi app nên có image riêng

---

## 🎯 Tại sao Docker Image là tốt nhất?

| Tiêu chí | Docker Image | Upload thủ công |
|----------|-------------|-----------------|
| **Complexity** | ⭐ 1 dòng code | ⭐⭐⭐⭐⭐ SSH vào N nodes |
| **Speed** | ⚡ 5 phút | 🐌 1 giờ+ |
| **Scalability** | ♾️ Auto scale | 😱 Manual per node |
| **Feasibility** | ✅ Khả thi | ❌ KHÔNG khả thi |
| **Maintenance** | 😊 Easy | 😫 Nightmare |

---

## 📊 Architecture

```
Docker Image (1 lần build)
    ↓
Registry (docker.io, gcr.io)
    ↓
K8s pulls image → All worker nodes
    ↓
Pods created from image
    ↓
All pods have JAR files ✓
```

---

## 🔧 Files trong project

### Modified
- `Dockerfile` - Thêm logic copy JDBC drivers

### New Scripts
- `build_and_push.sh` - Build script (Linux/Mac)
- `build_and_push.ps1` - Build script (Windows)

### New Docs
- `QUICKSTART_JDBC_K8S.md` - Quick start guide
- `FAQ_JDBC_K8S.md` - Câu hỏi thường gặp
- `SUMMARY_JDBC_K8S.md` - Tổng kết
- `WHY_NOT_MANUAL_UPLOAD.md` - Giải thích tại sao không manual
- `EXAMPLE_MYSQL_DRIVER.md` - Ví dụ MySQL
- `docs/JDBC_DRIVER_K8S.md` - Chi tiết
- `docs/TECHNICAL_JDBC_K8S.md` - Deep-dive

### Config
- `.gitignore` - Exclude JAR files
- `jars/README.md` - Download guide

---

## 🚀 Next Steps

1. **Đọc Quick Start**: [QUICKSTART_JDBC_K8S.md](QUICKSTART_JDBC_K8S.md)
2. **Có thắc mắc?** Đọc [FAQ_JDBC_K8S.md](FAQ_JDBC_K8S.md)
3. **Download Oracle JAR**: Xem [jars/README.md](jars/README.md)
4. **Build & Deploy**: Follow quick start guide

---

## 💬 Common Questions

### Q: "Cách này có phức tạp không?"
**A**: KHÔNG! Đây là cách **ĐƠN GIẢN NHẤT** cho K8s. Chỉ cần thêm 1 dòng vào Dockerfile.

### Q: "Tôi muốn dùng MySQL driver thì sao?"
**A**: Thêm 1 dòng nữa vào Dockerfile! Xem [EXAMPLE_MYSQL_DRIVER.md](EXAMPLE_MYSQL_DRIVER.md)

### Q: "Tại sao không upload lên worker nodes?"
**A**: KHÔNG khả thi vì container isolation. Xem [WHY_NOT_MANUAL_UPLOAD.md](WHY_NOT_MANUAL_UPLOAD.md)

### Q: "Có cách nào khác không?"
**A**: Có (S3 + --jars) nhưng phức tạp hơn. Docker image là simplest & best.

---

## 📞 Need Help?

- **Quick answer** → [FAQ_JDBC_K8S.md](FAQ_JDBC_K8S.md)
- **Step-by-step** → [QUICKSTART_JDBC_K8S.md](QUICKSTART_JDBC_K8S.md)
- **Technical details** → [docs/TECHNICAL_JDBC_K8S.md](docs/TECHNICAL_JDBC_K8S.md)

---

**🎉 Happy Deployment!**
