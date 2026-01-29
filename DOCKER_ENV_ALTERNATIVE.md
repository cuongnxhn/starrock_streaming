# ⚠️ CÁCH 2: Thêm .env vào Docker Image (KHÔNG KHUYẾN NGHỊ CHO PRODUCTION)

## Chỉ dùng cho Development/Testing

Nếu bạn muốn nhanh gọn cho môi trường development, có thể thêm file `.env` trực tiếp vào Docker image.

⚠️ **CẢNH BÁO**: 
- Passwords sẽ bị lưu trong Docker image
- Bất kỳ ai có quyền pull image đều có thể xem được
- KHÔNG BAO GIỜ dùng cách này cho Production!

---

## Cách thực hiện

### Bước 1: Cập nhật Dockerfile

Thêm dòng sau vào `Dockerfile` (sau dòng `COPY . .`):

```dockerfile
# Copy .env file (ONLY FOR DEVELOPMENT)
COPY .env /app/.env
```

**Dockerfile hoàn chỉnh**:
```dockerfile
FROM spark:3.5.0-python3

USER root

RUN mkdir -p /opt/spark/jars

COPY jars/mysql-*.jar /opt/spark/jars/
COPY jars/postgresql-42.7.8.jar /opt/spark/jars/

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# ⚠️ DEVELOPMENT ONLY - Copy .env file
COPY .env /app/.env

ENV PYTHONPATH=/app:$PYTHONPATH
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

USER 185
```

### Bước 2: Rebuild Docker Image

```bash
docker build -t 10.165.13.117:5000/starrock-streaming:latest .
docker push 10.165.13.117:5000/starrock-streaming:latest
```

### Bước 3: Deploy như bình thường

```bash
./deploy_k8s_per_topic_kubectl.sh <app_name> <topics>
```

---

## ⚠️ Rủi ro bảo mật

1. **Lộ thông tin nhạy cảm**: 
   - Bất kỳ ai có quyền `docker pull` đều có thể extract ra file `.env`
   - Nếu push lên public registry = lộ hết secrets

2. **Không thể cập nhật nhanh**:
   - Mỗi lần thay đổi env vars phải rebuild image
   - Phải push image mới lên registry
   - Phải restart tất cả pods

3. **Vi phạm best practices**:
   - Không tách biệt code và config
   - Khó quản lý multi-environment (dev/staging/prod)

---

## 🔍 Cách extract .env từ image (để test)

```bash
# Pull image
docker pull 10.165.13.117:5000/starrock-streaming:latest

# Run container tạm thời và cat file .env
docker run --rm 10.165.13.117:5000/starrock-streaming:latest cat /app/.env
```

👆 Đây chính là lý do tại sao **KHÔNG NÊN** dùng cách này cho production!

---

## ✅ Khuyến nghị

**SỬ DỤNG KUBERNETES SECRET** (Cách 1) như trong file `KUBERNETES_SECRET_GUIDE.md`

Chỉ dùng cách thêm .env vào image khi:
- ✅ Môi trường development cá nhân
- ✅ Testing nhanh
- ✅ Image được lưu trong private registry và có access control nghiêm ngặt
- ❌ KHÔNG dùng cho Production
- ❌ KHÔNG dùng khi có thông tin nhạy cảm như passwords, API keys

---

## 🛡️ Cách làm an toàn hơn (nếu bắt buộc phải dùng image)

Thay vì copy trực tiếp file `.env`, chỉ copy file template và override bằng Secret:

1. Tạo `.env.template` (không có giá trị thực):
   ```
   DB_HOST=
   DB_PORT=
   DB_USER=
   DB_PASSWORD=
   ```

2. Copy template vào image:
   ```dockerfile
   COPY .env.template /app/.env.template
   ```

3. Vẫn dùng Kubernetes Secret để inject giá trị thực

Như vậy vừa có template để tham khảo, vừa không lộ thông tin nhạy cảm!
