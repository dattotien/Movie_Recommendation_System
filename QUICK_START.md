# ⚡ QUICK START - Khởi động nhanh hệ thống

> **Lưu ý:** File này dành cho lần sau khi đã setup xong.  
> Nếu lần đầu, xem [SETUP_GUIDE.md](SETUP_GUIDE.md)

---

## 🚀 3 BƯỚC KHỞI ĐỘNG NHANH

### 1️⃣ Khởi động Docker (Ubuntu Terminal)

```bash
# Di chuyển vào thư mục project
cd /mnt/d/Big_Data/Movie_Recommendation_System

# Hoặc nếu mount ở vị trí khác
cd ~/Big_Data/Movie_Recommendation_System

# Khởi động containers
docker compose up -d

# Kiểm tra containers đang chạy
docker compose ps

# Đợi 30-60 giây để services khởi động
sleep 30
```

---

### 2️⃣ Chạy Spark Streaming (Terminal 1 - Ubuntu)

```bash
# Mở terminal mới (Ctrl+Shift+T)
cd /mnt/d/Big_Data/Movie_Recommendation_System

# Vào container app
docker compose exec app bash

 spark-submit src/batch/write_recommendations.py
# Chạy Spark Streaming
spark-submit \
  --packages \
  org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,\
com.datastax.spark:spark-cassandra-connector-assembly_2.12:3.3.0 \
  src/stream/process_stream.py

**Giữ terminal này chạy!** (Nhấn Ctrl+C để dừng)

---

### 3️⃣ Chạy Web Application (Terminal 2 - Ubuntu)

```bash
# Mở terminal mới (Ctrl+Shift+T)
cd /mnt/d/Big_Data/Movie_Recommendation_System

# Vào container app
docker compose exec app bash

# Di chuyển vào thư mục app
cd /app

# Chạy Flask app
python3 src/webapp/app.py
```

**Giữ terminal này chạy!** (Nhấn Ctrl+C để dừng)

---

### 4️⃣ Mở Browser

**http://localhost:5000**

Hoặc từ Ubuntu:

```bash
# Mở browser từ terminal
xdg-open http://localhost:5000

# Hoặc dùng browser có sẵn
firefox http://localhost:5000
# hoặc
google-chrome http://localhost:5000
```

---

## ✅ KIỂM TRA NHANH

### Test đọc recommendations:
1. Nhập User ID: `1`
2. Click "Xem Gợi Ý"
3. ✅ Thấy 10 phim

### Test gửi rating:
1. Nhập User ID, Movie ID, Rating
2. Click "Gửi Rating"
3. ✅ Thấy message thành công
4. Đợi 15 giây → Refresh → Xem lại gợi ý

---

## 🛑 DỪNG HỆ THỐNG

```bash
# Dừng tất cả containers (giữ data)
docker compose down

# Hoặc dừng và xóa data (cẩn thận!)
docker compose down -v

# Dừng một service cụ thể
docker compose stop [service_name]

# Khởi động lại một service
docker compose restart [service_name]
```

---

## 🔍 KIỂM TRA SERVICES (Ubuntu)

```bash
# Xem tất cả containers
docker compose ps

# Xem containers đang chạy
docker ps

# Xem logs của tất cả services
docker compose logs

# Xem logs của một service cụ thể
docker compose logs -f app
docker compose logs -f kafka
docker compose logs -f cassandra

# Xem logs real-time (theo dõi)
docker compose logs -f

# Vào container
docker compose exec app bash
docker compose exec kafka bash
docker compose exec cassandra bash

# Kiểm tra resource usage
docker stats

# Kiểm tra network
docker network ls
docker network inspect movie_recommendation_system_default
```

---

## 📋 CÁC PORT QUAN TRỌNG

- **5000:** Web Application
- **4040:** Spark UI
- **8888:** Jupyter Notebook
- **9870:** HDFS NameNode UI
- **9042:** Cassandra
- **9092:** Kafka

---

**Xem chi tiết:** [SETUP_GUIDE.md](SETUP_GUIDE.md)

