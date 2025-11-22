# 🚀 HƯỚNG DẪN KHỞI ĐỘNG HỆ THỐNG - MOVIE RECOMMENDATION SYSTEM

Hướng dẫn chi tiết từng bước để khởi động hệ thống từ đầu.

---

## 📋 MỤC LỤC

1. [Bước 1: Khởi động Docker](#bước-1-khởi-động-docker)
2. [Bước 2: Kiểm tra Services](#bước-2-kiểm-tra-services)
3. [Bước 3: Setup HDFS](#bước-3-setup-hdfs)
4. [Bước 4: Train Model (Nếu chưa có)](#bước-4-train-model-nếu-chưa-có)
5. [Bước 5: Push Model lên HDFS](#bước-5-push-model-lên-hdfs)
6. [Bước 6: Chạy Batch Job (Write Recommendations)](#bước-6-chạy-batch-job-write-recommendations)
7. [Bước 7: Tạo Kafka Topic](#bước-7-tạo-kafka-topic)
8. [Bước 8: Chạy Stream Processing](#bước-8-chạy-stream-processing)
9. [Bước 9: Chạy Web Application](#bước-9-chạy-web-application)
10. [Bước 10: Test Hệ Thống](#bước-10-test-hệ-thống)

---

## BƯỚC 1: Khởi động Docker

### 1.1. Mở terminal Ubuntu và di chuyển vào thư mục project:

```bash
# Trên Ubuntu/WSL
cd /mnt/d/Big_Data/Movie_Recommendation_System

# Hoặc nếu mount ở vị trí khác:
cd ~/Big_Data/Movie_Recommendation_System

# Kiểm tra đã vào đúng thư mục:
pwd
ls -la
```

### 1.2. Kiểm tra Docker đã cài đặt:

```bash
# Kiểm tra Docker version
docker --version

# Kiểm tra Docker Compose
docker compose version

# Nếu chưa có, cài đặt:
# sudo apt-get update
# sudo apt-get install docker.io docker-compose
```

### 1.3. Khởi động tất cả services:

```bash
# Khởi động tất cả containers
docker compose up -d

# Xem logs khi khởi động
docker compose up -d && docker compose logs -f
```

### 1.4. Kiểm tra containers đang chạy:

```bash
# Xem tất cả containers
docker compose ps

# Hoặc dùng lệnh Docker thông thường
docker ps

# Xem chi tiết một container
docker ps | grep app
```

**Kết quả mong đợi:** Tất cả 6 containers đều `Up`:
- `zookeeper`
- `kafka`
- `namenode`
- `datanode`
- `cassandra`
- `app`

### 1.5. Đợi services khởi động hoàn toàn (30-60 giây):

```bash
# Kiểm tra logs để đảm bảo không có lỗi
docker compose logs --tail=50

# Xem logs của từng service
docker compose logs namenode
docker compose logs datanode
docker compose logs cassandra
docker compose logs kafka

# Xem logs real-time
docker compose logs -f

# Kiểm tra health của containers
docker compose ps
# Tất cả phải có status "Up" và "healthy" (nếu có healthcheck)
```

---

## BƯỚC 2: Kiểm tra Services

### 2.1. Vào container `app`:

```bash
# Vào container app
docker compose exec app bash

# Kiểm tra đã vào container (prompt sẽ đổi thành root@...)
whoami
hostname
pwd
```

### 2.2. Kiểm tra kết nối các services:

```bash
# Kiểm tra HDFS
hdfs dfsadmin -report

# Kiểm tra HDFS có thể truy cập
hdfs dfs -ls /

# Kiểm tra Cassandra (từ trong container)
python3 -c "from cassandra.cluster import Cluster; c = Cluster(['cassandra']); c.connect(); print('✅ Cassandra OK')"

# Hoặc dùng cqlsh (nếu có)
# cqlsh cassandra 9042

# Kiểm tra Kafka (từ trong container)
# (Kafka sẽ được test ở bước 7)

# Kiểm tra Python và packages
python3 --version
pip list | grep -E "pyspark|cassandra|kafka"

# Thoát container (khi cần)
exit
```

---

## BƯỚC 3: Setup HDFS

### 3.1. Vào container app (nếu chưa vào):

```bash
docker compose exec app bash
```

### 3.2. Kiểm tra data có sẵn trong container:

```bash
# Kiểm tra data đã mount chưa
ls -la /app/data/
ls -la /app/data/ml-32m/ml-32m/

# Kiểm tra file sizes
du -sh /app/data/ml-32m/ml-32m/*
```

### 3.3. Tạo thư mục trên HDFS:

```bash
# Tạo thư mục trên HDFS
hdfs dfs -mkdir -p /user/hadoop
hdfs dfs -mkdir -p /user/hadoop/movielens
hdfs dfs -mkdir -p /user/hadoop/movielens/32M

# Kiểm tra đã tạo thành công
hdfs dfs -ls -R /user/hadoop
```

### 3.4. Upload data lên HDFS (nếu chưa có):

```bash
# Kiểm tra xem đã có data chưa
hdfs dfs -ls /user/hadoop/movielens/32M

# Nếu chưa có, upload từng file:
echo "Đang upload ratings.csv..."
hdfs dfs -put /app/data/ml-32m/ml-32m/ratings.csv /user/hadoop/movielens/32M/

echo "Đang upload movies.csv..."
hdfs dfs -put /app/data/ml-32m/ml-32m/movies.csv /user/hadoop/movielens/32M/

echo "Đang upload links.csv..."
hdfs dfs -put /app/data/ml-32m/ml-32m/links.csv /user/hadoop/movielens/32M/

# Kiểm tra lại
hdfs dfs -ls -h /user/hadoop/movielens/32M

# Xem kích thước file trên HDFS
hdfs dfs -du -h /user/hadoop/movielens/32M
```

**Hoặc dùng script có sẵn:**

```bash
# Kiểm tra script có quyền thực thi
ls -la /app/scripts/load_to_hdfs.sh

# Nếu chưa có quyền, thêm quyền
chmod +x /app/scripts/load_to_hdfs.sh

# Chạy script
bash /app/scripts/load_to_hdfs.sh

# Hoặc
/app/scripts/load_to_hdfs.sh
```

---

## BƯỚC 4: Train Model (Nếu chưa có)

### 4.1. Kiểm tra xem đã có model chưa:

```bash
# Kiểm tra trên HDFS
hdfs dfs -ls /user/hadoop/als_model

# Kiểm tra local trong container
ls -la /app/src/batch/als_model_32m

# Hoặc kiểm tra từ Ubuntu (ngoài container)
ls -la /mnt/d/Big_Data/Movie_Recommendation_System/src/batch/als_model_32m
```

### 4.2. Nếu CHƯA có model, train model:

```bash
# Mở Jupyter Notebook
# (Từ browser: http://localhost:8888)
# Hoặc chạy trực tiếp từ notebook:
jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root

# Mở file: /app/src/batch/train_model.ipynb
# Chạy các cells để train model
# Lưu model vào: /app/src/batch/als_model_32m
```

**Lưu ý:** Training model có thể mất 30-60 phút tùy vào cấu hình.

---

## BƯỚC 5: Push Model lên HDFS

### 5.1. Nếu model đã có ở local (`/app/src/batch/als_model_32m`):

```bash
# Vẫn trong container app
python3 /app/scripts/push_model_to_hdfs.py
```

### 5.2. Kiểm tra model đã được push:

```bash
hdfs dfs -ls /user/hadoop/als_model
```

**Kết quả mong đợi:** Thấy các file metadata và factors.

---

## BƯỚC 6: Chạy Batch Job (Write Recommendations)

### 6.1. Chạy script write recommendations:

```bash
# Vẫn trong container app
spark-submit \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 \
  /app/src/batch/write_recommendations.py
```

### 6.2. Kiểm tra kết quả:

```bash
# Kiểm tra Cassandra có data chưa
python3 -c "from utils import cassandra_connector as cc; cc.create_keyspace_and_table(); print(cc.read_recs('1'))"
```

**Kết quả mong đợi:** In ra list movie IDs (ví dụ: `['13399', '27373', ...]`)

---

## BƯỚC 7: Tạo Kafka Topic

### 7.1. Tạo topic `new_ratings`:

**Cách 1: Dùng kafka-topics (Khuyên dùng):**

```bash
# Từ Ubuntu (không cần vào container)
docker compose exec kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic new_ratings \
  --partitions 1 \
  --replication-factor 1

# Hoặc từ trong container kafka
docker compose exec kafka bash
kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic new_ratings \
  --partitions 1 \
  --replication-factor 1
exit
```

**Cách 2: Dùng Python (từ container app):**

```bash
# Vào container app
docker compose exec app bash

# Cài kafka-python nếu chưa có
pip install kafka-python

# Tạo topic bằng Python
python3 << 'EOF'
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

admin_client = KafkaAdminClient(
    bootstrap_servers=['kafka:9092'],
    client_id='admin'
)

topic = NewTopic(name='new_ratings', num_partitions=1, replication_factor=1)

try:
    admin_client.create_topics(new_topics=[topic], validate_only=False)
    print("✅ Topic 'new_ratings' đã được tạo")
except TopicAlreadyExistsError:
    print("ℹ️ Topic 'new_ratings' đã tồn tại")
EOF
```

### 7.2. Kiểm tra topic đã tạo:

```bash
# Từ Ubuntu
docker compose exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Xem chi tiết topic
docker compose exec kafka kafka-topics --describe \
  --bootstrap-server localhost:9092 \
  --topic new_ratings
```

---

## BƯỚC 8: Chạy Stream Processing

### 8.1. Mở terminal MỚI trên Ubuntu:

```bash
# Mở terminal mới (Ctrl+Shift+T hoặc tạo tab mới)
# Di chuyển vào thư mục project
cd /mnt/d/Big_Data/Movie_Recommendation_System

# Vào container app
docker compose exec app bash
```

### 8.2. Kiểm tra file script có tồn tại:

```bash
# Kiểm tra file
ls -la /app/src/stream/process_stream.py

# Xem nội dung (để đảm bảo đúng file)
head -20 /app/src/stream/process_stream.py
```

### 8.3. Chạy Spark Streaming:

```bash
# Chạy với packages cần thiết
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 \
  /app/src/stream/process_stream.py

# Hoặc nếu packages đã được cache, có thể bỏ qua
# spark-submit /app/src/stream/process_stream.py
```

### 8.3. Kiểm tra logs:

**Kết quả mong đợi:**
```
✅ Đã import Cassandra_connector thành công!
Khởi động job Spark Streaming (LỚP SPEED - PHIÊN BẢN THẬT)...
Đã kết nối Kafka, đang lắng nghe topic 'new_ratings'...
Đã khởi động query, đang chờ data...
```

**Giữ terminal này chạy!** (Stream processing chạy liên tục)

---

## BƯỚC 9: Chạy Web Application

### 9.1. Mở terminal MỚI trên Ubuntu (thứ 3):

```bash
# Mở terminal mới (Ctrl+Shift+T hoặc tạo tab mới)
# Di chuyển vào thư mục project
cd /mnt/d/Big_Data/Movie_Recommendation_System

# Vào container app
docker compose exec app bash
```

### 9.2. Kiểm tra và chạy Flask app:

```bash
# Kiểm tra file app.py
ls -la /app/src/webapp/app.py

# Kiểm tra dependencies
python3 -c "import flask; print('Flask OK')"
python3 -c "import pandas; print('Pandas OK')"

# Di chuyển vào thư mục app
cd /app

# Chạy Flask app
python3 src/webapp/app.py

# Hoặc chạy với debug mode (nếu cần)
# FLASK_DEBUG=1 python3 src/webapp/app.py
```

### 9.3. Kiểm tra webapp:

Mở browser: **http://localhost:5000**

**Kết quả mong đợi:** Thấy giao diện web với form nhập User ID.

---

## BƯỚC 10: Test Hệ Thống

### 10.1. Test đọc recommendations:

1. Mở browser: http://localhost:5000
2. Nhập User ID: `1`
3. Click "Xem Gợi Ý"
4. **Kết quả:** Thấy danh sách 10 phim được gợi ý

### 10.2. Test gửi rating mới:

1. Trong webapp, nhập:
   - User ID: `1`
   - Movie ID: `123` (hoặc bất kỳ)
   - Rating: `4.5`
2. Click "Gửi Rating"
3. **Kết quả:** Thấy message "Đã gửi rating vào Kafka!"

### 10.3. Kiểm tra Stream Processing đã xử lý:

1. Xem terminal Spark Streaming (Bước 8)
2. **Kết quả mong đợi:** Thấy log:
   ```
   Đang xử lý Batch ID: X
   Batch X có Y ratings mới.
   --- [REAL MODEL] Đang tính toán Top 10 cho Z user...
   --- [REAL DB] Đã ghi xong Z users vào Cassandra
   ```

### 10.4. Test cập nhật recommendations:

1. Đợi ~15 giây (trigger time)
2. Refresh webapp
3. Nhập lại User ID: `1`
4. Click "Xem Gợi Ý"
5. **Kết quả:** Thấy top 10 mới (có thể khác với lần trước)

---

## 🔧 TROUBLESHOOTING

### Lỗi: "Failed to find data source: kafka"

**Giải pháp:** Thêm `--packages` vào spark-submit:
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
  /app/src/stream/process_stream.py
```

### Lỗi: "UnknownTopicOrPartitionException"

**Giải pháp:** Tạo topic trước (Bước 7)

### Lỗi: "Cannot connect to Cassandra"

**Giải pháp:**
```bash
# Kiểm tra Cassandra đang chạy
docker compose ps cassandra

# Đợi Cassandra khởi động (30-60 giây)
docker compose logs cassandra
```

### Lỗi: "Model not found"

**Giải pháp:** 
1. Kiểm tra model đã được push lên HDFS (Bước 5)
2. Hoặc train model mới (Bước 4)

---

## 📝 TÓM TẮT CÁC TERMINAL

Khi hệ thống chạy đầy đủ, bạn cần **3 terminals**:

1. **Terminal 1:** Spark Streaming (Bước 8)
   ```bash
   spark-submit ... process_stream.py
   ```

2. **Terminal 2:** Web Application (Bước 9)
   ```bash
   python3 src/webapp/app.py
   ```

3. **Terminal 3:** Dùng để test/check logs
   ```bash
   docker compose logs -f
   ```

---

## 🎯 CHECKLIST HOÀN THÀNH

- [ ] Docker containers đang chạy
- [ ] HDFS đã setup và có data
- [ ] Model đã được train và push lên HDFS
- [ ] Batch job đã chạy (Cassandra có recommendations)
- [ ] Kafka topic `new_ratings` đã tạo
- [ ] Spark Streaming đang chạy
- [ ] Web Application đang chạy
- [ ] Test đọc recommendations thành công
- [ ] Test gửi rating thành công
- [ ] Test cập nhật recommendations thành công

---

## 🚀 LẦN SAU KHI MỞ LẠI

Nếu đã setup xong, chỉ cần:

1. **Khởi động Docker:**
   ```bash
   docker compose up -d
   ```

2. **Chạy Spark Streaming** (Terminal 1):
   ```bash
   docker compose exec app bash
   spark-submit --packages ... /app/src/stream/process_stream.py
   ```

3. **Chạy Web Application** (Terminal 2):
   ```bash
   docker compose exec app bash
   python3 src/webapp/app.py
   ```

4. **Mở browser:** http://localhost:5000

---

**Chúc bạn thành công! 🎉**