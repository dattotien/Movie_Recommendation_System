# Incremental Learning cho ALS Model

## Tổng quan

Incremental Learning cho phép cập nhật ALS Model với ratings mới mà không cần retrain toàn bộ model từ đầu.

## Cách tiếp cận

### 1. **Partial Retrain** (Đã implement)
- Retrain model với data cũ + data mới
- Giữ nguyên tham số (rank, regParam, ...)
- Chỉ retrain khi đủ số lượng ratings mới (MIN_NEW_RATINGS)

**Ưu điểm:**
- Đơn giản, dễ implement
- Đảm bảo chất lượng model

**Nhược điểm:**
- Vẫn cần retrain (tốn thời gian)
- Không thực sự "incremental"

### 2. **Online ALS** (Nâng cao - chưa implement)
- Cập nhật User/Item Factors bằng Gradient Descent
- Cập nhật từng rating một
- Không cần retrain

**Ưu điểm:**
- Nhanh, real-time
- Thực sự incremental

**Nhược điểm:**
- Phức tạp, khó implement
- Cần access trực tiếp vào factors

## Cách sử dụng

### Option 1: Tích hợp vào Stream Processing

```python
# Trong process_stream.py
from stream.incremental_als import process_batch_with_incremental_update

def process_batch(batch_df, batch_id):
    # ... code hiện tại ...
    
    # Định kỳ (ví dụ: mỗi 100 batches), cập nhật model
    if batch_id % 100 == 0:
        process_batch_with_incremental_update(batch_df, batch_id, spark)
```

### Option 2: Chạy riêng (Scheduled Job)

```bash
# Chạy định kỳ (ví dụ: mỗi giờ)
spark-submit src/stream/incremental_als.py
```

## Cấu hình

```python
# src/stream/incremental_als.py
MIN_NEW_RATINGS = 1000  # Số ratings tối thiểu để trigger update
HDFS_MODEL_PATH = "hdfs://namenode:9000/user/hadoop/als_model"
HDFS_NEW_RATINGS_PATH = "hdfs://namenode:9000/user/hadoop/new_ratings"
```

## Luồng hoạt động

```
Ratings mới → Kafka
    ↓
Stream Processing → Lưu vào HDFS (accumulate)
    ↓
Đủ MIN_NEW_RATINGS?
    ↓ YES
Load model cũ
    ↓
Gộp ratings (cũ + mới)
    ↓
Retrain model (với ít iterations)
    ↓
Lưu model mới lên HDFS
    ↓
Stream layer tự động dùng model mới
```

## Lưu ý

1. **Performance**: Retrain vẫn tốn thời gian, nhưng ít hơn retrain từ đầu
2. **Data Accumulation**: Cần lưu ratings mới vào HDFS/Cassandra để accumulate
3. **Model Versioning**: Nên lưu version của model để rollback nếu cần
4. **Cold Start**: Users/Items mới vẫn cần được thêm vào model

## Cải tiến tương lai

1. **Online ALS**: Implement gradient-based update
2. **Selective Update**: Chỉ update users/items có nhiều ratings mới
3. **Distributed Update**: Update factors trên nhiều nodes
4. **Model Versioning**: Quản lý nhiều version model

