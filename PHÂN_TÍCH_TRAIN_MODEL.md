# 📋 PHÂN TÍCH FILE train-model.ipynb

## ✅ NỘI DUNG ĐÚNG

1. **Import thư viện**: Đầy đủ các thư viện cần thiết (Spark, ALS, etc.)
2. **Xử lý dữ liệu**: 
   - Đọc và chuyển đổi kiểu dữ liệu đúng
   - Lọc dữ liệu theo min_user_ratings và min_item_ratings
   - Tạo mapping cho userId và movieId
3. **Huấn luyện mô hình**: 
   - Cấu hình ALS hợp lý (rank=100, maxIter=15, regParam=0.05)
   - Repartition và cache dữ liệu để tối ưu performance
4. **Lưu mô hình**: Có lưu mô hình sau khi huấn luyện

## ⚠️ CÁC VẤN ĐỀ CẦN SỬA

### 1. **Đường dẫn dữ liệu không đúng** ❌
```python
# HIỆN TẠI (SAI):
ratings_path = "/kaggle/input/bigdata-movies/ml-32m/ml-32m/ratings.csv"
movies_path = "/kaggle/input/bigdata-movies/ml-32m/ml-32m/movies.csv"

# NÊN SỬA THÀNH (ĐÚNG):
ratings_path = "/app/data/ml-32m/ml-32m/ratings.csv"
movies_path = "/app/data/ml-32m/ml-32m/movies.csv"

# HOẶC ĐỌC TỪ HDFS:
ratings_path = "hdfs://namenode:9000/movielens/32M/ratings.csv"
movies_path = "hdfs://namenode:9000/movielens/32M/movies.csv"
```

### 2. **Đường dẫn lưu model không đúng** ❌
```python
# HIỆN TẠI (SAI):
model_path = "/kaggle/working/als_model_32m"

# NÊN SỬA THÀNH (ĐÚNG):
model_path = "/app/src/batch/als_model_32m"
# HOẶC:
model_path = "file:///app/src/batch/als_model_32m"
```

### 3. **Đường dẫn lưu mapping không đúng** ❌
```python
# HIỆN TẠI (SAI):
user_mapping.write.mode("overwrite").parquet("/models/mappings/users.parquet")
movie_mapping.write.mode("overwrite").parquet("/models/mappings/movies.parquet")

# NÊN SỬA THÀNH (ĐÚNG):
user_mapping.write.mode("overwrite").parquet("/app/src/batch/mappings/users.parquet")
movie_mapping.write.mode("overwrite").parquet("/app/src/batch/mappings/movies.parquet")
```

### 4. **Warnings về Window operation** ⚠️
Có nhiều warnings:
```
WARN WindowExec: No Partition Defined for Window operation! 
Moving all data to a single partition, this can cause serious performance degradation.
```

**Nguyên nhân**: Window function không có partition, dẫn đến shuffle toàn bộ dữ liệu vào 1 partition.

**Giải pháp**: Thêm partition cho Window operation:
```python
# THAY VÌ:
user_mapping = filtered_als_data.select("userId").distinct().withColumn(
    "new_userId",
    F.row_number().over(Window.orderBy("userId")) - 1
)

# NÊN DÙNG:
user_mapping = filtered_als_data.select("userId").distinct().withColumn(
    "new_userId",
    F.row_number().over(Window.partitionBy().orderBy("userId")) - 1
)
# HOẶC partition theo một cột khác nếu có
```

### 5. **Thiếu validation và evaluation** ⚠️
- Không có train/test split
- Không có đánh giá mô hình (RMSE, MAE)
- Không có kiểm tra chất lượng recommendations

**Đề xuất thêm**:
```python
# Chia train/test
train_data, test_data = indexed_data.randomSplit([0.8, 0.2], seed=42)

# Đánh giá mô hình
evaluator = RegressionEvaluator(
    metricName="rmse",
    labelCol="rating",
    predictionCol="prediction"
)
predictions = model_32m.transform(test_data)
rmse = evaluator.evaluate(predictions)
print(f"RMSE: {rmse}")
```

### 6. **Thiếu xử lý lỗi và logging** ⚠️
- Không có try-catch cho các thao tác quan trọng
- Không có logging chi tiết

## 📝 ĐỀ XUẤT SỬA CHỮA

### File đã sửa nên có:

1. **Đường dẫn đúng với cấu trúc dự án Docker**
2. **Xử lý cả 2 trường hợp**: đọc từ local hoặc HDFS
3. **Thêm evaluation metrics**
4. **Sửa warnings về Window operation**
5. **Thêm error handling**
6. **Thêm logging chi tiết**

## 🔍 SO SÁNH VỚI write_recommendations.py

File `write_recommendations.py` đã làm đúng:
- ✅ Sử dụng biến môi trường cho đường dẫn
- ✅ Hỗ trợ load từ local hoặc HDFS
- ✅ Có error handling
- ✅ Có logging chi tiết

**Nên áp dụng pattern tương tự cho train-model.ipynb**

## 📌 KẾT LUẬN

File `train-model.ipynb` có **logic đúng** nhưng cần sửa:
1. ❌ **Đường dẫn** (quan trọng nhất - sẽ không chạy được trong Docker)
2. ⚠️ **Performance warnings** (Window operation)
3. ⚠️ **Thiếu evaluation** (không biết chất lượng mô hình)

**Mức độ ưu tiên sửa:**
1. 🔴 **CAO**: Sửa đường dẫn (bắt buộc)
2. 🟡 **TRUNG BÌNH**: Sửa Window operation warnings
3. 🟢 **THẤP**: Thêm evaluation (tùy chọn, nhưng nên có)



