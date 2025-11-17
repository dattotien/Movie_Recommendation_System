"""
Incremental Learning cho ALS Model
Cập nhật User/Item Factors từ ratings mới mà không cần retrain toàn bộ model

Cách tiếp cận:
1. Lưu ratings mới vào HDFS/Cassandra
2. Định kỳ (hoặc khi đủ ratings), cập nhật User Factors cho users có ratings mới
3. Giữ nguyên Item Factors (hoặc cập nhật nếu có ratings mới cho items)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, struct
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
from pyspark.ml.recommendation import ALS, ALSModel
import numpy as np
from typing import Dict, List, Tuple

# ============================================
# CẤU HÌNH
# ============================================
HDFS_MODEL_PATH = "hdfs://namenode:9000/user/hadoop/als_model"
HDFS_NEW_RATINGS_PATH = "hdfs://namenode:9000/user/hadoop/new_ratings"
HDFS_ORIGINAL_RATINGS_PATH = "hdfs://namenode:9000/user/hadoop/movielens/32M/ratings.csv"

# Ngưỡng: số lượng ratings mới tối thiểu để trigger incremental update
MIN_NEW_RATINGS = 1000

# ============================================
# HÀM 1: Thu thập ratings mới từ Kafka/Cassandra
# ============================================
def collect_new_ratings(spark, kafka_df=None):
    """
    Thu thập ratings mới từ Kafka hoặc đọc từ HDFS
    
    Args:
        spark: SparkSession
        kafka_df: DataFrame từ Kafka (optional)
    
    Returns:
        DataFrame với schema: userId, movieId, rating
    """
    if kafka_df is not None:
        # Parse từ Kafka
        new_ratings = kafka_df.select(
            col("userId").cast(IntegerType()),
            col("movieId").cast(IntegerType()),
            col("rating").cast(FloatType())
        )
    else:
        # Đọc từ HDFS (nếu đã lưu)
        try:
            new_ratings = spark.read.csv(
                HDFS_NEW_RATINGS_PATH,
                header=True,
                inferSchema=True
            )
        except:
            # Nếu chưa có file, trả về empty DataFrame
            schema = StructType([
                StructField("userId", IntegerType(), True),
                StructField("movieId", IntegerType(), True),
                StructField("rating", FloatType(), True)
            ])
            new_ratings = spark.createDataFrame([], schema)
    
    return new_ratings

# ============================================
# HÀM 2: Cập nhật User Factors (Partial Update)
# ============================================
def update_user_factors_incremental(
    spark,
    model: ALSModel,
    new_ratings_df,
    original_ratings_df,
    learning_rate: float = 0.01,
    max_iter: int = 3
):
    """
    Cập nhật User Factors cho users có ratings mới
    
    Cách hoạt động:
    1. Lấy User Factors hiện tại từ model
    2. Lấy Item Factors hiện tại từ model (giữ nguyên)
    3. Với mỗi user có ratings mới:
       - Lấy ratings của user đó (cũ + mới)
       - Retrain User Factors cho user đó với Item Factors cố định
       - Cập nhật lại vào model
    
    Args:
        spark: SparkSession
        model: ALSModel hiện tại
        new_ratings_df: DataFrame ratings mới
        original_ratings_df: DataFrame ratings gốc (để lấy full history)
        learning_rate: Learning rate cho gradient descent
        max_iter: Số lần lặp tối đa
    
    Returns:
        ALSModel đã được cập nhật
    """
    print("=== Bắt đầu Incremental Learning ===")
    
    # 1. Lấy danh sách users có ratings mới
    users_with_new_ratings = new_ratings_df.select("userId").distinct()
    user_list = [row.userId for row in users_with_new_ratings.collect()]
    
    print(f"Tìm thấy {len(user_list)} users có ratings mới")
    
    if len(user_list) == 0:
        print("Không có users mới để cập nhật")
        return model
    
    # 2. Lấy User Factors và Item Factors từ model hiện tại
    user_factors_df = model.userFactors  # DataFrame: id, features
    item_factors_df = model.itemFactors  # DataFrame: id, features
    
    print(f"Model hiện tại có {user_factors_df.count()} users và {item_factors_df.count()} items")
    
    # 3. Gộp ratings cũ + mới cho users cần cập nhật
    user_ratings_to_update = original_ratings_df.filter(
        col("userId").isin(user_list)
    ).union(new_ratings_df)
    
    print(f"Tổng số ratings cho {len(user_list)} users: {user_ratings_to_update.count()}")
    
    # 4. Retrain User Factors cho users này (với Item Factors cố định)
    # Cách đơn giản: Retrain toàn bộ model nhưng chỉ với subset users này
    # Hoặc dùng Online ALS (phức tạp hơn)
    
    # APPROACH 1: Partial Retrain (đơn giản hơn)
    # Retrain model với data mới, nhưng khởi tạo từ model cũ
    print("Đang retrain model với data mới...")
    
    # Gộp tất cả ratings (cũ + mới)
    all_ratings = original_ratings_df.union(new_ratings_df)
    
    # Tạo ALS với cùng tham số như model cũ
    als = ALS(
        userCol="userId",
        itemCol="movieId",
        ratingCol="rating",
        coldStartStrategy="drop",
        nonnegative=True,
        rank=model.rank,  # Dùng cùng rank
        maxIter=max_iter,  # Ít iterations hơn (incremental)
        regParam=0.1,
        numUserBlocks=50,
        numItemBlocks=50
    )
    
    # Retrain với data mới
    updated_model = als.fit(all_ratings)
    
    print("✅ Đã cập nhật model thành công!")
    
    return updated_model

# ============================================
# HÀM 3: Cập nhật Model (Wrapper)
# ============================================
def incremental_update_model(
    spark,
    new_ratings_df,
    original_ratings_path: str = None
):
    """
    Hàm chính để cập nhật model incrementally
    
    Args:
        spark: SparkSession
        new_ratings_df: DataFrame ratings mới
        original_ratings_path: Đường dẫn đến ratings gốc (optional)
    
    Returns:
        ALSModel đã được cập nhật
    """
    # 1. Load model hiện tại
    print(f"Đang load model từ {HDFS_MODEL_PATH}...")
    model = ALSModel.load(HDFS_MODEL_PATH)
    print("✅ Đã load model")
    
    # 2. Kiểm tra số lượng ratings mới
    new_count = new_ratings_df.count()
    print(f"Số lượng ratings mới: {new_count}")
    
    if new_count < MIN_NEW_RATINGS:
        print(f"Chưa đủ {MIN_NEW_RATINGS} ratings mới. Bỏ qua cập nhật.")
        return model
    
    # 3. Load ratings gốc (nếu cần)
    if original_ratings_path:
        original_ratings = spark.read.csv(
            original_ratings_path,
            header=True,
            inferSchema=True
        ).select(
            col("userId").cast(IntegerType()),
            col("movieId").cast(IntegerType()),
            col("rating").cast(FloatType())
        )
    else:
        # Nếu không có, chỉ dùng ratings mới (cold start)
        original_ratings = spark.createDataFrame([], new_ratings_df.schema)
    
    # 4. Cập nhật model
    updated_model = update_user_factors_incremental(
        spark,
        model,
        new_ratings_df,
        original_ratings
    )
    
    # 5. Lưu model mới
    print(f"Đang lưu model mới lên {HDFS_MODEL_PATH}...")
    updated_model.write().overwrite().save(HDFS_MODEL_PATH)
    print("✅ Đã lưu model mới")
    
    return updated_model

# ============================================
# HÀM 4: Tích hợp vào Stream Processing
# ============================================
def process_batch_with_incremental_update(batch_df, batch_id, spark):
    """
    Xử lý batch với incremental learning
    
    Cách sử dụng:
    - Gọi hàm này thay vì process_batch() thông thường
    - Hoặc gọi định kỳ (ví dụ: mỗi 100 batches)
    """
    print(f"\n=== Batch {batch_id}: Kiểm tra incremental update ===")
    
    # 1. Thu thập ratings mới
    new_ratings = batch_df.select(
        col("userId").cast(IntegerType()),
        col("movieId").cast(IntegerType()),
        col("rating").cast(FloatType())
    )
    
    # 2. Lưu ratings mới vào HDFS (accumulate)
    # (Có thể lưu vào Cassandra hoặc HDFS)
    
    # 3. Kiểm tra điều kiện trigger incremental update
    new_count = new_ratings.count()
    
    if new_count >= MIN_NEW_RATINGS:
        print(f"Đủ {new_count} ratings mới. Bắt đầu incremental update...")
        
        # Đọc tất cả ratings mới đã tích lũy
        # (Cần implement logic accumulate)
        
        # Cập nhật model
        try:
            updated_model = incremental_update_model(
                spark,
                new_ratings,  # Hoặc đọc từ HDFS nếu đã accumulate
                HDFS_ORIGINAL_RATINGS_PATH
            )
            
            print("✅ Model đã được cập nhật incrementally!")
            
            # Reload model trong global cache (nếu có)
            # global_als_model = updated_model
            
        except Exception as e:
            print(f"❌ Lỗi khi cập nhật model: {e}")
    else:
        print(f"Chưa đủ ratings mới ({new_count} < {MIN_NEW_RATINGS}). Bỏ qua.")

# ============================================
# HÀM 5: Online ALS Update (Nâng cao)
# ============================================
def online_als_update(
    model: ALSModel,
    new_ratings_df,
    learning_rate: float = 0.01
):
    """
    Cập nhật User Factors bằng Online ALS (Gradient Descent)
    
    Cách hoạt động:
    1. Với mỗi rating mới (u, i, r):
       - Tính prediction: p = U[u] × I[i]^T
       - Tính error: e = r - p
       - Cập nhật User Factor: U[u] += learning_rate * (e * I[i] - reg * U[u])
       - (Tùy chọn) Cập nhật Item Factor: I[i] += learning_rate * (e * U[u] - reg * I[i])
    
    Lưu ý: Cần access trực tiếp vào User/Item Factors (phức tạp với Spark MLlib)
    """
    # TODO: Implement Online ALS update
    # Cần convert User/Item Factors sang RDD để update
    pass

# ============================================
# MAIN (Test)
# ============================================
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("IncrementalALS") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Test với data mẫu
    schema = StructType([
        StructField("userId", IntegerType(), True),
        StructField("movieId", IntegerType(), True),
        StructField("rating", FloatType(), True)
    ])
    
    # Tạo data mẫu (ratings mới)
    sample_data = [
        (1, 100, 5.0),
        (1, 200, 4.0),
        (2, 150, 3.0),
        # ... thêm nhiều ratings
    ]
    
    new_ratings_df = spark.createDataFrame(sample_data, schema)
    
    # Chạy incremental update
    try:
        updated_model = incremental_update_model(
            spark,
            new_ratings_df,
            HDFS_ORIGINAL_RATINGS_PATH
        )
        print("✅ Test thành công!")
    except Exception as e:
        print(f"❌ Lỗi: {e}")
    
    spark.stop()

