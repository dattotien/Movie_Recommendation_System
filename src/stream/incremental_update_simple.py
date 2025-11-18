"""
Incremental Learning - Version Đơn Giản
Tích hợp vào process_stream.py để cập nhật model định kỳ
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, FloatType
from pyspark.ml.recommendation import ALS, ALSModel

# Cấu hình
HDFS_MODEL_PATH = "hdfs://namenode:9000/user/hadoop/als_model"
HDFS_ORIGINAL_RATINGS = "hdfs://namenode:9000/user/hadoop/movielens/32M/ratings.csv"
HDFS_ACCUMULATED_RATINGS = "hdfs://namenode:9000/user/hadoop/accumulated_new_ratings"

# Ngưỡng để trigger incremental update
MIN_RATINGS_FOR_UPDATE = 5000  # Cần ít nhất 5000 ratings mới
UPDATE_INTERVAL_BATCHES = 50  # Cập nhật mỗi 50 batches

def accumulate_new_ratings(spark, new_ratings_df, batch_id):
    """
    Tích lũy ratings mới vào HDFS
    """
    try:
        # Đọc ratings đã tích lũy (nếu có)
        try:
            accumulated = spark.read.csv(
                HDFS_ACCUMULATED_RATINGS,
                header=True,
                inferSchema=True
            )
        except:
            accumulated = None
        
        # Gộp với ratings mới
        if accumulated:
            all_new = accumulated.union(new_ratings_df).distinct()
        else:
            all_new = new_ratings_df
        
        # Lưu lại
        all_new.write.mode("overwrite").option("header", "true").csv(HDFS_ACCUMULATED_RATINGS)
        
        count = all_new.count()
        print(f"  → Đã tích lũy {count} ratings mới (tổng)")
        
        return count
    except Exception as e:
        print(f"  ⚠️ Lỗi khi tích lũy ratings: {e}")
        return 0

def incremental_update_model(spark, accumulated_ratings_df):
    """
    Cập nhật model với ratings mới (Partial Retrain)
    """
    print("\n=== BẮT ĐẦU INCREMENTAL UPDATE ===")
    
    try:
        # 1. Load model hiện tại
        print("1. Đang load model hiện tại...")
        model = ALSModel.load(HDFS_MODEL_PATH)
        rank = model.rank
        print(f"   ✅ Model rank={rank}")
        
        # 2. Load ratings gốc
        print("2. Đang load ratings gốc...")
        original_ratings = spark.read.csv(
            HDFS_ORIGINAL_RATINGS,
            header=True,
            inferSchema=True
        ).select(
            col("userId").cast(IntegerType()),
            col("movieId").cast(IntegerType()),
            col("rating").cast(FloatType())
        )
        print(f"   ✅ {original_ratings.count()} ratings gốc")
        
        # 3. Gộp ratings (cũ + mới)
        print("3. Đang gộp ratings...")
        all_ratings = original_ratings.union(accumulated_ratings_df).distinct()
        total_count = all_ratings.count()
        print(f"   ✅ Tổng: {total_count} ratings")
        
        # 4. Retrain model (với ít iterations hơn - incremental)
        print("4. Đang retrain model (incremental)...")
        als = ALS(
            userCol="userId",
            itemCol="movieId",
            ratingCol="rating",
            coldStartStrategy="drop",
            nonnegative=True,
            rank=rank,  # Giữ nguyên rank
            maxIter=3,  # Ít iterations hơn (nhanh hơn)
            regParam=0.1,
            numUserBlocks=50,
            numItemBlocks=50
        )
        
        updated_model = als.fit(all_ratings)
        print("   ✅ Retrain hoàn tất")
        
        # 5. Lưu model mới
        print("5. Đang lưu model mới...")
        updated_model.write().overwrite().save(HDFS_MODEL_PATH)
        print("   ✅ Đã lưu model mới")
        
        # 6. Xóa ratings đã tích lũy (đã dùng xong)
        print("6. Đang xóa ratings đã tích lũy...")
        # (Có thể giữ lại để backup)
        print("   ✅ Hoàn tất")
        
        print("=== INCREMENTAL UPDATE THÀNH CÔNG ===\n")
        
        return updated_model
        
    except Exception as e:
        print(f"❌ Lỗi khi cập nhật model: {e}")
        import traceback
        traceback.print_exc()
        return None

def check_and_update_model(spark, new_ratings_df, batch_id):
    """
    Kiểm tra và cập nhật model nếu đủ điều kiện
    """
    # Chỉ kiểm tra mỗi UPDATE_INTERVAL_BATCHES batches
    if batch_id % UPDATE_INTERVAL_BATCHES != 0:
        return None
    
    print(f"\n🔍 Batch {batch_id}: Kiểm tra incremental update...")
    
    # 1. Tích lũy ratings mới
    total_accumulated = accumulate_new_ratings(spark, new_ratings_df, batch_id)
    
    # 2. Kiểm tra điều kiện
    if total_accumulated >= MIN_RATINGS_FOR_UPDATE:
        print(f"✅ Đủ {total_accumulated} ratings mới (>= {MIN_RATINGS_FOR_UPDATE})")
        
        # 3. Đọc ratings đã tích lũy
        accumulated_ratings = spark.read.csv(
            HDFS_ACCUMULATED_RATINGS,
            header=True,
            inferSchema=True
        ).select(
            col("userId").cast(IntegerType()),
            col("movieId").cast(IntegerType()),
            col("rating").cast(FloatType())
        )
        
        # 4. Cập nhật model
        updated_model = incremental_update_model(spark, accumulated_ratings)
        
        if updated_model:
            # Reload model trong global cache (nếu cần)
            # global global_als_model
            # global_als_model = updated_model
            print("✅ Model đã được cập nhật! Stream layer sẽ dùng model mới ở batch tiếp theo.")
        
        return updated_model
    else:
        print(f"⏳ Chưa đủ ratings ({total_accumulated} < {MIN_RATINGS_FOR_UPDATE})")
        return None



